from __future__ import annotations

import fcntl
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import tarfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .config import AppConfig
from .models import FetchResponse
from .receipt_queue import ReceiptQueue

ARCHIVE_NAME_RE = re.compile(r"live-provider-responses-(\d{4}-\d{2}-\d{2})\.tgz$")
JOURNAL_REFERENCE_RE = re.compile(r"^journal:(?P<path>.+)#(?P<offset>\d+):(?P<length>\d+)$")
JOURNAL_FILENAME = "records.jsonl"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat()


def _parse_iso_datetime(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _safe_provider_uid(provider_uid: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", str(provider_uid or "").strip())
    return sanitized or "unknown-provider"


def _decode_body_text(payload_bytes: bytes) -> tuple[str, bool]:
    is_gzip = payload_bytes[:2] == b"\x1f\x8b"
    raw_bytes = payload_bytes
    if is_gzip:
        try:
            raw_bytes = gzip.decompress(payload_bytes)
        except OSError:
            raw_bytes = payload_bytes
            is_gzip = False
    return raw_bytes.decode("utf-8", errors="replace"), is_gzip


class ResponseLogWriter:
    def __init__(self, config: AppConfig):
        self.config = config
        self.root_dir = config.raw_payload_dir

    def write_http_response(self, *, provider_uid: str, fetched_at: str, response: FetchResponse) -> str:
        payload_sha256 = hashlib.sha256(response.body).hexdigest()
        body_text, body_is_gzip = _decode_body_text(response.body)
        record = {
            "kind": "http_response",
            "provider_uid": provider_uid,
            "fetched_at": fetched_at,
            "logged_at": _utc_now_iso(),
            "archive_date": self._archive_date_text(fetched_at),
            "http_status": int(response.http_status),
            "content_type": response.content_type,
            "headers_text": response.headers_text,
            "payload_sha256": payload_sha256,
            "payload_byte_length": len(response.body),
            "payload_is_gzip": body_is_gzip,
            "body_text": body_text,
        }
        filename = f"{self._filename_stamp()}-{int(response.http_status):03d}-{payload_sha256[:12]}.json"
        return self._write_record(provider_uid, record["archive_date"], filename, record)

    def write_fetch_failure(
        self,
        *,
        provider_uid: str,
        fetched_at: str,
        failure_kind: str,
        error_text: str,
    ) -> str:
        record = {
            "kind": "fetch_failure",
            "provider_uid": provider_uid,
            "fetched_at": fetched_at,
            "logged_at": _utc_now_iso(),
            "archive_date": self._archive_date_text(fetched_at),
            "failure_kind": failure_kind,
            "error_text": error_text,
        }
        filename = f"{self._filename_stamp()}-{failure_kind}.json"
        return self._write_record(provider_uid, record["archive_date"], filename, record)

    def write_push_request(
        self,
        *,
        provider_uid: str,
        received_at: str,
        payload_bytes: bytes,
        content_type: str,
        content_encoding: str,
        subscription_id: str = "",
        publication_id: str = "",
        request_path: str = "",
        request_query: str = "",
        request_headers: Mapping[str, Any] | None = None,
    ) -> str:
        payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
        body_text, body_is_gzip = _decode_body_text(payload_bytes)
        record = {
            "kind": "push_request",
            "provider_uid": provider_uid,
            "received_at": received_at,
            "logged_at": _utc_now_iso(),
            "archive_date": self._archive_date_text(received_at),
            "subscription_id": subscription_id,
            "publication_id": publication_id,
            "request_path": request_path,
            "request_query": request_query,
            "content_type": content_type,
            "content_encoding": content_encoding,
            "request_headers": self._normalize_headers(request_headers),
            "payload_sha256": payload_sha256,
            "payload_byte_length": len(payload_bytes),
            "payload_is_gzip": body_is_gzip,
            "body_text": body_text,
        }
        filename = f"{self._filename_stamp()}-push-{payload_sha256[:12]}.json"
        return self._write_record(provider_uid, record["archive_date"], filename, record)

    def read_record(self, reference: str) -> dict[str, Any]:
        return read_response_log_record(reference)

    def _archive_date_text(self, fetched_at: str) -> str:
        fetched_dt = _parse_iso_datetime(fetched_at) or _utc_now()
        return fetched_dt.astimezone(self.config.archive_timezone()).date().isoformat()

    def _filename_stamp(self) -> str:
        return _utc_now().strftime("%Y%m%dT%H%M%S%fZ")

    def _target_path(self, provider_uid: str, archive_date: str, filename: str) -> Path:
        target_dir = self.root_dir / _safe_provider_uid(provider_uid) / archive_date
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir / filename

    def _journal_path(self, provider_uid: str, archive_date: str) -> Path:
        target_dir = self.root_dir / _safe_provider_uid(provider_uid) / archive_date
        target_dir.mkdir(parents=True, exist_ok=True)
        return target_dir / JOURNAL_FILENAME

    def _write_record(self, provider_uid: str, archive_date: str, filename: str, payload: dict[str, Any]) -> str:
        record = dict(payload)
        record["archive_filename"] = filename
        journal_path = self._journal_path(provider_uid, archive_date)
        record_bytes = (json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
        with journal_path.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.seek(0, os.SEEK_END)
                offset = handle.tell()
                handle.write(record_bytes)
                handle.flush()
                os.fsync(handle.fileno())
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return f"journal:{journal_path}#{offset}:{len(record_bytes)}"

    def _write_json(self, target_path: Path, payload: dict[str, Any]) -> None:
        temp_path = target_path.with_suffix(f"{target_path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temp_path.replace(target_path)

    def _normalize_headers(self, request_headers: Mapping[str, Any] | None) -> dict[str, str]:
        if not request_headers:
            return {}
        return {
            str(key): str(value)
            for key, value in sorted(request_headers.items(), key=lambda item: str(item[0]).lower())
        }


@dataclass
class ArchiveBuildResult:
    archive_path: Path
    file_count: int
    provider_count: int


@dataclass(frozen=True)
class ArchiveSourceRecord:
    arcname: str
    provider_uid: str
    payload_bytes: bytes


def _parse_journal_reference(reference: str) -> tuple[Path, int, int] | None:
    match = JOURNAL_REFERENCE_RE.match(str(reference or "").strip())
    if not match:
        return None
    return (
        Path(match.group("path")),
        int(match.group("offset")),
        int(match.group("length")),
    )


def read_response_log_record(reference: str) -> dict[str, Any]:
    journal_reference = _parse_journal_reference(reference)
    if journal_reference is None:
        payload = json.loads(Path(reference).read_text(encoding="utf-8"))
    else:
        journal_path, offset, length = journal_reference
        with journal_path.open("rb") as handle:
            handle.seek(offset)
            payload = json.loads(handle.read(length).decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid_receipt_log:{reference}")
    return payload


class DailyResponseArchiver:
    def __init__(self, config: AppConfig, *, hf_api: Any | None = None):
        self.config = config
        self.hf_api = hf_api

    def default_target_date(self) -> date:
        now_local = _utc_now().astimezone(self.config.archive_timezone())
        return now_local.date() - timedelta(days=1)

    def archive_date(
        self,
        target_date: date | None = None,
        *,
        upload: bool = True,
        delete_source_on_success: bool = True,
        delete_archive_on_success: bool = True,
    ) -> dict[str, Any]:
        effective_date = target_date or self.default_target_date()
        if upload and not self.config.has_hf_archive_upload_config():
            file_count = self._count_source_files_for_date(effective_date)
            return {
                "result": "skipped_missing_upload_config",
                "target_date": effective_date.isoformat(),
                "file_count": file_count,
            }

        build_result = self._prepare_archive_for_date(effective_date)
        if build_result.file_count == 0:
            build_result.archive_path.unlink(missing_ok=True)
            return {"result": "no_files", "target_date": effective_date.isoformat(), "file_count": 0}

        archive_path = build_result.archive_path
        remote_path = ""
        if upload:
            remote_path = self._upload_archive(archive_path, effective_date)

        if remote_path and delete_archive_on_success:
            archive_path.unlink(missing_ok=True)

        if delete_source_on_success and (not upload or remote_path):
            self._delete_source_files_for_date(effective_date)

        return {
            "result": "uploaded" if remote_path else "archived_local_only",
            "target_date": effective_date.isoformat(),
            "file_count": build_result.file_count,
            "provider_count": build_result.provider_count,
            "archive_path": str(archive_path),
            "remote_path": remote_path,
        }

    def retry_pending_archives(
        self,
        *,
        before_date: date | None = None,
        delete_source_on_success: bool = True,
        delete_archive_on_success: bool = True,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        if not self.config.has_hf_archive_upload_config():
            return results

        uploaded_dates = self._uploaded_archive_dates(best_effort=True, cutoff_date=before_date)
        for target_date in self._iter_pending_dates(before_date=before_date):
            archive_path = self._archive_path(target_date)
            if target_date in uploaded_dates:
                if delete_archive_on_success:
                    archive_path.unlink(missing_ok=True)
                    self._temp_archive_path(target_date).unlink(missing_ok=True)
                if delete_source_on_success:
                    self._delete_source_files_for_date(target_date)
                results.append(
                    {
                        "result": "already_uploaded",
                        "target_date": target_date.isoformat(),
                        "archive_path": str(archive_path),
                        "remote_path": self.remote_path_for_date(target_date),
                    }
                )
                continue
            try:
                results.append(
                    self.archive_date(
                        target_date,
                        upload=True,
                        delete_source_on_success=delete_source_on_success,
                        delete_archive_on_success=delete_archive_on_success,
                    )
                )
            except Exception as exc:
                results.append(
                    {
                        "result": "failed",
                        "target_date": target_date.isoformat(),
                        "archive_path": str(archive_path),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
        return results

    def cleanup_uploaded_artifacts(self, *, cutoff_date: date | None = None) -> list[dict[str, Any]]:
        effective_cutoff = cutoff_date or self.default_target_date()
        uploaded_dates = self._uploaded_archive_dates(cutoff_date=effective_cutoff)
        cleanup_results: list[dict[str, Any]] = []
        for target_date in self._iter_pending_dates(before_date=effective_cutoff + timedelta(days=1)):
            if target_date not in uploaded_dates:
                continue
            queue_reference = self._active_queue_reference_for_date(target_date)
            if queue_reference is not None:
                cleanup_results.append(
                    {
                        "result": "skipped_queue_references",
                        "target_date": target_date.isoformat(),
                        "remote_path": self.remote_path_for_date(target_date),
                        "queue_reference": queue_reference,
                    }
                )
                continue
            source_cleanup = self._delete_source_files_for_date(target_date)
            archive_path = self._archive_path(target_date)
            temp_archive_path = self._temp_archive_path(target_date)
            removed_archive = archive_path.exists()
            removed_temp_archive = temp_archive_path.exists()
            archive_path.unlink(missing_ok=True)
            temp_archive_path.unlink(missing_ok=True)
            cleanup_results.append(
                {
                    "target_date": target_date.isoformat(),
                    "remote_path": self.remote_path_for_date(target_date),
                    "removed_raw_file_count": source_cleanup["removed_file_count"],
                    "removed_day_dir_count": source_cleanup["removed_day_dir_count"],
                    "removed_provider_dir_count": source_cleanup["removed_provider_dir_count"],
                    "removed_local_archive": removed_archive,
                    "removed_temp_archive": removed_temp_archive,
                }
            )
        return cleanup_results

    def _active_queue_reference_for_date(self, target_date: date) -> dict[str, str] | None:
        return ReceiptQueue(self.config).active_reference_for_archive_date(target_date.isoformat())

    def _queue_task_archive_date(self, payload: Mapping[str, Any]) -> date | None:
        receipt_log_path_text = str(payload.get("receipt_log_path") or "").strip()
        if receipt_log_path_text:
            receipt_log_path = Path(receipt_log_path_text)
            try:
                relative_path = receipt_log_path.relative_to(self.config.raw_payload_dir)
            except ValueError:
                relative_path = None
            if relative_path is not None and len(relative_path.parts) >= 2:
                try:
                    return date.fromisoformat(relative_path.parts[1])
                except ValueError:
                    pass

        receipt_at = _parse_iso_datetime(str(payload.get("receipt_at") or ""))
        if receipt_at is None:
            return None
        return receipt_at.astimezone(self.config.archive_timezone()).date()

    def _iter_source_files_for_date(self, target_date: date):
        archive_date = target_date.isoformat()
        root_dir = self.config.raw_payload_dir
        if not root_dir.exists():
            return
        for provider_dir in sorted(root_dir.iterdir()):
            if not provider_dir.is_dir():
                continue
            archive_dir = provider_dir / archive_date
            if not archive_dir.is_dir():
                continue
            for path in sorted(archive_dir.glob("*.json")):
                if path.is_file():
                    yield path

    def _iter_source_records_for_date(self, target_date: date):
        archive_date = target_date.isoformat()
        root_dir = self.config.raw_payload_dir
        if not root_dir.exists():
            return
        for provider_dir in sorted(root_dir.iterdir()):
            if not provider_dir.is_dir():
                continue
            archive_dir = provider_dir / archive_date
            if not archive_dir.is_dir():
                continue
            for path in sorted(archive_dir.glob("*.json")):
                if path.is_file():
                    yield ArchiveSourceRecord(
                        arcname=str(path.relative_to(root_dir)),
                        provider_uid=provider_dir.name,
                        payload_bytes=path.read_bytes(),
                    )
            journal_path = archive_dir / JOURNAL_FILENAME
            if journal_path.is_file():
                yield ArchiveSourceRecord(
                    arcname=str(journal_path.relative_to(root_dir)),
                    provider_uid=provider_dir.name,
                    payload_bytes=journal_path.read_bytes(),
                )

    def _count_source_files_for_date(self, target_date: date) -> int:
        return sum(1 for _ in self._iter_source_records_for_date(target_date))

    def _iter_source_dates(self) -> Iterable[date]:
        root_dir = self.config.raw_payload_dir
        if not root_dir.exists():
            return []

        dates: set[date] = set()
        for provider_dir in sorted(root_dir.iterdir()):
            if not provider_dir.is_dir():
                continue
            for archive_dir in sorted(provider_dir.iterdir()):
                if not archive_dir.is_dir():
                    continue
                try:
                    dates.add(date.fromisoformat(archive_dir.name))
                except ValueError:
                    continue
        return sorted(dates)

    def _archive_name(self, target_date: date) -> str:
        return f"live-provider-responses-{target_date.isoformat()}.tgz"

    def archive_name(self, target_date: date) -> str:
        return self._archive_name(target_date)

    def _archive_path(self, target_date: date) -> Path:
        return self.config.archive_dir / self._archive_name(target_date)

    def _temp_archive_path(self, target_date: date) -> Path:
        return self._archive_path(target_date).with_suffix(".tgz.tmp")

    def _prepare_archive_for_date(
        self,
        target_date: date,
        *,
        archive_path: Path | None = None,
    ) -> ArchiveBuildResult:
        effective_archive_path = archive_path or self._archive_path(target_date)
        if effective_archive_path.exists():
            existing_summary = self._inspect_archive(effective_archive_path)
            if existing_summary is not None:
                return ArchiveBuildResult(
                    archive_path=effective_archive_path,
                    file_count=existing_summary.file_count,
                    provider_count=existing_summary.provider_count,
                )
            effective_archive_path.unlink(missing_ok=True)
        return self._create_archive(target_date)

    def _create_archive(self, target_date: date) -> ArchiveBuildResult:
        self.config.archive_dir.mkdir(parents=True, exist_ok=True)
        archive_path = self._archive_path(target_date)
        temp_path = self._temp_archive_path(target_date)
        temp_path.unlink(missing_ok=True)
        file_count = 0
        provider_uids: set[str] = set()
        with tarfile.open(temp_path, mode="w:gz") as archive_handle:
            for source_record in self._iter_source_records_for_date(target_date):
                source_info = tarfile.TarInfo(name=source_record.arcname)
                source_info.size = len(source_record.payload_bytes)
                archive_handle.addfile(source_info, io.BytesIO(source_record.payload_bytes))
                file_count += 1
                provider_uids.add(source_record.provider_uid)
            if file_count == 0:
                temp_path.unlink(missing_ok=True)
                return ArchiveBuildResult(archive_path=archive_path, file_count=0, provider_count=0)
            manifest = {
                "target_date": target_date.isoformat(),
                "created_at": _utc_now_iso(),
                "archive_timezone": self.config.archive_timezone_name,
                "file_count": file_count,
                "provider_count": len(provider_uids),
                "providers": sorted(provider_uids),
            }
            manifest_bytes = (json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
            manifest_info = tarfile.TarInfo(name="manifest.json")
            manifest_info.size = len(manifest_bytes)
            archive_handle.addfile(manifest_info, io.BytesIO(manifest_bytes))
        temp_path.replace(archive_path)
        return ArchiveBuildResult(
            archive_path=archive_path,
            file_count=file_count,
            provider_count=len(provider_uids),
        )

    def _inspect_archive(self, archive_path: Path) -> ArchiveBuildResult | None:
        try:
            return self._inspect_archive_unchecked(archive_path)
        except (EOFError, OSError, tarfile.TarError, json.JSONDecodeError, UnicodeDecodeError):
            return None

    def _inspect_archive_unchecked(self, archive_path: Path) -> ArchiveBuildResult:
        file_count = 0
        provider_uids: set[str] = set()
        manifest: dict[str, Any] | None = None
        with tarfile.open(archive_path, mode="r|gz") as archive_handle:
            for member in archive_handle:
                if not member.isfile():
                    continue
                if member.name == "manifest.json":
                    extracted = archive_handle.extractfile(member)
                    if extracted is None:
                        continue
                    manifest = json.loads(extracted.read().decode("utf-8"))
                    continue
                if not (member.name.endswith(".json") or member.name.endswith(".jsonl")):
                    continue
                file_count += 1
                parts = Path(member.name).parts
                if parts:
                    provider_uids.add(parts[0])

        if manifest:
            manifest_file_count = int(manifest.get("file_count") or file_count)
            manifest_provider_count = int(manifest.get("provider_count") or len(manifest.get("providers") or provider_uids))
            file_count = manifest_file_count
            provider_count = manifest_provider_count
        else:
            provider_count = len(provider_uids)

        return ArchiveBuildResult(
            archive_path=archive_path,
            file_count=file_count,
            provider_count=provider_count,
        )

    def _iter_pending_archive_paths(self) -> Iterable[tuple[date, Path]]:
        archive_dir = self.config.archive_dir
        if not archive_dir.exists():
            return []

        pending: list[tuple[date, Path]] = []
        for archive_path in sorted(archive_dir.glob("live-provider-responses-*.tgz")):
            match = ARCHIVE_NAME_RE.match(archive_path.name)
            if not match:
                continue
            pending.append((date.fromisoformat(match.group(1)), archive_path))
        return pending

    def _iter_pending_temp_archive_dates(self) -> Iterable[date]:
        archive_dir = self.config.archive_dir
        if not archive_dir.exists():
            return []

        pending: list[date] = []
        for temp_path in sorted(archive_dir.glob("live-provider-responses-*.tgz.tmp")):
            archive_name = temp_path.name.removesuffix(".tmp")
            match = ARCHIVE_NAME_RE.match(archive_name)
            if not match:
                continue
            pending.append(date.fromisoformat(match.group(1)))
        return pending

    def _iter_pending_dates(self, *, before_date: date | None = None) -> list[date]:
        pending_dates = {target_date for target_date in self._iter_source_dates()}
        pending_dates.update(target_date for target_date, _ in self._iter_pending_archive_paths())
        pending_dates.update(self._iter_pending_temp_archive_dates())
        if before_date is not None:
            pending_dates = {target_date for target_date in pending_dates if target_date < before_date}
        return sorted(pending_dates)

    def _remote_path_for_archive(self, target_date: date, archive_name: str) -> str:
        parts = [
            self.config.hf_archive_path_prefix,
            f"{target_date.year:04d}",
            f"{target_date.month:02d}",
            archive_name,
        ]
        return "/".join(part for part in parts if part)

    def remote_path_for_date(self, target_date: date) -> str:
        return self._remote_path_for_archive(target_date, self._archive_name(target_date))

    def _upload_archive(self, archive_path: Path, target_date: date) -> str:
        token = self.config.hf_archive_token()
        if self.hf_api is None:
            from huggingface_hub import HfApi

            api: Any = HfApi(token=token)
        else:
            api = self.hf_api
        remote_path = self._remote_path_for_archive(target_date, archive_path.name)
        api.upload_file(
            path_or_fileobj=str(archive_path),
            path_in_repo=remote_path,
            repo_id=self.config.hf_archive_repo_id,
            repo_type=self.config.hf_archive_repo_type,
            commit_message=f"Add live provider response archive for {target_date.isoformat()}",
        )
        return remote_path

    def _uploaded_archive_dates(
        self,
        *,
        best_effort: bool = False,
        cutoff_date: date | None = None,
    ) -> set[date]:
        if not self.config.has_hf_archive_upload_config():
            return set()
        if self.hf_api is not None and not hasattr(self.hf_api, "list_repo_files"):
            return set()

        try:
            archives = DailyResponseArchiveDownloader(self.config, hf_api=self.hf_api).list_available_archives()
        except Exception:
            if best_effort:
                return set()
            raise

        uploaded_dates: set[date] = set()
        for row in archives:
            target_date = date.fromisoformat(str(row["target_date"]))
            if cutoff_date is not None and target_date > cutoff_date:
                continue
            uploaded_dates.add(target_date)
        return uploaded_dates

    def _delete_source_files_for_date(self, target_date: date) -> dict[str, int]:
        archive_date = target_date.isoformat()
        root_dir = self.config.raw_payload_dir
        if not root_dir.exists():
            return {
                "removed_file_count": 0,
                "removed_day_dir_count": 0,
                "removed_provider_dir_count": 0,
            }

        removed_file_count = 0
        removed_day_dir_count = 0
        removed_provider_dir_count = 0
        for provider_dir in sorted(root_dir.iterdir()):
            if not provider_dir.is_dir():
                continue
            archive_dir = provider_dir / archive_date
            if not archive_dir.is_dir():
                continue
            removed_file_count += sum(1 for path in archive_dir.rglob("*") if path.is_file())
            shutil.rmtree(archive_dir, ignore_errors=True)
            removed_day_dir_count += 1
            if provider_dir.exists() and provider_dir != root_dir and not any(provider_dir.iterdir()):
                provider_dir.rmdir()
                removed_provider_dir_count += 1
        return {
            "removed_file_count": removed_file_count,
            "removed_day_dir_count": removed_day_dir_count,
            "removed_provider_dir_count": removed_provider_dir_count,
        }


class DailyResponseArchiveDownloader:
    def __init__(self, config: AppConfig, *, download_file: Any | None = None, hf_api: Any | None = None):
        self.config = config
        self.download_file = download_file
        self.hf_api = hf_api
        self.archiver = DailyResponseArchiver(config)

    def default_target_date(self) -> date:
        return self.archiver.default_target_date()

    def _client(self) -> Any:
        if self.hf_api is not None:
            return self.hf_api
        from huggingface_hub import HfApi

        return HfApi(token=(self.config.hf_archive_token() or None))

    def list_available_archives(self) -> list[dict[str, Any]]:
        if not self.config.hf_archive_repo_id:
            return []

        prefix = self.config.hf_archive_path_prefix.strip("/")
        prefix_root = f"{prefix}/" if prefix else ""
        rows: list[dict[str, Any]] = []
        for repo_path in self._client().list_repo_files(
            repo_id=self.config.hf_archive_repo_id,
            repo_type=self.config.hf_archive_repo_type,
        ):
            remote_path = str(repo_path).strip()
            if prefix_root and not remote_path.startswith(prefix_root):
                continue
            match = ARCHIVE_NAME_RE.fullmatch(Path(remote_path).name)
            if match is None:
                continue
            archive_date = date.fromisoformat(match.group(1))
            local_path = self.config.archive_dir / Path(remote_path).name
            rows.append(
                {
                    "target_date": archive_date.isoformat(),
                    "archive_name": local_path.name,
                    "remote_path": remote_path,
                    "local_path": str(local_path),
                    "is_downloaded": local_path.exists(),
                    "file_byte_length": int(local_path.stat().st_size) if local_path.exists() else 0,
                }
            )
        rows.sort(key=lambda row: (row["target_date"], row["remote_path"]))
        return rows

    def latest_available_date(self) -> date | None:
        archives = self.list_available_archives()
        if not archives:
            return None
        return date.fromisoformat(str(archives[-1]["target_date"]))

    def download_date(self, target_date: date | None = None, *, force: bool = False) -> dict[str, Any]:
        effective_date = target_date or self.default_target_date()
        if not self.config.hf_archive_repo_id:
            return {
                "result": "skipped_missing_repo_config",
                "target_date": effective_date.isoformat(),
            }

        archive_name = self.archiver.archive_name(effective_date)
        remote_path = self.archiver.remote_path_for_date(effective_date)
        target_path = self.config.archive_dir / archive_name
        self.config.archive_dir.mkdir(parents=True, exist_ok=True)

        if target_path.exists() and not force:
            return {
                "result": "already_present",
                "target_date": effective_date.isoformat(),
                "target_path": str(target_path),
                "remote_path": remote_path,
                "file_byte_length": int(target_path.stat().st_size),
            }

        downloader = self.download_file
        if downloader is None:
            from huggingface_hub import hf_hub_download

            downloader = hf_hub_download

        downloaded_path = Path(
            downloader(
                repo_id=self.config.hf_archive_repo_id,
                repo_type=self.config.hf_archive_repo_type,
                filename=remote_path,
                token=(self.config.hf_archive_token() or None),
                force_download=force,
            )
        )
        shutil.copyfile(downloaded_path, target_path)
        return {
            "result": "downloaded",
            "target_date": effective_date.isoformat(),
            "target_path": str(target_path),
            "remote_path": remote_path,
            "file_byte_length": int(target_path.stat().st_size),
        }
