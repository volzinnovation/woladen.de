#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import ssl
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from commercial_backend.config import AppConfig  # noqa: E402
from commercial_backend.pt_mobie import (  # noqa: E402
    COUNTRY_CODE,
    PT_MOBIE_STATIC_SOURCE_UID,
    PT_MOBIE_STATIC_URL,
    PT_MOBIE_STATUS_SOURCE_UID,
    PT_MOBIE_STATUS_URL,
    count_xml_records_from_binary_stream,
)
from commercial_backend.store import create_ingest_store  # noqa: E402

USER_AGENT = "woladen.de public PT MOBI.E ingester"


@dataclass(frozen=True)
class SourceSpec:
    key: str
    source_uid: str
    display_name: str
    url: str
    suffix: str
    source_kind: str
    task_kind: str


SOURCES = {
    "static-datex": SourceSpec(
        key="static-datex",
        source_uid=PT_MOBIE_STATIC_SOURCE_UID,
        display_name="PT MOBI.E DATEX II v3 charging infrastructure table",
        url=PT_MOBIE_STATIC_URL,
        suffix=".xml",
        source_kind="pt_mobie_datex_3_static_table",
        task_kind="parse_static_payload",
    ),
    "status-datex": SourceSpec(
        key="status-datex",
        source_uid=PT_MOBIE_STATUS_SOURCE_UID,
        display_name="PT MOBI.E DATEX II v3 charging infrastructure status",
        url=PT_MOBIE_STATUS_URL,
        suffix=".xml",
        source_kind="pt_mobie_datex_3_dynamic_status",
        task_kind="parse_dynamic_payload",
    ),
}


def _temp_payload_path(config: AppConfig, spec: SourceSpec) -> Path:
    incoming_dir = config.raw_payload_dir / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=incoming_dir,
        prefix=f"pt-mobie-{spec.key}-",
        suffix=spec.suffix,
        delete=False,
    )
    handle.close()
    return Path(handle.name)


def _int_header(headers: dict[str, str], name: str) -> int:
    text = str(headers.get(name.lower()) or "").strip()
    try:
        return int(text)
    except ValueError:
        return 0


def _headers_from_curl_header_file(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8", errors="replace")
    blocks = [block for block in text.replace("\r\n", "\n").split("\n\n") if block.strip()]
    headers: dict[str, str] = {}
    for line in (blocks[-1].splitlines() if blocks else []):
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        headers[key.strip().lower()] = value.strip()
    return headers


def _hash_file(path: Path) -> tuple[str, int]:
    payload_sha256 = hashlib.sha256()
    byte_length = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            payload_sha256.update(chunk)
            byte_length += len(chunk)
    return payload_sha256.hexdigest(), byte_length


def _ensure_complete_payload(path: Path, byte_length: int, headers: dict[str, str]) -> None:
    expected_length = _int_header(headers, "content-length")
    if expected_length and byte_length != expected_length:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        raise RuntimeError(f"incomplete_http_read:{byte_length}/{expected_length}")


def _fetch_to_file_with_curl(
    spec: SourceSpec,
    *,
    config: AppConfig,
    timeout_seconds: int,
    reason: str,
) -> tuple[Path, str, int, dict[str, str]]:
    curl_path = shutil.which("curl")
    if not curl_path:
        raise RuntimeError(f"pt_mobie_curl_fallback_unavailable:{reason}")
    payload_path = _temp_payload_path(config, spec)
    header_handle = tempfile.NamedTemporaryFile(
        dir=config.raw_payload_dir / "_incoming",
        prefix=f"pt-mobie-{spec.key}-headers-",
        suffix=".txt",
        delete=False,
    )
    header_handle.close()
    header_path = Path(header_handle.name)
    try:
        subprocess.run(
            [
                curl_path,
                "-fsS",
                "--retry",
                "2",
                "--retry-all-errors",
                "--connect-timeout",
                str(min(timeout_seconds, 30)),
                "--max-time",
                str(timeout_seconds),
                "-L",
                "-H",
                "Accept: application/xml,text/xml,*/*",
                "-H",
                f"User-Agent: {USER_AGENT}",
                "-D",
                str(header_path),
                "-o",
                str(payload_path),
                spec.url,
            ],
            check=True,
        )
        headers = _headers_from_curl_header_file(header_path)
        payload_sha256, byte_length = _hash_file(payload_path)
        _ensure_complete_payload(payload_path, byte_length, headers)
        return payload_path, payload_sha256, byte_length, headers
    except Exception:
        try:
            payload_path.unlink()
        except FileNotFoundError:
            pass
        raise
    finally:
        try:
            header_path.unlink()
        except FileNotFoundError:
            pass


def _fetch_to_file(
    spec: SourceSpec,
    *,
    config: AppConfig,
    timeout_seconds: int,
) -> tuple[Path, str, int, dict[str, str]]:
    request = urllib.request.Request(
        spec.url,
        headers={
            "Accept": "application/xml,text/xml,*/*",
            "User-Agent": USER_AGENT,
        },
    )

    def write_response(response) -> tuple[Path, str, int, dict[str, str]]:
        payload_path = _temp_payload_path(config, spec)
        payload_sha256 = hashlib.sha256()
        byte_length = 0
        headers = {key.lower(): value for key, value in response.headers.items()}
        with payload_path.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
                payload_sha256.update(chunk)
                byte_length += len(chunk)
        _ensure_complete_payload(payload_path, byte_length, headers)
        return (
            payload_path,
            payload_sha256.hexdigest(),
            byte_length,
            headers,
        )

    last_error: Exception | None = None
    for _attempt in range(2):
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                return write_response(response)
        except (ssl.SSLError, urllib.error.URLError):
            context = ssl._create_unverified_context()
            try:
                with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
                    return write_response(response)
            except Exception as exc:
                last_error = exc
        except Exception as exc:
            last_error = exc
    return _fetch_to_file_with_curl(
        spec,
        config=config,
        timeout_seconds=timeout_seconds,
        reason=str(last_error or "urllib_fetch_failed"),
    )


def _count_archived_xml_records(store: Any, storage_uri: str, content_encoding: str) -> dict[str, Any]:
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        return count_xml_records_from_binary_stream(payload_stream, content_encoding=content_encoding)


def _quarantine_invalid_payload(
    *,
    config: AppConfig,
    spec: SourceSpec,
    temp_path: Path,
    payload_sha256: str,
    byte_length: int,
    reason: str,
) -> Path:
    now = datetime.now(timezone.utc)
    target_dir = (
        config.raw_payload_dir
        / "_invalid"
        / COUNTRY_CODE
        / spec.source_uid
        / f"{now:%Y}"
        / f"{now:%m}"
        / f"{now:%d}"
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{now:%Y%m%dT%H%M%SZ}-{payload_sha256[:16]}{spec.suffix}"
    shutil.move(str(temp_path), target_path)
    manifest_row = {
        "received_at": now.isoformat(),
        "country_code": COUNTRY_CODE,
        "source": spec.key,
        "source_uid": spec.source_uid,
        "url": spec.url,
        "payload_sha256": payload_sha256,
        "byte_length": byte_length,
        "reason": reason,
        "path": str(target_path),
    }
    with (target_dir / "invalid_payloads.ndjson").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(manifest_row, sort_keys=True) + "\n")
    return target_path


def _validate_datex_payload(
    *,
    config: AppConfig,
    spec: SourceSpec,
    temp_path: Path,
    payload_sha256: str,
    byte_length: int,
    content_encoding: str,
) -> dict[str, Any]:
    try:
        with temp_path.open("rb") as handle:
            return count_xml_records_from_binary_stream(handle, content_encoding=content_encoding)
    except Exception as exc:
        quarantine_path = _quarantine_invalid_payload(
            config=config,
            spec=spec,
            temp_path=temp_path,
            payload_sha256=payload_sha256,
            byte_length=byte_length,
            reason=str(exc),
        )
        raise RuntimeError(
            f"invalid_pt_mobie_payload:{spec.key}:{exc}; quarantined={quarantine_path}"
        ) from exc


def _selected_sources(value: str) -> list[SourceSpec]:
    if value == "all-datex":
        return [SOURCES["static-datex"], SOURCES["status-datex"]]
    return [SOURCES[value]]


def _archive_source(
    *,
    spec: SourceSpec,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
) -> dict[str, Any]:
    temp_path, payload_sha256, byte_length, headers = _fetch_to_file(
        spec,
        config=config,
        timeout_seconds=timeout_seconds,
    )
    datex_summary = _validate_datex_payload(
        config=config,
        spec=spec,
        temp_path=temp_path,
        payload_sha256=payload_sha256,
        byte_length=byte_length,
        content_encoding=headers.get("content-encoding", ""),
    )
    receipt = store.record_pull_payload_file(
        country_code=COUNTRY_CODE,
        source_uid=spec.source_uid,
        payload_path=temp_path,
        payload_sha256=payload_sha256,
        byte_length=byte_length,
        content_type=headers.get("content-type", ""),
        content_encoding=headers.get("content-encoding", ""),
        request_path=spec.url,
        request_query="",
        request_headers={
            "accept": "application/xml,text/xml,*/*",
            "user-agent": USER_AGENT,
            "source-url": spec.url,
        },
        source_kind=spec.source_kind,
        display_name=spec.display_name,
        task_kind=spec.task_kind,
        remote_addr="",
    )
    summary: dict[str, Any] = {
        "source": spec.key,
        "source_uid": spec.source_uid,
        "url": spec.url,
        "task_kind": spec.task_kind,
        "payload_sha256": payload_sha256,
        "byte_length": byte_length,
        "duplicate_payload": receipt.duplicate_payload,
        "queued": receipt.queued,
        "storage_uri": receipt.storage_uri,
    }
    if parse_summary:
        summary["datex_summary"] = datex_summary
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and archive PT MOBI.E DATEX payloads.")
    parser.add_argument(
        "--source",
        choices=("static-datex", "status-datex", "all-datex"),
        default="status-datex",
        help="PT MOBI.E DATEX source payload to fetch.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument(
        "--no-parse-summary",
        action="store_true",
        help="Only archive raw bytes; skip lightweight DATEX record counts.",
    )
    args = parser.parse_args(argv)

    config = AppConfig()
    store = create_ingest_store(config)
    store.initialize()
    summaries = [
        _archive_source(
            spec=spec,
            config=config,
            store=store,
            timeout_seconds=args.timeout_seconds,
            parse_summary=not args.no_parse_summary,
        )
        for spec in _selected_sources(args.source)
    ]
    print(json.dumps({"ok": True, "country_code": COUNTRY_CODE, "sources": summaries}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
