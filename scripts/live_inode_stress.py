#!/usr/bin/env python3

from __future__ import annotations

import argparse
import errno
import json
import os
import shutil
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.archive import DailyResponseArchiver, ResponseLogWriter
from backend.config import AppConfig
from backend.models import FetchResponse
from backend.receipt_queue import ReceiptQueue

JOURNAL_FILENAME = "records.jsonl"


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from exc


def _archive_datetime(target_date: date) -> datetime:
    return datetime.combine(target_date, time(hour=12), tzinfo=timezone.utc)


def _statvfs_payload(path: Path) -> dict[str, Any]:
    path.mkdir(parents=True, exist_ok=True)
    stat = os.statvfs(path)
    block_size = stat.f_frsize or stat.f_bsize
    return {
        "path": str(path),
        "block_size": block_size,
        "total_bytes": int(stat.f_blocks * block_size),
        "available_bytes": int(stat.f_bavail * block_size),
        "total_inodes": int(stat.f_files),
        "available_inodes": int(stat.f_favail),
    }


def _tree_counts(root: Path) -> dict[str, int]:
    file_count = 0
    dir_count = 0
    if root.exists():
        for _, dirnames, filenames in os.walk(root):
            dir_count += len(dirnames)
            file_count += len(filenames)
    return {
        "file_count": file_count,
        "dir_count": dir_count,
    }


def _assert_safe_state_paths(config: AppConfig) -> None:
    repo_data_dir = (REPO_ROOT / "data").resolve()
    guarded_paths = (
        config.db_path,
        config.raw_payload_dir,
        config.archive_dir,
        config.queue_dir,
    )
    if os.environ.get("WOLADEN_LIVE_ALLOW_REPO_STRESS", "").strip().lower() in {"1", "true", "yes", "on"}:
        return
    for path in guarded_paths:
        resolved = path.expanduser().resolve(strict=False)
        if resolved == repo_data_dir or repo_data_dir in resolved.parents:
            raise SystemExit(
                "Refusing to write stress data below the repository data/ directory. "
                "Set WOLADEN_LIVE_DB_PATH, WOLADEN_LIVE_RAW_PAYLOAD_DIR, "
                "WOLADEN_LIVE_ARCHIVE_DIR, and WOLADEN_LIVE_QUEUE_DIR to an isolated "
                "state directory, or set WOLADEN_LIVE_ALLOW_REPO_STRESS=1."
            )


def _synthetic_body(index: int, fetched_at: str) -> bytes:
    payload = {
        "stress_index": index,
        "fetched_at": fetched_at,
        "payloadPublication": {
            "publicationTime": fetched_at,
            "lang": "de",
        },
    }
    return json.dumps(payload, sort_keys=True).encode("utf-8")


def write_journal_and_queue_records(
    config: AppConfig,
    *,
    record_count: int,
    provider_uid: str,
    target_date: date,
) -> dict[str, Any]:
    writer = ResponseLogWriter(config)
    queue = ReceiptQueue(config)
    queue.initialize()
    fetched_at = _archive_datetime(target_date).isoformat()
    response_headers = "HTTP/1.1 200 OK\nContent-Type: application/json\n"
    last_reference = ""

    for index in range(record_count):
        response = FetchResponse(
            body=_synthetic_body(index, fetched_at),
            content_type="application/json",
            http_status=200,
            headers_text=response_headers,
        )
        receipt_reference = writer.write_http_response(
            provider_uid=provider_uid,
            fetched_at=fetched_at,
            response=response,
        )
        task = queue.build_task(
            task_kind="poll",
            provider_uid=provider_uid,
            run_id=index + 1,
            receipt_log_path=receipt_reference,
            receipt_at=fetched_at,
            content_type=response.content_type,
            http_status=response.http_status,
        )
        queue.enqueue(task)
        last_reference = receipt_reference

    journal_path = config.raw_payload_dir / provider_uid / target_date.isoformat() / JOURNAL_FILENAME
    archive_result = DailyResponseArchiver(config).archive_date(
        target_date,
        upload=False,
        delete_source_on_success=False,
        delete_archive_on_success=False,
    )

    return {
        "records_written": record_count,
        "provider_uid": provider_uid,
        "archive_date": target_date.isoformat(),
        "journal_path": str(journal_path),
        "journal_bytes": journal_path.stat().st_size if journal_path.exists() else 0,
        "last_receipt_reference": last_reference,
        "queue_stats": queue.stats(),
        "archive_result": archive_result,
    }


def run_legacy_file_probe(
    config: AppConfig,
    *,
    target_date: date,
    limit: int,
    bytes_per_file: int,
    cleanup: bool,
) -> dict[str, Any]:
    probe_root = config.raw_payload_dir / "_legacy_inode_probe" / target_date.isoformat()
    probe_root.mkdir(parents=True, exist_ok=True)
    payload = (b'{"legacy_file_probe":true}\n').ljust(max(bytes_per_file, 1), b" ")
    created = 0
    failure: dict[str, Any] | None = None

    for index in range(limit):
        try:
            (probe_root / f"{index:08d}.json").write_bytes(payload)
        except OSError as exc:
            failure = {
                "errno": exc.errno,
                "strerror": exc.strerror,
                "filename": str(exc.filename or ""),
            }
            break
        created += 1

    if failure is None:
        result = "limit_reached"
    elif failure.get("errno") in {errno.ENOSPC, errno.EDQUOT}:
        result = "inode_or_space_exhausted"
    else:
        result = "os_error"

    probe_result = {
        "result": result,
        "probe_root": str(probe_root),
        "created_files": created,
        "limit": limit,
        "bytes_per_file": len(payload),
        "failure": failure,
        "cleaned_up": cleanup,
    }
    if cleanup:
        shutil.rmtree(probe_root.parent, ignore_errors=True)
    return probe_result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stress local live raw journaling and SQLite queue state under constrained disk/inode limits."
    )
    parser.add_argument("--records", type=int, default=1000, help="Synthetic journal and queue records to write")
    parser.add_argument("--provider", default="inode_stress", help="Synthetic provider UID")
    parser.add_argument("--date", type=_parse_date, default=date.today(), help="Archive date, YYYY-MM-DD")
    parser.add_argument(
        "--legacy-file-probe",
        action="store_true",
        help="Also create many tiny legacy-style files until the low inode/file budget is visible",
    )
    parser.add_argument("--legacy-file-limit", type=int, default=10000, help="Max legacy probe files to create")
    parser.add_argument("--legacy-file-bytes", type=int, default=32, help="Bytes per legacy probe file")
    parser.add_argument(
        "--keep-probe-files",
        action="store_true",
        help="Leave legacy probe files in place for manual inspection",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.records < 0:
        raise SystemExit("--records must be >= 0")
    if args.legacy_file_limit < 0:
        raise SystemExit("--legacy-file-limit must be >= 0")

    config = AppConfig()
    _assert_safe_state_paths(config)

    before = {
        "state": _statvfs_payload(config.db_path.parent),
        "raw": _statvfs_payload(config.raw_payload_dir),
        "queue": _statvfs_payload(config.queue_dir),
        "archives": _statvfs_payload(config.archive_dir),
    }
    journal_queue_result = write_journal_and_queue_records(
        config,
        record_count=args.records,
        provider_uid=args.provider,
        target_date=args.date,
    )
    legacy_probe_result = None
    if args.legacy_file_probe:
        legacy_probe_result = run_legacy_file_probe(
            config,
            target_date=args.date,
            limit=args.legacy_file_limit,
            bytes_per_file=args.legacy_file_bytes,
            cleanup=not args.keep_probe_files,
        )

    after = {
        "state": _statvfs_payload(config.db_path.parent),
        "raw": _statvfs_payload(config.raw_payload_dir),
        "queue": _statvfs_payload(config.queue_dir),
        "archives": _statvfs_payload(config.archive_dir),
    }
    payload = {
        "result": "ok",
        "config": {
            "db_path": str(config.db_path),
            "raw_payload_dir": str(config.raw_payload_dir),
            "queue_dir": str(config.queue_dir),
            "archive_dir": str(config.archive_dir),
        },
        "before": before,
        "journal_queue": journal_queue_result,
        "legacy_file_probe": legacy_probe_result,
        "after": after,
        "file_tree_counts": {
            "raw": _tree_counts(config.raw_payload_dir),
            "queue": _tree_counts(config.queue_dir),
            "archives": _tree_counts(config.archive_dir),
        },
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
