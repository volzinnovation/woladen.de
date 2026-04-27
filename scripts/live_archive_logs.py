#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.archive import DailyResponseArchiver
from backend.config import AppConfig, load_env_file

ARCHIVE_ENV_FILE_KEYS = frozenset(
    {
        "WOLADEN_LIVE_RAW_PAYLOAD_DIR",
        "WOLADEN_LIVE_ARCHIVE_DIR",
        "WOLADEN_LIVE_QUEUE_DIR",
        "WOLADEN_LIVE_ARCHIVE_TIMEZONE",
        "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID",
        "WOLADEN_LIVE_HF_ARCHIVE_REPO_TYPE",
        "WOLADEN_LIVE_HF_ARCHIVE_PATH_PREFIX",
        "WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE",
        "WOLADEN_LIVE_HF_ARCHIVE_TOKEN",
        "HF_TOKEN",
        "HUGGINGFACE_HUB_TOKEN",
        "HUGGINGFACE_TOKEN",
    }
)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Archive live provider response logs for a single day")
    parser.add_argument("--date", dest="target_date", type=_parse_date, default=None, help="Archive date in YYYY-MM-DD")
    parser.add_argument("--env-file", type=Path, default=None, help="Optional runtime env file with archive settings")
    parser.add_argument("--local-only", action="store_true", help="Create the tgz locally without uploading it")
    parser.add_argument("--keep-source", action="store_true", help="Keep the source response log files after success")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.env_file is not None:
        load_env_file(args.env_file, allowed_keys=ARCHIVE_ENV_FILE_KEYS)
    if not sys.stdout.isatty():
        os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")
    archiver = DailyResponseArchiver(AppConfig())
    effective_target_date = args.target_date or archiver.default_target_date()
    cleanup_after_upload = not args.keep_source and not args.local_only
    upload = not args.local_only

    pending_results = archiver.retry_pending_archives(
        before_date=effective_target_date,
        delete_source_on_success=False,
        delete_archive_on_success=False,
    ) if upload else []

    current_result: dict[str, object]
    exit_code = 0
    try:
        current_result = archiver.archive_date(
            effective_target_date,
            upload=upload,
            delete_source_on_success=False,
            delete_archive_on_success=False,
        )
    except Exception as exc:
        current_result = {
            "result": "failed",
            "target_date": effective_target_date.isoformat(),
            "error": f"{type(exc).__name__}: {exc}",
        }
        exit_code = 1

    cleanup_results: list[dict[str, object]] = []
    cleanup_error: dict[str, object] | None = None
    if upload and cleanup_after_upload:
        try:
            cleanup_results = archiver.cleanup_uploaded_artifacts(cutoff_date=effective_target_date)
        except Exception as exc:
            cleanup_error = {
                "result": "failed",
                "cutoff_date": effective_target_date.isoformat(),
                "error": f"{type(exc).__name__}: {exc}",
            }
            exit_code = 1

    if any(item.get("result") == "failed" for item in pending_results):
        exit_code = 1

    result = {
        "target_date": effective_target_date.isoformat(),
        "pending_uploads": pending_results,
        "current": current_result,
        "cleanup": cleanup_results,
    }
    if cleanup_error is not None:
        result["cleanup_error"] = cleanup_error
    print(json.dumps(result, ensure_ascii=False, indent=2))
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
