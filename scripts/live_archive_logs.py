#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
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
    archiver = DailyResponseArchiver(AppConfig())
    result = archiver.archive_date(
        args.target_date,
        upload=not args.local_only,
        delete_source_on_success=(not args.keep_source and not args.local_only),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
