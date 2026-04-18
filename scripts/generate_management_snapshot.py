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

from analysis.management_snapshot import DEFAULT_MANAGEMENT_OUTPUT_ROOT, generate_management_snapshot
from backend.config import AppConfig, load_env_file

ARCHIVE_ENV_FILE_KEYS = frozenset(
    {
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
    parser = argparse.ArgumentParser(description="Generate one public AFIR management snapshot from a local HF archive tgz")
    parser.add_argument("--date", type=_parse_date, required=True, help="Archive day in YYYY-MM-DD")
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=None,
        help="Directory containing live-provider-responses-YYYY-MM-DD.tgz archives",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_MANAGEMENT_OUTPUT_ROOT,
        help="Public management output root",
    )
    parser.add_argument("--env-file", type=Path, default=None, help="Optional runtime env file with archive settings")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.env_file is not None:
        load_env_file(args.env_file, allowed_keys=ARCHIVE_ENV_FILE_KEYS)
    result = generate_management_snapshot(
        target_date=args.date,
        archive_dir=args.archive_dir,
        output_root=args.output_root,
        config=AppConfig(),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
