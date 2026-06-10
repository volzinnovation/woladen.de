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
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=1000,
        help="Emit progress to stderr after this many archive records. Use 0 to disable periodic progress.",
    )
    parser.add_argument("--quiet", action="store_true", help="Suppress progress logs")
    return parser.parse_args()


def _progress_logger(target_date: date):
    def log(event: dict[str, object]) -> None:
        phase = str(event.get("phase") or "")
        archive_path = str(event.get("archive_path") or "")
        archive_name = Path(archive_path).name if archive_path else ""
        if phase == "analysis_started":
            print(
                f"[management] {target_date.isoformat()}: starting archive analysis "
                f"for {event.get('archive_count', 0)} archive(s)",
                file=sys.stderr,
                flush=True,
            )
        elif phase == "archive_started":
            print(
                f"[management] {target_date.isoformat()}: reading {archive_name} "
                f"({event.get('archive_index', 0)}/{event.get('archive_count', 0)})",
                file=sys.stderr,
                flush=True,
            )
        elif phase == "records_streamed":
            print(
                f"[management] {target_date.isoformat()}: streamed "
                f"{event.get('processed_records', 0)} archive records; "
                f"observations={event.get('observation_rows', 0)}, "
                f"status_changes={event.get('status_change_rows', 0)}",
                file=sys.stderr,
                flush=True,
            )
        elif phase == "archive_finished":
            print(
                f"[management] {target_date.isoformat()}: finished {archive_name}; "
                f"records={event.get('archive_records', 0)}, "
                f"observations={event.get('observation_rows', 0)}",
                file=sys.stderr,
                flush=True,
            )
        elif phase == "status_changes_finalized":
            print(
                f"[management] {target_date.isoformat()}: finalized status changes; "
                f"status_changes={event.get('status_change_rows', 0)}",
                file=sys.stderr,
                flush=True,
            )
        elif phase == "history_streamed":
            print(
                f"[management] {target_date.isoformat()}: building daily summaries; "
                f"messages={event.get('message_rows', 0)}, "
                f"observations={event.get('observation_rows', 0)}, "
                f"status_changes={event.get('status_change_rows', 0)}",
                file=sys.stderr,
                flush=True,
            )
        elif phase == "analysis_finished":
            print(
                f"[management] {target_date.isoformat()}: analysis CSVs ready; "
                f"station_days={event.get('station_daily_rows', 0)}, "
                f"provider_days={event.get('provider_daily_rows', 0)}",
                file=sys.stderr,
                flush=True,
            )
        elif phase == "management_analysis_finished":
            print(
                f"[management] {target_date.isoformat()}: management analysis complete; "
                f"messages={event.get('message_rows', 0)}, "
                f"observations={event.get('observation_rows', 0)}",
                file=sys.stderr,
                flush=True,
            )
        elif phase == "management_snapshot_published":
            print(
                f"[management] {target_date.isoformat()}: snapshot published; "
                f"available_dates={event.get('available_dates', 0)}",
                file=sys.stderr,
                flush=True,
            )

    return log


def main() -> None:
    args = parse_args()
    if args.env_file is not None:
        load_env_file(args.env_file, allowed_keys=ARCHIVE_ENV_FILE_KEYS)
    progress_callback = None if args.quiet else _progress_logger(args.date)
    result = generate_management_snapshot(
        target_date=args.date,
        archive_dir=args.archive_dir,
        output_root=args.output_root,
        config=AppConfig(),
        progress_callback=progress_callback,
        progress_interval=max(0, args.progress_interval),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
