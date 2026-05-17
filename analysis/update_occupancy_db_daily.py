#!/usr/bin/env python3
"""Update the rolling station occupancy SQLite database."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analysis.batch_station_occupancy import DEFAULT_DAYS, archive_path_for_date, date_window, default_end_date  # noqa: E402
from analysis.download_hf_archives import configure_archive_environment, download_archive  # noqa: E402
from analysis.occupancy_store import DEFAULT_OCCUPANCY_DB_PATH, OccupancyStore  # noqa: E402
from backend.archive import DailyResponseArchiveDownloader  # noqa: E402
from backend.config import AppConfig  # noqa: E402


DEFAULT_HF_CACHE_ROOTS = (
    Path.home() / ".cache" / "huggingface" / "hub",
    REPO_ROOT / "tmp" / "hf-cache",
)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def _resolve_target_dates(config: AppConfig, *, start_date: date | None, end_date: date | None, days: int) -> list[date]:
    if days < 1:
        raise SystemExit("--days must be at least 1")
    effective_end_date = end_date or default_end_date(config)
    if start_date is not None:
        if effective_end_date < start_date:
            raise SystemExit("--date must be on or after --start-date")
        return date_window(effective_end_date, (effective_end_date - start_date).days + 1)
    return date_window(effective_end_date, days)


def _clear_cache_roots(extra_roots: Iterable[Path] = ()) -> list[str]:
    removed: list[str] = []
    for cache_root in (*DEFAULT_HF_CACHE_ROOTS, *tuple(extra_roots)):
        if not cache_root.exists():
            continue
        shutil.rmtree(cache_root, ignore_errors=True)
        removed.append(str(cache_root))
    return removed


def _json_from_stdout(stdout: str) -> dict[str, Any]:
    text = stdout.strip()
    if not text:
        return {}
    parsed = json.loads(text)
    if not isinstance(parsed, dict):
        raise ValueError("expected JSON object")
    return parsed


def _dates_requiring_import(
    store: OccupancyStore,
    *,
    target_dates: list[date],
    force: bool,
) -> tuple[list[str], list[date]]:
    requested_dates = [target_date.isoformat() for target_date in target_dates]
    existing_dates = store.available_dates(start_date=requested_dates[0], end_date=requested_dates[-1])
    existing_date_set = set(existing_dates)
    if force:
        return existing_dates, target_dates
    return existing_dates, [target_date for target_date in target_dates if target_date.isoformat() not in existing_date_set]


def _ensure_archives_for_import(
    config: AppConfig,
    *,
    target_dates: list[date],
    require_complete: bool,
    force_download: bool,
    no_download: bool,
    clear_hf_cache: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, str]], list[str]]:
    archive_results: list[dict[str, Any]] = []
    missing_archives: list[dict[str, str]] = []
    cleared_cache_roots: list[str] = []
    downloader = DailyResponseArchiveDownloader(config)

    for target_date in target_dates:
        archive_path = archive_path_for_date(config, target_date)
        if archive_path.exists() and not force_download:
            archive_results.append(
                {
                    "result": "already_present_local",
                    "target_date": target_date.isoformat(),
                    "target_path": str(archive_path),
                    "file_byte_length": int(archive_path.stat().st_size),
                }
            )
            continue

        if no_download:
            missing_archives.append(
                {
                    "date": target_date.isoformat(),
                    "error": f"local archive is missing: {archive_path}",
                }
            )
            continue

        result, missing = download_archive(downloader, target_date, force=force_download)
        if result is not None:
            archive_results.append(result)
        if missing is not None:
            missing_archives.append(missing)
        if clear_hf_cache:
            cleared_cache_roots.extend(_clear_cache_roots())

    if missing_archives and require_complete:
        missing_dates = ", ".join(row["date"] for row in missing_archives)
        raise SystemExit(f"Missing required occupancy archives: {missing_dates}")

    return archive_results, missing_archives, sorted(set(cleared_cache_roots))


def _build_import_command(
    args: argparse.Namespace,
    *,
    start_date: date,
    end_date: date,
    retain_days: int,
) -> list[str]:
    command = [
        sys.executable,
        str(REPO_ROOT / "analysis" / "build_occupancy_db.py"),
        "--start-date",
        start_date.isoformat(),
        "--end-date",
        end_date.isoformat(),
        "--days",
        str(args.days),
        "--db",
        str(args.db),
        "--retain-days",
        str(retain_days),
        "--scope",
        args.scope,
    ]
    if args.archive_dir is not None:
        command.extend(["--archive-dir", str(args.archive_dir)])
    if args.env_file is not None:
        command.extend(["--env-file", str(args.env_file)])
    if args.require_complete:
        command.append("--require-complete")
    if args.force:
        command.append("--force")
    if args.raw_prefilter:
        command.append("--raw-prefilter")
    if args.store_events:
        command.append("--store-events")
    if args.clear_events:
        command.append("--clear-events")
    if args.quiet:
        command.append("--quiet")
    return command


def _run_db_import(
    args: argparse.Namespace,
    *,
    start_date: date,
    end_date: date,
    retain_days: int,
) -> dict[str, Any]:
    command = _build_import_command(args, start_date=start_date, end_date=end_date, retain_days=retain_days)
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        if completed.stdout.strip():
            print(completed.stdout, file=sys.stderr, end="" if completed.stdout.endswith("\n") else "\n")
        if completed.stderr.strip():
            print(completed.stderr, file=sys.stderr, end="" if completed.stderr.endswith("\n") else "\n")
        raise SystemExit(completed.returncode)
    try:
        build_summary = _json_from_stdout(completed.stdout)
    except (json.JSONDecodeError, ValueError) as exc:
        raise SystemExit(f"Could not parse build_occupancy_db.py output as JSON: {exc}") from exc
    if completed.stderr.strip() and not args.quiet:
        print(completed.stderr, file=sys.stderr, end="" if completed.stderr.endswith("\n") else "\n")
    return build_summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update the rolling station occupancy SQLite database")
    parser.add_argument("--date", type=_parse_date, default=None, help="End date in YYYY-MM-DD")
    parser.add_argument("--start-date", type=_parse_date, default=None, help="First date in YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Trailing days when --start-date is omitted")
    parser.add_argument("--archive-dir", type=Path, default=None, help="Directory with local AFIR response archives")
    parser.add_argument("--db", type=Path, default=DEFAULT_OCCUPANCY_DB_PATH, help="SQLite occupancy analytics DB")
    parser.add_argument("--env-file", type=Path, default=None, help="Optional env file with archive settings")
    parser.add_argument("--scope", choices=["fast", "all"], default="fast", help="Station catalog scope")
    parser.add_argument("--retain-days", type=int, default=None, help="Keep only this trailing day window after import")
    parser.add_argument("--require-complete", action="store_true", help="Fail if any required archive or DB day is missing")
    parser.add_argument("--force", action="store_true", help="Reimport all days in the target window")
    parser.add_argument("--force-download", action="store_true", help="Download archives even when local files exist")
    parser.add_argument("--no-download", action="store_true", help="Do not download missing archives from Hugging Face")
    parser.add_argument("--raw-prefilter", action="store_true", help="Prefilter raw archive records by mapped identifiers")
    parser.add_argument("--store-events", action="store_true", help="Also persist raw normalized EVSE status events")
    parser.add_argument("--clear-events", action="store_true", help="Delete previously stored raw status events")
    parser.add_argument("--clear-hf-cache", action="store_true", help="Clear Hugging Face cache after each download")
    parser.add_argument("--hf-repo-id", default="", help="Hugging Face dataset repo id for archive downloads")
    parser.add_argument("--hf-path-prefix", default=None, help="Archive path prefix in the Hugging Face dataset")
    parser.add_argument("--hf-token-file", type=Path, default=None, help="File containing a Hugging Face token")
    parser.add_argument("--quiet", action="store_true", help="Suppress parser progress from the DB import")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_archive_environment(args)
    config = AppConfig()
    target_dates = _resolve_target_dates(config, start_date=args.start_date, end_date=args.date, days=args.days)
    start_date = target_dates[0]
    end_date = target_dates[-1]
    retain_days = args.retain_days or len(target_dates)
    if retain_days < 1:
        raise SystemExit("--retain-days must be at least 1")

    store = OccupancyStore(args.db)
    store.initialize()
    existing_dates, import_dates = _dates_requiring_import(store, target_dates=target_dates, force=args.force)
    archive_results, missing_archives, cleared_cache_roots = _ensure_archives_for_import(
        config,
        target_dates=import_dates,
        require_complete=args.require_complete,
        force_download=args.force_download,
        no_download=args.no_download,
        clear_hf_cache=args.clear_hf_cache,
    )
    build_summary = _run_db_import(args, start_date=start_date, end_date=end_date, retain_days=retain_days)

    print(
        json.dumps(
            {
                "db": str(args.db.resolve()),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "days": len(target_dates),
                "retain_days": retain_days,
                "existing_dates_before_import": existing_dates,
                "dates_requiring_import": [target_date.isoformat() for target_date in import_dates],
                "archive_downloads": archive_results,
                "missing_archives": missing_archives,
                "cleared_hf_cache_roots": cleared_cache_roots,
                "build": build_summary,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
