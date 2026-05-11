#!/usr/bin/env python3
"""Import AFIR archive occupancy history into a SQLite analytics database."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analysis.batch_station_occupancy import (  # noqa: E402
    DEFAULT_DAYS,
    archive_path_for_date,
    build_evse_station_maps,
    build_provider_prefilters,
    build_provider_scopes,
    build_site_station_maps,
    date_window,
    default_end_date,
    load_station_scope,
    process_archive,
)
from analysis.occupancy_store import (  # noqa: E402
    DEFAULT_OCCUPANCY_DB_PATH,
    OccupancyStore,
    daily_rows_from_provider_station,
    hourly_rows_from_provider_station,
)
from backend.config import AppConfig, load_env_file  # noqa: E402

LOCAL_ARCHIVE_ENV_FILE_KEYS = frozenset(
    {
        "WOLADEN_LIVE_ARCHIVE_DIR",
        "WOLADEN_LIVE_ARCHIVE_TIMEZONE",
    }
)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _date_range(start_date: date, end_date: date) -> list[date]:
    if end_date < start_date:
        raise SystemExit("--end-date must be on or after --start-date")
    days = (end_date - start_date).days + 1
    return date_window(end_date, days)


def _event_rows(day: Any) -> list[dict[str, Any]]:
    return [
        {
            "archive_member": event.archive_member,
            "record_index": event.record_index,
            "event_index": event.event_index,
            "provider_uid": event.provider_uid,
            "station_id": event.station_id,
            "provider_evse_id": event.provider_evse_id,
            "source_observed_at": event.source_observed_at,
            "availability_status": event.availability_status,
            "operational_status": event.operational_status,
            "message_timestamp": event.message_timestamp,
            "payload_sha256": event.payload_sha256,
        }
        for event in day.status_events
    ]


def _matching_observation_count(rows: list[dict[str, Any]]) -> int:
    return sum(int(row.get("matching_observations") or 0) for row in rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Import AFIR occupancy archives into SQLite")
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument("--date", type=_parse_date, default=None, help="Single archive day in YYYY-MM-DD")
    date_group.add_argument("--end-date", type=_parse_date, default=None, help="End date in YYYY-MM-DD")
    parser.add_argument("--start-date", type=_parse_date, default=None, help="First archive day in YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Trailing days when --date/--start-date is omitted")
    parser.add_argument("--archive-dir", type=Path, default=None, help="Directory with live-provider-responses-YYYY-MM-DD.tgz")
    parser.add_argument("--db", type=Path, default=DEFAULT_OCCUPANCY_DB_PATH, help="SQLite occupancy analytics DB")
    parser.add_argument("--env-file", type=Path, default=None, help="Optional env file with local archive settings")
    parser.add_argument("--scope", choices=["fast", "all"], default="fast", help="Station catalog scope")
    parser.add_argument("--require-complete", action="store_true", help="Fail if any archive is missing")
    parser.add_argument("--force", action="store_true", help="Reimport days even if the archive hash is unchanged")
    parser.add_argument("--raw-prefilter", action="store_true", help="Prefilter raw archive records by mapped identifiers")
    parser.add_argument("--store-events", action="store_true", help="Also persist raw normalized EVSE status events")
    parser.add_argument("--clear-events", action="store_true", help="Delete previously stored raw status events and compact the DB")
    parser.add_argument("--retain-days", type=int, default=0, help="Keep only this trailing day window after import")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-archive parser progress")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.env_file is not None:
        load_env_file(args.env_file, allowed_keys=LOCAL_ARCHIVE_ENV_FILE_KEYS)

    config = AppConfig()
    if args.archive_dir is not None:
        config = replace(config, archive_dir=args.archive_dir)

    if args.start_date is not None:
        end_date = args.end_date or args.date or default_end_date(config)
        target_dates = _date_range(args.start_date, end_date)
    elif args.date is not None:
        target_dates = [args.date]
    else:
        end_date = args.end_date or default_end_date(config)
        target_dates = date_window(end_date, args.days)
    retain_start_date: date | None = None
    retain_end_date: date | None = None
    if args.retain_days:
        if args.retain_days < 1:
            raise SystemExit("--retain-days must be at least 1")
        retain_end_date = target_dates[-1]
        retain_start_date = date_window(retain_end_date, args.retain_days)[0]

    store = OccupancyStore(args.db)
    store.initialize()
    cleared_event_count = store.clear_status_events(vacuum=True) if args.clear_events else 0

    station_catalog = load_station_scope(config, args.scope)
    station_ids = set(station_catalog)
    site_station_maps = build_site_station_maps(config)
    evse_station_maps = build_evse_station_maps(config)
    provider_scopes = build_provider_scopes(config, station_ids)
    provider_prefilters = build_provider_prefilters(provider_scopes) if args.raw_prefilter else {}

    imported: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    missing: list[str] = []
    incomplete_dates: list[str] = []
    pruned_archive_days = 0

    for target_date in target_dates:
        archive_path = archive_path_for_date(config, target_date)
        archive_date = target_date.isoformat()
        existing = store.archive_day(archive_date)
        if not archive_path.exists():
            if existing is not None and not args.force:
                skipped.append({"archive_date": archive_date, "reason": "existing_without_local_archive"})
                continue
            missing.append(archive_date)
            continue

        archive_sha256 = _sha256_file(archive_path)
        if existing and existing.get("archive_sha256") == archive_sha256 and not args.force:
            skipped.append({"archive_date": archive_date, "reason": "unchanged"})
            continue

        day = process_archive(
            archive_path,
            target_date=target_date,
            config=config,
            station_ids=station_ids,
            site_station_maps=site_station_maps,
            evse_station_maps=evse_station_maps,
            provider_scopes=provider_scopes,
            provider_prefilters=provider_prefilters,
            quiet=args.quiet,
            collect_status_events=args.store_events,
        )
        daily_rows = daily_rows_from_provider_station(archive_date, day.provider_station)
        hourly_rows = hourly_rows_from_provider_station(archive_date, day.provider_station)
        matching_observation_count = _matching_observation_count(daily_rows)
        status_change_count = sum(int(row["status_changes"]) for row in daily_rows)
        event_rows = _event_rows(day) if args.store_events else []
        store.replace_archive_day(
            archive_date=archive_date,
            archive_path=archive_path,
            archive_sha256=archive_sha256,
            record_count=day.stats.records_seen,
            mapped_event_count=matching_observation_count,
            stored_event_count=len(event_rows),
            provider_station_count=len(day.provider_station),
            status_change_count=status_change_count,
            events=event_rows,
            daily_rows=daily_rows,
            hourly_rows=hourly_rows,
        )
        imported.append(
            {
                "archive_date": archive_date,
                "record_count": day.stats.records_seen,
                "mapped_event_count": matching_observation_count,
                "stored_event_count": len(event_rows),
                "provider_station_count": len(day.provider_station),
                "status_change_count": status_change_count,
            }
        )

    requested_date_texts = [value.isoformat() for value in target_dates]
    available_dates = store.available_dates(start_date=requested_date_texts[0], end_date=requested_date_texts[-1])
    available_date_set = set(available_dates)
    incomplete_date_set = {
        archive_date for archive_date in requested_date_texts if archive_date not in available_date_set
    }
    if args.force:
        incomplete_date_set.update(missing)
    incomplete_dates = [archive_date for archive_date in requested_date_texts if archive_date in incomplete_date_set]
    if incomplete_dates and args.require_complete:
        raise SystemExit(
            "Missing occupancy DB days after import: "
            f"{', '.join(incomplete_dates)}. Missing local archives: {', '.join(missing) or 'none'}"
        )

    if retain_start_date is not None and retain_end_date is not None:
        pruned_archive_days = store.prune_archive_window(
            start_date=retain_start_date.isoformat(),
            end_date=retain_end_date.isoformat(),
            vacuum=True,
        )
        available_dates = store.available_dates(
            start_date=retain_start_date.isoformat(),
            end_date=retain_end_date.isoformat(),
        )

    print(
        json.dumps(
            {
                "db": str(args.db.resolve()),
                "imported": imported,
                "skipped": skipped,
                "missing": missing,
                "available_dates": available_dates,
                "incomplete_dates": incomplete_dates,
                "cleared_event_count": cleared_event_count,
                "pruned_archive_days": pruned_archive_days,
                "retained_window": {
                    "start_date": retain_start_date.isoformat() if retain_start_date is not None else "",
                    "end_date": retain_end_date.isoformat() if retain_end_date is not None else "",
                    "days": args.retain_days,
                },
                "target_dates": [value.isoformat() for value in target_dates],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
