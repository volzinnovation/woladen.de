#!/usr/bin/env python3
"""Export public station occupancy chart JSON from the occupancy SQLite DB."""

from __future__ import annotations

import argparse
import json
import tempfile
import sys
from contextlib import contextmanager
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Iterator

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analysis.batch_station_occupancy import (  # noqa: E402
    DEFAULT_DAYS,
    ProviderStationAggregate,
    build_payload,
    date_window,
    default_end_date,
    load_station_scope,
)
from analysis.occupancy_store import DEFAULT_OCCUPANCY_DB_PATH, OccupancyStore  # noqa: E402
from analysis.output_io import publish_staged_directory, write_json  # noqa: E402
from backend.config import AppConfig, load_env_file  # noqa: E402

DEFAULT_WEB_OUTPUT_DIR = REPO_ROOT / "web" / "data" / "station-occupancy"
HOUR_LABELS = [f"{hour:02d}:00" for hour in range(24)]
LOCAL_ARCHIVE_ENV_FILE_KEYS = frozenset(
    {
        "WOLADEN_LIVE_ARCHIVE_TIMEZONE",
    }
)


class EmptyArchiveStats:
    def to_dict(self) -> dict[str, int]:
        return {}


def _parse_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def _safe_station_filename(station_id: str) -> str:
    safe_station_id = station_id.replace("/", "_").replace("\\", "_")
    return f"{safe_station_id}.json"


def _load_aggregates(
    store: OccupancyStore,
    *,
    start_date: date,
    end_date: date,
) -> dict[tuple[str, str], ProviderStationAggregate]:
    start_text = start_date.isoformat()
    end_text = end_date.isoformat()
    aggregates: dict[tuple[str, str], ProviderStationAggregate] = defaultdict(ProviderStationAggregate)

    with store.connection() as conn:
        daily_rows = conn.execute(
            """
            SELECT *
            FROM station_daily_occupancy
            WHERE archive_date BETWEEN ? AND ?
            ORDER BY archive_date, provider_uid, station_id
            """,
            (start_text, end_text),
        ).fetchall()

        for row in daily_rows:
            key = (str(row["provider_uid"]), str(row["station_id"]))
            aggregate = aggregates[key]
            aggregate.observed_days.add(str(row["archive_date"]))
            try:
                observed_evse_ids = json.loads(str(row["observed_evse_ids_json"] or "[]"))
            except json.JSONDecodeError:
                observed_evse_ids = []
            if isinstance(observed_evse_ids, list):
                aggregate.observed_evses.update(str(value) for value in observed_evse_ids if str(value).strip())
            aggregate.matching_observations += int(row["matching_observations"] or 0)
            aggregate.occupied_observations += int(row["occupied_observations"] or 0)
            aggregate.status_changes += int(row["status_changes"] or 0)
            latest_event_timestamp = str(row["latest_event_timestamp"] or "")
            if latest_event_timestamp > aggregate.latest_event_timestamp:
                aggregate.latest_event_timestamp = latest_event_timestamp

        hourly_rows = conn.execute(
            """
            SELECT
                provider_uid,
                station_id,
                hour,
                    occupied_seconds
            FROM station_hourly_occupancy INDEXED BY idx_station_hourly_occupancy_provider_station_hour_date
            WHERE archive_date BETWEEN ? AND ?
            ORDER BY provider_uid, station_id, hour
            """,
            (start_text, end_text),
        )
        for row in hourly_rows:
            key = (str(row["provider_uid"]), str(row["station_id"]))
            aggregate = aggregates[key]
            hour = int(row["hour"] or 0)
            if 0 <= hour <= 23:
                aggregate.hourly_occupied_sum[hour] += int(row["occupied_seconds"] or 0) / 3600

    return aggregates


def _station_public_payload(source: dict[str, Any]) -> dict[str, Any]:
    hourly_values = source.get("hourly_average_occupied")
    if isinstance(hourly_values, list):
        hourly = {
            label: round(float(hourly_values[index] or 0), 3)
            for index, label in enumerate(HOUR_LABELS)
            if index < len(hourly_values)
        }
    elif isinstance(hourly_values, dict):
        hourly = {label: round(float(hourly_values.get(label, 0) or 0), 3) for label in HOUR_LABELS}
    else:
        hourly = {label: 0.0 for label in HOUR_LABELS}
    hourly.update({label: 0.0 for label in HOUR_LABELS if label not in hourly})

    station = {
        key: source[key]
        for key in ("station_id", "operator", "address", "postcode", "city", "charging_points_count", "max_power_kw")
        if key in source
    }
    return {
        "station_id": source.get("station_id"),
        "start_date": source.get("start_date"),
        "end_date": source.get("end_date"),
        "included_days": source.get("included_days"),
        "requested_days": source.get("requested_days"),
        "timezone": source.get("timezone", "Europe/Berlin"),
        "metric": source.get("metric"),
        "station": station,
        "provider_uids": [source.get("provider_uid")] if source.get("provider_uid") else [],
        "hourly_average_occupied": hourly,
    }


def _log(message: str, *, quiet: bool) -> None:
    if quiet:
        return
    print(message, file=sys.stderr, flush=True)


def _write_public_files(
    payload: dict[str, Any],
    output_dir: Path,
    *,
    pretty: bool,
    quiet: bool,
    progress_interval: int,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    station_paths: dict[str, str] = {}
    stations = [station for station in payload.get("stations") or [] if isinstance(station, dict)]
    total_stations = len(stations)
    for index, station in enumerate(stations, start=1):
        if not isinstance(station, dict):
            continue
        station_id = str(station.get("station_id") or "").strip()
        if not station_id:
            continue
        filename = _safe_station_filename(station_id)
        station_payload = _station_public_payload({**payload, **station})
        write_json(output_dir / filename, station_payload, pretty=pretty)
        station_paths[station_id] = filename
        if progress_interval > 0 and (index % progress_interval == 0 or index == total_stations):
            _log(f"Wrote {index}/{total_stations} station occupancy files", quiet=quiet)

    index_payload = {
        "schema_version": 1,
        "generated_at": payload.get("generated_at"),
        "start_date": payload.get("start_date"),
        "end_date": payload.get("end_date"),
        "requested_days": payload.get("requested_days"),
        "included_days": payload.get("included_days"),
        "station_count": len(station_paths),
        "stations": station_paths,
    }
    write_json(output_dir / "index.json", index_payload, pretty=pretty)
    return index_payload


def _prune_stale_public_files(output_dir: Path, expected_paths: set[str]) -> None:
    if not output_dir.exists():
        return
    for target_path in sorted(output_dir.rglob("*"), reverse=True):
        if target_path.is_dir():
            try:
                target_path.rmdir()
            except OSError:
                pass
            continue
        relative_path = target_path.relative_to(output_dir).as_posix()
        if relative_path not in expected_paths:
            target_path.unlink()


@contextmanager
def _staged_output_directory(target_dir: Path) -> Iterator[Path]:
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f"{target_dir.name}-staging-", dir=target_dir.parent) as temp_dir:
        yield Path(temp_dir)


def write_public_files(
    payload: dict[str, Any],
    output_dir: Path,
    *,
    pretty: bool,
    quiet: bool = False,
    progress_interval: int = 1000,
) -> dict[str, Any]:
    with _staged_output_directory(output_dir) as staged_dir:
        index_payload = _write_public_files(
            payload,
            staged_dir,
            pretty=pretty,
            quiet=quiet,
            progress_interval=progress_interval,
        )
        expected_paths = {"index.json", *index_payload["stations"].values()}
        publish_staged_directory(staged_dir, output_dir)
    _prune_stale_public_files(output_dir, expected_paths)
    return index_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export station occupancy web JSON from SQLite")
    parser.add_argument("--date", type=_parse_date, default=None, help="End date in YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=DEFAULT_DAYS, help="Trailing days to average")
    parser.add_argument("--db", type=Path, default=DEFAULT_OCCUPANCY_DB_PATH, help="SQLite occupancy analytics DB")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_WEB_OUTPUT_DIR, help="Output directory for public JSON")
    parser.add_argument("--scope", choices=["fast", "all"], default="fast", help="Station catalog scope")
    parser.add_argument(
        "--denominator",
        choices=["included-days", "observed-days"],
        default="included-days",
        help="How to average hourly values",
    )
    parser.add_argument("--env-file", type=Path, default=None, help="Optional env file with local runtime settings")
    parser.add_argument("--require-complete", action="store_true", help="Fail if any requested date is missing in the DB")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print generated JSON")
    parser.add_argument("--quiet", action="store_true", help="Suppress progress logs")
    parser.add_argument("--progress-interval", type=int, default=1000, help="Progress log interval while writing station files")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.env_file is not None:
        load_env_file(args.env_file, allowed_keys=LOCAL_ARCHIVE_ENV_FILE_KEYS)

    config = AppConfig()
    end_date = args.date or default_end_date(config)
    target_dates = date_window(end_date, args.days)
    start_date = target_dates[0]
    target_date_texts = [value.isoformat() for value in target_dates]

    store = OccupancyStore(args.db)
    store.initialize()
    included_dates = store.available_dates(start_date=start_date.isoformat(), end_date=end_date.isoformat())
    missing_dates = [value for value in target_date_texts if value not in set(included_dates)]
    if missing_dates and args.require_complete:
        raise SystemExit(f"Missing DB archive days for requested window: {', '.join(missing_dates)}")
    if not included_dates:
        raise SystemExit("No imported archive days matched the requested window")

    station_catalog = load_station_scope(config, args.scope)
    _log(f"Loaded {len(station_catalog)} station catalog rows", quiet=args.quiet)
    aggregates = _load_aggregates(store, start_date=start_date, end_date=end_date)
    _log(f"Loaded {len(aggregates)} provider-station aggregate rows from SQLite", quiet=args.quiet)
    payload = build_payload(
        start_date=start_date,
        end_date=end_date,
        requested_days=args.days,
        included_dates=[date.fromisoformat(value) for value in included_dates],
        missing_archives=missing_dates,
        station_scope=args.scope,
        denominator_mode=args.denominator,
        raw_prefilter=False,
        station_catalog=station_catalog,
        aggregates=aggregates,
        stats=EmptyArchiveStats(),
    )
    _log(f"Selected {len(payload.get('stations') or [])} station chart payloads", quiet=args.quiet)
    index_payload = write_public_files(
        payload,
        args.output_dir,
        pretty=args.pretty,
        quiet=args.quiet,
        progress_interval=args.progress_interval,
    )
    print(
        json.dumps(
            {
                "db": str(args.db.resolve()),
                "output_dir": str(args.output_dir.resolve()),
                "start_date": payload["start_date"],
                "end_date": payload["end_date"],
                "included_days": payload["included_days"],
                "missing_dates": missing_dates,
                "station_count": index_payload["station_count"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
