#!/usr/bin/env python3
"""Publish generated station occupancy data for the static web app."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATION_ID = "2d6cff515ceed554"
HOUR_LABELS = [f"{hour:02d}:00" for hour in range(24)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create the small public occupancy-history JSON used by the web app."
    )
    parser.add_argument("--station", default=DEFAULT_STATION_ID, help="Internal station id.")
    parser.add_argument(
        "--input",
        type=Path,
        help=(
            "Generated analysis JSON. Defaults to "
            "data/station_occupancy/<station>-hourly-occupancy-latest.json."
        ),
    )
    parser.add_argument(
        "--web-output-dir",
        type=Path,
        default=ROOT / "web" / "data" / "station-occupancy",
        help="Directory for web source data.",
    )
    parser.add_argument(
        "--site-output-dir",
        type=Path,
        default=ROOT / "site" / "data" / "station-occupancy",
        help="Directory for generated site data.",
    )
    return parser.parse_args()


def load_source(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        raise SystemExit(f"Expected JSON object in {path}")
    return payload


def find_station_payload(payload: dict[str, Any], station_id: str) -> dict[str, Any]:
    station = payload.get("station")
    if isinstance(station, dict) and station.get("station_id") == station_id:
        return payload

    if payload.get("station_id") == station_id:
        return payload

    stations = payload.get("stations")
    if isinstance(stations, list):
        for item in stations:
            if isinstance(item, dict) and item.get("station_id") == station_id:
                merged = {key: value for key, value in payload.items() if key != "stations"}
                merged.update(item)
                return merged

    raise SystemExit(f"Station {station_id} not found in source data")


def normalize_hourly_values(values: object) -> dict[str, float]:
    if isinstance(values, dict):
        hourly = {
            label: round(float(values.get(label, 0) or 0), 3)
            for label in HOUR_LABELS
        }
    elif isinstance(values, list):
        hourly = {
            label: round(float(values[index] or 0), 3)
            for index, label in enumerate(HOUR_LABELS)
            if index < len(values)
        }
        hourly.update({label: 0.0 for label in HOUR_LABELS if label not in hourly})
    else:
        raise SystemExit("Source data does not include hourly_average_occupied")
    return hourly


def public_payload(source: dict[str, Any], station_id: str) -> dict[str, Any]:
    station = source.get("station")
    if not isinstance(station, dict):
        station = {
            key: source[key]
            for key in ("station_id", "operator", "address", "city", "charging_points_count", "max_power_kw")
            if key in source
        }

    return {
        "station_id": station_id,
        "start_date": source.get("start_date"),
        "end_date": source.get("end_date"),
        "included_days": source.get("included_days"),
        "requested_days": source.get("requested_days"),
        "timezone": source.get("timezone", "Europe/Berlin"),
        "metric": source.get("metric"),
        "station": station,
        "provider_uids": source.get("provider_uids", []),
        "site_ids": source.get("site_ids", []),
        "evse_ids": source.get("evse_ids", []),
        "hourly_average_occupied": normalize_hourly_values(
            source.get("hourly_average_occupied")
        ),
    }


def write_payload(payload: dict[str, Any], output_dir: Path, station_id: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{station_id}.json"
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=True, indent=2)
        file.write("\n")
    return output_path


def main() -> None:
    args = parse_args()
    station_id = args.station.strip()
    if not station_id:
        raise SystemExit("station id must not be empty")

    input_path = args.input or (
        ROOT
        / "data"
        / "station_occupancy"
        / f"{station_id}-hourly-occupancy-latest.json"
    )
    source = find_station_payload(load_source(input_path), station_id)
    payload = public_payload(source, station_id)

    outputs = [
        write_payload(payload, args.web_output_dir, station_id),
        write_payload(payload, args.site_output_dir, station_id),
    ]
    for output in outputs:
        print(output.relative_to(ROOT))


if __name__ == "__main__":
    main()
