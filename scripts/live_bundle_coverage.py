#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig
from backend.status import build_status_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Report how many GeoJSON bundle stations currently have live state",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=AppConfig().db_path,
        help="Path to live_state.sqlite3",
    )
    parser.add_argument(
        "--geojson-path",
        type=Path,
        default=AppConfig().chargers_geojson_path,
        help="Path to the charger GeoJSON bundle",
    )
    parser.add_argument("--json", action="store_true", help="Print the report as JSON")
    return parser.parse_args()


def build_report(*, db_path: Path, geojson_path: Path) -> dict[str, Any]:
    config = AppConfig(db_path=db_path, chargers_geojson_path=geojson_path)
    return build_status_report(config=config)


def format_human_report(report: dict[str, Any]) -> str:
    lines = [
        f"DB: {report['db_path']}",
        f"GeoJSON bundle: {report['geojson_path']}",
        f"Bundle stations: {report['bundle_station_count']}",
        f"Stations with current live state: {report['stations_with_any_live_observation']} "
        f"({report['coverage_ratio'] * 100:.2f}%)",
        f"Stations with current live state: {report['stations_with_current_live_state']}",
        f"Providers with current live state: {report['providers_with_any_live_observation']}",
        f"Last received update: {report['last_received_update_at'] or 'n/a'}",
        f"Latest updated station ID: {report['latest_updated_station_id'] or 'n/a'}",
        f"Observed station IDs not in bundle: {report['observed_station_ids_not_in_bundle']}",
        f"Current-state station IDs not in bundle: {report['current_state_station_ids_not_in_bundle']}",
    ]

    duplicate_count = int(report.get("bundle_duplicate_station_id_count", 0) or 0)
    if duplicate_count > 0:
        lines.append(f"Duplicate station IDs in bundle: {duplicate_count}")

    if report["providers"]:
        lines.append("")
        lines.append("Providers:")
        for provider in report["providers"]:
            lines.append(
                f"- {provider['provider_uid']}: {provider['stations_with_any_live_observation']} stations, "
                f"{provider['observation_rows']} current EVSE rows, "
                f"last received {provider['last_received_update_at'] or 'n/a'}, "
                f"latest station {provider['latest_updated_station_id'] or 'n/a'}"
            )
        lines.append(
            "Provider counts are not additive: "
            f"sum={report['provider_station_count_sum']}, "
            f"union={report['stations_with_any_live_observation']}, "
            f"overlap_excess={report['provider_station_overlap_excess']}"
        )

    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    report = build_report(db_path=args.db_path, geojson_path=args.geojson_path)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return
    print(format_human_report(report))


if __name__ == "__main__":
    main()
