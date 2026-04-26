#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig


def _text(value: Any) -> str:
    return str(value or "").strip()


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _format_percent(value: float) -> str:
    return f"{value * 100:.2f}%"


def load_station_catalog(path: Path) -> dict[str, dict[str, str]]:
    stations: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            station_id = _text(row.get("station_id"))
            if station_id:
                stations[station_id] = row
    return stations


def load_static_station_ids(path: Path, *, station_ids: set[str]) -> set[str]:
    static_station_ids: set[str] = set()
    if not path.exists():
        return static_station_ids

    with path.open("r", encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            station_id = _text(row.get("station_id"))
            if station_id and station_id in station_ids:
                static_station_ids.add(station_id)
    return static_station_ids


def load_dynamic_station_ids(path: Path, *, station_ids: set[str]) -> tuple[set[str], str | None]:
    dynamic_station_ids: set[str] = set()
    snapshot_at: str | None = None
    if not path.exists():
        return dynamic_station_ids, snapshot_at

    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        snapshot_row = conn.execute("SELECT MAX(fetched_at) AS snapshot_at FROM evse_current_state").fetchone()
        snapshot_at = _text(snapshot_row["snapshot_at"]) if snapshot_row else None
        rows = conn.execute(
            """
            SELECT DISTINCT station_id
            FROM evse_current_state
            WHERE station_id <> ''
            """
        ).fetchall()
    finally:
        conn.close()

    for row in rows:
        station_id = _text(row["station_id"])
        if station_id and station_id in station_ids:
            dynamic_station_ids.add(station_id)

    return dynamic_station_ids, snapshot_at


def build_summary(
    *,
    station_catalog: dict[str, dict[str, str]],
    static_station_ids: set[str],
    dynamic_station_ids: set[str],
    dynamic_snapshot_at: str | None,
    station_catalog_path: Path,
    static_matches_path: Path,
    dynamic_db_path: Path,
) -> dict[str, Any]:
    catalog_station_ids = set(station_catalog)
    known_station_ids = static_station_ids | dynamic_station_ids
    operator_station_ids: dict[str, set[str]] = defaultdict(set)
    for station_id, row in station_catalog.items():
        operator = _text(row.get("operator")) or "n/a"
        operator_station_ids[operator].add(station_id)

    operator_rows: list[dict[str, Any]] = []
    for operator, total_station_ids in operator_station_ids.items():
        total_count = len(total_station_ids)
        static_ids = total_station_ids & static_station_ids
        dynamic_ids = total_station_ids & dynamic_station_ids
        known_ids = total_station_ids & known_station_ids
        overlap_ids = static_ids & dynamic_ids
        row = {
            "operator": operator,
            "station_count": total_count,
            "static_station_count": len(static_ids),
            "dynamic_station_count": len(dynamic_ids),
            "known_station_count": len(known_ids),
            "static_only_station_count": len(static_ids - dynamic_ids),
            "dynamic_only_station_count": len(dynamic_ids - static_ids),
            "overlap_station_count": len(overlap_ids),
            "missing_dynamic_station_count": total_count - len(dynamic_ids),
            "missing_known_station_count": total_count - len(known_ids),
            "static_coverage_ratio": _ratio(len(static_ids), total_count),
            "dynamic_coverage_ratio": _ratio(len(dynamic_ids), total_count),
            "known_coverage_ratio": _ratio(len(known_ids), total_count),
            "missing_dynamic_ratio": _ratio(total_count - len(dynamic_ids), total_count),
            "missing_known_ratio": _ratio(total_count - len(known_ids), total_count),
        }
        operator_rows.append(row)

    operator_rows.sort(
        key=lambda row: (
            -int(row["missing_dynamic_station_count"]),
            -int(row["missing_known_station_count"]),
            -int(row["station_count"]),
            str(row["operator"]),
        )
    )

    top_missing_dynamic_rows = [row for row in operator_rows if int(row["missing_dynamic_station_count"]) > 0][:30]
    top_missing_known_rows = [row for row in operator_rows if int(row["missing_known_station_count"]) > 0][:30]
    static_only_rows = [row for row in operator_rows if int(row["static_only_station_count"]) > 0][:30]

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "station_catalog_path": str(station_catalog_path.resolve()),
        "static_matches_path": str(static_matches_path.resolve()),
        "dynamic_db_path": str(dynamic_db_path.resolve()),
        "dynamic_snapshot_at": dynamic_snapshot_at,
        "catalog_station_count": len(catalog_station_ids),
        "stations_with_static_match_count": len(static_station_ids),
        "stations_with_static_match_ratio": _ratio(len(static_station_ids), len(catalog_station_ids)),
        "stations_with_dynamic_match_count": len(dynamic_station_ids),
        "stations_with_dynamic_match_ratio": _ratio(len(dynamic_station_ids), len(catalog_station_ids)),
        "stations_with_known_match_count": len(known_station_ids),
        "stations_with_known_match_ratio": _ratio(len(known_station_ids), len(catalog_station_ids)),
        "stations_missing_dynamic_count": len(catalog_station_ids - dynamic_station_ids),
        "stations_missing_dynamic_ratio": _ratio(len(catalog_station_ids - dynamic_station_ids), len(catalog_station_ids)),
        "stations_missing_known_count": len(catalog_station_ids - known_station_ids),
        "stations_missing_known_ratio": _ratio(len(catalog_station_ids - known_station_ids), len(catalog_station_ids)),
        "operator_rows": operator_rows,
        "top_missing_dynamic_rows": top_missing_dynamic_rows,
        "top_missing_known_rows": top_missing_known_rows,
        "static_only_rows": static_only_rows,
    }


def render_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Operator Coverage Gap Report",
        "",
        f"- Generated at: {summary['generated_at']}",
        f"- Station catalog: `{summary['station_catalog_path']}`",
        f"- Static match source: `{summary['static_matches_path']}`",
        f"- Dynamic source DB: `{summary['dynamic_db_path']}`",
        f"- Dynamic snapshot timestamp: {summary['dynamic_snapshot_at'] or 'n/a'}",
        "",
        "## Snapshot",
        "",
        f"- Stations in catalog: {summary['catalog_station_count']}",
        f"- Stations with any static match: {summary['stations_with_static_match_count']} ({_format_percent(summary['stations_with_static_match_ratio'])})",
        f"- Stations with any dynamic data: {summary['stations_with_dynamic_match_count']} ({_format_percent(summary['stations_with_dynamic_match_ratio'])})",
        f"- Stations with any known static or dynamic match: {summary['stations_with_known_match_count']} ({_format_percent(summary['stations_with_known_match_ratio'])})",
        f"- Stations missing dynamic data: {summary['stations_missing_dynamic_count']} ({_format_percent(summary['stations_missing_dynamic_ratio'])})",
        f"- Stations with no known static or dynamic match: {summary['stations_missing_known_count']} ({_format_percent(summary['stations_missing_known_ratio'])})",
        "",
        "## Top Operators Missing Dynamic Data",
        "",
        "| Operator | Total | Dynamic | Known | Static-only | Missing Dynamic | Missing Known |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]

    for row in summary["top_missing_dynamic_rows"]:
        lines.append(
            f"| {row['operator']} | {row['station_count']} | {row['dynamic_station_count']} | "
            f"{row['known_station_count']} | {row['static_only_station_count']} | "
            f"{row['missing_dynamic_station_count']} ({_format_percent(row['missing_dynamic_ratio'])}) | "
            f"{row['missing_known_station_count']} ({_format_percent(row['missing_known_ratio'])}) |"
        )

    if summary["top_missing_known_rows"]:
        lines.extend(
            [
                "",
                "## Top Operators With No Known Static Or Dynamic Match",
                "",
                "| Operator | Total | Known | Missing Known |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for row in summary["top_missing_known_rows"]:
            lines.append(
                f"| {row['operator']} | {row['station_count']} | {row['known_station_count']} | "
                f"{row['missing_known_station_count']} ({_format_percent(row['missing_known_ratio'])}) |"
            )

    if summary["static_only_rows"]:
        lines.extend(
            [
                "",
                "## Operators With Static Match But No Dynamic Data",
                "",
                "| Operator | Static-only | Total |",
                "| --- | ---: | ---: |",
            ]
        )
        for row in summary["static_only_rows"]:
            lines.append(
                f"| {row['operator']} | {row['static_only_station_count']} | {row['station_count']} |"
            )

    return "\n".join(lines) + "\n"


def write_operator_csv(path: Path, operator_rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "operator",
        "station_count",
        "static_station_count",
        "dynamic_station_count",
        "known_station_count",
        "static_only_station_count",
        "dynamic_only_station_count",
        "overlap_station_count",
        "missing_dynamic_station_count",
        "missing_known_station_count",
        "static_coverage_ratio",
        "dynamic_coverage_ratio",
        "known_coverage_ratio",
        "missing_dynamic_ratio",
        "missing_known_ratio",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(operator_rows)


def parse_args() -> argparse.Namespace:
    config = AppConfig()
    parser = argparse.ArgumentParser(description="Build an operator coverage gap report from static matches and a live sqlite snapshot")
    parser.add_argument(
        "--stations-csv",
        type=Path,
        default=config.chargers_csv_path,
        help="Path to a station catalog such as data/chargers_fast.csv or data/chargers_full.csv",
    )
    parser.add_argument(
        "--static-matches-path",
        type=Path,
        default=config.site_match_path,
        help="Path to data/mobilithek_afir_static_matches.csv",
    )
    parser.add_argument(
        "--dynamic-db-path",
        type=Path,
        default=config.db_path,
        help="Path to a live_state.sqlite3 snapshot",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        required=True,
        help="Where to write the operator CSV report",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        required=True,
        help="Where to write the Markdown report",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    station_catalog = load_station_catalog(args.stations_csv)
    station_ids = set(station_catalog)
    static_station_ids = load_static_station_ids(args.static_matches_path, station_ids=station_ids)
    dynamic_station_ids, dynamic_snapshot_at = load_dynamic_station_ids(
        args.dynamic_db_path,
        station_ids=station_ids,
    )
    summary = build_summary(
        station_catalog=station_catalog,
        static_station_ids=static_station_ids,
        dynamic_station_ids=dynamic_station_ids,
        dynamic_snapshot_at=dynamic_snapshot_at,
        station_catalog_path=args.stations_csv,
        static_matches_path=args.static_matches_path,
        dynamic_db_path=args.dynamic_db_path,
    )
    write_operator_csv(args.output_csv, summary["operator_rows"])
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    args.output_md.write_text(render_markdown(summary), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
