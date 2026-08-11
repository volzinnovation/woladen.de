#!/usr/bin/env python3
"""Generate the small web-facing open-static bundle summary."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_OUTPUT_PATH = DATA_DIR / "open_static_summary.json"
DEFAULT_BUNDLE_CANDIDATES = (
    DATA_DIR / "eu27_ch_static" / "open_static.sqlite3",
    ROOT.parent / "Woladen.de-analytics" / "data" / "eu27_ch_static" / "open_static.sqlite3",
    ROOT.parent / "woladen.de-analytics" / "data" / "eu27_ch_static" / "open_static.sqlite3",
)

COUNTRY_NAMES_DE = {
    "AT": "Österreich",
    "BE": "Belgien",
    "CH": "Schweiz",
    "CY": "Zypern",
    "CZ": "Tschechien",
    "DE": "Deutschland",
    "DK": "Dänemark",
    "ES": "Spanien",
    "FI": "Finnland",
    "FR": "Frankreich",
    "GR": "Griechenland",
    "HU": "Ungarn",
    "LT": "Litauen",
    "LU": "Luxemburg",
    "LV": "Lettland",
    "MT": "Malta",
    "NL": "Niederlande",
    "NO": "Norwegen",
    "PL": "Polen",
    "PT": "Portugal",
    "SE": "Schweden",
    "SI": "Slowenien",
}


def _read_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _one(conn: sqlite3.Connection, sql: str) -> Any:
    row = conn.execute(sql).fetchone()
    return row[0] if row else None


def _counts_by_country(conn: sqlite3.Connection, sql: str) -> dict[str, int]:
    return {
        str(row["country_code"] or "").strip().upper(): int(row["count"] or 0)
        for row in conn.execute(sql)
        if str(row["country_code"] or "").strip()
    }


def _source_display_name(row: sqlite3.Row) -> str:
    attribution = _read_json(row["attribution_json"])
    for key in ("source_name", "display_name", "source_uid"):
        value = str(attribution.get(key) or "").strip()
        if value:
            return value
    return str(row["source_uid"] or "").strip()


def _parse_utc_timestamp(value: Any) -> datetime | None:
    timestamp = str(value or "").strip()
    if not timestamp:
        return None
    normalized = f"{timestamp[:-1]}+00:00" if timestamp.endswith("Z") else timestamp
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _catalog_updated_at(build_metadata: dict[str, Any]) -> str:
    candidates = [
        str(build_metadata.get("generated_at") or "").strip(),
        str(build_metadata.get("catalog_updated_at") or "").strip(),
    ]
    patches = build_metadata.get("patches")
    if isinstance(patches, list):
        candidates.extend(
            str(patch.get("patched_at") or "").strip()
            for patch in patches
            if isinstance(patch, dict)
        )
    dated_candidates = [
        (parsed, timestamp)
        for timestamp in candidates
        if (parsed := _parse_utc_timestamp(timestamp)) is not None
    ]
    return max(dated_candidates, default=(None, ""), key=lambda item: item[0])[1]


def build_summary(bundle_path: Path) -> dict[str, Any]:
    conn = sqlite3.connect(f"file:{bundle_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        build_metadata = _read_json(
            _one(conn, "select json_value from bundle_metadata where key='build'")
        )
        station_counts = _counts_by_country(
            conn,
            """
            select country_code, count(*) as count
            from stations
            group by country_code
            order by country_code
            """,
        )
        charger_counts = _counts_by_country(
            conn,
            """
            select country_code, count(*) as count
            from chargers
            group by country_code
            order by country_code
            """,
        )
        fast_station_counts = _counts_by_country(
            conn,
            """
            select country_code, count(*) as count
            from stations
            where coalesce(max_power_kw, 0) >= 50
            group by country_code
            order by country_code
            """,
        )
        countries = [
            {
                "code": code,
                "name": COUNTRY_NAMES_DE.get(code, code),
                "station_count": station_counts.get(code, 0),
                "charger_count": charger_counts.get(code, 0),
                "fast_station_count": fast_station_counts.get(code, 0),
            }
            for code in sorted(set(station_counts) | set(charger_counts))
        ]
        sources = [
            {
                "country_code": str(row["country_code"] or "").strip().upper(),
                "source_uid": str(row["source_uid"] or "").strip(),
                "display_name": _source_display_name(row),
                "source_url": str(row["source_url"] or "").strip(),
                "license": str(row["license"] or "").strip(),
            }
            for row in conn.execute(
                """
                select source_uid, country_code, source_url, license, attribution_json
                from sources
                order by country_code, source_uid
                """
            )
        ]
        bundle_generated_at = str(build_metadata.get("generated_at") or "").strip()
        return {
            "schema_version": 1,
            # Keep the original build time for provenance and backwards
            # compatibility. Catalog contents may have changed through a
            # documented patch after that immutable base build.
            "generated_at": bundle_generated_at,
            "bundle_generated_at": bundle_generated_at,
            "catalog_updated_at": _catalog_updated_at(build_metadata),
            "bundle": {
                "kind": build_metadata.get("kind") or "",
                "schema_version": build_metadata.get("schema_version"),
                "station_count": int(_one(conn, "select count(*) from stations") or 0),
                "charger_count": int(_one(conn, "select count(*) from chargers") or 0),
                "country_count": len(countries),
            },
            "countries": countries,
            "sources": sources,
        }
    finally:
        conn.close()


def resolve_bundle_path(raw_path: str | None = None) -> Path | None:
    if raw_path:
        candidate = Path(raw_path).expanduser()
        return candidate if candidate.exists() else None
    env_path = os.environ.get("WOLADEN_OPEN_STATIC_BUNDLE")
    if env_path:
        candidate = Path(env_path).expanduser()
        return candidate if candidate.exists() else None
    for candidate in DEFAULT_BUNDLE_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def write_summary(bundle_path: Path, output_path: Path) -> None:
    payload = build_summary(bundle_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=str, default="")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args(argv)

    bundle_path = resolve_bundle_path(args.bundle)
    if not bundle_path:
        if args.output.exists():
            print(f"Open-static bundle not found; keeping {args.output}")
            return 0
        raise SystemExit("Open-static bundle not found and no generated summary exists")

    write_summary(bundle_path, args.output)
    print(f"Wrote {args.output} from {bundle_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
