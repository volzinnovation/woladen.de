#!/usr/bin/env python3
"""Export station rating aggregates from the live SQLite store."""

from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "live_state.sqlite3"
DEFAULT_OUTPUT_PATH = ROOT / "data" / "station_ratings.json"


def export_ratings(db_path: Path, output_path: Path) -> dict[str, object]:
    ratings: list[dict[str, object]] = []
    if db_path.exists():
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT
                    station_id,
                    ROUND(AVG(rating), 2) AS average_rating,
                    COUNT(*) AS rating_count
                FROM station_ratings
                GROUP BY station_id
                ORDER BY station_id
                """
            ).fetchall()
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc).lower():
                raise
            rows = []
        finally:
            conn.close()

        ratings = [
            {
                "station_id": str(row["station_id"] or ""),
                "average_rating": float(row["average_rating"] or 0.0),
                "rating_count": int(row["rating_count"] or 0),
            }
            for row in rows
            if str(row["station_id"] or "").strip() and int(row["rating_count"] or 0) > 0
        ]

    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "ratings": ratings,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False),
        encoding="utf-8",
    )
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH, help="Path to live_state.sqlite3")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH, help="Output JSON path")
    args = parser.parse_args()

    payload = export_ratings(args.db, args.output)
    print(f"Exported {len(payload['ratings'])} station rating summaries to {args.output}")


if __name__ == "__main__":
    main()
