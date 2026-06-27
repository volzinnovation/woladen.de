from __future__ import annotations

import sqlite3
from pathlib import Path

from scripts.refresh_occupancy_stats_sidecar import (
    _publish_sqlite,
    _select_latest_remote_asset,
)


def _write_stats_db(path: Path, *, station_id: str = "station-1") -> None:
    with sqlite3.connect(path) as connection:
        connection.executescript(
            """
            CREATE TABLE station_occupancy_profiles (
                station_id TEXT PRIMARY KEY,
                country_code TEXT NOT NULL,
                measured_seconds INTEGER NOT NULL,
                occupied_seconds INTEGER NOT NULL,
                out_of_order_seconds INTEGER NOT NULL,
                occupancy_share REAL,
                out_of_order_share REAL,
                confidence_label TEXT NOT NULL
            );
            """
        )
        connection.execute(
            """
            INSERT INTO station_occupancy_profiles (
                station_id, country_code, measured_seconds, occupied_seconds,
                out_of_order_seconds, occupancy_share, out_of_order_share,
                confidence_label
            ) VALUES (?, 'DE', 7200, 3600, 0, 0.5, 0.0, 'medium')
            """,
            (station_id,),
        )
        connection.commit()


def test_select_latest_remote_occupancy_stats_asset() -> None:
    remote_path, artifact_date = _select_latest_remote_asset(
        [
            "AFIR/commercial/analytics/occupancy/merged/2026-06-20/occupancy_stats.sqlite3.zst",
            "AFIR/commercial/analytics/occupancy/merged/2026-06-22/occupancy_stats.sqlite3.zst",
            "AFIR/commercial/analytics/occupancy/merged/countries/DE/2026-06-23/occupancy-DE.sqlite3",
            "AFIR/commercial/analytics/occupancy/merged/2026-06-21/occupancy_chart_smoke.json",
        ],
        prefix="AFIR/commercial/analytics/occupancy/merged",
        asset_name="occupancy_stats.sqlite3.zst",
    )

    assert remote_path == "AFIR/commercial/analytics/occupancy/merged/2026-06-22/occupancy_stats.sqlite3.zst"
    assert artifact_date == "2026-06-22"


def test_publish_sqlite_validates_and_replaces_atomically(tmp_path: Path) -> None:
    source_path = tmp_path / "source.sqlite3"
    output_path = tmp_path / "state" / "occupancy_stats.sqlite3"
    _write_stats_db(source_path)

    result = _publish_sqlite(source_path, output_path)

    assert output_path.exists()
    assert result["integrity_check"] == "ok"
    assert result["profile_count"] == 1
    assert result["countries"] == ["DE"]
    with sqlite3.connect(output_path) as connection:
        station_id = connection.execute("SELECT station_id FROM station_occupancy_profiles").fetchone()[0]
    assert station_id == "station-1"
