from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

from scripts.operator_coverage_gap_report import (
    build_summary,
    load_dynamic_station_ids,
    load_static_station_ids,
    load_station_catalog,
    render_markdown,
)


def _write_station_catalog(path: Path) -> None:
    rows = [
        {"station_id": "station-a", "operator": "Alpha Charge", "city": "Berlin", "address": "A-Str. 1"},
        {"station_id": "station-b", "operator": "Alpha Charge", "city": "Berlin", "address": "B-Str. 2"},
        {"station_id": "station-c", "operator": "Beta Charge", "city": "Hamburg", "address": "C-Str. 3"},
        {"station_id": "station-d", "operator": "Gamma Charge", "city": "Köln", "address": "D-Str. 4"},
        {"station_id": "station-e", "operator": "Delta Charge", "city": "Bonn", "address": "E-Str. 5"},
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_static_matches_csv(path: Path) -> None:
    rows = [
        {"provider_uid": "alpha", "station_id": "station-a", "station_in_bundle": "1"},
        {"provider_uid": "beta", "station_id": "station-c", "station_in_bundle": "1"},
        {"provider_uid": "gamma", "station_id": "station-d", "station_in_bundle": "1"},
        {"provider_uid": "ignored", "station_id": "outside", "station_in_bundle": "0"},
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_dynamic_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE evse_current_state (
                provider_uid TEXT NOT NULL,
                station_id TEXT NOT NULL,
                fetched_at TEXT NOT NULL
            )
            """
        )
        conn.executemany(
            "INSERT INTO evse_current_state (provider_uid, station_id, fetched_at) VALUES (?, ?, ?)",
            [
                ("alpha", "station-b", "2026-04-22T10:10:00+00:00"),
                ("beta", "station-c", "2026-04-22T10:11:00+00:00"),
                ("ignored", "outside", "2026-04-22T10:12:00+00:00"),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def test_build_summary_groups_missing_dynamic_and_known_by_operator(tmp_path: Path):
    stations_path = tmp_path / "stations.csv"
    static_path = tmp_path / "matches.csv"
    db_path = tmp_path / "live_state.sqlite3"
    _write_station_catalog(stations_path)
    _write_static_matches_csv(static_path)
    _write_dynamic_db(db_path)

    station_catalog = load_station_catalog(stations_path)
    station_ids = set(station_catalog)
    summary = build_summary(
        station_catalog=station_catalog,
        static_station_ids=load_static_station_ids(static_path, station_ids=station_ids),
        dynamic_station_ids=load_dynamic_station_ids(db_path, station_ids=station_ids)[0],
        dynamic_snapshot_at="2026-04-22T10:12:00+00:00",
        station_catalog_path=stations_path,
        static_matches_path=static_path,
        dynamic_db_path=db_path,
    )

    assert summary["catalog_station_count"] == 5
    assert summary["stations_with_static_match_count"] == 3
    assert summary["stations_with_dynamic_match_count"] == 2
    assert summary["stations_with_known_match_count"] == 4
    assert summary["stations_missing_dynamic_count"] == 3
    assert summary["stations_missing_known_count"] == 1

    rows = {row["operator"]: row for row in summary["operator_rows"]}
    assert rows["Alpha Charge"]["station_count"] == 2
    assert rows["Alpha Charge"]["dynamic_station_count"] == 1
    assert rows["Alpha Charge"]["known_station_count"] == 2
    assert rows["Alpha Charge"]["static_only_station_count"] == 1
    assert rows["Alpha Charge"]["missing_dynamic_station_count"] == 1
    assert rows["Alpha Charge"]["missing_known_station_count"] == 0

    assert rows["Gamma Charge"]["static_only_station_count"] == 1
    assert rows["Gamma Charge"]["missing_dynamic_station_count"] == 1
    assert rows["Gamma Charge"]["missing_known_station_count"] == 0

    assert rows["Delta Charge"]["dynamic_station_count"] == 0
    assert rows["Delta Charge"]["known_station_count"] == 0
    assert rows["Delta Charge"]["missing_dynamic_station_count"] == 1
    assert rows["Delta Charge"]["missing_known_station_count"] == 1


def test_render_markdown_includes_missing_dynamic_and_static_only_sections(tmp_path: Path):
    stations_path = tmp_path / "stations.csv"
    static_path = tmp_path / "matches.csv"
    db_path = tmp_path / "live_state.sqlite3"
    _write_station_catalog(stations_path)
    _write_static_matches_csv(static_path)
    _write_dynamic_db(db_path)

    station_catalog = load_station_catalog(stations_path)
    station_ids = set(station_catalog)
    summary = build_summary(
        station_catalog=station_catalog,
        static_station_ids=load_static_station_ids(static_path, station_ids=station_ids),
        dynamic_station_ids=load_dynamic_station_ids(db_path, station_ids=station_ids)[0],
        dynamic_snapshot_at="2026-04-22T10:12:00+00:00",
        station_catalog_path=stations_path,
        static_matches_path=static_path,
        dynamic_db_path=db_path,
    )

    markdown = render_markdown(summary)

    assert "# Operator Coverage Gap Report" in markdown
    assert "- Stations missing dynamic data: 3 (60.00%)" in markdown
    assert "- Stations with no known static or dynamic match: 1 (20.00%)" in markdown
    assert "## Top Operators Missing Dynamic Data" in markdown
    assert "| Alpha Charge | 2 | 1 | 2 | 1 | 1 (50.00%) | 0 (0.00%) |" in markdown
    assert "## Operators With Static Match But No Dynamic Data" in markdown
    assert "| Gamma Charge | 1 | 1 |" in markdown
