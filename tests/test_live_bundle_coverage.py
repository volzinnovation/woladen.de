from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from scripts.live_bundle_coverage import build_report, format_human_report


def _write_geojson_fixture(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [13.4, 52.5]},
                        "properties": {"station_id": "station-1"},
                    },
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [13.5, 52.6]},
                        "properties": {"station_id": "station-2"},
                    },
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [13.6, 52.7]},
                        "properties": {"station_id": "station-3"},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )


def _write_live_db_fixture(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.executescript(
            """
            CREATE TABLE evse_current_state (
                provider_uid TEXT NOT NULL,
                provider_evse_id TEXT NOT NULL DEFAULT '',
                provider_site_id TEXT NOT NULL DEFAULT '',
                provider_station_ref TEXT NOT NULL DEFAULT '',
                station_id TEXT NOT NULL,
                fetched_at TEXT NOT NULL DEFAULT '',
                source_observed_at TEXT NOT NULL DEFAULT '',
                availability_status TEXT NOT NULL DEFAULT '',
                operational_status TEXT NOT NULL DEFAULT '',
                price_display TEXT NOT NULL DEFAULT '',
                price_currency TEXT NOT NULL DEFAULT '',
                price_energy_eur_kwh_min TEXT NOT NULL DEFAULT '',
                price_energy_eur_kwh_max TEXT NOT NULL DEFAULT '',
                price_time_eur_min_min REAL,
                price_time_eur_min_max REAL,
                price_quality TEXT NOT NULL DEFAULT '',
                price_complex INTEGER NOT NULL DEFAULT 0,
                next_available_charging_slots TEXT NOT NULL DEFAULT '',
                supplemental_facility_status TEXT NOT NULL DEFAULT '',
                ingested_at TEXT NOT NULL DEFAULT '',
                payload_sha256 TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE station_current_state (
                station_id TEXT PRIMARY KEY,
                provider_uid TEXT NOT NULL DEFAULT '',
                availability_status TEXT NOT NULL DEFAULT '',
                available_evses INTEGER NOT NULL DEFAULT 0,
                occupied_evses INTEGER NOT NULL DEFAULT 0,
                out_of_order_evses INTEGER NOT NULL DEFAULT 0,
                unknown_evses INTEGER NOT NULL DEFAULT 0,
                total_evses INTEGER NOT NULL DEFAULT 0,
                price_display TEXT NOT NULL DEFAULT '',
                price_currency TEXT NOT NULL DEFAULT '',
                price_energy_eur_kwh_min TEXT NOT NULL DEFAULT '',
                price_energy_eur_kwh_max TEXT NOT NULL DEFAULT '',
                price_time_eur_min_min REAL,
                price_time_eur_min_max REAL,
                price_complex INTEGER NOT NULL DEFAULT 0,
                source_observed_at TEXT NOT NULL DEFAULT '',
                fetched_at TEXT NOT NULL DEFAULT '',
                ingested_at TEXT NOT NULL DEFAULT ''
            );
            """
        )
        conn.executemany(
            """
            INSERT INTO evse_current_state (
                provider_uid,
                provider_evse_id,
                provider_site_id,
                provider_station_ref,
                station_id,
                fetched_at,
                source_observed_at,
                availability_status,
                operational_status,
                price_display,
                price_currency,
                price_energy_eur_kwh_min,
                price_energy_eur_kwh_max,
                price_time_eur_min_min,
                price_time_eur_min_max,
                price_quality,
                price_complex,
                next_available_charging_slots,
                supplemental_facility_status,
                ingested_at,
                payload_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "enbwmobility",
                    "evse-1",
                    "site-1",
                    "station-ref-1",
                    "station-1",
                    "2026-04-15T10:05:00+00:00",
                    "2026-04-15T10:04:00+00:00",
                    "occupied",
                    "OCCUPIED",
                    "0,61 €/kWh",
                    "EUR",
                    "0.61",
                    "0.61",
                    None,
                    None,
                    "simple",
                    0,
                    '[{"expectedAvailableFromTime":"2026-04-15T10:30:00+00:00"}]',
                    '["parkingRestricted"]',
                    "2026-04-15T10:05:01+00:00",
                    "sha-enbw",
                ),
                (
                    "chargecloud",
                    "evse-2",
                    "site-2",
                    "station-ref-2",
                    "station-2",
                    "2026-04-15T11:00:00+00:00",
                    "2026-04-15T10:59:00+00:00",
                    "free",
                    "AVAILABLE",
                    "0,49 €/kWh",
                    "EUR",
                    "0.49",
                    "0.49",
                    None,
                    None,
                    "simple",
                    0,
                    "",
                    "",
                    "2026-04-15T11:00:01+00:00",
                    "sha-chargecloud",
                ),
                (
                    "tesla",
                    "evse-3",
                    "site-3",
                    "station-ref-3",
                    "station-2",
                    "2026-04-15T12:00:00+00:00",
                    "2026-04-15T11:59:00+00:00",
                    "occupied",
                    "CHARGING",
                    "0,39 €/kWh",
                    "EUR",
                    "0.39",
                    "0.39",
                    None,
                    None,
                    "simple",
                    0,
                    "",
                    "",
                    "2026-04-15T12:00:01+00:00",
                    "sha-tesla-1",
                ),
                (
                    "tesla",
                    "evse-4",
                    "site-4",
                    "station-ref-4",
                    "station-x",
                    "2026-04-15T09:00:00+00:00",
                    "2026-04-15T08:59:00+00:00",
                    "free",
                    "AVAILABLE",
                    "0,35 €/kWh",
                    "EUR",
                    "0.35",
                    "0.35",
                    None,
                    None,
                    "simple",
                    0,
                    "",
                    "",
                    "2026-04-15T09:00:01+00:00",
                    "sha-tesla-2",
                ),
                (
                    "wirelane",
                    "evse-5",
                    "site-5",
                    "station-ref-5",
                    "",
                    "2026-04-15T08:00:00+00:00",
                    "2026-04-15T07:59:00+00:00",
                    "free",
                    "AVAILABLE",
                    "",
                    "",
                    "",
                    "",
                    None,
                    None,
                    "",
                    0,
                    "",
                    "",
                    "2026-04-15T08:00:01+00:00",
                    "sha-wirelane",
                ),
            ],
        )
        conn.executemany(
            "INSERT INTO station_current_state (station_id) VALUES (?)",
            [
                ("station-1",),
                ("station-2",),
                ("station-x",),
            ],
        )
        conn.commit()
    finally:
        conn.close()


def test_build_report_counts_bundle_station_coverage_from_live_observations(tmp_path: Path):
    geojson_path = tmp_path / "chargers_fast.geojson"
    db_path = tmp_path / "live_state.sqlite3"
    _write_geojson_fixture(geojson_path)
    _write_live_db_fixture(db_path)

    report = build_report(db_path=db_path, geojson_path=geojson_path)

    assert report["bundle_station_count"] == 3
    assert report["stations_with_any_live_observation"] == 2
    assert report["stations_with_current_live_state"] == 2
    assert report["coverage_ratio"] == 2 / 3
    assert report["observed_station_ids_not_in_bundle"] == 1
    assert report["current_state_station_ids_not_in_bundle"] == 1
    assert report["provider_station_count_sum"] == 3
    assert report["provider_station_overlap_excess"] == 1
    assert report["latest_updated_station_id"] == "station-2"
    provider_by_uid = {item["provider_uid"]: item for item in report["providers"]}
    assert provider_by_uid["enbwmobility"]["stations_with_any_live_observation"] == 1
    assert provider_by_uid["enbwmobility"]["observation_rows"] == 1
    assert provider_by_uid["enbwmobility"]["latest_updated_station_id"] == "station-1"
    assert provider_by_uid["enbwmobility"]["latest_attribute_updates"]["availability_status"]["value"] == "occupied"
    assert provider_by_uid["enbwmobility"]["latest_attribute_updates"]["price_display"]["value"] == "0,61 €/kWh"
    assert provider_by_uid["enbwmobility"]["latest_attribute_updates"]["next_available_charging_slots"]["value"] == [
        {"expectedAvailableFromTime": "2026-04-15T10:30:00+00:00"}
    ]
    assert provider_by_uid["enbwmobility"]["latest_attribute_updates"]["supplemental_facility_status"]["value"] == [
        "parkingRestricted"
    ]
    assert provider_by_uid["chargecloud"]["stations_with_any_live_observation"] == 1
    assert provider_by_uid["chargecloud"]["observation_rows"] == 1
    assert provider_by_uid["chargecloud"]["latest_updated_station_id"] == "station-2"
    assert provider_by_uid["chargecloud"]["latest_attribute_updates"]["price_energy_eur_kwh_min"]["value"] == "0.49"
    assert provider_by_uid["tesla"]["stations_with_any_live_observation"] == 1
    assert provider_by_uid["tesla"]["observation_rows"] == 1
    assert provider_by_uid["tesla"]["latest_updated_station_id"] == "station-2"
    assert provider_by_uid["tesla"]["latest_attribute_updates"]["operational_status"]["value"] == "CHARGING"
    assert provider_by_uid["enbwmobility"]["last_received_update_at"] == "2026-04-15T10:05:00+00:00"
    assert report["providers_with_any_live_observation"] == 3


def test_format_human_report_mentions_non_additive_provider_counts(tmp_path: Path):
    geojson_path = tmp_path / "chargers_fast.geojson"
    db_path = tmp_path / "live_state.sqlite3"
    _write_geojson_fixture(geojson_path)
    _write_live_db_fixture(db_path)

    report = build_report(db_path=db_path, geojson_path=geojson_path)
    text = format_human_report(report)

    assert "Stations with current live state: 2 (66.67%)" in text
    assert "Providers with current live state: 3" in text
    assert "Latest updated station ID: station-2" in text
    assert "Provider counts are not additive: sum=3, union=2, overlap_excess=1" in text
