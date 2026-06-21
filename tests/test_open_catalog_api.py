from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

from fastapi.testclient import TestClient

from backend.api import create_app


def _create_open_static_sqlite(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE stations (
            station_uid INTEGER PRIMARY KEY,
            country_code TEXT NOT NULL,
            station_id TEXT NOT NULL UNIQUE,
            source_uid TEXT,
            source_station_id TEXT,
            license TEXT,
            provider_uid TEXT,
            operator_name TEXT,
            station_name TEXT,
            address TEXT,
            postal_code TEXT,
            city TEXT,
            latitude REAL,
            longitude REAL,
            charger_count INTEGER,
            max_power_kw REAL,
            connector_types TEXT,
            source_url TEXT,
            public_bundle_status TEXT,
            opening_hours TEXT,
            payment_methods TEXT,
            auth_methods TEXT,
            green_energy INTEGER,
            helpdesk_phone TEXT,
            price_display TEXT,
            price_currency TEXT,
            detail_last_updated TEXT
        );
        CREATE TABLE chargers (
            charger_uid INTEGER PRIMARY KEY,
            country_code TEXT NOT NULL,
            station_uid INTEGER NOT NULL,
            station_id TEXT NOT NULL,
            charger_id TEXT NOT NULL,
            source_uid TEXT,
            provider_uid TEXT,
            source_station_id TEXT,
            source_evse_id TEXT,
            connector_id TEXT,
            connector_type TEXT,
            current_type TEXT,
            max_power_kw REAL,
            operator_name TEXT,
            license TEXT,
            source_url TEXT,
            public_bundle_status TEXT
        );
        CREATE TABLE station_amenities (
            station_uid INTEGER PRIMARY KEY,
            country_code TEXT NOT NULL,
            station_id TEXT NOT NULL,
            amenity_radius_m INTEGER,
            amenities_total INTEGER,
            amenity_category_counts_json TEXT,
            amenity_examples_json TEXT,
            nearest_amenity_kind TEXT,
            nearest_amenity_name TEXT,
            nearest_amenity_distance_m REAL,
            osm_pbf_url TEXT,
            osm_pbf_sha256 TEXT,
            osm_extracted_at TEXT,
            osm_extraction_status TEXT
        );
        CREATE VIRTUAL TABLE station_rtree USING rtree(
            station_uid,
            min_lon,
            max_lon,
            min_lat,
            max_lat
        );
        """
    )
    stations = [
        (
            1,
            "AT",
            "at:test:fast",
            "at:test",
            "fast-source",
            "open",
            "test-provider",
            "Fast Operator",
            "Fast Station",
            "Fast Street 1",
            "1010",
            "Wien",
            48.2082,
            16.3738,
            2,
            150.0,
            "ccs,type2",
            "https://example.test/fast",
            "open_static",
            "24/7",
            "card",
            "rfid",
            1,
            "+431234",
            "0.50 EUR/kWh",
            "EUR",
            "2026-06-09T10:00:00Z",
        ),
        (
            2,
            "AT",
            "at:test:slow",
            "at:test",
            "slow-source",
            "open",
            "test-provider",
            "Slow Operator",
            "Slow Station",
            "Slow Street 2",
            "1020",
            "Wien",
            48.2090,
            16.3740,
            1,
            11.0,
            "type2",
            "https://example.test/slow",
            "open_static",
            "",
            "",
            "",
            0,
            "",
            "",
            "",
            "2026-06-09T10:00:00Z",
        ),
        (
            3,
            "DE",
            "de:test:far",
            "de:test",
            "far-source",
            "open",
            "test-provider",
            "Far Operator",
            "Far Station",
            "Far Street 3",
            "10115",
            "Berlin",
            52.52,
            13.405,
            1,
            300.0,
            "ccs",
            "https://example.test/far",
            "open_static",
            "",
            "",
            "",
            None,
            "",
            "",
            "",
            "2026-06-09T10:00:00Z",
        ),
    ]
    connection.executemany(
        """
        INSERT INTO stations VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        stations,
    )
    connection.executemany(
        """
        INSERT INTO chargers VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            (
                1,
                "AT",
                1,
                "at:test:fast",
                "at:test:fast:1",
                "at:test",
                "test-provider",
                "fast-source",
                "AT*FAST*1",
                "1",
                "ccs",
                "dc",
                150.0,
                "Fast Operator",
                "open",
                "https://example.test/fast",
                "open_static",
            ),
            (
                2,
                "AT",
                2,
                "at:test:slow",
                "at:test:slow:1",
                "at:test",
                "test-provider",
                "slow-source",
                "AT*SLOW*1",
                "1",
                "type2",
                "ac",
                11.0,
                "Slow Operator",
                "open",
                "https://example.test/slow",
                "open_static",
            ),
        ],
    )
    connection.executemany(
        "INSERT INTO station_rtree VALUES (?, ?, ?, ?, ?)",
        [(row[0], row[13], row[13], row[12], row[12]) for row in stations],
    )
    connection.executemany(
        """
        INSERT INTO station_amenities VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """,
        [
            (
                1,
                "AT",
                "at:test:fast",
                100,
                3,
                json.dumps({"restaurant": 1, "toilets": 1, "shop": 1}),
                json.dumps([{"kind": "restaurant", "name": "Nearby Food"}]),
                "restaurant",
                "Nearby Food",
                42.0,
                "https://download.geofabrik.de/europe/austria-latest.osm.pbf",
                "sha",
                "2026-06-09T09:00:00Z",
                "extracted_from_pbf",
            ),
            (
                2,
                "AT",
                "at:test:slow",
                100,
                0,
                "{}",
                "[]",
                "",
                "",
                None,
                "https://download.geofabrik.de/europe/austria-latest.osm.pbf",
                "sha",
                "2026-06-09T09:00:00Z",
                "extracted_from_pbf",
            ),
        ],
    )
    connection.commit()
    connection.close()


def test_catalog_search_defaults_to_travel_fast_chargers(app_config, tmp_path: Path):
    sqlite_path = tmp_path / "open_static.sqlite3"
    _create_open_static_sqlite(sqlite_path)
    client = TestClient(create_app(replace(app_config, open_static_sqlite_path=sqlite_path)))

    response = client.get(
        "/v1/catalog/search",
        params={"lat": 48.2082, "lon": 16.3738, "radius_m": 2_000, "country": "AT"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert [station["station_id"] for station in payload["stations"]] == ["at:test:fast"]
    assert payload["stations"][0]["max_power_kw"] == 150.0
    assert payload["stations"][0]["amenity_category_counts"]["restaurant"] == 1
    assert payload["query"]["min_power_kw"] == 50.0


def test_catalog_search_local_mode_allows_connector_filter(app_config, tmp_path: Path):
    sqlite_path = tmp_path / "open_static.sqlite3"
    _create_open_static_sqlite(sqlite_path)
    client = TestClient(create_app(replace(app_config, open_static_sqlite_path=sqlite_path)))

    response = client.get(
        "/v1/catalog/search",
        params={
            "lat": 48.2082,
            "lon": 16.3738,
            "radius_m": 2_000,
            "mode": "local",
            "connector_type": "type2",
            "operator": "Slow",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert [station["station_id"] for station in payload["stations"]] == ["at:test:slow"]
    assert payload["stations"][0]["max_power_kw"] == 11.0
    assert payload["stats"]["returned_count"] == 1


def test_catalog_station_detail_returns_chargers_and_amenities(app_config, tmp_path: Path):
    sqlite_path = tmp_path / "open_static.sqlite3"
    _create_open_static_sqlite(sqlite_path)
    client = TestClient(create_app(replace(app_config, open_static_sqlite_path=sqlite_path)))

    response = client.get("/v1/catalog/stations/at:test:fast")

    assert response.status_code == 200
    payload = response.json()
    assert payload["station"]["station_id"] == "at:test:fast"
    assert payload["station"]["source_url"] == "https://example.test/fast"
    assert payload["chargers"][0]["charger_id"] == "at:test:fast:1"
    assert payload["chargers"][0]["current_type"] == "dc"
    assert payload["amenities"]["amenities_total"] == 3
    assert payload["amenities"]["amenity_examples"][0]["name"] == "Nearby Food"


def test_catalog_summary_returns_web_info_contract(app_config, tmp_path: Path):
    open_summary_path = tmp_path / "open_static_summary.json"
    build_summary_path = tmp_path / "summary.json"
    open_summary_path.write_text(
        json.dumps(
            {
                "bundle": {
                    "station_count": 42,
                    "charger_count": 84,
                    "country_count": 1,
                    "schema_version": 4,
                },
                "countries": [
                    {
                        "code": "AT",
                        "name": "Österreich",
                        "station_count": 42,
                        "charger_count": 84,
                    }
                ],
                "generated_at": "2026-06-21T09:04:42+00:00",
                "schema_version": 4,
                "sources": [
                    {
                        "country_code": "AT",
                        "display_name": "AT E-Control",
                        "source_uid": "at_econtrol",
                        "source_url": "https://example.test/at",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    build_summary_path.write_text(
        json.dumps(
            {
                "run": {"finished_at": "2026-06-21T09:00:00+00:00"},
                "records": {"raw_rows": 84, "full_registry_active_stations_total": 42},
            }
        ),
        encoding="utf-8",
    )
    client = TestClient(
        create_app(
            replace(
                app_config,
                open_static_summary_path=open_summary_path,
                build_summary_path=build_summary_path,
            )
        )
    )

    response = client.get("/v1/catalog/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["open_static_summary"]["bundle"]["station_count"] == 42
    assert payload["open_static_summary"]["countries"][0]["code"] == "AT"
    assert payload["open_static_summary"]["sources"][0]["source_url"] == "https://example.test/at"
    assert payload["summary"]["records"]["raw_rows"] == 84


def test_catalog_endpoints_return_503_when_bundle_missing(app_config, tmp_path: Path):
    client = TestClient(create_app(replace(app_config, open_static_sqlite_path=tmp_path / "missing.sqlite3")))

    response = client.get("/v1/catalog/search", params={"lat": 48.2, "lon": 16.3})

    assert response.status_code == 503
    assert response.json()["detail"] == "open_static_sqlite_unavailable"


def test_catalog_summary_returns_503_when_open_summary_missing(app_config, tmp_path: Path):
    client = TestClient(
        create_app(replace(app_config, open_static_summary_path=tmp_path / "missing-summary.json"))
    )

    response = client.get("/v1/catalog/summary")

    assert response.status_code == 503
    assert response.json()["detail"] == "open_static_summary_unavailable"
