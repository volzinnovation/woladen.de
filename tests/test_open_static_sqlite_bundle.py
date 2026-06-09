from __future__ import annotations

import csv
import json
import sqlite3

from scripts.build_open_static_sqlite_bundle import (
    SCHEMA_VERSION,
    aggregate_sqlite_parts,
    build_country_sqlite_from_csv_bundle,
    check_sqlite_counts_against_source,
)


def _write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_normalized_bundle(root):
    _write_csv(
        root / "stations.csv",
        [
            "country_code",
            "station_id",
            "source_uid",
            "source_station_id",
            "license",
            "operator_name",
            "latitude",
            "longitude",
            "charger_count",
            "max_power_kw",
            "helpdesk_phone",
            "price_display",
            "price_energy_eur_kwh_min",
            "price_energy_eur_kwh_max",
            "price_currency",
            "price_quality",
        ],
        [
            {
                "country_code": "CH",
                "station_id": "ch:coord:46.23432:6.055602",
                "source_uid": "ch_bfe_ladestationen_static",
                "source_station_id": "CH*CCI*E22078|CH*CCI*E22079",
                "license": "source_terms_pending_review",
                "operator_name": "SIG",
                "latitude": "46.23432",
                "longitude": "6.055602",
                "charger_count": "2",
                "max_power_kw": "150",
                "helpdesk_phone": "",
                "price_display": "",
                "price_energy_eur_kwh_min": "",
                "price_energy_eur_kwh_max": "",
                "price_currency": "",
                "price_quality": "",
            },
            {
                "country_code": "NL",
                "station_id": "nl:ocpi:fas:loc-a",
                "source_uid": "nl_ndw_dotnl_ocpi_locations",
                "source_station_id": "loc-a",
                "license": "CC0-1.0",
                "operator_name": "Fastned",
                "latitude": "52.1",
                "longitude": "5.1",
                "charger_count": "1",
                "max_power_kw": "150",
                "helpdesk_phone": "+3188123456",
                "price_display": "0,42 €/kWh",
                "price_energy_eur_kwh_min": "0.42",
                "price_energy_eur_kwh_max": "0.42",
                "price_currency": "EUR",
                "price_quality": "source_ocpi_tariff",
            },
        ],
    )
    _write_csv(
        root / "chargers.csv",
        [
            "country_code",
            "station_id",
            "charger_id",
            "source_uid",
            "source_station_id",
            "source_evse_id",
            "connector_id",
            "connector_type",
            "current_type",
            "max_power_kw",
            "operator_name",
        ],
        [
            {
                "country_code": "CH",
                "station_id": "ch:coord:46.23432:6.055602",
                "charger_id": "ch:oicp:CH*CCI*E22078",
                "source_uid": "ch_bfe_ladestationen_static",
                "source_station_id": "CH*CCI*E22078",
                "source_evse_id": "CH*CCI*E22078",
                "connector_id": "",
                "connector_type": "Type 2 Outlet",
                "current_type": "AC_3_PHASE",
                "max_power_kw": "22",
                "operator_name": "SIG",
            },
            {
                "country_code": "CH",
                "station_id": "ch:coord:46.23432:6.055602",
                "charger_id": "ch:oicp:CH*CCI*E22079",
                "source_uid": "ch_bfe_ladestationen_static",
                "source_station_id": "CH*CCI*E22079",
                "source_evse_id": "CH*CCI*E22079",
                "connector_id": "",
                "connector_type": "CCS",
                "current_type": "DC",
                "max_power_kw": "150",
                "operator_name": "SIG",
            },
            {
                "country_code": "NL",
                "station_id": "nl:ocpi:fas:loc-a",
                "charger_id": "nl:ocpi:nl-fas-e1",
                "source_uid": "nl_ndw_dotnl_ocpi_locations",
                "source_station_id": "loc-a",
                "source_evse_id": "NL*FAS*E1",
                "connector_id": "1;2",
                "connector_type": "IEC_62196_T2;IEC_62196_T2_COMBO",
                "current_type": "DC",
                "max_power_kw": "150",
                "operator_name": "Fastned",
            },
        ],
    )
    _write_csv(
        root / "dedupe_report.csv",
        ["issue", "country_code", "station_id", "source_uid", "details"],
        [
            {
                "issue": "duplicate_nl_source_evse_location_alias",
                "country_code": "NL",
                "station_id": "nl:ocpi:fas:loc-alias",
                "source_uid": "nl_ndw_dotnl_ocpi_locations",
                "details": (
                    "source_evse_id=NL*FAS*E1 alias_source_station_id=loc-alias "
                    "aliases canonical charger_id=nl:ocpi:nl-fas-e1 "
                    "station_id=nl:ocpi:fas:loc-a canonical_source_station_id=loc-a"
                ),
            }
        ],
    )
    _write_csv(
        root / "station_amenities.csv",
        [
            "country_code",
            "station_id",
            "amenity_radius_m",
            "amenities_total",
            "amenity_category_counts",
            "amenity_examples",
            "nearest_amenity_kind",
            "nearest_amenity_name",
            "nearest_amenity_distance_m",
            "osm_pbf_url",
            "osm_pbf_sha256",
            "osm_extracted_at",
            "osm_extraction_status",
        ],
        [
            {
                "country_code": "CH",
                "station_id": "ch:coord:46.23432:6.055602",
                "amenity_radius_m": "250",
                "amenities_total": "1",
                "amenity_category_counts": '{"restaurant":1}',
                "amenity_examples": '[{"category":"restaurant","distance_m":30}]',
                "nearest_amenity_kind": "amenity:restaurant",
                "nearest_amenity_name": "Lunch",
                "nearest_amenity_distance_m": "30",
                "osm_pbf_url": "https://example.test/ch.osm.pbf",
                "osm_pbf_sha256": "abc",
                "osm_extracted_at": "2026-05-07T12:00:00+00:00",
                "osm_extraction_status": "extracted_from_pbf",
            },
            {
                "country_code": "NL",
                "station_id": "nl:ocpi:fas:loc-a",
                "amenity_radius_m": "250",
                "amenities_total": "0",
                "amenity_category_counts": "{}",
                "amenity_examples": "[]",
                "nearest_amenity_kind": "",
                "nearest_amenity_name": "",
                "nearest_amenity_distance_m": "",
                "osm_pbf_url": "https://example.test/nl.osm.pbf",
                "osm_pbf_sha256": "",
                "osm_extracted_at": "",
                "osm_extraction_status": "pbf_missing",
            },
        ],
    )
    (root / "source_attribution.json").write_text(
        json.dumps(
            {
                "sources": {
                    "CH": {
                        "source_uid": "ch_bfe_ladestationen_static",
                        "url": "https://example.test/ch.json",
                        "license": "source_terms_pending_review",
                    },
                    "NL": {
                        "source_uid": "nl_ndw_dotnl_ocpi_locations",
                        "url": "https://example.test/nl.json",
                        "license": "CC0-1.0",
                    },
                    "OSM": {"license": "ODbL-1.0", "attribution": "OpenStreetMap contributors"},
                }
            }
        ),
        encoding="utf-8",
    )


def _query_plan_details(conn: sqlite3.Connection, sql: str, parameters=()) -> list[str]:
    return [str(row[3]) for row in conn.execute(f"EXPLAIN QUERY PLAN {sql}", parameters)]


def test_builds_country_sqlite_with_rtree_and_alias_rows(tmp_path):
    _write_normalized_bundle(tmp_path)
    output_path = tmp_path / "nl.sqlite3"

    result = build_country_sqlite_from_csv_bundle(input_dir=tmp_path, country_code="NL", output_path=output_path)

    assert result["station_count"] == 1
    assert result["charger_count"] == 1
    assert result["station_amenity_count"] == 1
    with sqlite3.connect(output_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        station_uid_info = {
            row[1]: row
            for row in conn.execute("PRAGMA table_info(station_amenities)")
        }["station_uid"]
        assert station_uid_info[5] == 1
        assert conn.execute(
            """
            SELECT 1
            FROM sqlite_schema
            WHERE type = 'index'
              AND tbl_name = 'station_amenities'
              AND name LIKE 'sqlite_autoindex_station_amenities%'
            """
        ).fetchone() is None
        assert conn.execute("SELECT count(*) FROM station_rtree").fetchone()[0] == 1
        assert conn.execute("SELECT amenities_total FROM station_amenities").fetchone()[0] == 0
        assert conn.execute("SELECT min(stations.station_uid), min(chargers.charger_uid) FROM stations CROSS JOIN chargers").fetchone() == (1, 1)
        assert conn.execute("SELECT charger_id FROM chargers").fetchone()[0] == "nl:ocpi:nl-fas-e1"
        assert conn.execute("SELECT price_display FROM stations").fetchone()[0] == "0,42 €/kWh"
        assert conn.execute("SELECT helpdesk_phone FROM stations").fetchone()[0] == "+3188123456"
        bbox_rows = conn.execute(
            """
            SELECT s.station_id
            FROM station_rtree r
            JOIN stations s ON s.station_uid = r.station_uid
            WHERE r.max_lon >= ? AND r.min_lon <= ?
              AND r.max_lat >= ? AND r.min_lat <= ?
            """,
            (5.0, 5.2, 52.0, 52.2),
        ).fetchall()
        assert [row[0] for row in bbox_rows] == ["nl:ocpi:fas:loc-a"]
        amenity_plan = _query_plan_details(
            conn,
            """
            SELECT amenities_total
            FROM station_amenities
            WHERE station_uid = ?
            """,
            (1,),
        )
        assert any("SEARCH station_amenities USING INTEGER PRIMARY KEY" in detail for detail in amenity_plan)
        assert all("AUTOMATIC" not in detail for detail in amenity_plan)
        assert conn.execute("SELECT source_station_id FROM charger_aliases").fetchone()[0] == "loc-alias"


def test_aggregates_country_sqlite_parts(tmp_path):
    _write_normalized_bundle(tmp_path)
    ch_path = tmp_path / "ch.sqlite3"
    nl_path = tmp_path / "nl.sqlite3"
    output_path = tmp_path / "open_static.sqlite3"
    build_country_sqlite_from_csv_bundle(input_dir=tmp_path, country_code="CH", output_path=ch_path)
    build_country_sqlite_from_csv_bundle(input_dir=tmp_path, country_code="NL", output_path=nl_path)

    result = aggregate_sqlite_parts(part_paths=[ch_path, nl_path], output_path=output_path)

    assert result["part_count"] == 2
    assert result["station_count"] == 2
    assert result["charger_count"] == 3
    assert result["station_amenity_count"] == 2
    with sqlite3.connect(output_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert conn.execute("SELECT count(*) FROM station_rtree").fetchone()[0] == 2
        assert conn.execute("SELECT sum(amenities_total) FROM station_amenities").fetchone()[0] == 1
        assert conn.execute("SELECT count(*) FROM chargers WHERE charger_id LIKE 'ch:oicp:%'").fetchone()[0] == 2
        assert {
            row[1]: row
            for row in conn.execute("PRAGMA table_info(station_amenities)")
        }["station_uid"][5] == 1


def test_checks_aggregate_counts_against_source_rows(tmp_path):
    _write_normalized_bundle(tmp_path)
    ch_path = tmp_path / "ch.sqlite3"
    nl_path = tmp_path / "nl.sqlite3"
    output_path = tmp_path / "open_static.sqlite3"
    build_country_sqlite_from_csv_bundle(input_dir=tmp_path, country_code="CH", output_path=ch_path)
    build_country_sqlite_from_csv_bundle(input_dir=tmp_path, country_code="NL", output_path=nl_path)
    aggregate_sqlite_parts(part_paths=[ch_path, nl_path], output_path=output_path)

    result = check_sqlite_counts_against_source(
        db_path=output_path,
        expected_dir=tmp_path,
        countries=["CH", "NL"],
    )

    assert result["ok"] is True
    assert result["expected"]["stations_by_country"] == {"CH": 1, "NL": 1}
    assert result["actual"]["chargers_by_country"] == {"CH": 2, "NL": 1}
    assert result["actual"]["station_amenities_by_country"] == {"CH": 1, "NL": 1}


def test_country_build_fails_when_row_source_uid_is_not_attributed(tmp_path):
    _write_normalized_bundle(tmp_path)
    attribution_path = tmp_path / "source_attribution.json"
    attribution = json.loads(attribution_path.read_text(encoding="utf-8"))
    attribution["sources"]["CH"]["source_uid"] = "ch_bfe_ladestationen_other"
    attribution_path.write_text(json.dumps(attribution), encoding="utf-8")

    try:
        build_country_sqlite_from_csv_bundle(
            input_dir=tmp_path,
            country_code="CH",
            output_path=tmp_path / "ch.sqlite3",
        )
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected source attribution check to fail")

    assert "missing_source_attribution:CH" in message
    assert "stations:ch_bfe_ladestationen_static" in message
    assert "chargers:ch_bfe_ladestationen_static" in message


def test_country_build_accepts_secondary_archived_source_uid(tmp_path):
    _write_normalized_bundle(tmp_path)
    attribution_path = tmp_path / "source_attribution.json"
    attribution = json.loads(attribution_path.read_text(encoding="utf-8"))
    attribution["sources"]["NL"]["secondary_archived_source_uid"] = "nl_secondary_static"
    attribution_path.write_text(json.dumps(attribution), encoding="utf-8")

    stations_path = tmp_path / "stations.csv"
    with stations_path.open(encoding="utf-8", newline="") as handle:
        station_rows = list(csv.DictReader(handle))
    station_rows.append(
        {
            "country_code": "NL",
            "station_id": "nl:secondary:loc-b",
            "source_uid": "nl_secondary_static",
            "source_station_id": "loc-b",
            "license": "CC0-1.0",
            "operator_name": "Secondary",
            "latitude": "52.2",
            "longitude": "5.2",
            "charger_count": "1",
            "max_power_kw": "50",
            "helpdesk_phone": "",
            "price_display": "",
            "price_energy_eur_kwh_min": "",
            "price_energy_eur_kwh_max": "",
            "price_currency": "",
            "price_quality": "",
        }
    )
    _write_csv(stations_path, station_rows[0].keys(), station_rows)

    chargers_path = tmp_path / "chargers.csv"
    with chargers_path.open(encoding="utf-8", newline="") as handle:
        charger_rows = list(csv.DictReader(handle))
    charger_rows.append(
        {
            "country_code": "NL",
            "station_id": "nl:secondary:loc-b",
            "charger_id": "nl:secondary:e1",
            "source_uid": "nl_secondary_static",
            "source_station_id": "loc-b",
            "source_evse_id": "NL*SEC*E1",
            "connector_id": "1",
            "connector_type": "IEC_62196_T2_COMBO",
            "current_type": "DC",
            "max_power_kw": "50",
            "operator_name": "Secondary",
        }
    )
    _write_csv(chargers_path, charger_rows[0].keys(), charger_rows)

    amenities_path = tmp_path / "station_amenities.csv"
    with amenities_path.open(encoding="utf-8", newline="") as handle:
        amenity_rows = list(csv.DictReader(handle))
    extra_amenity = dict(amenity_rows[-1])
    extra_amenity["station_id"] = "nl:secondary:loc-b"
    amenity_rows.append(extra_amenity)
    _write_csv(amenities_path, amenity_rows[0].keys(), amenity_rows)

    output_path = tmp_path / "nl.sqlite3"
    build_country_sqlite_from_csv_bundle(input_dir=tmp_path, country_code="NL", output_path=output_path)

    with sqlite3.connect(output_path) as conn:
        assert {
            row[0] for row in conn.execute("SELECT source_uid FROM sources")
        } >= {"nl_ndw_dotnl_ocpi_locations", "nl_secondary_static"}


def test_country_build_accepts_additional_attributed_sources(tmp_path):
    _write_normalized_bundle(tmp_path)
    attribution_path = tmp_path / "source_attribution.json"
    attribution = json.loads(attribution_path.read_text(encoding="utf-8"))
    attribution["sources"]["NL"]["additional_sources"] = [
        {
            "source_uid": "nl_additional_static",
            "url": "https://example.test/nl-additional.json",
            "license": "CC-BY-4.0-pending-review",
        }
    ]
    attribution_path.write_text(json.dumps(attribution), encoding="utf-8")

    stations_path = tmp_path / "stations.csv"
    with stations_path.open(encoding="utf-8", newline="") as handle:
        station_rows = list(csv.DictReader(handle))
    station_rows.append(
        {
            "country_code": "NL",
            "station_id": "nl:additional:loc-b",
            "source_uid": "nl_additional_static",
            "source_station_id": "loc-b",
            "license": "CC-BY-4.0-pending-review",
            "operator_name": "Additional",
            "latitude": "52.3",
            "longitude": "5.3",
            "charger_count": "1",
            "max_power_kw": "50",
            "helpdesk_phone": "",
            "price_display": "",
            "price_energy_eur_kwh_min": "",
            "price_energy_eur_kwh_max": "",
            "price_currency": "",
            "price_quality": "",
        }
    )
    _write_csv(stations_path, station_rows[0].keys(), station_rows)

    chargers_path = tmp_path / "chargers.csv"
    with chargers_path.open(encoding="utf-8", newline="") as handle:
        charger_rows = list(csv.DictReader(handle))
    charger_rows.append(
        {
            "country_code": "NL",
            "station_id": "nl:additional:loc-b",
            "charger_id": "nl:additional:e1",
            "source_uid": "nl_additional_static",
            "source_station_id": "loc-b",
            "source_evse_id": "NL*ADD*E1",
            "connector_id": "1",
            "connector_type": "IEC_62196_T2_COMBO",
            "current_type": "DC",
            "max_power_kw": "50",
            "operator_name": "Additional",
        }
    )
    _write_csv(chargers_path, charger_rows[0].keys(), charger_rows)

    amenities_path = tmp_path / "station_amenities.csv"
    with amenities_path.open(encoding="utf-8", newline="") as handle:
        amenity_rows = list(csv.DictReader(handle))
    extra_amenity = dict(amenity_rows[-1])
    extra_amenity["station_id"] = "nl:additional:loc-b"
    amenity_rows.append(extra_amenity)
    _write_csv(amenities_path, amenity_rows[0].keys(), amenity_rows)

    output_path = tmp_path / "nl.sqlite3"
    build_country_sqlite_from_csv_bundle(input_dir=tmp_path, country_code="NL", output_path=output_path)

    with sqlite3.connect(output_path) as conn:
        assert {
            row[0] for row in conn.execute("SELECT source_uid FROM sources")
        } >= {"nl_ndw_dotnl_ocpi_locations", "nl_additional_static"}


def test_country_build_fails_when_station_amenity_row_is_missing(tmp_path):
    _write_normalized_bundle(tmp_path)
    _write_csv(
        tmp_path / "station_amenities.csv",
        [
            "country_code",
            "station_id",
            "amenity_radius_m",
            "amenities_total",
            "amenity_category_counts",
            "amenity_examples",
            "nearest_amenity_kind",
            "nearest_amenity_name",
            "nearest_amenity_distance_m",
            "osm_pbf_url",
            "osm_pbf_sha256",
            "osm_extracted_at",
            "osm_extraction_status",
        ],
        [],
    )

    try:
        build_country_sqlite_from_csv_bundle(
            input_dir=tmp_path,
            country_code="CH",
            output_path=tmp_path / "ch.sqlite3",
        )
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected station amenity coverage check to fail")

    assert "station_amenity_coverage_failed:CH" in message
    assert "amenity_rows_missing_for_stations:ch:coord:46.23432:6.055602" in message


def test_count_check_fails_when_selected_country_is_missing(tmp_path):
    _write_normalized_bundle(tmp_path)
    ch_path = tmp_path / "ch.sqlite3"
    output_path = tmp_path / "open_static.sqlite3"
    build_country_sqlite_from_csv_bundle(input_dir=tmp_path, country_code="CH", output_path=ch_path)
    aggregate_sqlite_parts(part_paths=[ch_path], output_path=output_path)

    try:
        check_sqlite_counts_against_source(
            db_path=output_path,
            expected_dir=tmp_path,
            countries=["CH", "NL"],
        )
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected count check to fail")

    assert "chargers_country_counts_mismatch" in message


def test_count_check_allows_explicit_expected_empty_country(tmp_path):
    _write_normalized_bundle(tmp_path)
    ch_path = tmp_path / "ch.sqlite3"
    output_path = tmp_path / "open_static.sqlite3"
    build_country_sqlite_from_csv_bundle(input_dir=tmp_path, country_code="CH", output_path=ch_path)
    aggregate_sqlite_parts(part_paths=[ch_path], output_path=output_path)

    result = check_sqlite_counts_against_source(
        db_path=output_path,
        expected_dir=tmp_path,
        countries=["CH", "LT"],
        allow_empty_expected_countries=["LT"],
    )

    assert result["ok"] is True
    assert result["allow_empty_expected_countries"] == ["LT"]
    assert result["expected"]["stations_by_country"]["LT"] == 0
    assert result["actual"]["stations_by_country"]["LT"] == 0


def test_count_check_fails_when_sqlite_station_amenities_are_missing(tmp_path):
    _write_normalized_bundle(tmp_path)
    ch_path = tmp_path / "ch.sqlite3"
    nl_path = tmp_path / "nl.sqlite3"
    output_path = tmp_path / "open_static.sqlite3"
    build_country_sqlite_from_csv_bundle(input_dir=tmp_path, country_code="CH", output_path=ch_path)
    build_country_sqlite_from_csv_bundle(input_dir=tmp_path, country_code="NL", output_path=nl_path)
    aggregate_sqlite_parts(part_paths=[ch_path, nl_path], output_path=output_path)
    with sqlite3.connect(output_path) as conn:
        conn.execute("DELETE FROM station_amenities WHERE country_code = 'CH'")

    try:
        check_sqlite_counts_against_source(
            db_path=output_path,
            expected_dir=tmp_path,
            countries=["CH", "NL"],
        )
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("expected station amenity count check to fail")

    assert "station_amenities_country_counts_mismatch" in message
    assert "stations_missing_station_amenities:1" in message
    assert "chargers_missing_station_amenities:2" in message
