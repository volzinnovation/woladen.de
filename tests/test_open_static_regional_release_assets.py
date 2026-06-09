from __future__ import annotations

import csv
import json
import sqlite3
import zlib

import pytest

from scripts.build_open_static_regional_release_assets import REGIONAL_PACKS
from scripts.build_open_static_regional_release_assets import build_regional_release_assets
from scripts.build_open_static_sqlite_bundle import build_country_sqlite_from_csv_bundle


def _write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_minimal_normalized_bundle(root):
    station_fields = [
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
    ]
    charger_fields = [
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
    ]
    amenity_fields = [
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
    ]
    _write_csv(
        root / "stations.csv",
        station_fields,
        [
            {
                "country_code": "CH",
                "station_id": "ch:station:1",
                "source_uid": "ch_source",
                "source_station_id": "ch-1",
                "license": "source_terms_pending_review",
                "operator_name": "Swiss Operator",
                "latitude": "46.2",
                "longitude": "6.1",
                "charger_count": "1",
                "max_power_kw": "150",
            },
            {
                "country_code": "NL",
                "station_id": "nl:station:1",
                "source_uid": "nl_source",
                "source_station_id": "nl-1",
                "license": "CC0-1.0",
                "operator_name": "Dutch Operator",
                "latitude": "52.1",
                "longitude": "5.1",
                "charger_count": "1",
                "max_power_kw": "150",
            },
        ],
    )
    _write_csv(
        root / "chargers.csv",
        charger_fields,
        [
            {
                "country_code": "CH",
                "station_id": "ch:station:1",
                "charger_id": "ch:charger:1",
                "source_uid": "ch_source",
                "source_station_id": "ch-1",
                "source_evse_id": "CH*1",
                "connector_id": "1",
                "connector_type": "CCS",
                "current_type": "DC",
                "max_power_kw": "150",
                "operator_name": "Swiss Operator",
            },
            {
                "country_code": "NL",
                "station_id": "nl:station:1",
                "charger_id": "nl:charger:1",
                "source_uid": "nl_source",
                "source_station_id": "nl-1",
                "source_evse_id": "NL*1",
                "connector_id": "1",
                "connector_type": "CCS",
                "current_type": "DC",
                "max_power_kw": "150",
                "operator_name": "Dutch Operator",
            },
        ],
    )
    _write_csv(root / "dedupe_report.csv", ["issue", "country_code", "station_id", "source_uid", "details"], [])
    _write_csv(
        root / "station_amenities.csv",
        amenity_fields,
        [
            {
                "country_code": "CH",
                "station_id": "ch:station:1",
                "amenity_radius_m": "250",
                "amenities_total": "0",
                "amenity_category_counts": "{}",
                "amenity_examples": "[]",
                "nearest_amenity_kind": "",
                "nearest_amenity_name": "",
                "nearest_amenity_distance_m": "",
                "osm_pbf_url": "",
                "osm_pbf_sha256": "",
                "osm_extracted_at": "",
                "osm_extraction_status": "pbf_missing",
            },
            {
                "country_code": "NL",
                "station_id": "nl:station:1",
                "amenity_radius_m": "250",
                "amenities_total": "1",
                "amenity_category_counts": '{"restaurant":1}',
                "amenity_examples": '[{"category":"restaurant","distance_m":30}]',
                "nearest_amenity_kind": "amenity:restaurant",
                "nearest_amenity_name": "Lunch",
                "nearest_amenity_distance_m": "30",
                "osm_pbf_url": "",
                "osm_pbf_sha256": "",
                "osm_extracted_at": "",
                "osm_extraction_status": "extracted_from_pbf",
            },
        ],
    )
    (root / "source_attribution.json").write_text(
        json.dumps(
            {
                "sources": {
                    "CH": {"source_uid": "ch_source", "url": "https://example.test/ch", "license": "reviewed"},
                    "NL": {"source_uid": "nl_source", "url": "https://example.test/nl", "license": "CC0-1.0"},
                    "OSM": {"license": "ODbL-1.0", "attribution": "OpenStreetMap contributors"},
                }
            }
        ),
        encoding="utf-8",
    )


def test_builds_github_regional_release_assets_from_country_parts(tmp_path):
    source = tmp_path / "source"
    parts = tmp_path / "parts"
    out = tmp_path / "regional"
    _write_minimal_normalized_bundle(source)
    build_country_sqlite_from_csv_bundle(input_dir=source, country_code="CH", output_path=parts / "open-static-CH.sqlite3")
    build_country_sqlite_from_csv_bundle(input_dir=source, country_code="NL", output_path=parts / "open-static-NL.sqlite3")

    result = build_regional_release_assets(
        parts_dir=parts,
        output_dir=out,
        github_owner="volzinnovation",
        github_repo="woladen.de",
        github_release_tag="open-static-ios-regional-latest",
    )

    assert result["pack_count"] == 2
    dach_manifest = json.loads((out / "open-static-DACH.manifest.json").read_text(encoding="utf-8"))
    benelux_manifest = json.loads((out / "open-static-BENELUX.manifest.json").read_text(encoding="utf-8"))
    index = json.loads((out / "regional_pack_index.json").read_text(encoding="utf-8"))

    assert dach_manifest["format"] == "woladen.open-static.regional-pack.manifest"
    assert dach_manifest["schemaVersion"] == 2
    assert dach_manifest["version"] == "open-static-DACH"
    assert dach_manifest["countries"] == ["CH"]
    assert dach_manifest["assetPackGroup"]["missingCountries"] == ["DE", "AT"]
    assert dach_manifest["sqlite"]["file"] == "open-static-DACH.sqlite3"
    assert dach_manifest["compressedSQLite"]["file"] == "open-static-DACH.sqlite3.zlib"
    assert dach_manifest["compressedSQLite"]["algorithm"] == "zlib"
    assert dach_manifest["compressedSQLite"]["uncompressedSHA256"] == dach_manifest["sqlite"]["sha256"]
    assert dach_manifest["compressedSQLite"]["url"].endswith(
        "/releases/download/open-static-ios-regional-latest/open-static-DACH.sqlite3.zlib"
    )
    assert benelux_manifest["countries"] == ["NL"]
    assert {pack["groupID"] for pack in index["packs"]} == {"DACH", "BENELUX"}
    assert all(pack["compression"] == "zlib" for pack in index["packs"])
    assert not (out / "open-static-BENELUX.sqlite3").exists()

    decompressed = zlib.decompress((out / "open-static-BENELUX.sqlite3.zlib").read_bytes())
    expanded = tmp_path / "open-static-BENELUX.sqlite3"
    expanded.write_bytes(decompressed)
    with sqlite3.connect(expanded) as connection:
        assert connection.execute("SELECT count(*) FROM stations WHERE country_code = 'NL'").fetchone()[0] == 1
    assert (out / "open-static-BENELUX.sqlite3.sha256").read_text(encoding="utf-8").endswith(
        "  open-static-BENELUX.sqlite3\n"
    )
    assert (out / "open-static-BENELUX.sqlite3.zlib.sha256").read_text(encoding="utf-8").endswith(
        "  open-static-BENELUX.sqlite3.zlib\n"
    )


def test_rest_europe_regional_pack_includes_lithuania():
    rest_europe = next(pack for pack in REGIONAL_PACKS if pack.group_id == "REST-EUROPE")

    assert "LT" in rest_europe.countries


def test_nordics_regional_pack_includes_denmark():
    nordics = next(pack for pack in REGIONAL_PACKS if pack.group_id == "NORDICS")

    assert "DK" in nordics.countries


def test_regional_builder_can_fail_on_missing_country_parts(tmp_path):
    source = tmp_path / "source"
    parts = tmp_path / "parts"
    _write_minimal_normalized_bundle(source)
    build_country_sqlite_from_csv_bundle(input_dir=source, country_code="CH", output_path=parts / "open-static-CH.sqlite3")

    with pytest.raises(FileNotFoundError, match="regional_pack_missing_country_parts:DACH:DE,AT"):
        build_regional_release_assets(
            parts_dir=parts,
            output_dir=tmp_path / "regional",
            fail_on_missing_country=True,
            selected_group_ids=["DACH"],
        )
