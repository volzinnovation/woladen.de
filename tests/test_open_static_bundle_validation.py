from __future__ import annotations

import csv
import json

from scripts.validate_open_static_bundle import validate_bundle


def _write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_valid_bundle(bundle_dir):
    _write_csv(
        bundle_dir / "chargers_full.csv",
        ["station_id", "operator", "status"],
        [{"station_id": "station-1", "operator": "Operator", "status": "In Betrieb"}],
    )
    _write_csv(
        bundle_dir / "chargers_fast.csv",
        ["station_id", "operator", "status"],
        [{"station_id": "station-1", "operator": "Operator", "status": "In Betrieb"}],
    )
    _write_csv(
        bundle_dir / "stations.csv",
        ["country_code", "station_id", "source_uid", "source_station_id", "license"],
        [
            {
                "country_code": "CH",
                "station_id": "station-1",
                "source_uid": "source",
                "source_station_id": "source-station",
                "license": "source_terms_pending_review",
            }
        ],
    )
    _write_csv(
        bundle_dir / "chargers.csv",
        [
            "country_code",
            "station_id",
            "charger_id",
            "source_uid",
            "source_station_id",
            "source_evse_id",
        ],
        [
            {
                "country_code": "CH",
                "station_id": "station-1",
                "charger_id": "charger-1",
                "source_uid": "source",
                "source_station_id": "source-station",
                "source_evse_id": "evse-1",
            }
        ],
    )
    _write_csv(
        bundle_dir / "station_amenities.csv",
        [
            "country_code",
            "station_id",
            "amenity_radius_m",
            "amenities_total",
            "amenity_category_counts",
            "amenity_examples",
            "osm_pbf_url",
            "osm_extraction_status",
        ],
        [
            {
                "country_code": "CH",
                "station_id": "station-1",
                "amenity_radius_m": "250",
                "amenities_total": "0",
                "amenity_category_counts": "{}",
                "amenity_examples": "[]",
                "osm_pbf_url": "https://example.test/ch.osm.pbf",
                "osm_extraction_status": "extracted_from_pbf",
            }
        ],
    )
    (bundle_dir / "chargers_fast.geojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [8.54, 47.37]},
                        "properties": {"station_id": "station-1", "operator": "Operator"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (bundle_dir / "operators.json").write_text("{}", encoding="utf-8")
    (bundle_dir / "summary.json").write_text("{}", encoding="utf-8")
    (bundle_dir / "source_attribution.json").write_text(
        json.dumps(
            {
                "sources": {
                    "CH": {
                        "url": "https://example.test/ch.json",
                        "license": "source_terms_pending_review",
                    },
                    "OSM": {
                        "license": "ODbL-1.0",
                        "attribution": "OpenStreetMap contributors",
                    },
                }
            }
        ),
        encoding="utf-8",
    )


def test_validates_open_static_bundle_join_keys_and_attribution(tmp_path):
    _write_valid_bundle(tmp_path)

    result = validate_bundle(tmp_path, require_normalized_rows=True)

    assert result["ok"] is True
    assert result["errors"] == []


def test_validates_normalized_only_artifact_without_legacy_bundle_files(tmp_path):
    _write_valid_bundle(tmp_path)
    for name in ("chargers_full.csv", "chargers_fast.csv", "chargers_fast.geojson", "operators.json"):
        (tmp_path / name).unlink()

    result = validate_bundle(tmp_path, require_normalized_rows=True, normalized_only=True)

    assert result["ok"] is True
    assert result["errors"] == []


def test_rejects_duplicate_normalized_charger_ids(tmp_path):
    _write_valid_bundle(tmp_path)
    _write_csv(
        tmp_path / "chargers.csv",
        [
            "country_code",
            "station_id",
            "charger_id",
            "source_uid",
            "source_station_id",
            "source_evse_id",
        ],
        [
            {
                "country_code": "NL",
                "station_id": "station-1",
                "charger_id": "nl:ocpi:nl-fas-e1",
                "source_uid": "source",
                "source_station_id": "source-station-1",
                "source_evse_id": "NL*FAS*E1",
            },
            {
                "country_code": "NL",
                "station_id": "station-2",
                "charger_id": "nl:ocpi:nl-fas-e1",
                "source_uid": "source",
                "source_station_id": "source-station-2",
                "source_evse_id": "NL*FAS*E1",
            },
        ],
    )

    result = validate_bundle(tmp_path, require_normalized_rows=True)

    assert result["ok"] is False
    assert result["errors"] == [
        "chargers.csv row 2 duplicates charger_id: nl:ocpi:nl-fas-e1",
        "chargers.csv references unknown station_id values: station-2",
        "station_amenities.csv missing rows for charger station_ids: station-2",
    ]


def test_rejects_private_fields_in_static_csv(tmp_path):
    _write_valid_bundle(tmp_path)
    _write_csv(
        tmp_path / "chargers_fast.csv",
        ["station_id", "operator", "occupancy_share"],
        [{"station_id": "station-1", "operator": "Operator", "occupancy_share": "0.4"}],
    )

    result = validate_bundle(tmp_path, require_normalized_rows=True)

    assert result["ok"] is False
    assert result["errors"] == [
        "chargers_fast.csv contains private dynamic fields: occupancy_share"
    ]


def test_rejects_private_fields_in_geojson(tmp_path):
    _write_valid_bundle(tmp_path)
    (tmp_path / "chargers_fast.geojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [8.54, 47.37]},
                        "properties": {"station_id": "station-1", "station_class": "underused"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = validate_bundle(tmp_path, require_normalized_rows=True)

    assert result["ok"] is False
    assert result["errors"] == [
        "chargers_fast.geojson feature 0 contains private dynamic fields: station_class"
    ]


def test_rejects_missing_osm_attribution(tmp_path):
    _write_valid_bundle(tmp_path)
    (tmp_path / "source_attribution.json").write_text(
        json.dumps({"sources": {"CH": {"url": "https://example.test", "license": "test"}}}),
        encoding="utf-8",
    )

    result = validate_bundle(tmp_path, require_normalized_rows=True)

    assert result["ok"] is False
    assert result["errors"] == ["source_attribution.json missing OSM attribution"]


def test_rejects_pbf_missing_when_requested(tmp_path):
    _write_valid_bundle(tmp_path)
    _write_csv(
        tmp_path / "station_amenities.csv",
        [
            "country_code",
            "station_id",
            "amenity_radius_m",
            "amenities_total",
            "amenity_category_counts",
            "amenity_examples",
            "osm_pbf_url",
            "osm_extraction_status",
        ],
        [
            {
                "country_code": "CH",
                "station_id": "station-1",
                "amenity_radius_m": "250",
                "amenities_total": "0",
                "amenity_category_counts": "{}",
                "amenity_examples": "[]",
                "osm_pbf_url": "https://example.test/ch.osm.pbf",
                "osm_extraction_status": "pbf_missing",
            }
        ],
    )

    result = validate_bundle(tmp_path, require_normalized_rows=True, fail_on_pbf_missing=True)

    assert result["ok"] is False
    assert result["errors"] == ["station_amenities.csv has 1 pbf_missing rows"]


def test_rejects_missing_station_amenity_rows_for_normalized_stations(tmp_path):
    _write_valid_bundle(tmp_path)
    _write_csv(
        tmp_path / "stations.csv",
        ["country_code", "station_id", "source_uid", "source_station_id", "license"],
        [
            {
                "country_code": "CH",
                "station_id": "station-1",
                "source_uid": "source",
                "source_station_id": "source-station",
                "license": "source_terms_pending_review",
            },
            {
                "country_code": "CH",
                "station_id": "station-2",
                "source_uid": "source",
                "source_station_id": "source-station-2",
                "license": "source_terms_pending_review",
            },
        ],
    )

    result = validate_bundle(tmp_path, require_normalized_rows=True)

    assert result["ok"] is False
    assert result["errors"] == ["station_amenities.csv missing rows for stations: station-2"]


def test_rejects_charger_station_without_amenity_row(tmp_path):
    _write_valid_bundle(tmp_path)
    _write_csv(
        tmp_path / "chargers.csv",
        [
            "country_code",
            "station_id",
            "charger_id",
            "source_uid",
            "source_station_id",
            "source_evse_id",
        ],
        [
            {
                "country_code": "CH",
                "station_id": "station-2",
                "charger_id": "charger-2",
                "source_uid": "source",
                "source_station_id": "source-station-2",
                "source_evse_id": "evse-2",
            }
        ],
    )

    result = validate_bundle(tmp_path, require_normalized_rows=True)

    assert result["ok"] is False
    assert result["errors"] == [
        "chargers.csv references unknown station_id values: station-2",
        "station_amenities.csv missing rows for charger station_ids: station-2",
    ]


def test_rejects_overpass_summary_backend(tmp_path):
    _write_valid_bundle(tmp_path)
    (tmp_path / "summary.json").write_text(
        json.dumps({"params": {"amenity_backend": "overpass"}, "amenity_lookup": {"backend": "overpass"}}),
        encoding="utf-8",
    )

    result = validate_bundle(tmp_path, require_normalized_rows=True)

    assert result["ok"] is False
    assert result["errors"] == [
        "summary.json declares overpass amenity backend; production open-static bundles must use country PBFs"
    ]
