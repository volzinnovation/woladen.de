from __future__ import annotations

import csv
import json
from pathlib import Path

from scripts import enrich_open_static_amenities as enrich
from scripts import osm_amenities


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_de_geojson_enrichment_preserves_amenity_payload(tmp_path):
    input_dir = tmp_path / "source"
    woladen_de_data_dir = tmp_path / "woladen-de-data"
    output_dir = tmp_path / "out"
    _write_csv(
        input_dir / "stations.csv",
        ["country_code", "station_id"],
        [
            {"country_code": "DE", "station_id": "DE:abc"},
            {"country_code": "DE", "station_id": "DE:missing"},
        ],
    )
    examples = [
        {
            "category": "restaurant",
            "name": "Exact Bistro",
            "opening_hours": "Mo-Fr 08:00-18:00",
            "distance_m": 12,
            "lat": 52.5,
            "lon": 13.4,
            "osm_ref": "node/123",
        },
        {"category": "cafe", "name": "Second", "distance_m": 20, "lat": 52.5001, "lon": 13.4001},
    ]
    (woladen_de_data_dir / "chargers_fast.geojson").parent.mkdir(parents=True, exist_ok=True)
    (woladen_de_data_dir / "chargers_fast.geojson").write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [13.4, 52.5]},
                        "properties": {
                            "station_id": "abc",
                            "amenity_restaurant": 2,
                            "amenity_cafe": 1,
                            "amenity_fast_food": 0,
                            "amenity_toilets": 0,
                            "amenity_supermarket": 0,
                            "amenity_bakery": 0,
                            "amenity_convenience": 0,
                            "amenity_pharmacy": 0,
                            "amenity_hotel": 0,
                            "amenity_museum": 0,
                            "amenity_playground": 0,
                            "amenity_park": 0,
                            "amenity_ice_cream": 0,
                            "amenities_total": 3,
                            "amenity_examples": examples,
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    summary = enrich.build_country_amenities(
        input_dir=input_dir,
        country_code="DE",
        output_dir=output_dir,
        woladen_de_data_dir=woladen_de_data_dir,
        pbf_cache_dir=tmp_path / "pbf",
        download_osm_pbf=False,
        amenity_radius_m=250.0,
    )

    rows = _read_csv(output_dir / "station_amenities-DE.csv")
    by_station = {row["station_id"]: row for row in rows}
    assert summary["matched_station_rows"] == 1
    assert by_station["DE:abc"]["amenities_total"] == "3"
    assert json.loads(by_station["DE:abc"]["amenity_category_counts"]) == {"cafe": 1, "restaurant": 2}
    assert json.loads(by_station["DE:abc"]["amenity_examples"]) == examples
    assert by_station["DE:abc"]["nearest_amenity_kind"] == "amenity:restaurant"
    assert by_station["DE:abc"]["nearest_amenity_name"] == "Exact Bistro"
    assert by_station["DE:abc"]["nearest_amenity_distance_m"] == "12"
    assert by_station["DE:abc"]["osm_extraction_status"] == "copied_from_woladen_de_fast_geojson"
    assert by_station["DE:missing"]["osm_extraction_status"] == "copied_from_woladen_de_geojson_no_fast_row"


def test_merge_country_amenities_replaces_rows_and_updates_metadata(tmp_path):
    source_dir = tmp_path / "source"
    parts_dir = tmp_path / "parts"
    output_dir = tmp_path / "ready"
    _write_csv(
        source_dir / "station_amenities.csv",
        list(osm_amenities.STATION_AMENITY_FIELDS),
        [
            {
                "country_code": "DE",
                "station_id": "DE:abc",
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
                "osm_extraction_status": "old",
            },
            {
                "country_code": "FR",
                "station_id": "fr:1",
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
        ],
    )
    _write_csv(
        parts_dir / "station_amenities-DE.csv",
        list(osm_amenities.STATION_AMENITY_FIELDS),
        [
            {
                "country_code": "DE",
                "station_id": "DE:abc",
                "amenity_radius_m": "250",
                "amenities_total": "1",
                "amenity_category_counts": '{"restaurant":1}',
                "amenity_examples": '[{"category":"restaurant","distance_m":10}]',
                "nearest_amenity_kind": "amenity:restaurant",
                "nearest_amenity_name": "",
                "nearest_amenity_distance_m": "10",
                "osm_pbf_url": "",
                "osm_pbf_sha256": "",
                "osm_extracted_at": "",
                "osm_extraction_status": "copied_from_woladen_de_fast_geojson",
            }
        ],
    )
    (parts_dir / "amenity-summary-DE.json").write_text(
        json.dumps({"country_code": "DE", "status": "copied_from_woladen_de_fast_geojson"}),
        encoding="utf-8",
    )
    (source_dir / "source_attribution.json").write_text(json.dumps({"sources": {"OSM": {}}}), encoding="utf-8")
    (source_dir / "summary.json").write_text(json.dumps({"params": {}, "amenity_lookup": {}}), encoding="utf-8")
    (source_dir / "catalog_summary.json").write_text(json.dumps({}), encoding="utf-8")
    (source_dir / "stations.csv").write_text("country_code,station_id\n", encoding="utf-8")

    result = enrich.merge_country_amenities(
        input_dir=source_dir,
        amenity_parts_dir=parts_dir,
        output_dir=output_dir,
    )

    rows = _read_csv(output_dir / "station_amenities.csv")
    by_station = {row["station_id"]: row for row in rows}
    assert result["replaced_rows"] == 1
    assert by_station["DE:abc"]["amenities_total"] == "1"
    assert by_station["fr:1"]["osm_extraction_status"] == "pbf_missing"
    source_attribution = json.loads((output_dir / "source_attribution.json").read_text(encoding="utf-8"))
    assert source_attribution["sources"]["OSM"]["amenity_status_by_country"]["DE"] == "copied_from_woladen_de_fast_geojson"
    assert source_attribution["sources"]["OSM"]["matrix_enrichment"]["DE"]["status"] == "copied_from_woladen_de_fast_geojson"


def test_reuse_previous_amenities_keeps_existing_rows_and_marks_new_stations(tmp_path):
    source_dir = tmp_path / "source"
    previous_dir = tmp_path / "previous"
    output_dir = tmp_path / "ready"
    _write_csv(
        source_dir / "stations.csv",
        ["country_code", "station_id"],
        [
            {"country_code": "DE", "station_id": "DE:old"},
            {"country_code": "FR", "station_id": "fr:new"},
        ],
    )
    _write_csv(source_dir / "chargers.csv", ["country_code", "station_id"], [])
    _write_csv(source_dir / "dedupe_report.csv", ["issue"], [])
    (source_dir / "source_attribution.json").write_text(json.dumps({"sources": {"OSM": {}}}), encoding="utf-8")
    (source_dir / "summary.json").write_text(json.dumps({"params": {}, "amenity_lookup": {}}), encoding="utf-8")
    (source_dir / "catalog_summary.json").write_text(json.dumps({}), encoding="utf-8")
    (source_dir / "bundle_quality_report.json").write_text(json.dumps({}), encoding="utf-8")
    _write_csv(
        previous_dir / "station_amenities.csv",
        list(osm_amenities.STATION_AMENITY_FIELDS),
        [
            {
                "country_code": "DE",
                "station_id": "DE:old",
                "amenity_radius_m": "250",
                "amenities_total": "2",
                "amenity_category_counts": '{"restaurant":2}',
                "amenity_examples": "[]",
                "nearest_amenity_kind": "",
                "nearest_amenity_name": "",
                "nearest_amenity_distance_m": "",
                "osm_pbf_url": "",
                "osm_pbf_sha256": "old-sha",
                "osm_extracted_at": "2026-05-01T00:00:00+00:00",
                "osm_extraction_status": "extracted_from_pbf",
            }
        ],
    )

    result = enrich.reuse_previous_amenities(input_dir=source_dir, previous_dir=previous_dir, output_dir=output_dir)

    rows = {row["station_id"]: row for row in _read_csv(output_dir / "station_amenities.csv")}
    assert result["reused_rows"] == 1
    assert result["missing_rows"] == 1
    assert rows["DE:old"]["osm_pbf_sha256"] == "old-sha"
    assert rows["fr:new"]["osm_extraction_status"] == "previous_amenity_missing"
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["params"]["amenity_backend"] == "reused_previous_station_amenities"
    assert summary["amenity_lookup"]["reuse_summary"]["reused_rows"] == 1
