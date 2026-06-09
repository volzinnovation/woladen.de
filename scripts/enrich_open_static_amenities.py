#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts import osm_amenities  # noqa: E402


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, fields: Sequence[str], rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _copy_source_bundle(input_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for path in input_dir.iterdir():
        if path.is_file():
            shutil.copy2(path, output_dir / path.name)


def _country_stations(input_dir: Path, country_code: str) -> list[dict[str, str]]:
    country_code = country_code.upper()
    return [
        row
        for row in _read_csv_rows(input_dir / "stations.csv")
        if _text(row.get("country_code")).upper() == country_code
    ]


def _de_station_id(value: Any) -> str:
    station_id = _text(value)
    if not station_id:
        return ""
    if station_id.upper().startswith("DE:"):
        return f"DE:{station_id[3:]}"
    return f"DE:{station_id}"


def _load_de_geojson_fast_rows(woladen_de_data_dir: Path) -> dict[str, dict[str, Any]]:
    path = woladen_de_data_dir / "chargers_fast.geojson"
    payload = json.loads(path.read_text(encoding="utf-8"))
    features = payload.get("features") if isinstance(payload, dict) else None
    if not isinstance(features, list):
        raise ValueError(f"{path} does not contain a GeoJSON FeatureCollection")
    rows: dict[str, dict[str, Any]] = {}
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties")
        if not isinstance(properties, dict):
            continue
        station_id = _de_station_id(properties.get("station_id"))
        if station_id:
            rows[station_id] = dict(properties)
    return rows


def _geojson_json_value(value: Any, fallback: Any) -> Any:
    if isinstance(value, str):
        if not value:
            return fallback
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return fallback
    return value if value not in (None, "") else fallback


def _de_geojson_amenity_row(
    *,
    station: Mapping[str, Any],
    fast_row: Mapping[str, Any],
    amenity_radius_m: float,
) -> dict[str, Any]:
    category_counts: dict[str, int] = {}
    for category in osm_amenities.AMENITY_BUNDLE_CATEGORIES:
        raw_count = fast_row.get(f"amenity_{category}")
        try:
            count = int(float(_text(raw_count) or "0"))
        except ValueError:
            count = 0
        if count > 0:
            category_counts[category] = count

    examples_raw = _geojson_json_value(fast_row.get("amenity_examples"), [])
    examples = [dict(item) for item in examples_raw] if isinstance(examples_raw, list) else []

    nearest_kind = ""
    nearest_name = ""
    nearest_distance = ""
    if examples:
        nearest = examples[0]
        category = _text(nearest.get("category"))
        if category:
            nearest_kind = f"amenity:{category}"
        nearest_name = _text(nearest.get("name"))
        nearest_distance = _text(nearest.get("distance_m"))

    amenities_total = _text(fast_row.get("amenities_total"))
    if not amenities_total:
        amenities_total = str(sum(category_counts.values()))

    row = {
        "country_code": "DE",
        "station_id": _text(station.get("station_id")),
        "amenity_radius_m": f"{float(amenity_radius_m):.3f}".rstrip("0").rstrip("."),
        "amenities_total": amenities_total,
        "amenity_category_counts": category_counts,
        "amenity_examples": examples,
        "nearest_amenity_kind": nearest_kind,
        "nearest_amenity_name": nearest_name,
        "nearest_amenity_distance_m": nearest_distance,
        "osm_pbf_url": osm_amenities.COUNTRY_PBF_URLS["DE"],
        "osm_pbf_sha256": "",
        "osm_extracted_at": "",
        "osm_extraction_status": "copied_from_woladen_de_fast_geojson",
    }
    return osm_amenities.station_amenity_row_to_csv(row)


def _build_de_geojson_rows(
    *,
    stations: Sequence[Mapping[str, Any]],
    woladen_de_data_dir: Path,
    amenity_radius_m: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    fast_rows = _load_de_geojson_fast_rows(woladen_de_data_dir)
    rows: list[dict[str, Any]] = []
    matched = 0
    for station in stations:
        station_id = _text(station.get("station_id"))
        fast_row = fast_rows.get(station_id)
        if fast_row is not None:
            matched += 1
            row = _de_geojson_amenity_row(
                station=station,
                fast_row=fast_row,
                amenity_radius_m=amenity_radius_m,
            )
        else:
            row = osm_amenities.empty_station_amenity_row(
                station,
                radius_m=amenity_radius_m,
                osm_pbf_url=osm_amenities.COUNTRY_PBF_URLS["DE"],
                osm_extraction_status="copied_from_woladen_de_geojson_no_fast_row",
            )
            row = osm_amenities.station_amenity_row_to_csv(row)
        rows.append(row)
    return rows, {
        "country_code": "DE",
        "backend": "woladen_de_fast_geojson",
        "station_count": len(stations),
        "geojson_fast_rows": len(fast_rows),
        "matched_station_rows": matched,
        "missing_geojson_station_rows": max(0, len(stations) - matched),
        "status": "copied_from_woladen_de_fast_geojson",
    }


def _build_pbf_rows(
    *,
    country_code: str,
    stations: Sequence[Mapping[str, Any]],
    pbf_cache_dir: Path,
    download_osm_pbf: bool,
    amenity_radius_m: float,
    pbf_progress_every: int,
    pbf_download_progress_mb: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    pbf_url = osm_amenities.COUNTRY_PBF_URLS.get(country_code, "")
    if not pbf_url:
        rows = [
            osm_amenities.station_amenity_row_to_csv(
                osm_amenities.empty_station_amenity_row(
                    station,
                    radius_m=amenity_radius_m,
                    osm_pbf_url="",
                    osm_extraction_status="pbf_url_missing",
                )
            )
            for station in stations
        ]
        return rows, {
            "country_code": country_code,
            "backend": "osm-pbf",
            "station_count": len(stations),
            "status": "pbf_url_missing",
        }

    pbf_path = osm_amenities.pbf_cache_path(country_code, pbf_cache_dir)
    points: list[osm_amenities.AmenityPoint] = []
    pbf_sha256 = ""
    extracted_at = ""
    status = "pbf_missing"
    pbf_stats: dict[str, Any] = {}

    if download_osm_pbf:
        pbf_path = osm_amenities.download_pbf(
            country_code,
            pbf_cache_dir,
            progress_every_mb=pbf_download_progress_mb,
        )

    if pbf_path.exists() and pbf_path.stat().st_size > 0:
        points, pbf_stats = osm_amenities.collect_amenity_points_from_pbf(
            pbf_path=pbf_path,
            stations=stations,
            radius_m=amenity_radius_m,
            pbf_progress_every=pbf_progress_every,
        )
        pbf_sha256 = osm_amenities.sha256_file(pbf_path)
        extracted_at = osm_amenities.utc_now_iso()
        status = "extracted_from_pbf"

    if points:
        rows = osm_amenities.join_station_amenity_rows(
            stations=stations,
            points=points,
            radius_m=amenity_radius_m,
            osm_pbf_url=pbf_url,
            osm_extraction_status=status,
            osm_pbf_sha256=pbf_sha256,
            osm_extracted_at=extracted_at,
        )
    else:
        rows = [
            osm_amenities.empty_station_amenity_row(
                station,
                radius_m=amenity_radius_m,
                osm_pbf_url=pbf_url,
                osm_extraction_status=status,
                osm_pbf_sha256=pbf_sha256,
                osm_extracted_at=extracted_at,
            )
            for station in stations
        ]
    csv_rows = [osm_amenities.station_amenity_row_to_csv(row) for row in rows]
    return csv_rows, {
        "country_code": country_code,
        "backend": "osm-pbf",
        "station_count": len(stations),
        "pbf_url": pbf_url,
        "pbf_path": str(pbf_path),
        "pbf_sha256": pbf_sha256,
        "extracted_at": extracted_at,
        "status": status,
        "points": len(points),
        **pbf_stats,
    }


def build_country_amenities(
    *,
    input_dir: Path,
    country_code: str,
    output_dir: Path,
    woladen_de_data_dir: Path,
    pbf_cache_dir: Path,
    download_osm_pbf: bool,
    amenity_radius_m: float,
    pbf_progress_every: int = 0,
    pbf_download_progress_mb: int = 0,
) -> dict[str, Any]:
    country_code = country_code.upper()
    stations = _country_stations(input_dir, country_code)
    if country_code == "DE":
        rows, summary = _build_de_geojson_rows(
            stations=stations,
            woladen_de_data_dir=woladen_de_data_dir,
            amenity_radius_m=amenity_radius_m,
        )
    else:
        rows, summary = _build_pbf_rows(
            country_code=country_code,
            stations=stations,
            pbf_cache_dir=pbf_cache_dir,
            download_osm_pbf=download_osm_pbf,
            amenity_radius_m=amenity_radius_m,
            pbf_progress_every=pbf_progress_every,
            pbf_download_progress_mb=pbf_download_progress_mb,
        )

    rows.sort(key=lambda row: (_text(row.get("country_code")), _text(row.get("station_id"))))
    output_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(output_dir / f"station_amenities-{country_code}.csv", osm_amenities.STATION_AMENITY_FIELDS, rows)
    status_counts = Counter(_text(row.get("osm_extraction_status")) or "unknown" for row in rows)
    positive_rows = sum(1 for row in rows if int(float(_text(row.get("amenities_total")) or "0")) > 0)
    summary = {
        **summary,
        "output_rows": len(rows),
        "positive_station_rows": positive_rows,
        "status_counts": dict(sorted(status_counts.items())),
    }
    _write_json(output_dir / f"amenity-summary-{country_code}.json", summary)
    return summary


def _read_amenity_part_rows(parts_dir: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(parts_dir.rglob("station_amenities-*.csv")):
        rows.extend(_read_csv_rows(path))
    return rows


def _read_amenity_part_summaries(parts_dir: Path) -> dict[str, Any]:
    summaries: dict[str, Any] = {}
    for path in sorted(parts_dir.rglob("amenity-summary-*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        country_code = _text(payload.get("country_code")).upper() or path.stem.rsplit("-", 1)[-1].upper()
        summaries[country_code] = payload
    return dict(sorted(summaries.items()))


def _update_json_file(path: Path, updater: Any) -> None:
    if not path.exists():
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    updater(payload)
    _write_json(path, payload)


def merge_country_amenities(
    *,
    input_dir: Path,
    amenity_parts_dir: Path,
    output_dir: Path,
) -> dict[str, Any]:
    _copy_source_bundle(input_dir, output_dir)
    original_rows = _read_csv_rows(input_dir / "station_amenities.csv")
    part_rows = _read_amenity_part_rows(amenity_parts_dir)
    replacements = {_text(row.get("station_id")): row for row in part_rows if _text(row.get("station_id"))}

    merged_rows: list[dict[str, Any]] = []
    seen_station_ids: set[str] = set()
    replaced_count = 0
    for row in original_rows:
        station_id = _text(row.get("station_id"))
        replacement = replacements.get(station_id)
        if replacement is not None:
            merged_rows.append(replacement)
            replaced_count += 1
        else:
            merged_rows.append(row)
        if station_id:
            seen_station_ids.add(station_id)
    appended_count = 0
    for station_id, row in sorted(replacements.items()):
        if station_id not in seen_station_ids:
            merged_rows.append(row)
            appended_count += 1

    merged_rows.sort(key=lambda row: (_text(row.get("country_code")), _text(row.get("station_id"))))
    _write_csv(output_dir / "station_amenities.csv", osm_amenities.STATION_AMENITY_FIELDS, merged_rows)

    summaries = _read_amenity_part_summaries(amenity_parts_dir)
    status_by_country = osm_amenities.amenity_status_by_country(merged_rows)
    pbf_missing_rows = sum(1 for row in merged_rows if _text(row.get("osm_extraction_status")) == "pbf_missing")
    positive_rows = sum(1 for row in merged_rows if int(float(_text(row.get("amenities_total")) or "0")) > 0)

    def update_source_attribution(payload: dict[str, Any]) -> None:
        sources = payload.setdefault("sources", {})
        osm = sources.setdefault("OSM", {})
        osm["amenity_status_by_country"] = status_by_country
        osm["matrix_enrichment"] = summaries

    def update_summary(payload: dict[str, Any]) -> None:
        params = payload.setdefault("params", {})
        params["include_osm"] = True
        params["amenity_backend"] = "matrix_de_geojson_and_country_pbf"
        amenity_lookup = payload.setdefault("amenity_lookup", {})
        amenity_lookup["backend"] = "matrix_de_geojson_and_country_pbf"
        amenity_lookup["amenity_status_by_country"] = status_by_country
        amenity_lookup["station_amenity_rows"] = len(merged_rows)
        amenity_lookup["pbf_missing_station_rows"] = pbf_missing_rows
        amenity_lookup["stations_with_mapped_amenities"] = positive_rows
        amenity_lookup["matrix_enrichment"] = summaries

    def update_catalog_summary(payload: dict[str, Any]) -> None:
        payload["station_amenity_count"] = len(merged_rows)
        payload["amenity_status_by_country"] = status_by_country

    _update_json_file(output_dir / "source_attribution.json", update_source_attribution)
    _update_json_file(output_dir / "summary.json", update_summary)
    _update_json_file(output_dir / "catalog_summary.json", update_catalog_summary)

    result = {
        "output_dir": str(output_dir.resolve()),
        "original_rows": len(original_rows),
        "part_rows": len(part_rows),
        "merged_rows": len(merged_rows),
        "replaced_rows": replaced_count,
        "appended_rows": appended_count,
        "pbf_missing_rows": pbf_missing_rows,
        "positive_station_rows": positive_rows,
        "amenity_status_by_country": status_by_country,
        "matrix_enrichment": summaries,
    }
    _write_json(output_dir / "amenity_merge_summary.json", result)
    return result


def reuse_previous_amenities(
    *,
    input_dir: Path,
    previous_dir: Path,
    output_dir: Path,
    amenity_radius_m: float = osm_amenities.DEFAULT_AMENITY_RADIUS_M,
) -> dict[str, Any]:
    _copy_source_bundle(input_dir, output_dir)
    stations = _read_csv_rows(input_dir / "stations.csv")
    previous_rows = {
        _text(row.get("station_id")): row
        for row in _read_csv_rows(previous_dir / "station_amenities.csv")
        if _text(row.get("station_id"))
    }

    rows: list[dict[str, Any]] = []
    reused_count = 0
    missing_count = 0
    for station in stations:
        station_id = _text(station.get("station_id"))
        previous = previous_rows.get(station_id)
        if previous is not None:
            rows.append(previous)
            reused_count += 1
            continue
        country_code = _text(station.get("country_code")).upper()
        rows.append(
            osm_amenities.station_amenity_row_to_csv(
                osm_amenities.empty_station_amenity_row(
                    station,
                    radius_m=amenity_radius_m,
                    osm_pbf_url=osm_amenities.COUNTRY_PBF_URLS.get(country_code, ""),
                    osm_extraction_status="previous_amenity_missing",
                )
            )
        )
        missing_count += 1

    rows.sort(key=lambda row: (_text(row.get("country_code")), _text(row.get("station_id"))))
    _write_csv(output_dir / "station_amenities.csv", osm_amenities.STATION_AMENITY_FIELDS, rows)

    status_by_country = osm_amenities.amenity_status_by_country(rows)
    positive_rows = sum(1 for row in rows if int(float(_text(row.get("amenities_total")) or "0")) > 0)
    reuse_summary = {
        "enabled": True,
        "mode": "reused_previous_station_amenities",
        "input_station_rows": len(stations),
        "previous_amenity_rows": len(previous_rows),
        "reused_rows": reused_count,
        "missing_rows": missing_count,
        "dropped_previous_rows": max(0, len(previous_rows) - reused_count),
        "positive_station_rows": positive_rows,
        "amenity_status_by_country": status_by_country,
    }

    def update_source_attribution(payload: dict[str, Any]) -> None:
        sources = payload.setdefault("sources", {})
        osm = sources.setdefault("OSM", {})
        osm["amenity_status_by_country"] = status_by_country
        osm["reuse_summary"] = reuse_summary

    def update_summary(payload: dict[str, Any]) -> None:
        params = payload.setdefault("params", {})
        params["include_osm"] = False
        params["amenity_backend"] = "reused_previous_station_amenities"
        amenity_lookup = payload.setdefault("amenity_lookup", {})
        amenity_lookup["backend"] = "reused_previous_station_amenities"
        amenity_lookup["amenity_status_by_country"] = status_by_country
        amenity_lookup["station_amenity_rows"] = len(rows)
        amenity_lookup["pbf_missing_station_rows"] = sum(
            1 for row in rows if _text(row.get("osm_extraction_status")) == "pbf_missing"
        )
        amenity_lookup["stations_with_mapped_amenities"] = positive_rows
        amenity_lookup["reuse_summary"] = reuse_summary

    def update_catalog_summary(payload: dict[str, Any]) -> None:
        payload["station_amenity_count"] = len(rows)
        payload["amenity_status_by_country"] = status_by_country

    _update_json_file(output_dir / "source_attribution.json", update_source_attribution)
    _update_json_file(output_dir / "summary.json", update_summary)
    _update_json_file(output_dir / "catalog_summary.json", update_catalog_summary)
    _write_json(output_dir / "amenity_reuse_summary.json", reuse_summary)
    _write_json(output_dir / "amenity_merge_summary.json", reuse_summary)
    return reuse_summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Enrich an existing open-static normalized bundle with OSM amenities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    country_parser = subparsers.add_parser("country", help="Build station_amenities.csv rows for one country.")
    country_parser.add_argument("--input-dir", type=Path, required=True)
    country_parser.add_argument("--country", required=True)
    country_parser.add_argument("--output-dir", type=Path, required=True)
    country_parser.add_argument("--woladen-de-data-dir", type=Path, default=REPO_ROOT.parent / "woladen.de" / "data")
    country_parser.add_argument("--pbf-cache-dir", type=Path, default=REPO_ROOT / "data" / "osm_pbf_cache")
    country_parser.add_argument("--download-osm-pbf", action="store_true")
    country_parser.add_argument("--amenity-radius-m", type=float, default=osm_amenities.DEFAULT_AMENITY_RADIUS_M)
    country_parser.add_argument("--pbf-progress-every", type=int, default=0)
    country_parser.add_argument("--pbf-download-progress-mb", type=int, default=0)

    merge_parser = subparsers.add_parser("merge", help="Merge country station_amenities parts into a normalized bundle.")
    merge_parser.add_argument("--input-dir", type=Path, required=True)
    merge_parser.add_argument("--amenity-parts-dir", type=Path, required=True)
    merge_parser.add_argument("--output-dir", type=Path, required=True)

    reuse_parser = subparsers.add_parser("reuse", help="Reuse station_amenities.csv rows from a previous normalized bundle.")
    reuse_parser.add_argument("--input-dir", type=Path, required=True)
    reuse_parser.add_argument("--previous-dir", type=Path, required=True)
    reuse_parser.add_argument("--output-dir", type=Path, required=True)
    reuse_parser.add_argument("--amenity-radius-m", type=float, default=osm_amenities.DEFAULT_AMENITY_RADIUS_M)

    args = parser.parse_args(argv)
    if args.command == "country":
        result = build_country_amenities(
            input_dir=args.input_dir,
            country_code=args.country,
            output_dir=args.output_dir,
            woladen_de_data_dir=args.woladen_de_data_dir,
            pbf_cache_dir=args.pbf_cache_dir,
            download_osm_pbf=args.download_osm_pbf,
            amenity_radius_m=args.amenity_radius_m,
            pbf_progress_every=args.pbf_progress_every,
            pbf_download_progress_mb=args.pbf_download_progress_mb,
        )
    elif args.command == "merge":
        result = merge_country_amenities(
            input_dir=args.input_dir,
            amenity_parts_dir=args.amenity_parts_dir,
            output_dir=args.output_dir,
        )
    elif args.command == "reuse":
        result = reuse_previous_amenities(
            input_dir=args.input_dir,
            previous_dir=args.previous_dir,
            output_dir=args.output_dir,
            amenity_radius_m=args.amenity_radius_m,
        )
    else:
        raise AssertionError(args.command)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
