#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUNDLE_DIR = REPO_ROOT / "data" / "onboarded_static"

OPEN_STATIC_FILES = (
    "chargers_full.csv",
    "chargers_fast.csv",
    "chargers_fast.geojson",
    "operators.json",
    "summary.json",
    "source_attribution.json",
)
OPTIONAL_NORMALIZED_FILES = ("stations.csv", "chargers.csv", "station_amenities.csv")
FORBIDDEN_PRIVATE_FIELDS = {
    "occupancy_share",
    "out_of_order_share",
    "unavailable_share",
    "confidence_score",
    "confidence_label",
    "station_class",
    "reliability_class",
    "utilization_class",
    "action_priority",
    "availability_status",
    "operational_status",
    "current_availability_status",
    "current_operational_status",
    "live_availability",
    "live_status",
}
REQUIRED_STATION_FIELDS = {
    "country_code",
    "station_id",
    "source_uid",
    "source_station_id",
    "license",
}
REQUIRED_CHARGER_FIELDS = {
    "country_code",
    "station_id",
    "charger_id",
    "source_uid",
    "source_station_id",
    "source_evse_id",
}
REQUIRED_STATION_AMENITY_FIELDS = {
    "country_code",
    "station_id",
    "amenity_radius_m",
    "amenities_total",
    "amenity_category_counts",
    "amenity_examples",
    "osm_pbf_url",
    "osm_extraction_status",
}


def _read_csv_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        return next(reader, [])


def _iter_csv_rows(path: Path) -> Iterable[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        yield from csv.DictReader(handle)


def _forbidden_fields(fields: Iterable[str]) -> list[str]:
    return sorted(FORBIDDEN_PRIVATE_FIELDS & {str(field or "").strip() for field in fields})


def _validate_csv_forbidden_fields(path: Path, errors: list[str]) -> None:
    header = _read_csv_header(path)
    forbidden = _forbidden_fields(header)
    if forbidden:
        errors.append(f"{path.name} contains private dynamic fields: {', '.join(forbidden)}")


def _validate_geojson_forbidden_fields(path: Path, errors: list[str]) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        errors.append(f"{path.name} must be a GeoJSON object")
        return
    for index, feature in enumerate(payload.get("features") or []):
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties") or {}
        if not isinstance(properties, dict):
            continue
        forbidden = _forbidden_fields(properties.keys())
        if forbidden:
            errors.append(
                f"{path.name} feature {index} contains private dynamic fields: {', '.join(forbidden)}"
            )
            return


def _validate_attribution(path: Path, errors: list[str]) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    sources = payload.get("sources") if isinstance(payload, dict) else None
    if not isinstance(sources, dict) or not sources:
        errors.append("source_attribution.json must contain a non-empty sources object")
        return

    def validate_source(source_key: str, source: dict[str, Any], *, require_url: bool) -> None:
        if not str(source.get("license") or "").strip():
            errors.append(f"source_attribution.json source {source_key} missing license")
        if require_url and not str(source.get("url") or source.get("resource_url") or "").strip():
            errors.append(f"source_attribution.json source {source_key} missing url")

    for source_key, source in sources.items():
        if not isinstance(source, dict):
            errors.append(f"source_attribution.json source {source_key} must be an object")
            continue
        validate_source(source_key, source, require_url=source_key != "OSM")
        additional_sources = source.get("additional_sources")
        if isinstance(additional_sources, list):
            for index, additional_source in enumerate(additional_sources):
                if not isinstance(additional_source, dict):
                    errors.append(
                        f"source_attribution.json source {source_key} additional_sources[{index}] must be an object"
                    )
                    continue
                additional_key = str(additional_source.get("source_uid") or f"{source_key}.additional_sources[{index}]")
                validate_source(additional_key, additional_source, require_url=True)
    osm = sources.get("OSM")
    if not isinstance(osm, dict):
        errors.append("source_attribution.json missing OSM attribution")
        return
    if osm.get("license") != "ODbL-1.0":
        errors.append("OSM attribution must declare ODbL-1.0")
    if "OpenStreetMap contributors" not in str(osm.get("attribution") or ""):
        errors.append("OSM attribution must mention OpenStreetMap contributors")


def _validate_normalized_csv(
    *,
    path: Path,
    required_fields: set[str],
    row_identity_field: str,
    require_rows: bool,
    errors: list[str],
    warnings: list[str],
) -> None:
    if not path.exists():
        warnings.append(f"{path.name} not present; normalized join-key export will be required for publication")
        return
    header = set(_read_csv_header(path))
    missing = sorted(required_fields - header)
    if missing:
        errors.append(f"{path.name} missing fields: {', '.join(missing)}")
        return
    row_count = 0
    seen_identity_values: set[str] = set()
    for row_count, row in enumerate(_iter_csv_rows(path), start=1):
        identity_value = str(row.get(row_identity_field) or "").strip()
        if not identity_value:
            errors.append(f"{path.name} row {row_count} missing {row_identity_field}")
            return
        if identity_value in seen_identity_values:
            errors.append(f"{path.name} row {row_count} duplicates {row_identity_field}: {identity_value}")
            return
        seen_identity_values.add(identity_value)
        if not str(row.get("station_id") or "").strip():
            errors.append(f"{path.name} row {row_count} missing station_id")
            return
    if require_rows and row_count == 0:
        errors.append(f"{path.name} must contain at least one data row")


def _sample_values(values: Iterable[str], *, limit: int = 5) -> str:
    ordered = sorted(value for value in values if value)
    sample = ordered[:limit]
    suffix = "..." if len(ordered) > limit else ""
    return ", ".join(sample) + suffix


def _csv_station_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {
        str(row.get("station_id") or "").strip()
        for row in _iter_csv_rows(path)
        if str(row.get("station_id") or "").strip()
    }


def _validate_chargers_reference_stations(
    chargers_path: Path,
    *,
    station_ids: set[str],
    errors: list[str],
) -> None:
    if not chargers_path.exists() or not station_ids:
        return
    missing = {
        str(row.get("station_id") or "").strip()
        for row in _iter_csv_rows(chargers_path)
        if str(row.get("station_id") or "").strip()
        and str(row.get("station_id") or "").strip() not in station_ids
    }
    if missing:
        errors.append(f"chargers.csv references unknown station_id values: {_sample_values(missing)}")


def _validate_station_amenities_csv(
    path: Path,
    *,
    require_rows: bool,
    station_ids: set[str],
    charger_station_ids: set[str],
    fail_on_pbf_missing: bool,
    errors: list[str],
    warnings: list[str],
) -> None:
    if not path.exists():
        if require_rows:
            errors.append("station_amenities.csv missing; OSM amenity provenance rows are required")
            return
        warnings.append("station_amenities.csv not present; OSM amenity provenance table will be required for publication")
        return
    header = set(_read_csv_header(path))
    missing = sorted(REQUIRED_STATION_AMENITY_FIELDS - header)
    if missing:
        errors.append(f"{path.name} missing fields: {', '.join(missing)}")
        return
    row_count = 0
    seen_station_ids: set[str] = set()
    pbf_missing_rows = 0
    for row_count, row in enumerate(_iter_csv_rows(path), start=1):
        station_id = str(row.get("station_id") or "").strip()
        if not station_id:
            errors.append(f"{path.name} row {row_count} missing station_id")
            return
        if station_id in seen_station_ids:
            errors.append(f"{path.name} row {row_count} duplicates station_id: {station_id}")
            return
        seen_station_ids.add(station_id)
        status = str(row.get("osm_extraction_status") or "").strip()
        if not status:
            errors.append(f"{path.name} row {row_count} missing osm_extraction_status")
            return
        if status == "pbf_missing":
            pbf_missing_rows += 1
    if row_count == 0:
        if require_rows:
            errors.append("station_amenities.csv must contain at least one data row")
        else:
            warnings.append("station_amenities.csv contains no data rows")
    if station_ids:
        missing_station_ids = station_ids - seen_station_ids
        extra_station_ids = seen_station_ids - station_ids
        if missing_station_ids:
            errors.append(f"station_amenities.csv missing rows for stations: {_sample_values(missing_station_ids)}")
        if extra_station_ids:
            errors.append(f"station_amenities.csv references unknown stations: {_sample_values(extra_station_ids)}")
    if charger_station_ids:
        missing_charger_station_ids = charger_station_ids - seen_station_ids
        if missing_charger_station_ids:
            errors.append(
                "station_amenities.csv missing rows for charger station_ids: "
                f"{_sample_values(missing_charger_station_ids)}"
            )
    if fail_on_pbf_missing and pbf_missing_rows:
        errors.append(f"station_amenities.csv has {pbf_missing_rows} pbf_missing rows")


def _validate_summary_no_overpass(path: Path, errors: list[str]) -> None:
    if not path.exists():
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return
    params = payload.get("params") if isinstance(payload.get("params"), dict) else {}
    lookup = payload.get("amenity_lookup") if isinstance(payload.get("amenity_lookup"), dict) else {}
    backend_values = {
        str(params.get("amenity_backend") or "").strip(),
        str(lookup.get("backend") or "").strip(),
    }
    if "overpass" in backend_values:
        errors.append("summary.json declares overpass amenity backend; production open-static bundles must use country PBFs")


def validate_bundle(
    bundle_dir: Path,
    *,
    require_normalized_rows: bool = False,
    fail_on_pbf_missing: bool = False,
    normalized_only: bool = False,
) -> dict[str, Any]:
    errors: list[str] = []
    warnings: list[str] = []
    bundle_dir = bundle_dir.resolve()
    for name in OPEN_STATIC_FILES:
        if not normalized_only and not (bundle_dir / name).exists():
            errors.append(f"missing open static bundle file: {name}")

    for name in ("chargers_full.csv", "chargers_fast.csv"):
        path = bundle_dir / name
        if path.exists():
            _validate_csv_forbidden_fields(path, errors)

    geojson_path = bundle_dir / "chargers_fast.geojson"
    if geojson_path.exists():
        _validate_geojson_forbidden_fields(geojson_path, errors)

    attribution_path = bundle_dir / "source_attribution.json"
    if attribution_path.exists():
        _validate_attribution(attribution_path, errors)

    _validate_normalized_csv(
        path=bundle_dir / "stations.csv",
        required_fields=REQUIRED_STATION_FIELDS,
        row_identity_field="station_id",
        require_rows=require_normalized_rows,
        errors=errors,
        warnings=warnings,
    )
    _validate_normalized_csv(
        path=bundle_dir / "chargers.csv",
        required_fields=REQUIRED_CHARGER_FIELDS,
        row_identity_field="charger_id",
        require_rows=require_normalized_rows,
        errors=errors,
        warnings=warnings,
    )
    station_ids = _csv_station_ids(bundle_dir / "stations.csv")
    charger_station_ids = _csv_station_ids(bundle_dir / "chargers.csv")
    _validate_chargers_reference_stations(
        bundle_dir / "chargers.csv",
        station_ids=station_ids,
        errors=errors,
    )
    _validate_station_amenities_csv(
        bundle_dir / "station_amenities.csv",
        require_rows=require_normalized_rows,
        station_ids=station_ids,
        charger_station_ids=charger_station_ids,
        fail_on_pbf_missing=fail_on_pbf_missing,
        errors=errors,
        warnings=warnings,
    )
    _validate_summary_no_overpass(bundle_dir / "summary.json", errors)

    return {
        "ok": not errors,
        "bundle_dir": str(bundle_dir),
        "errors": errors,
        "warnings": warnings,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the Woladen open static bundle boundary")
    parser.add_argument("--bundle-dir", type=Path, default=DEFAULT_BUNDLE_DIR)
    parser.add_argument("--require-normalized-rows", action="store_true")
    parser.add_argument("--fail-on-pbf-missing", action="store_true")
    parser.add_argument(
        "--normalized-only",
        action="store_true",
        help="Validate normalized source artifacts without requiring legacy CSV/GeoJSON compatibility files.",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    result = validate_bundle(
        args.bundle_dir,
        require_normalized_rows=args.require_normalized_rows,
        fail_on_pbf_missing=args.fail_on_pbf_missing,
        normalized_only=args.normalized_only,
    )
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        for warning in result["warnings"]:
            print(f"WARNING: {warning}")
        for error in result["errors"]:
            print(f"ERROR: {error}")
        if result["ok"]:
            print(f"{args.bundle_dir}: open static bundle validation passed")
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
