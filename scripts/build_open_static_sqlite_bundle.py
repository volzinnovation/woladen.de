#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SCHEMA_VERSION = 4
STATION_COLUMNS = (
    "country_code",
    "station_id",
    "source_uid",
    "source_station_id",
    "license",
    "provider_uid",
    "operator_name",
    "station_name",
    "address",
    "postal_code",
    "city",
    "latitude",
    "longitude",
    "charger_count",
    "max_power_kw",
    "connector_types",
    "source_url",
    "public_bundle_status",
    "id_rule",
    "opening_hours",
    "payment_methods",
    "auth_methods",
    "green_energy",
    "helpdesk_phone",
    "price_display",
    "price_energy_eur_kwh_min",
    "price_energy_eur_kwh_max",
    "price_currency",
    "price_quality",
    "detail_last_updated",
)
CHARGER_COLUMNS = (
    "country_code",
    "station_id",
    "charger_id",
    "source_uid",
    "provider_uid",
    "source_station_id",
    "source_evse_id",
    "connector_id",
    "connector_type",
    "current_type",
    "max_power_kw",
    "operator_name",
    "license",
    "source_url",
    "public_bundle_status",
)
DEDUPE_COLUMNS = ("issue", "country_code", "station_id", "source_uid", "details")
STATION_AMENITY_COLUMNS = (
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
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _text(value: Any) -> str:
    return str(value or "").strip()


def _float_or_none(value: Any) -> float | None:
    text = _text(value).replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _int_or_none(value: Any) -> int | None:
    text = _text(value)
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _country_filter(row: dict[str, str], country_code: str | None) -> bool:
    if country_code is None:
        return True
    country = _text(row.get("country_code")).upper()
    if country:
        return country == country_code
    station_id = _text(row.get("station_id"))
    return station_id.upper().startswith(f"{country_code}:")


def _country_codes_from_text(value: str | None) -> list[str]:
    countries: list[str] = []
    for raw in _text(value).split(","):
        country = raw.strip().upper()
        if country and country not in countries:
            countries.append(country)
    return countries


def _connect_for_build(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn


def _ensure_rtree_available(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("CREATE VIRTUAL TABLE temp._rtree_check USING rtree(id, min_x, max_x, min_y, max_y)")
        conn.execute("DROP TABLE temp._rtree_check")
    except sqlite3.DatabaseError as exc:
        raise RuntimeError("sqlite_rtree_extension_unavailable") from exc


def _create_schema(conn: sqlite3.Connection) -> None:
    _ensure_rtree_available(conn)
    conn.executescript(
        """
        PRAGMA foreign_keys=ON;
        CREATE TABLE bundle_metadata (
          key TEXT PRIMARY KEY,
          json_value TEXT NOT NULL
        ) WITHOUT ROWID;

        CREATE TABLE sources (
          source_uid TEXT PRIMARY KEY,
          country_code TEXT NOT NULL,
          source_url TEXT NOT NULL,
          license TEXT NOT NULL,
          attribution_json TEXT NOT NULL
        ) WITHOUT ROWID;

        CREATE TABLE stations (
          station_uid INTEGER PRIMARY KEY CHECK (station_uid >= 0),
          country_code TEXT NOT NULL,
          station_id TEXT NOT NULL UNIQUE,
          source_uid TEXT NOT NULL,
          source_station_id TEXT NOT NULL,
          license TEXT NOT NULL,
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
          id_rule TEXT,
          opening_hours TEXT,
          payment_methods TEXT,
          auth_methods TEXT,
          green_energy TEXT,
          helpdesk_phone TEXT NOT NULL DEFAULT '',
          price_display TEXT NOT NULL DEFAULT '',
          price_energy_eur_kwh_min TEXT NOT NULL DEFAULT '',
          price_energy_eur_kwh_max TEXT NOT NULL DEFAULT '',
          price_currency TEXT NOT NULL DEFAULT '',
          price_quality TEXT NOT NULL DEFAULT '',
          detail_last_updated TEXT
        );

        CREATE TABLE chargers (
          charger_uid INTEGER PRIMARY KEY CHECK (charger_uid >= 0),
          country_code TEXT NOT NULL,
          station_uid INTEGER NOT NULL REFERENCES stations(station_uid),
          station_id TEXT NOT NULL,
          charger_id TEXT NOT NULL UNIQUE,
          source_uid TEXT NOT NULL,
          provider_uid TEXT,
          source_station_id TEXT NOT NULL,
          source_evse_id TEXT NOT NULL,
          connector_id TEXT,
          connector_type TEXT,
          current_type TEXT,
          max_power_kw REAL,
          operator_name TEXT,
          license TEXT,
          source_url TEXT,
          public_bundle_status TEXT
        );

        CREATE TABLE charger_aliases (
          source_uid TEXT NOT NULL,
          source_station_id TEXT NOT NULL,
          source_evse_id TEXT NOT NULL,
          charger_id TEXT NOT NULL,
          station_id TEXT NOT NULL,
          alias_status TEXT NOT NULL,
          PRIMARY KEY (source_uid, source_station_id, source_evse_id)
        ) WITHOUT ROWID;

        CREATE TABLE dedupe_issues (
          issue_uid INTEGER PRIMARY KEY CHECK (issue_uid >= 0),
          issue TEXT NOT NULL,
          country_code TEXT NOT NULL,
          station_id TEXT,
          source_uid TEXT,
          details TEXT
        );

        CREATE TABLE station_amenities (
          station_uid INTEGER PRIMARY KEY CHECK (station_uid >= 0) REFERENCES stations(station_uid),
          country_code TEXT NOT NULL,
          station_id TEXT NOT NULL,
          amenity_radius_m REAL NOT NULL,
          amenities_total INTEGER NOT NULL,
          amenity_category_counts_json TEXT NOT NULL,
          amenity_examples_json TEXT NOT NULL,
          nearest_amenity_kind TEXT,
          nearest_amenity_name TEXT,
          nearest_amenity_distance_m REAL,
          osm_pbf_url TEXT,
          osm_pbf_sha256 TEXT,
          osm_extracted_at TEXT,
          osm_extraction_status TEXT NOT NULL
        );

        CREATE VIRTUAL TABLE station_rtree USING rtree(
          station_uid,
          min_lon, max_lon,
          min_lat, max_lat
        );
        """
    )
    conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")


def _insert_metadata(conn: sqlite3.Connection, key: str, value: Any) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO bundle_metadata(key, json_value) VALUES(?, ?)",
        (key, _json_dumps(value)),
    )


def _source_items(source_attribution: dict[str, Any], country_code: str | None) -> Iterable[tuple[str, dict[str, Any]]]:
    sources = source_attribution.get("sources") if isinstance(source_attribution, dict) else {}
    if not isinstance(sources, dict):
        return []
    items: list[tuple[str, dict[str, Any]]] = []
    for key, source in sources.items():
        if not isinstance(source, dict):
            continue
        normalized_key = _text(key).upper()
        if country_code is not None and normalized_key not in {country_code, "OSM"}:
            continue
        items.append((_text(key), source))
    return items


def _insert_sources(conn: sqlite3.Connection, source_attribution: dict[str, Any], country_code: str | None) -> None:
    def insert_source(key: str, source: dict[str, Any], source_uid: str) -> None:
        source_payload = dict(source)
        source_payload.pop("additional_sources", None)
        source_payload["source_uid"] = source_uid
        url = _text(source_payload.get("url")) or _text(source_payload.get("resource_url"))
        conn.execute(
            """
            INSERT OR REPLACE INTO sources(source_uid, country_code, source_url, license, attribution_json)
            VALUES(?, ?, ?, ?, ?)
            """,
            (
                source_uid,
                key.upper(),
                url,
                _text(source_payload.get("license")),
                _json_dumps(source_payload),
            ),
        )

    for key, source in _source_items(source_attribution, country_code):
        source_uid = _text(source.get("source_uid")) or key
        insert_source(key, source, source_uid)
        secondary_source_uid = _text(source.get("secondary_archived_source_uid"))
        if secondary_source_uid:
            insert_source(key, source, secondary_source_uid)
        additional_sources = source.get("additional_sources")
        if isinstance(additional_sources, list):
            for additional_source in additional_sources:
                if not isinstance(additional_source, dict):
                    continue
                additional_source_uid = _text(additional_source.get("source_uid"))
                if additional_source_uid:
                    insert_source(key, additional_source, additional_source_uid)


def _available_source_uids(conn: sqlite3.Connection) -> set[str]:
    return {
        _text(row["source_uid"])
        for row in conn.execute("SELECT source_uid FROM sources")
        if _text(row["source_uid"])
    }


def _validate_csv_source_uids_are_attributed(
    conn: sqlite3.Connection,
    *,
    country_code: str,
    stations: Sequence[dict[str, str]],
    chargers: Sequence[dict[str, str]],
    dedupe_rows: Sequence[dict[str, str]],
) -> None:
    available = _available_source_uids(conn)
    missing: dict[str, set[str]] = {}
    for table_name, rows in (
        ("stations", stations),
        ("chargers", chargers),
        ("dedupe_issues", dedupe_rows),
    ):
        for row in rows:
            source_uid = _text(row.get("source_uid"))
            if source_uid and source_uid not in available:
                missing.setdefault(table_name, set()).add(source_uid)
    if missing:
        details = ";".join(
            f"{table_name}:{','.join(sorted(source_uids))}"
            for table_name, source_uids in sorted(missing.items())
        )
        raise ValueError(f"missing_source_attribution:{country_code}:{details}")


def _validate_database_source_uids_are_attributed(conn: sqlite3.Connection) -> None:
    available = _available_source_uids(conn)
    missing: dict[str, set[str]] = {}
    for table_name in ("stations", "chargers", "dedupe_issues"):
        for row in conn.execute(
            f"""
            SELECT DISTINCT source_uid
            FROM {table_name}
            WHERE source_uid IS NOT NULL
              AND source_uid != ''
            """
        ):
            source_uid = _text(row["source_uid"])
            if source_uid and source_uid not in available:
                missing.setdefault(table_name, set()).add(source_uid)
    if missing:
        details = ";".join(
            f"{table_name}:{','.join(sorted(source_uids))}"
            for table_name, source_uids in sorted(missing.items())
        )
        raise ValueError(f"missing_source_attribution:aggregate:{details}")


def _station_tuple(row: dict[str, str], station_uid: int) -> tuple[Any, ...]:
    return (
        station_uid,
        _text(row.get("country_code")),
        _text(row.get("station_id")),
        _text(row.get("source_uid")),
        _text(row.get("source_station_id")),
        _text(row.get("license")),
        _text(row.get("provider_uid")),
        _text(row.get("operator_name")),
        _text(row.get("station_name")),
        _text(row.get("address")),
        _text(row.get("postal_code")),
        _text(row.get("city")),
        _float_or_none(row.get("latitude")),
        _float_or_none(row.get("longitude")),
        _int_or_none(row.get("charger_count")),
        _float_or_none(row.get("max_power_kw")),
        _text(row.get("connector_types")),
        _text(row.get("source_url")),
        _text(row.get("public_bundle_status")),
        _text(row.get("id_rule")),
        _text(row.get("opening_hours")),
        _text(row.get("payment_methods")),
        _text(row.get("auth_methods")),
        _text(row.get("green_energy")),
        _text(row.get("helpdesk_phone")),
        _text(row.get("price_display")),
        _text(row.get("price_energy_eur_kwh_min")),
        _text(row.get("price_energy_eur_kwh_max")),
        _text(row.get("price_currency")),
        _text(row.get("price_quality")),
        _text(row.get("detail_last_updated")),
    )


def _charger_tuple(row: dict[str, str], charger_uid: int, station_uid: int) -> tuple[Any, ...]:
    return (
        charger_uid,
        _text(row.get("country_code")),
        station_uid,
        _text(row.get("station_id")),
        _text(row.get("charger_id")),
        _text(row.get("source_uid")),
        _text(row.get("provider_uid")),
        _text(row.get("source_station_id")),
        _text(row.get("source_evse_id")),
        _text(row.get("connector_id")),
        _text(row.get("connector_type")),
        _text(row.get("current_type")),
        _float_or_none(row.get("max_power_kw")),
        _text(row.get("operator_name")),
        _text(row.get("license")),
        _text(row.get("source_url")),
        _text(row.get("public_bundle_status")),
    )


def _station_amenity_tuple(row: dict[str, str], station_uid: int) -> tuple[Any, ...]:
    return (
        station_uid,
        _text(row.get("country_code")),
        _text(row.get("station_id")),
        _float_or_none(row.get("amenity_radius_m")) or 0.0,
        _int_or_none(row.get("amenities_total")) or 0,
        _text(row.get("amenity_category_counts")) or "{}",
        _text(row.get("amenity_examples")) or "[]",
        _text(row.get("nearest_amenity_kind")),
        _text(row.get("nearest_amenity_name")),
        _float_or_none(row.get("nearest_amenity_distance_m")),
        _text(row.get("osm_pbf_url")),
        _text(row.get("osm_pbf_sha256")),
        _text(row.get("osm_extracted_at")),
        _text(row.get("osm_extraction_status")) or "unknown",
    )


def _sample_values(values: Iterable[str], *, limit: int = 5) -> str:
    ordered = sorted(value for value in values if value)
    sample = ordered[:limit]
    suffix = "..." if len(ordered) > limit else ""
    return ", ".join(sample) + suffix


def _validate_station_amenity_coverage(
    *,
    country_code: str,
    stations: Sequence[dict[str, str]],
    chargers: Sequence[dict[str, str]],
    station_amenity_rows: Sequence[dict[str, str]],
) -> None:
    station_ids = {_text(row.get("station_id")) for row in stations if _text(row.get("station_id"))}
    charger_station_ids = {_text(row.get("station_id")) for row in chargers if _text(row.get("station_id"))}
    amenity_station_ids: set[str] = set()
    duplicate_amenity_ids: set[str] = set()
    for row in station_amenity_rows:
        station_id = _text(row.get("station_id"))
        if not station_id:
            continue
        if station_id in amenity_station_ids:
            duplicate_amenity_ids.add(station_id)
        amenity_station_ids.add(station_id)

    errors: list[str] = []
    missing_station_ids = station_ids - amenity_station_ids
    missing_charger_station_ids = charger_station_ids - amenity_station_ids
    unknown_station_ids = amenity_station_ids - station_ids
    if missing_station_ids:
        errors.append(f"amenity_rows_missing_for_stations:{_sample_values(missing_station_ids)}")
    if missing_charger_station_ids:
        errors.append(f"amenity_rows_missing_for_charger_stations:{_sample_values(missing_charger_station_ids)}")
    if unknown_station_ids:
        errors.append(f"amenity_rows_reference_unknown_stations:{_sample_values(unknown_station_ids)}")
    if duplicate_amenity_ids:
        errors.append(f"amenity_rows_duplicate_station_ids:{_sample_values(duplicate_amenity_ids)}")
    if errors:
        raise ValueError(f"station_amenity_coverage_failed:{country_code}:" + ";".join(errors))


_ALIAS_PATTERNS = {
    "source_evse_id": re.compile(r"(?:^|\s)source_evse_id=([^\s]+)"),
    "alias_source_station_id": re.compile(r"(?:^|\s)alias_source_station_id=([^\s]+)"),
    "charger_id": re.compile(r"(?:^|\s)charger_id=([^\s]+)"),
    "station_id": re.compile(r"(?:^|\s)station_id=([^\s]+)"),
}


def _alias_from_dedupe(row: dict[str, str]) -> tuple[str, str, str, str, str, str] | None:
    if _text(row.get("issue")) != "duplicate_nl_source_evse_location_alias":
        return None
    details = _text(row.get("details"))
    values: dict[str, str] = {}
    for key, pattern in _ALIAS_PATTERNS.items():
        match = pattern.search(details)
        if match:
            values[key] = match.group(1)
    if not {"source_evse_id", "alias_source_station_id", "charger_id", "station_id"} <= values.keys():
        return None
    return (
        _text(row.get("source_uid")),
        values["alias_source_station_id"],
        values["source_evse_id"],
        values["charger_id"],
        values["station_id"],
        "duplicate_source_evse_location_alias",
    )


def build_country_sqlite_from_csv_bundle(
    *,
    input_dir: Path,
    country_code: str,
    output_path: Path,
) -> dict[str, Any]:
    country_code = country_code.upper()
    stations = [row for row in _read_csv_rows(input_dir / "stations.csv") if _country_filter(row, country_code)]
    chargers = [row for row in _read_csv_rows(input_dir / "chargers.csv") if _country_filter(row, country_code)]
    dedupe_rows = [row for row in _read_csv_rows(input_dir / "dedupe_report.csv") if _country_filter(row, country_code)]
    station_amenity_rows = [
        row for row in _read_csv_rows(input_dir / "station_amenities.csv") if _country_filter(row, country_code)
    ]
    source_attribution = _read_json(input_dir / "source_attribution.json")
    _validate_station_amenity_coverage(
        country_code=country_code,
        stations=stations,
        chargers=chargers,
        station_amenity_rows=station_amenity_rows,
    )

    conn = _connect_for_build(output_path)
    try:
        _create_schema(conn)
        _insert_metadata(
            conn,
            "build",
            {
                "generated_at": _utc_now_iso(),
                "kind": "country_part",
                "country_code": country_code,
                "input_dir": str(input_dir.resolve()),
                "schema_version": SCHEMA_VERSION,
            },
        )
        for name in ("summary.json", "catalog_summary.json", "source_attribution.json"):
            payload = _read_json(input_dir / name)
            if payload:
                _insert_metadata(conn, name, payload)
        _insert_sources(conn, source_attribution, country_code)
        _validate_csv_source_uids_are_attributed(
            conn,
            country_code=country_code,
            stations=stations,
            chargers=chargers,
            dedupe_rows=dedupe_rows,
        )

        station_uid_by_id: dict[str, int] = {}
        rtree_rows: list[tuple[Any, ...]] = []
        for station_uid, row in enumerate(stations, start=1):
            station_id = _text(row.get("station_id"))
            if not station_id:
                raise ValueError(f"missing_station_id:{country_code}:{station_uid}")
            station_uid_by_id[station_id] = station_uid
            conn.execute(
                "INSERT INTO stations VALUES(" + ",".join("?" for _ in range(31)) + ")",
                _station_tuple(row, station_uid),
            )
            latitude = _float_or_none(row.get("latitude"))
            longitude = _float_or_none(row.get("longitude"))
            if latitude is not None and longitude is not None:
                rtree_rows.append((station_uid, longitude, longitude, latitude, latitude))
        if rtree_rows:
            conn.executemany("INSERT INTO station_rtree VALUES(?, ?, ?, ?, ?)", rtree_rows)

        for charger_uid, row in enumerate(chargers, start=1):
            station_id = _text(row.get("station_id"))
            station_uid = station_uid_by_id.get(station_id)
            if station_uid is None:
                raise ValueError(f"charger_station_id_missing:{_text(row.get('charger_id'))}:{station_id}")
            conn.execute(
                "INSERT INTO chargers VALUES(" + ",".join("?" for _ in range(17)) + ")",
                _charger_tuple(row, charger_uid, station_uid),
            )

        for row in station_amenity_rows:
            station_id = _text(row.get("station_id"))
            station_uid = station_uid_by_id.get(station_id)
            if station_uid is None:
                raise ValueError(f"amenity_station_id_missing:{station_id}")
            conn.execute(
                "INSERT INTO station_amenities VALUES(" + ",".join("?" for _ in range(14)) + ")",
                _station_amenity_tuple(row, station_uid),
            )

        for row in dedupe_rows:
            conn.execute(
                "INSERT INTO dedupe_issues(issue, country_code, station_id, source_uid, details) VALUES(?, ?, ?, ?, ?)",
                tuple(_text(row.get(column)) for column in DEDUPE_COLUMNS),
            )
            alias = _alias_from_dedupe(row)
            if alias is not None:
                conn.execute(
                    "INSERT OR REPLACE INTO charger_aliases VALUES(?, ?, ?, ?, ?, ?)",
                    alias,
                )

        _finalize(conn)
        return {
            "country_code": country_code,
            "output_path": str(output_path.resolve()),
            "station_count": len(stations),
            "charger_count": len(chargers),
            "station_amenity_count": len(station_amenity_rows),
            "dedupe_issue_count": len(dedupe_rows),
        }
    finally:
        conn.close()


def _finalize(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS stations_country_idx ON stations(country_code);
        CREATE INDEX IF NOT EXISTS stations_source_station_idx ON stations(country_code, source_uid, source_station_id);
        CREATE INDEX IF NOT EXISTS chargers_station_idx ON chargers(station_uid);
        CREATE INDEX IF NOT EXISTS chargers_source_evse_idx ON chargers(country_code, source_uid, source_evse_id);
        CREATE INDEX IF NOT EXISTS chargers_provider_evse_idx ON chargers(provider_uid, source_evse_id);
        CREATE INDEX IF NOT EXISTS station_amenities_country_idx ON station_amenities(country_code);
        CREATE INDEX IF NOT EXISTS station_amenities_status_idx ON station_amenities(osm_extraction_status);
        CREATE INDEX IF NOT EXISTS dedupe_country_idx ON dedupe_issues(country_code);
        ANALYZE;
        PRAGMA integrity_check;
        VACUUM;
        """
    )


def _part_paths(parts_dir: Path | None, part_paths: Sequence[Path]) -> list[Path]:
    paths = [path for path in part_paths]
    if parts_dir is not None:
        paths.extend(sorted(parts_dir.glob("*.sqlite3")))
    resolved = []
    seen: set[Path] = set()
    for path in paths:
        normalized = path.resolve()
        if normalized not in seen:
            resolved.append(normalized)
            seen.add(normalized)
    return resolved


def _country_counts_from_csv(path: Path, *, countries: Sequence[str]) -> dict[str, int]:
    selected = set(countries)
    counts: dict[str, int] = {country: 0 for country in countries}
    for row in _read_csv_rows(path):
        country = _text(row.get("country_code")).upper()
        if not country:
            country = _text(row.get("station_id")).split(":", 1)[0].upper()
        if selected and country not in selected:
            continue
        counts[country] = counts.get(country, 0) + 1
    return dict(sorted(counts.items()))


def _country_counts_from_sqlite(conn: sqlite3.Connection, table: str, *, countries: Sequence[str]) -> dict[str, int]:
    selected = set(countries)
    counts: dict[str, int] = {country: 0 for country in countries}
    for row in conn.execute(f"SELECT country_code, count(*) AS row_count FROM {table} GROUP BY country_code"):
        country = _text(row["country_code"]).upper()
        if selected and country not in selected:
            continue
        counts[country] = int(row["row_count"])
    return dict(sorted(counts.items()))


def _total(counts: dict[str, int]) -> int:
    return sum(counts.values())


def check_sqlite_counts_against_source(
    *,
    db_path: Path,
    expected_dir: Path,
    countries: Sequence[str],
    allow_empty_expected_countries: Sequence[str] = (),
) -> dict[str, Any]:
    if not db_path.exists():
        raise FileNotFoundError(f"sqlite_bundle_missing:{db_path}")
    countries = [country.strip().upper() for country in countries if country.strip()]
    if not countries:
        countries = sorted(
            {
                _text(row.get("country_code")).upper()
                for row in _read_csv_rows(expected_dir / "stations.csv")
                if _text(row.get("country_code"))
            }
        )

    expected = {
        "stations": _country_counts_from_csv(expected_dir / "stations.csv", countries=countries),
        "chargers": _country_counts_from_csv(expected_dir / "chargers.csv", countries=countries),
        "station_amenities": _country_counts_from_csv(expected_dir / "station_amenities.csv", countries=countries),
    }
    allow_empty_expected = {
        country.strip().upper()
        for country in allow_empty_expected_countries
        if country.strip()
    }
    errors: list[str] = []
    for table, counts in expected.items():
        for country in countries:
            if counts.get(country, 0) <= 0 and country not in allow_empty_expected:
                errors.append(f"expected_{table}_missing_for_country:{country}")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        actual = {
            "stations": _country_counts_from_sqlite(conn, "stations", countries=countries),
            "chargers": _country_counts_from_sqlite(conn, "chargers", countries=countries),
            "station_amenities": _country_counts_from_sqlite(conn, "station_amenities", countries=countries),
        }
        selected = set(countries)
        country_filter = ""
        params: tuple[Any, ...] = ()
        if selected:
            placeholders = ",".join("?" for _ in countries)
            country_filter = f" AND s.country_code IN ({placeholders})"
            params = tuple(countries)
        missing_station_amenities = conn.execute(
            f"""
            SELECT count(*)
            FROM stations s
            LEFT JOIN station_amenities a ON a.station_uid = s.station_uid
            WHERE a.station_uid IS NULL{country_filter}
            """,
            params,
        ).fetchone()[0]
        country_filter = ""
        params = ()
        if selected:
            placeholders = ",".join("?" for _ in countries)
            country_filter = f" AND c.country_code IN ({placeholders})"
            params = tuple(countries)
        charger_station_amenity_gaps = conn.execute(
            f"""
            SELECT count(*)
            FROM chargers c
            LEFT JOIN station_amenities a ON a.station_uid = c.station_uid
            WHERE a.station_uid IS NULL{country_filter}
            """,
            params,
        ).fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            errors.append(f"sqlite_integrity_check_failed:{integrity}")

    for table in ("stations", "chargers", "station_amenities"):
        if actual[table] != expected[table]:
            errors.append(f"{table}_country_counts_mismatch")
        if _total(actual[table]) != _total(expected[table]):
            errors.append(f"{table}_total_mismatch")
    if missing_station_amenities:
        errors.append(f"stations_missing_station_amenities:{missing_station_amenities}")
    if charger_station_amenity_gaps:
        errors.append(f"chargers_missing_station_amenities:{charger_station_amenity_gaps}")

    result = {
        "ok": not errors,
        "db_path": str(db_path.resolve()),
        "expected_dir": str(expected_dir.resolve()),
        "countries": countries,
        "allow_empty_expected_countries": sorted(allow_empty_expected),
        "expected": {
            "station_count": _total(expected["stations"]),
            "charger_count": _total(expected["chargers"]),
            "station_amenity_count": _total(expected["station_amenities"]),
            "stations_by_country": expected["stations"],
            "chargers_by_country": expected["chargers"],
            "station_amenities_by_country": expected["station_amenities"],
        },
        "actual": {
            "station_count": _total(actual["stations"]),
            "charger_count": _total(actual["chargers"]),
            "station_amenity_count": _total(actual["station_amenities"]),
            "stations_by_country": actual["stations"],
            "chargers_by_country": actual["chargers"],
            "station_amenities_by_country": actual["station_amenities"],
        },
        "errors": errors,
    }
    if errors:
        raise ValueError(_json_dumps(result))
    return result


def aggregate_sqlite_parts(*, part_paths: Sequence[Path], output_path: Path) -> dict[str, Any]:
    paths = [path for path in part_paths if path.exists()]
    if not paths:
        raise FileNotFoundError("no_country_part_sqlite_files")

    conn = _connect_for_build(output_path)
    station_uid_by_id: dict[str, int] = {}
    station_count = 0
    charger_count = 0
    station_amenity_count = 0
    dedupe_count = 0
    alias_count = 0
    try:
        _create_schema(conn)
        _insert_metadata(
            conn,
            "build",
            {
                "generated_at": _utc_now_iso(),
                "kind": "aggregate",
                "part_paths": [str(path) for path in paths],
                "schema_version": SCHEMA_VERSION,
            },
        )
        for path in paths:
            with sqlite3.connect(path) as part:
                part.row_factory = sqlite3.Row
                for row in part.execute("SELECT * FROM bundle_metadata ORDER BY key"):
                    _insert_metadata(conn, f"part:{path.stem}:{row['key']}", json.loads(row["json_value"]))
                for row in part.execute("SELECT * FROM sources ORDER BY source_uid"):
                    conn.execute(
                        "INSERT OR REPLACE INTO sources VALUES(?, ?, ?, ?, ?)",
                        tuple(row[column] for column in ("source_uid", "country_code", "source_url", "license", "attribution_json")),
                    )
                old_to_new_station_uid: dict[int, int] = {}
                for row in part.execute("SELECT * FROM stations ORDER BY station_uid"):
                    station_id = _text(row["station_id"])
                    if station_id in station_uid_by_id:
                        raise ValueError(f"duplicate_station_id_across_parts:{station_id}")
                    station_count += 1
                    old_to_new_station_uid[int(row["station_uid"])] = station_count
                    station_uid_by_id[station_id] = station_count
                    values = [station_count] + [row[column] for column in STATION_COLUMNS]
                    conn.execute(
                        "INSERT INTO stations VALUES(" + ",".join("?" for _ in range(31)) + ")",
                        values,
                    )
                for row in part.execute("SELECT * FROM station_rtree ORDER BY station_uid"):
                    new_station_uid = old_to_new_station_uid.get(int(row["station_uid"]))
                    if new_station_uid is not None:
                        conn.execute(
                            "INSERT INTO station_rtree VALUES(?, ?, ?, ?, ?)",
                            (new_station_uid, row["min_lon"], row["max_lon"], row["min_lat"], row["max_lat"]),
                        )
                has_station_amenities = part.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'station_amenities'"
                ).fetchone()
                if has_station_amenities:
                    for row in part.execute("SELECT * FROM station_amenities ORDER BY station_uid, station_id"):
                        new_station_uid = old_to_new_station_uid.get(int(row["station_uid"]))
                        if new_station_uid is None:
                            raise ValueError(f"amenity_station_uid_missing_in_aggregate:{row['station_id']}")
                        station_amenity_count += 1
                        conn.execute(
                            "INSERT INTO station_amenities VALUES(" + ",".join("?" for _ in range(14)) + ")",
                            (
                                new_station_uid,
                                row["country_code"],
                                row["station_id"],
                                row["amenity_radius_m"],
                                row["amenities_total"],
                                row["amenity_category_counts_json"],
                                row["amenity_examples_json"],
                                row["nearest_amenity_kind"],
                                row["nearest_amenity_name"],
                                row["nearest_amenity_distance_m"],
                                row["osm_pbf_url"],
                                row["osm_pbf_sha256"],
                                row["osm_extracted_at"],
                                row["osm_extraction_status"],
                            ),
                        )
                for row in part.execute("SELECT * FROM chargers ORDER BY charger_uid"):
                    station_id = _text(row["station_id"])
                    station_uid = station_uid_by_id.get(station_id)
                    if station_uid is None:
                        raise ValueError(f"charger_station_id_missing_in_aggregate:{row['charger_id']}:{station_id}")
                    charger_count += 1
                    values = [
                        charger_count,
                        row["country_code"],
                        station_uid,
                        row["station_id"],
                        row["charger_id"],
                        row["source_uid"],
                        row["provider_uid"],
                        row["source_station_id"],
                        row["source_evse_id"],
                        row["connector_id"],
                        row["connector_type"],
                        row["current_type"],
                        row["max_power_kw"],
                        row["operator_name"],
                        row["license"],
                        row["source_url"],
                        row["public_bundle_status"],
                    ]
                    conn.execute(
                        "INSERT INTO chargers VALUES(" + ",".join("?" for _ in range(17)) + ")",
                        values,
                    )
                for row in part.execute("SELECT * FROM charger_aliases ORDER BY source_uid, source_station_id, source_evse_id"):
                    alias_count += 1
                    conn.execute(
                        "INSERT OR REPLACE INTO charger_aliases VALUES(?, ?, ?, ?, ?, ?)",
                        tuple(row[column] for column in ("source_uid", "source_station_id", "source_evse_id", "charger_id", "station_id", "alias_status")),
                    )
                for row in part.execute("SELECT * FROM dedupe_issues ORDER BY issue_uid"):
                    dedupe_count += 1
                    conn.execute(
                        "INSERT INTO dedupe_issues(issue, country_code, station_id, source_uid, details) VALUES(?, ?, ?, ?, ?)",
                        tuple(row[column] for column in ("issue", "country_code", "station_id", "source_uid", "details")),
                    )

        _validate_database_source_uids_are_attributed(conn)
        _finalize(conn)
        return {
            "output_path": str(output_path.resolve()),
            "part_count": len(paths),
            "station_count": station_count,
            "charger_count": charger_count,
            "station_amenity_count": station_amenity_count,
            "dedupe_issue_count": dedupe_count,
            "charger_alias_count": alias_count,
        }
    finally:
        conn.close()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Woladen open-static SQLite bundle parts and aggregates.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    country_parser = subparsers.add_parser("country", help="Build one country SQLite bundle part from normalized CSV rows.")
    country_parser.add_argument("--input-dir", type=Path, required=True)
    country_parser.add_argument("--country", required=True)
    country_parser.add_argument("--output-path", type=Path, required=True)

    aggregate_parser = subparsers.add_parser("aggregate", help="Merge country SQLite bundle parts into a full bundle.")
    aggregate_parser.add_argument("--part", type=Path, action="append", default=[])
    aggregate_parser.add_argument("--parts-dir", type=Path)
    aggregate_parser.add_argument("--output-path", type=Path, required=True)

    check_parser = subparsers.add_parser("check-counts", help="Validate SQLite row counts against normalized CSV source rows.")
    check_parser.add_argument("--db-path", type=Path, required=True)
    check_parser.add_argument("--expected-dir", type=Path, required=True)
    check_parser.add_argument("--countries", default="")
    check_parser.add_argument(
        "--allow-empty-expected-country",
        action="append",
        default=[],
        help="Selected country code allowed to have zero expected CSV rows. May be passed multiple times.",
    )

    args = parser.parse_args(argv)
    if args.command == "country":
        result = build_country_sqlite_from_csv_bundle(
            input_dir=args.input_dir,
            country_code=args.country,
            output_path=args.output_path,
        )
    elif args.command == "aggregate":
        result = aggregate_sqlite_parts(
            part_paths=_part_paths(args.parts_dir, args.part),
            output_path=args.output_path,
        )
    elif args.command == "check-counts":
        result = check_sqlite_counts_against_source(
            db_path=args.db_path,
            expected_dir=args.expected_dir,
            countries=_country_codes_from_text(args.countries),
            allow_empty_expected_countries=_country_codes_from_text(",".join(args.allow_empty_expected_country)),
        )
    else:
        raise AssertionError(args.command)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
