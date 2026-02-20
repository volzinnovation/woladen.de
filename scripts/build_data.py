#!/usr/bin/env python3
"""Build dataset for woladen.de.

Pipeline steps:
1. Fetch latest BNetzA charging registry CSV (with local cache fallback).
2. Filter to active fast chargers (>= min power).
3. Enrich chargers with nearby OSM amenities via local Germany PBF or Overpass.
4. Write derived CSV + GeoJSON + summary artifacts and update README status block.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import importlib.util
import json
import math
import re
import sys
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

BNETZA_START_URL = (
    "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/"
    "E-Mobilitaet/start.html"
)
BNETZA_FILE_FALLBACKS = [
    "https://data.bundesnetzagentur.de/Bundesnetzagentur/DE/Fachthemen/"
    "ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenregister_BNetzA.csv",
    "https://data.bundesnetzagentur.de/Bundesnetzagentur/SharedDocs/Downloads/DE/"
    "Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/"
    "Ladesaeulenregister_BNetzA.xlsx",
    "https://data.bundesnetzagentur.de/Bundesnetzagentur/DE/Fachthemen/"
    "ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenregister_BNetzA_2026-01-28.csv",
]
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OSM_GERMANY_PBF_URL = "https://download.geofabrik.de/europe/germany-latest.osm.pbf"

DATA_DIR = Path("data")
README_PATH = Path("README.md")

RAW_CACHE_PATH = DATA_DIR / "bnetza_cache.csv"
RAW_META_PATH = DATA_DIR / "bnetza_source.json"
AMENITY_CACHE_PATH = DATA_DIR / "osm_amenity_cache.json"
FAST_CSV_PATH = DATA_DIR / "chargers_fast.csv"
FAST_GEOJSON_PATH = DATA_DIR / "chargers_fast.geojson"
SUMMARY_JSON_PATH = DATA_DIR / "summary.json"
OPERATORS_JSON_PATH = DATA_DIR / "operators.json"
RUN_HISTORY_PATH = DATA_DIR / "run_history.csv"

README_START = "<!-- DATA_STATUS_START -->"
README_END = "<!-- DATA_STATUS_END -->"

AMENITY_SCHEMA_VERSION = 1
CSV_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
BNETZA_POWER_COLUMN_INDEX = 6  # 7th column in source file
EARTH_RADIUS_M = 6_371_000.0

AMENITY_BACKEND_OVERPASS = "overpass"
AMENITY_BACKEND_OSM_PBF = "osm-pbf"
AMENITY_BACKEND_AUTO = "auto"
AMENITY_BACKEND_CHOICES = (
    AMENITY_BACKEND_AUTO,
    AMENITY_BACKEND_OVERPASS,
    AMENITY_BACKEND_OSM_PBF,
)


@dataclass(frozen=True)
class AmenityRule:
    key: str
    selectors: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class DownloadCandidate:
    url: str
    filetype: str
    date_token: str


@dataclass(frozen=True)
class AmenityPoint:
    lat: float
    lon: float
    categories: tuple[str, ...]
    name: str
    opening_hours: str


AMENITY_RULES: tuple[AmenityRule, ...] = (
    AmenityRule("restaurant", (("amenity", "restaurant"),)),
    AmenityRule("cafe", (("amenity", "cafe"),)),
    AmenityRule("fast_food", (("amenity", "fast_food"),)),
    AmenityRule("toilets", (("amenity", "toilets"),)),
    AmenityRule("supermarket", (("shop", "supermarket"),)),
    AmenityRule("bakery", (("shop", "bakery"),)),
    AmenityRule("convenience", (("shop", "convenience"),)),
    AmenityRule("pharmacy", (("amenity", "pharmacy"), ("shop", "chemist"))),
    AmenityRule("hotel", (("tourism", "hotel"),)),
    AmenityRule("museum", (("tourism", "museum"),)),
    AmenityRule("playground", (("leisure", "playground"),)),
    AmenityRule("park", (("leisure", "park"),)),
    AmenityRule("ice_cream", (("amenity", "ice_cream"),)),
)
AMENITY_EXAMPLES_PER_STATION = 12


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build woladen.de data artifacts")
    parser.add_argument("--min-power-kw", type=float, default=50.0)
    parser.add_argument("--radius-m", type=int, default=100)
    parser.add_argument(
        "--amenity-backend",
        type=str,
        default=AMENITY_BACKEND_AUTO,
        choices=AMENITY_BACKEND_CHOICES,
        help=(
            "Amenity lookup backend: auto=prefer local osm-pbf if file exists, "
            "otherwise use overpass"
        ),
    )
    parser.add_argument(
        "--query-budget",
        type=int,
        default=500,
        help="Maximum new Overpass lookups per run (overpass backend only)",
    )
    parser.add_argument(
        "--refresh-days",
        type=int,
        default=30,
        help="Refresh cached amenity lookups older than this many days (overpass only)",
    )
    parser.add_argument(
        "--max-stations",
        type=int,
        default=0,
        help="Optional cap for processed chargers (0 = no cap)",
    )
    parser.add_argument("--overpass-delay-ms", type=int, default=250)
    parser.add_argument(
        "--osm-pbf-path",
        type=str,
        default=str(DATA_DIR / "germany-latest.osm.pbf"),
        help="Path to local Germany OSM PBF file (used by osm-pbf backend)",
    )
    parser.add_argument(
        "--osm-pbf-url",
        type=str,
        default=OSM_GERMANY_PBF_URL,
        help="Download URL for Germany OSM PBF when --download-osm-pbf is enabled",
    )
    parser.add_argument(
        "--download-osm-pbf",
        action="store_true",
        help="Download osm-pbf file if missing and osm-pbf backend is selected",
    )
    parser.add_argument(
        "--pbf-progress-every",
        type=int,
        default=1_000_000,
        help="Progress interval while scanning local OSM PBF objects",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=250,
        help="Progress update frequency while processing stations",
    )
    parser.add_argument(
        "--operator-min-stations",
        type=int,
        default=100,
        help="Minimum station count per operator in the precomputed UI list",
    )
    parser.add_argument("--force-refresh", action="store_true")
    return parser.parse_args()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def log_info(message: str) -> None:
    timestamp = utc_now().replace(microsecond=0).isoformat()
    print(f"[{timestamp}] {message}", flush=True)


def format_duration(total_seconds: float) -> str:
    total_seconds = int(max(0, total_seconds))
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{minutes:02d}:{seconds:02d}"


def normalize_text(value: str) -> str:
    value = (
        value.replace("Ä", "Ae")
        .replace("Ö", "Oe")
        .replace("Ü", "Ue")
        .replace("ä", "ae")
        .replace("ö", "oe")
        .replace("ü", "ue")
        .replace("ß", "ss")
    )
    folded = unicodedata.normalize("NFKD", value)
    ascii_only = "".join(ch for ch in folded if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]", "", ascii_only.lower())


def find_column(df: pd.DataFrame, candidates: list[str], contains: bool = False) -> str | None:
    normalized = {col: normalize_text(col) for col in df.columns}
    wanted = [normalize_text(item) for item in candidates]

    for col, norm in normalized.items():
        if norm in wanted:
            return col

    if contains:
        for col, norm in normalized.items():
            if any(token in norm for token in wanted):
                return col

    return None


def to_float(series: pd.Series) -> pd.Series:
    clean = (
        series.astype(str)
        .str.replace(",", ".", regex=False)
        .str.replace(r"[^0-9+\-.]", "", regex=True)
        .replace("", pd.NA)
    )
    return pd.to_numeric(clean, errors="coerce")


def detect_csv_encoding(csv_path: Path) -> str:
    sample = csv_path.read_bytes()[:256_000]
    for encoding in CSV_ENCODINGS:
        try:
            sample.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    return "latin-1"


def detect_header_row(csv_path: Path, encoding: str) -> int:
    with csv_path.open("r", encoding=encoding, errors="replace") as handle:
        for idx, line in enumerate(handle):
            if idx > 80:
                break
            norm = normalize_text(line)
            if ";" in line and ("breitengrad" in norm or "laengengrad" in norm):
                return idx
    return 0


def request_with_retries(
    method: str,
    url: str,
    session: requests.Session,
    *,
    timeout: int,
    max_attempts: int = 4,
    **kwargs: Any,
) -> requests.Response:
    delay = 1.2
    for attempt in range(1, max_attempts + 1):
        try:
            response = session.request(method, url, timeout=timeout, **kwargs)
            if response.status_code < 400:
                return response
            if response.status_code in {429, 500, 502, 503, 504} and attempt < max_attempts:
                time.sleep(delay)
                delay *= 1.8
                continue
            response.raise_for_status()
        except requests.RequestException:
            if attempt >= max_attempts:
                raise
            time.sleep(delay)
            delay *= 1.8
    raise RuntimeError("retry loop terminated unexpectedly")


def extract_download_candidates(html_text: str) -> list[DownloadCandidate]:
    scopes: list[str] = []
    for match in re.finditer(r"downloads?\s+und\s+formulare", html_text, flags=re.IGNORECASE):
        start = max(0, match.start() - 2000)
        end = min(len(html_text), match.end() + 20000)
        scopes.append(html_text[start:end])

    if not scopes:
        scopes = [html_text]

    href_pattern = re.compile(r"""href\s*=\s*(["'])(.*?)\1""", re.IGNORECASE | re.DOTALL)
    seen: set[str] = set()
    candidates: list[DownloadCandidate] = []

    for scope in scopes:
        for _, raw_href in href_pattern.findall(scope):
            decoded_href = html.unescape(raw_href.strip())
            absolute_url = urljoin(BNETZA_START_URL, decoded_href)
            parsed = urlparse(absolute_url)
            path_lower = parsed.path.lower()

            if "ladesaeulenregister" not in path_lower:
                continue

            if path_lower.endswith(".csv"):
                filetype = "csv"
            elif path_lower.endswith(".xlsx"):
                filetype = "xlsx"
            else:
                continue

            if absolute_url in seen:
                continue
            seen.add(absolute_url)

            date_match = re.search(r"(20\d{2}-\d{2}-\d{2})", absolute_url)
            date_token = date_match.group(1) if date_match else ""
            candidates.append(
                DownloadCandidate(
                    url=absolute_url,
                    filetype=filetype,
                    date_token=date_token,
                )
            )

    candidates.sort(
        key=lambda c: (c.date_token, c.filetype == "csv", c.url),
        reverse=True,
    )
    return candidates


def discover_latest_bnetza_downloads(session: requests.Session) -> list[str]:
    try:
        response = request_with_retries("GET", BNETZA_START_URL, session, timeout=25)
    except requests.RequestException:
        return []

    candidates = extract_download_candidates(response.text)
    return [candidate.url for candidate in candidates]


def fetch_bnetza_csv(session: requests.Session, cache_path: Path, meta_path: Path) -> dict[str, Any]:
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    candidate_urls: list[str] = []
    candidate_urls.extend(discover_latest_bnetza_downloads(session))
    candidate_urls.extend(BNETZA_FILE_FALLBACKS)
    # Keep discovery order but avoid duplicate attempts.
    candidate_urls = list(dict.fromkeys(candidate_urls))
    log_info(f"Source candidates: {len(candidate_urls)} URL(s)")

    errors: list[str] = []

    for url in candidate_urls:
        try:
            log_info(f"Fetching source candidate: {url}")
            response = request_with_retries("GET", url, session, timeout=60)
            content = response.content
            cache_path.write_bytes(content)
            metadata = {
                "source_url": url,
                "fetched_at": utc_now().isoformat(),
                "bytes": len(content),
                "content_type": response.headers.get("content-type", ""),
            }
            meta_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
            log_info(f"Fetched source ({len(content)} bytes)")
            return metadata
        except requests.RequestException as exc:
            errors.append(f"{url}: {exc}")
            log_info(f"Source fetch failed: {url}")

    if cache_path.exists():
        fallback_meta: dict[str, Any] = {
            "source_url": "cache_fallback",
            "fetched_at": utc_now().isoformat(),
            "cache_only": True,
            "errors": errors,
        }
        if meta_path.exists():
            try:
                previous_meta = json.loads(meta_path.read_text(encoding="utf-8"))
                fallback_meta["previous_source_url"] = previous_meta.get("source_url")
            except json.JSONDecodeError:
                pass
        return fallback_meta

    raise RuntimeError(
        "Could not fetch BNetzA source and no local cache exists. "
        f"Attempts: {' | '.join(errors)}"
    )


def detect_header_row_excel(xlsx_path: Path) -> int:
    preview = pd.read_excel(
        xlsx_path,
        sheet_name=0,
        header=None,
        dtype=str,
    )
    max_rows = min(80, len(preview))
    for idx in range(max_rows):
        row_text = ";".join(str(value) for value in preview.iloc[idx].tolist() if pd.notna(value))
        norm = normalize_text(row_text)
        if "breitengrad" in norm and "laengengrad" in norm:
            return idx
    return 0


def load_raw_dataframe(path: Path) -> pd.DataFrame:
    sample = path.read_bytes()[:8_192]
    sample_lower = sample.lower()

    if sample.startswith(b"PK\x03\x04"):
        header_row = detect_header_row_excel(path)
        df = pd.read_excel(
            path,
            sheet_name=0,
            header=header_row,
            dtype=str,
        )
        df = df.dropna(axis=1, how="all")
        return df

    if b"<html" in sample_lower or b"<!doctype html" in sample_lower:
        raise RuntimeError(
            "BNetzA source appears to be HTML instead of CSV/XLSX. "
            "The download URL likely changed or returned an error page."
        )

    encoding = detect_csv_encoding(path)
    header_row = detect_header_row(path, encoding=encoding)
    df = pd.read_csv(
        path,
        sep=";",
        skiprows=header_row,
        dtype=str,
        encoding=encoding,
        engine="python",
        on_bad_lines="skip",
    )

    # Drop entirely empty columns that can show up due to trailing separators.
    df = df.dropna(axis=1, how="all")
    return df


def build_fast_charger_frame(raw_df: pd.DataFrame, min_power_kw: float) -> pd.DataFrame:
    df = raw_df.copy()

    lat_col = find_column(df, ["breitengrad", "latitude", "lat"], contains=True)
    lon_col = find_column(df, ["laengengrad", "long", "longitude", "lon"], contains=True)
    status_col = find_column(df, ["status", "betriebsstatus"], contains=True)
    operator_col = find_column(
        df,
        ["betreiber", "betreibername", "operator", "anbieter"],
        contains=True,
    )

    if not lat_col or not lon_col:
        raise RuntimeError("Could not identify latitude/longitude columns in BNetzA CSV")

    if len(df.columns) <= BNETZA_POWER_COLUMN_INDEX:
        raise RuntimeError(
            "BNetzA CSV does not have a 7th column for power extraction "
            f"(found {len(df.columns)} columns)"
        )
    power_col = df.columns[BNETZA_POWER_COLUMN_INDEX]
    # to_float() explicitly normalizes comma decimals (e.g. \"150,0\" -> 150.0).
    df["max_power_kw"] = to_float(df[power_col])

    charging_points_col = find_column(
        df,
        ["anzahl ladepunkte", "anzahlladepunkte", "ladepunkte"],
        contains=True,
    )
    if charging_points_col:
        df["charging_points_count_row"] = to_float(df[charging_points_col]).fillna(1.0)
    else:
        df["charging_points_count_row"] = 1.0
    df["charging_points_count_row"] = df["charging_points_count_row"].clip(lower=1.0)

    connector_power_cols = [
        col for col in df.columns if normalize_text(col).startswith("nennleistungstecker")
    ]
    if connector_power_cols:
        connector_power_frame = pd.DataFrame(
            {col: to_float(df[col]) for col in connector_power_cols},
            index=df.index,
        )
        connector_max = connector_power_frame.max(axis=1, skipna=True)
    else:
        connector_max = pd.Series([pd.NA] * len(df), index=df.index, dtype="float64")

    per_point_fallback = df["max_power_kw"] / df["charging_points_count_row"].replace(0, pd.NA)
    df["max_individual_power_kw_row"] = connector_max.fillna(per_point_fallback)

    df["lat"] = to_float(df[lat_col])
    df["lon"] = to_float(df[lon_col])

    # If columns appear swapped, correct them.
    if df["lat"].abs().max(skipna=True) > 90 and df["lon"].abs().max(skipna=True) <= 90:
        df[["lat", "lon"]] = df[["lon", "lat"]]

    if status_col:
        status_norm = df[status_col].fillna("").str.lower().str.strip()
        active_mask = status_norm.isin({"in betrieb", "inbetrieb", "in betriebnahme"})
        active_mask |= status_norm.str.contains("in betrieb", regex=False)
    else:
        active_mask = pd.Series([True] * len(df), index=df.index)

    df = df[active_mask & (df["max_power_kw"] >= min_power_kw)]

    # Geographic sanity filter for Germany (+small buffer).
    df = df[(df["lat"].between(46.0, 56.5)) & (df["lon"].between(5.0, 16.5))]

    address_parts: list[str] = []
    for hint in ["strasse", "straße", "hausnummer", "postleitzahl", "ort", "stadt"]:
        col = find_column(df, [hint], contains=True)
        if col and col not in address_parts:
            address_parts.append(col)

    if operator_col:
        df["operator"] = df[operator_col].fillna("").str.strip()
    else:
        df["operator"] = "Unbekannt"

    if address_parts:
        df["address"] = (
            df[address_parts]
            .fillna("")
            .astype(str)
            .agg(" ".join, axis=1)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
    else:
        df["address"] = ""

    city_col = find_column(df, ["ort", "stadt"], contains=True)
    zip_col = find_column(df, ["postleitzahl", "plz"], contains=True)
    status_out_col = status_col if status_col else None

    if city_col:
        df["city"] = df[city_col].fillna("").astype(str).str.strip()
    else:
        df["city"] = ""

    if zip_col:
        df["postcode"] = df[zip_col].fillna("").astype(str).str.strip()
    else:
        df["postcode"] = ""

    if status_out_col:
        df["status"] = df[status_out_col].fillna("").astype(str).str.strip()
    else:
        df["status"] = ""

    def first_nonempty(series: pd.Series) -> str:
        for value in series:
            text = str(value).strip()
            if text:
                return text
        return ""

    df = (
        df.groupby(["lat", "lon", "operator"], as_index=False)
        .agg(
            status=("status", first_nonempty),
            max_power_kw=("max_power_kw", "max"),
            charging_points_count=("charging_points_count_row", "sum"),
            max_individual_power_kw=("max_individual_power_kw_row", "max"),
            postcode=("postcode", first_nonempty),
            city=("city", first_nonempty),
            address=("address", first_nonempty),
        )
        .reset_index(drop=True)
    )

    df["charging_points_count"] = (
        df["charging_points_count"]
        .fillna(1.0)
        .round()
        .astype(int)
        .clip(lower=1)
    )
    df["max_individual_power_kw"] = (
        df["max_individual_power_kw"]
        .fillna(df["max_power_kw"])
        .fillna(0.0)
    )

    def station_id(row: pd.Series) -> str:
        raw = f"{row['lat']:.5f}|{row['lon']:.5f}|{row['operator']}|{row['address']}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    df["station_id"] = df.apply(station_id, axis=1)

    selected_columns = [
        "station_id",
        "operator",
        "status",
        "max_power_kw",
        "charging_points_count",
        "max_individual_power_kw",
        "lat",
        "lon",
        "postcode",
        "city",
        "address",
    ]

    return df[selected_columns].copy()


def load_amenity_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "meta": {
                "schema_version": AMENITY_SCHEMA_VERSION,
                "radius_m": None,
            },
            "entries": {},
        }

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("cache root is not an object")
        data.setdefault("meta", {})
        data.setdefault("entries", {})
        if data["meta"].get("schema_version") != AMENITY_SCHEMA_VERSION:
            return {
                "meta": {
                    "schema_version": AMENITY_SCHEMA_VERSION,
                    "radius_m": None,
                },
                "entries": {},
            }
        return data
    except (json.JSONDecodeError, ValueError):
        return {
            "meta": {
                "schema_version": AMENITY_SCHEMA_VERSION,
                "radius_m": None,
            },
            "entries": {},
        }


def build_overpass_query(lat: float, lon: float, radius_m: int) -> str:
    lines = ["[out:json][timeout:30];", "("]
    for rule in AMENITY_RULES:
        for key, value in rule.selectors:
            lines.append(f'  node["{key}"="{value}"](around:{radius_m},{lat:.6f},{lon:.6f});')
            lines.append(f'  way["{key}"="{value}"](around:{radius_m},{lat:.6f},{lon:.6f});')
            lines.append(f'  relation["{key}"="{value}"](around:{radius_m},{lat:.6f},{lon:.6f});')
    lines.extend([
        ");",
        "out tags center;",
    ])
    return "\n".join(lines)


def classify_tags(tags: dict[str, Any]) -> list[str]:
    matched: list[str] = []
    for rule in AMENITY_RULES:
        for key, value in rule.selectors:
            if tags.get(key) == value:
                matched.append(rule.key)
                break
    return matched


def normalize_optional_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    # Collapse whitespace to keep popup payload compact and readable.
    return re.sub(r"\s+", " ", text)


def build_amenity_example(
    *,
    category: str,
    name: str,
    opening_hours: str,
    distance_m: float | None,
    amenity_lat: float | None = None,
    amenity_lon: float | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "category": category,
    }
    if name:
        payload["name"] = name
    if opening_hours:
        payload["opening_hours"] = opening_hours
    if distance_m is not None:
        payload["distance_m"] = int(round(max(0.0, distance_m)))
    if amenity_lat is not None and amenity_lon is not None:
        payload["lat"] = round(float(amenity_lat), 6)
        payload["lon"] = round(float(amenity_lon), 6)
    return payload


def limit_amenity_examples(examples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def sort_key(item: dict[str, Any]) -> tuple[int, str, str]:
        distance = item.get("distance_m")
        distance_int = int(distance) if isinstance(distance, (int, float)) else 10_000_000
        category = str(item.get("category", ""))
        name = str(item.get("name", ""))
        return (distance_int, category, name.lower())

    ranked = sorted(examples, key=sort_key)
    if len(ranked) <= AMENITY_EXAMPLES_PER_STATION:
        return ranked
    return ranked[:AMENITY_EXAMPLES_PER_STATION]


def encode_amenity_examples(examples: list[dict[str, Any]]) -> str:
    return json.dumps(examples, ensure_ascii=False, separators=(",", ":"))


def decode_amenity_examples(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, list):
        return [item for item in raw if isinstance(item, dict)]
    if not isinstance(raw, str) or not raw:
        return []
    try:
        payload = json.loads(raw)
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
    except json.JSONDecodeError:
        return []
    return []


def resolve_amenity_backend(requested: str, osm_pbf_path: Path) -> str:
    if requested == AMENITY_BACKEND_AUTO:
        if osm_pbf_path.exists():
            if importlib.util.find_spec("osmium") is None:
                log_info(
                    "Local OSM PBF file found but python package 'osmium' is not installed; "
                    "falling back to overpass backend (auto mode)."
                )
                return AMENITY_BACKEND_OVERPASS
            return AMENITY_BACKEND_OSM_PBF
        return AMENITY_BACKEND_OVERPASS
    return requested


def ensure_osm_pbf_file(
    *,
    session: requests.Session,
    osm_pbf_path: Path,
    osm_pbf_url: str,
    download_if_missing: bool,
) -> dict[str, Any]:
    if osm_pbf_path.exists() and osm_pbf_path.stat().st_size > 0:
        return {
            "downloaded": False,
            "path": str(osm_pbf_path),
            "bytes": int(osm_pbf_path.stat().st_size),
        }

    if not download_if_missing:
        raise RuntimeError(
            "Local OSM PBF backend selected but file is missing. "
            f"Expected: {osm_pbf_path}. "
            "Provide --download-osm-pbf or set --amenity-backend overpass."
        )

    osm_pbf_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = osm_pbf_path.with_suffix(osm_pbf_path.suffix + ".part")
    if temp_path.exists():
        temp_path.unlink()

    log_info(f"Downloading OSM PBF: {osm_pbf_url}")
    response = request_with_retries(
        "GET",
        osm_pbf_url,
        session,
        timeout=120,
        stream=True,
    )
    total_bytes = int(response.headers.get("content-length", "0") or 0)
    downloaded = 0
    started = time.monotonic()
    last_log = started

    with temp_path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=4 * 1024 * 1024):
            if not chunk:
                continue
            handle.write(chunk)
            downloaded += len(chunk)
            now_mono = time.monotonic()
            if now_mono - last_log >= 5.0:
                elapsed = max(0.001, now_mono - started)
                rate = downloaded / elapsed
                if total_bytes > 0:
                    pct = downloaded / total_bytes * 100.0
                    remaining = max(0, total_bytes - downloaded)
                    eta = remaining / rate if rate > 0 else 0
                    log_info(
                        "OSM PBF download "
                        f"{pct:5.1f}% ({downloaded / 1_048_576:.1f}/{total_bytes / 1_048_576:.1f} MiB) "
                        f"rate {rate / 1_048_576:.1f} MiB/s eta {format_duration(eta)}"
                    )
                else:
                    log_info(
                        "OSM PBF download "
                        f"{downloaded / 1_048_576:.1f} MiB, rate {rate / 1_048_576:.1f} MiB/s"
                    )
                last_log = now_mono

    temp_path.replace(osm_pbf_path)
    file_size = int(osm_pbf_path.stat().st_size)
    log_info(f"OSM PBF ready: {osm_pbf_path} ({file_size / 1_048_576:.1f} MiB)")
    return {
        "downloaded": True,
        "path": str(osm_pbf_path),
        "bytes": file_size,
        "source_url": osm_pbf_url,
    }


def radius_deltas_deg(radius_m: int, lat_deg: float) -> tuple[float, float]:
    lat_delta = float(radius_m) / 111_320.0
    cos_lat = max(0.1, math.cos(math.radians(lat_deg)))
    lon_delta = float(radius_m) / (111_320.0 * cos_lat)
    return lat_delta, lon_delta


def cell_key(lat: float, lon: float, lat_step: float, lon_step: float) -> tuple[int, int]:
    return (
        int(math.floor(lat / lat_step)),
        int(math.floor(lon / lon_step)),
    )


def haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(d_phi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_M * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))


def build_coarse_station_cells(
    df: pd.DataFrame,
    *,
    radius_m: int,
    lat_step: float,
    lon_step: float,
) -> set[tuple[int, int]]:
    cells: set[tuple[int, int]] = set()

    for _, row in df.iterrows():
        lat = float(row["lat"])
        lon = float(row["lon"])
        lat_delta, lon_delta = radius_deltas_deg(radius_m, lat)

        lat_min_idx = int(math.floor((lat - lat_delta) / lat_step))
        lat_max_idx = int(math.floor((lat + lat_delta) / lat_step))
        lon_min_idx = int(math.floor((lon - lon_delta) / lon_step))
        lon_max_idx = int(math.floor((lon + lon_delta) / lon_step))

        for lat_idx in range(lat_min_idx, lat_max_idx + 1):
            for lon_idx in range(lon_min_idx, lon_max_idx + 1):
                cells.add((lat_idx, lon_idx))

    return cells


def collect_amenity_points_from_pbf(
    *,
    pbf_path: Path,
    station_cells: set[tuple[int, int]],
    coarse_lat_step: float,
    coarse_lon_step: float,
    pbf_progress_every: int,
) -> tuple[list[AmenityPoint], dict[str, int]]:
    try:
        import osmium  # type: ignore
    except Exception as exc:
        install_cmd = f"{sys.executable} -m pip install osmium"
        raise RuntimeError(
            "The osm-pbf backend requires the Python package 'osmium'. "
            f"Install with this interpreter: {install_cmd}"
        ) from exc

    class AmenityCollector(osmium.SimpleHandler):  # type: ignore[misc]
        def __init__(self) -> None:
            super().__init__()
            self.points: list[AmenityPoint] = []
            self.nodes_seen = 0
            self.ways_seen = 0
            self.relations_seen = 0
            self.nodes_kept = 0
            self.ways_kept = 0
            self.started = time.monotonic()
            self.last_log = self.started

        def _maybe_log(self) -> None:
            seen = self.nodes_seen + self.ways_seen + self.relations_seen
            if pbf_progress_every <= 0:
                return
            if seen <= 0 or seen % pbf_progress_every != 0:
                return
            elapsed = max(0.001, time.monotonic() - self.started)
            rate = seen / elapsed
            log_info(
                "PBF scan progress: "
                f"objects={seen:,} (nodes={self.nodes_seen:,}, ways={self.ways_seen:,}, relations={self.relations_seen:,}) "
                f"kept_points={len(self.points):,} rate={rate:,.0f} obj/s"
            )
            self.last_log = time.monotonic()

        def _append_if_relevant(self, *, lat: float, lon: float, tags: Any, source: str) -> None:
            categories = classify_tags(tags)
            if not categories:
                return

            if cell_key(lat, lon, coarse_lat_step, coarse_lon_step) not in station_cells:
                return

            name = normalize_optional_text(tags.get("name"))
            opening_hours = normalize_optional_text(tags.get("opening_hours"))

            self.points.append(
                AmenityPoint(
                    lat=lat,
                    lon=lon,
                    categories=tuple(categories),
                    name=name,
                    opening_hours=opening_hours,
                )
            )
            if source == "node":
                self.nodes_kept += 1
            elif source == "way":
                self.ways_kept += 1

        def node(self, node: Any) -> None:
            self.nodes_seen += 1
            self._maybe_log()
            if not node.location.valid():
                return
            self._append_if_relevant(
                lat=float(node.location.lat),
                lon=float(node.location.lon),
                tags=node.tags,
                source="node",
            )

        def way(self, way: Any) -> None:
            self.ways_seen += 1
            self._maybe_log()

            categories = classify_tags(way.tags)
            if not categories:
                return

            sum_lat = 0.0
            sum_lon = 0.0
            count = 0
            for node_ref in way.nodes:
                if not node_ref.location.valid():
                    continue
                sum_lat += float(node_ref.location.lat)
                sum_lon += float(node_ref.location.lon)
                count += 1

            if count <= 0:
                return

            lat = sum_lat / count
            lon = sum_lon / count

            if cell_key(lat, lon, coarse_lat_step, coarse_lon_step) not in station_cells:
                return

            self.points.append(
                AmenityPoint(
                    lat=lat,
                    lon=lon,
                    categories=tuple(categories),
                    name=normalize_optional_text(way.tags.get("name")),
                    opening_hours=normalize_optional_text(way.tags.get("opening_hours")),
                )
            )
            self.ways_kept += 1

        def relation(self, relation: Any) -> None:
            self.relations_seen += 1
            self._maybe_log()

    collector = AmenityCollector()
    collector.apply_file(str(pbf_path), locations=True)

    stats = {
        "nodes_seen": collector.nodes_seen,
        "ways_seen": collector.ways_seen,
        "relations_seen": collector.relations_seen,
        "nodes_kept": collector.nodes_kept,
        "ways_kept": collector.ways_kept,
    }
    return collector.points, stats


def build_point_grid_index(
    points: list[AmenityPoint],
    *,
    lat_step: float,
    lon_step: float,
) -> dict[tuple[int, int], list[int]]:
    grid: dict[tuple[int, int], list[int]] = defaultdict(list)
    for idx, point in enumerate(points):
        grid[cell_key(point.lat, point.lon, lat_step, lon_step)].append(idx)
    return grid


def lookup_amenities(
    session: requests.Session,
    *,
    lat: float,
    lon: float,
    radius_m: int,
) -> tuple[dict[str, int], list[dict[str, Any]]]:
    query = build_overpass_query(lat, lon, radius_m)
    response = request_with_retries(
        "POST",
        OVERPASS_URL,
        session,
        timeout=60,
        data={"data": query},
    )
    payload = response.json()

    counts: dict[str, int] = {f"amenity_{rule.key}": 0 for rule in AMENITY_RULES}
    seen: set[tuple[str, int, str]] = set()
    seen_examples: set[tuple[str, int, str]] = set()
    examples: list[dict[str, Any]] = []

    for element in payload.get("elements", []):
        if not isinstance(element, dict):
            continue
        elem_type = element.get("type")
        elem_id = element.get("id")
        tags = element.get("tags")
        if elem_type not in {"node", "way", "relation"}:
            continue
        if not isinstance(elem_id, int) or not isinstance(tags, dict):
            continue

        categories = classify_tags(tags)
        name = normalize_optional_text(tags.get("name"))
        opening_hours = normalize_optional_text(tags.get("opening_hours"))

        elem_lat: float | None = None
        elem_lon: float | None = None
        if isinstance(element.get("lat"), (int, float)) and isinstance(element.get("lon"), (int, float)):
            elem_lat = float(element["lat"])
            elem_lon = float(element["lon"])
        else:
            center = element.get("center")
            if isinstance(center, dict):
                if isinstance(center.get("lat"), (int, float)) and isinstance(center.get("lon"), (int, float)):
                    elem_lat = float(center["lat"])
                    elem_lon = float(center["lon"])

        for cat in categories:
            marker = (elem_type, elem_id, cat)
            if marker in seen:
                continue
            seen.add(marker)
            counts[f"amenity_{cat}"] += 1

            example_marker = (elem_type, elem_id, cat)
            if example_marker in seen_examples:
                continue
            seen_examples.add(example_marker)

            distance_m: float | None = None
            if elem_lat is not None and elem_lon is not None:
                distance_m = haversine_distance_m(lat, lon, elem_lat, elem_lon)
            examples.append(
                build_amenity_example(
                    category=cat,
                    name=name,
                    opening_hours=opening_hours,
                    distance_m=distance_m,
                    amenity_lat=elem_lat,
                    amenity_lon=elem_lon,
                )
            )

    return counts, limit_amenity_examples(examples)


def enrich_with_amenities_overpass(
    df: pd.DataFrame,
    *,
    session: requests.Session,
    radius_m: int,
    query_budget: int,
    refresh_days: int,
    overpass_delay_ms: int,
    progress_every: int,
    force_refresh: bool,
    cache_path: Path,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    cache = load_amenity_cache(cache_path)
    entries: dict[str, Any] = cache.get("entries", {})

    now = utc_now()
    stale_before = now - timedelta(days=refresh_days)

    queries_used = 0
    cache_hits = 0
    cache_misses = 0
    deferred = 0
    lookup_errors = 0

    amenity_columns = [f"amenity_{rule.key}" for rule in AMENITY_RULES]

    for col in amenity_columns:
        df[col] = 0

    df["amenities_total"] = 0
    df["amenities_source"] = ""
    df["amenity_examples"] = "[]"

    total_rows = len(df)
    started = time.monotonic()
    last_log = started
    tty_progress = sys.stdout.isatty()

    def emit_progress(processed: int, *, force: bool = False) -> None:
        nonlocal last_log
        now_mono = time.monotonic()
        interval_hit = (now_mono - last_log) >= 8.0
        step_hit = progress_every > 0 and (processed % progress_every == 0)
        done = processed >= total_rows
        if not (force or interval_hit or step_hit or done):
            return
        elapsed = max(0.001, now_mono - started)
        rate = processed / elapsed
        remaining = max(0, total_rows - processed)
        eta_seconds = (remaining / rate) if rate > 0 else 0
        pct = (processed / total_rows * 100.0) if total_rows > 0 else 100.0
        line = (
            f"Amenities {processed}/{total_rows} ({pct:5.1f}%) | "
            f"live:{queries_used} cache:{cache_hits} deferred:{deferred} errors:{lookup_errors} | "
            f"{rate:5.1f} rows/s | elapsed {format_duration(elapsed)} | eta {format_duration(eta_seconds)}"
        )
        if tty_progress:
            suffix = "\n" if done else ""
            sys.stdout.write("\r" + line.ljust(160) + suffix)
            sys.stdout.flush()
        else:
            log_info(line)
        last_log = now_mono

    if total_rows > 0:
        emit_progress(0, force=True)

    for idx, row in df.iterrows():
        key = f"{row['lat']:.5f},{row['lon']:.5f}"
        cache_entry = entries.get(key)
        use_cache = False

        if cache_entry and not force_refresh:
            checked_raw = cache_entry.get("checked_at")
            try:
                checked_at = datetime.fromisoformat(checked_raw)
                if checked_at.tzinfo is None:
                    checked_at = checked_at.replace(tzinfo=timezone.utc)
            except Exception:
                checked_at = datetime(1970, 1, 1, tzinfo=timezone.utc)

            radius_matches = int(cache_entry.get("radius_m", -1)) == radius_m
            if radius_matches and checked_at >= stale_before:
                use_cache = True

        if use_cache:
            counts = cache_entry.get("counts", {})
            examples = decode_amenity_examples(cache_entry.get("examples", ""))
            cache_hits += 1
            source = "cache"
        else:
            if queries_used >= query_budget:
                cache_misses += 1
                deferred += 1
                source = "deferred"
                counts = {col: 0 for col in amenity_columns}
                examples = []
            else:
                try:
                    counts, examples = lookup_amenities(
                        session,
                        lat=float(row["lat"]),
                        lon=float(row["lon"]),
                        radius_m=radius_m,
                    )
                    source = "live"
                except Exception:
                    counts = {col: 0 for col in amenity_columns}
                    examples = []
                    source = "error"
                    lookup_errors += 1

                queries_used += 1
                cache_misses += 1

                entries[key] = {
                    "checked_at": now.isoformat(),
                    "radius_m": radius_m,
                    "counts": counts,
                    "examples": examples,
                }

                if overpass_delay_ms > 0:
                    time.sleep(overpass_delay_ms / 1000.0)

        for col in amenity_columns:
            value = int(counts.get(col, 0))
            df.at[idx, col] = value

        total = int(sum(int(counts.get(col, 0)) for col in amenity_columns))
        df.at[idx, "amenities_total"] = total
        df.at[idx, "amenities_source"] = source
        df.at[idx, "amenity_examples"] = encode_amenity_examples(examples)
        emit_progress(idx + 1)

    if total_rows == 0:
        log_info("Amenities 0/0 (100.0%) | no rows to process")

    cache["meta"] = {
        "schema_version": AMENITY_SCHEMA_VERSION,
        "radius_m": radius_m,
        "updated_at": now.isoformat(),
    }
    cache["entries"] = entries

    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

    stats = {
        "backend": AMENITY_BACKEND_OVERPASS,
        "queries_used": queries_used,
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "deferred": deferred,
        "lookup_errors": lookup_errors,
        "cache_entries": len(entries),
    }
    return df, stats


def enrich_with_amenities_osm_pbf(
    df: pd.DataFrame,
    *,
    session: requests.Session,
    radius_m: int,
    progress_every: int,
    osm_pbf_path: Path,
    osm_pbf_url: str,
    download_osm_pbf: bool,
    pbf_progress_every: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    amenity_columns = [f"amenity_{rule.key}" for rule in AMENITY_RULES]
    for col in amenity_columns:
        df[col] = 0

    df["amenities_total"] = 0
    df["amenities_source"] = "osm-pbf"
    df["amenity_examples"] = "[]"

    total_rows = len(df)
    if total_rows == 0:
        stats: dict[str, Any] = {
            "backend": AMENITY_BACKEND_OSM_PBF,
            "queries_used": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "deferred": 0,
            "lookup_errors": 0,
            "cache_entries": 0,
            "osm_pbf_path": str(osm_pbf_path),
            "osm_pbf_points": 0,
        }
        return df, stats

    pbf_file_meta = ensure_osm_pbf_file(
        session=session,
        osm_pbf_path=osm_pbf_path,
        osm_pbf_url=osm_pbf_url,
        download_if_missing=download_osm_pbf,
    )

    log_info("Building station coarse grid for local PBF filtering")
    coarse_lat_step = 0.02
    coarse_lon_step = 0.03
    station_cells = build_coarse_station_cells(
        df,
        radius_m=radius_m,
        lat_step=coarse_lat_step,
        lon_step=coarse_lon_step,
    )
    log_info(f"Station coarse cells: {len(station_cells):,}")

    log_info("Scanning OSM PBF for relevant amenity tags")
    pbf_scan_started = time.monotonic()
    points, pbf_scan_stats = collect_amenity_points_from_pbf(
        pbf_path=osm_pbf_path,
        station_cells=station_cells,
        coarse_lat_step=coarse_lat_step,
        coarse_lon_step=coarse_lon_step,
        pbf_progress_every=pbf_progress_every,
    )
    log_info(
        "PBF scan done: "
        f"points={len(points):,}, nodes_seen={pbf_scan_stats['nodes_seen']:,}, "
        f"ways_seen={pbf_scan_stats['ways_seen']:,}, took={format_duration(time.monotonic() - pbf_scan_started)}"
    )

    fine_lat_step = max(0.0005, radius_m / 111_320.0)
    fine_lon_step = max(
        0.0005,
        radius_m / (111_320.0 * max(0.1, math.cos(math.radians(56.5)))),
    )
    point_grid = build_point_grid_index(
        points,
        lat_step=fine_lat_step,
        lon_step=fine_lon_step,
    )
    log_info(f"Amenity grid cells indexed: {len(point_grid):,}")

    started = time.monotonic()
    last_log = started
    tty_progress = sys.stdout.isatty()

    def emit_progress(processed: int, *, force: bool = False) -> None:
        nonlocal last_log
        now_mono = time.monotonic()
        interval_hit = (now_mono - last_log) >= 8.0
        step_hit = progress_every > 0 and (processed % progress_every == 0)
        done = processed >= total_rows
        if not (force or interval_hit or step_hit or done):
            return
        elapsed = max(0.001, now_mono - started)
        rate = processed / elapsed
        remaining = max(0, total_rows - processed)
        eta_seconds = (remaining / rate) if rate > 0 else 0
        pct = (processed / total_rows * 100.0) if total_rows > 0 else 100.0
        line = (
            f"Amenities {processed}/{total_rows} ({pct:5.1f}%) | "
            f"backend:osm-pbf points:{len(points):,} | "
            f"{rate:5.1f} rows/s | elapsed {format_duration(elapsed)} | eta {format_duration(eta_seconds)}"
        )
        if tty_progress:
            suffix = "\n" if done else ""
            sys.stdout.write("\r" + line.ljust(160) + suffix)
            sys.stdout.flush()
        else:
            log_info(line)
        last_log = now_mono

    emit_progress(0, force=True)

    for idx, row in df.iterrows():
        lat = float(row["lat"])
        lon = float(row["lon"])
        lat_delta, lon_delta = radius_deltas_deg(radius_m, lat)

        lat_idx, lon_idx = cell_key(lat, lon, fine_lat_step, fine_lon_step)
        lat_reach = max(1, int(math.ceil(lat_delta / fine_lat_step)) + 1)
        lon_reach = max(1, int(math.ceil(lon_delta / fine_lon_step)) + 1)

        candidate_indices: set[int] = set()
        for d_lat in range(-lat_reach, lat_reach + 1):
            for d_lon in range(-lon_reach, lon_reach + 1):
                candidate_indices.update(point_grid.get((lat_idx + d_lat, lon_idx + d_lon), []))

        counts = {col: 0 for col in amenity_columns}
        examples: list[dict[str, Any]] = []
        for point_idx in candidate_indices:
            point = points[point_idx]
            distance_m = haversine_distance_m(lat, lon, point.lat, point.lon)
            if distance_m > radius_m:
                continue
            for category in point.categories:
                key = f"amenity_{category}"
                counts[key] += 1
                examples.append(
                    build_amenity_example(
                        category=category,
                        name=point.name,
                        opening_hours=point.opening_hours,
                        distance_m=distance_m,
                        amenity_lat=point.lat,
                        amenity_lon=point.lon,
                    )
                )

        for col in amenity_columns:
            value = int(counts.get(col, 0))
            df.at[idx, col] = value

        total = int(sum(int(counts.get(col, 0)) for col in amenity_columns))
        df.at[idx, "amenities_total"] = total
        df.at[idx, "amenity_examples"] = encode_amenity_examples(limit_amenity_examples(examples))
        emit_progress(idx + 1)

    stats = {
        "backend": AMENITY_BACKEND_OSM_PBF,
        "queries_used": 0,
        "cache_hits": 0,
        "cache_misses": 0,
        "deferred": 0,
        "lookup_errors": 0,
        "cache_entries": 0,
        "osm_pbf_path": str(osm_pbf_path),
        "osm_pbf_bytes": int(pbf_file_meta.get("bytes", 0)),
        "osm_pbf_downloaded": bool(pbf_file_meta.get("downloaded", False)),
        "osm_pbf_points": int(len(points)),
        "osm_pbf_grid_cells": int(len(point_grid)),
        "osm_pbf_nodes_seen": int(pbf_scan_stats["nodes_seen"]),
        "osm_pbf_ways_seen": int(pbf_scan_stats["ways_seen"]),
        "osm_pbf_relations_seen": int(pbf_scan_stats["relations_seen"]),
        "osm_pbf_nodes_kept": int(pbf_scan_stats["nodes_kept"]),
        "osm_pbf_ways_kept": int(pbf_scan_stats["ways_kept"]),
    }
    return df, stats


def enrich_with_amenities(
    df: pd.DataFrame,
    *,
    session: requests.Session,
    radius_m: int,
    query_budget: int,
    refresh_days: int,
    overpass_delay_ms: int,
    progress_every: int,
    force_refresh: bool,
    cache_path: Path,
    amenity_backend: str,
    osm_pbf_path: Path,
    osm_pbf_url: str,
    download_osm_pbf: bool,
    pbf_progress_every: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    backend = resolve_amenity_backend(amenity_backend, osm_pbf_path)
    log_info(f"Amenity backend resolved to: {backend}")

    if backend == AMENITY_BACKEND_OSM_PBF:
        return enrich_with_amenities_osm_pbf(
            df,
            session=session,
            radius_m=radius_m,
            progress_every=progress_every,
            osm_pbf_path=osm_pbf_path,
            osm_pbf_url=osm_pbf_url,
            download_osm_pbf=download_osm_pbf,
            pbf_progress_every=pbf_progress_every,
        )

    return enrich_with_amenities_overpass(
        df,
        session=session,
        radius_m=radius_m,
        query_budget=query_budget,
        refresh_days=refresh_days,
        overpass_delay_ms=overpass_delay_ms,
        progress_every=progress_every,
        force_refresh=force_refresh,
        cache_path=cache_path,
    )


def dataframe_to_geojson(df: pd.DataFrame, source_meta: dict[str, Any]) -> dict[str, Any]:
    features: list[dict[str, Any]] = []

    for _, row in df.iterrows():
        properties: dict[str, Any] = {
            "station_id": row["station_id"],
            "operator": row["operator"],
            "status": row["status"],
            "max_power_kw": float(row["max_power_kw"]),
            "charging_points_count": int(row["charging_points_count"]),
            "max_individual_power_kw": float(row["max_individual_power_kw"]),
            "postcode": row["postcode"],
            "city": row["city"],
            "address": row["address"],
            "amenities_total": int(row["amenities_total"]),
            "amenities_source": row["amenities_source"],
            "amenity_examples": decode_amenity_examples(row.get("amenity_examples", "[]")),
        }

        for rule in AMENITY_RULES:
            key = f"amenity_{rule.key}"
            properties[key] = int(row[key])

        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(row["lon"]), float(row["lat"])],
            },
            "properties": properties,
        }
        features.append(feature)

    return {
        "type": "FeatureCollection",
        "generated_at": utc_now().isoformat(),
        "source": source_meta,
        "features": features,
    }


def build_operator_list(df: pd.DataFrame, min_stations: int) -> dict[str, Any]:
    normalized_operators = (
        df["operator"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", "Unbekannt")
    )
    counts = normalized_operators.value_counts()
    filtered = counts[counts >= int(min_stations)]

    operators = [
        {"name": operator, "stations": int(station_count)}
        for operator, station_count in filtered.items()
    ]
    operators.sort(key=lambda item: (-item["stations"], item["name"].lower()))

    return {
        "generated_at": utc_now().isoformat(),
        "min_stations": int(min_stations),
        "total_operators": len(operators),
        "operators": operators,
    }


def write_run_history(path: Path, summary: dict[str, Any]) -> None:
    fieldnames = [
        "timestamp",
        "amenity_backend",
        "source_url",
        "stations_total",
        "stations_with_amenities",
        "query_budget",
        "queries_used",
        "cache_hits",
        "cache_misses",
        "deferred",
    ]

    row = {
        "timestamp": summary["run"]["started_at"],
        "amenity_backend": summary["amenity_lookup"].get("backend", "overpass"),
        "source_url": summary["source"].get("source_url", ""),
        "stations_total": summary["records"]["fast_chargers_total"],
        "stations_with_amenities": summary["records"]["stations_with_amenities"],
        "query_budget": summary["params"]["query_budget"],
        "queries_used": summary["amenity_lookup"]["queries_used"],
        "cache_hits": summary["amenity_lookup"]["cache_hits"],
        "cache_misses": summary["amenity_lookup"]["cache_misses"],
        "deferred": summary["amenity_lookup"]["deferred"],
    }

    path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []

    if path.exists():
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for previous in reader:
                normalized = {key: previous.get(key, "") for key in fieldnames}
                if not normalized.get("amenity_backend"):
                    normalized["amenity_backend"] = "overpass"

                # Repair rows written with the new schema but an old header.
                shifted = (
                    str(normalized.get("source_url", "")) in AMENITY_BACKEND_CHOICES
                    and str(normalized.get("stations_total", "")).startswith("http")
                )
                if shifted:
                    repaired_deferred = str(normalized.get("deferred", ""))
                    normalized = {
                        "timestamp": str(normalized.get("timestamp", "")),
                        "amenity_backend": str(normalized.get("source_url", "")) or "overpass",
                        "source_url": str(normalized.get("stations_total", "")),
                        "stations_total": str(normalized.get("stations_with_amenities", "")),
                        "stations_with_amenities": str(normalized.get("query_budget", "")),
                        "query_budget": str(normalized.get("queries_used", "")),
                        "queries_used": str(normalized.get("cache_hits", "")),
                        "cache_hits": str(normalized.get("cache_misses", "")),
                        "cache_misses": str(normalized.get("deferred", "")),
                        "deferred": repaired_deferred,
                    }

                rows.append(normalized)

    rows.append(row)

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def update_readme_status(readme_path: Path, summary: dict[str, Any]) -> None:
    if readme_path.exists():
        text = readme_path.read_text(encoding="utf-8")
    else:
        text = "# woladen.de\n"

    run = summary["run"]
    source = summary["source"]
    records = summary["records"]
    lookup = summary["amenity_lookup"]

    block = "\n".join(
        [
            README_START,
            "## Data Build Status",
            "",
            f"- Last build (UTC): `{run['started_at']}`",
            f"- Source: `{source.get('source_url', 'unknown')}`",
            f"- Fast chargers (>= {summary['params']['min_power_kw']} kW): `{records['fast_chargers_total']}`",
            f"- Chargers with >=1 nearby amenity: `{records['stations_with_amenities']}`",
            f"- Amenity backend: `{lookup.get('backend', 'overpass')}`",
            (
                f"- Live amenity lookups this run: `{lookup['queries_used']}` "
                f"(cache hits: `{lookup['cache_hits']}`, deferred: `{lookup['deferred']}`)"
            ),
            "",
            "Generated files:",
            "- `data/bnetza_cache.csv`",
            "- `data/chargers_fast.csv`",
            "- `data/chargers_fast.geojson`",
            "- `data/operators.json`",
            "- `data/summary.json`",
            README_END,
        ]
    )

    if README_START in text and README_END in text:
        pattern = re.compile(
            re.escape(README_START) + r".*?" + re.escape(README_END),
            re.DOTALL,
        )
        text = pattern.sub(block, text)
    else:
        if not text.endswith("\n"):
            text += "\n"
        text += "\n" + block + "\n"

    readme_path.write_text(text, encoding="utf-8")


def main() -> None:
    args = parse_args()
    osm_pbf_path = Path(args.osm_pbf_path)

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    started_at = utc_now().replace(microsecond=0).isoformat()
    pipeline_started = time.monotonic()
    log_info("Pipeline started")
    log_info(
        "Params: "
        f"min_power_kw={args.min_power_kw}, radius_m={args.radius_m}, "
        f"query_budget={args.query_budget}, refresh_days={args.refresh_days}, "
        f"max_stations={args.max_stations}, operator_min_stations={args.operator_min_stations}, "
        f"amenity_backend={args.amenity_backend}, osm_pbf_path={osm_pbf_path}"
    )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "woladen.de-data-pipeline/1.0 (+https://woladen.de)",
            "Accept": "application/json,text/plain,*/*",
        }
    )

    log_info("Stage 1/6: Fetching BNetzA source")
    source_meta = fetch_bnetza_csv(session, RAW_CACHE_PATH, RAW_META_PATH)
    log_info(f"Source ready: {source_meta.get('source_url', 'unknown')}")

    log_info("Stage 2/6: Loading and normalizing raw source")
    raw_df = load_raw_dataframe(RAW_CACHE_PATH)
    log_info(f"Raw rows loaded: {len(raw_df)}")

    log_info("Stage 3/6: Building filtered fast charger frame")
    fast_df = build_fast_charger_frame(raw_df, min_power_kw=args.min_power_kw)
    log_info(f"Fast chargers after filter: {len(fast_df)}")

    if args.max_stations and args.max_stations > 0:
        fast_df = fast_df.head(args.max_stations).reset_index(drop=True)
        log_info(f"Applied max_stations cap: {len(fast_df)} rows")

    log_info("Stage 4/6: Enriching chargers with nearby amenities")
    enriched_df, amenity_stats = enrich_with_amenities(
        fast_df,
        session=session,
        radius_m=args.radius_m,
        query_budget=args.query_budget,
        refresh_days=args.refresh_days,
        overpass_delay_ms=args.overpass_delay_ms,
        progress_every=args.progress_every,
        force_refresh=args.force_refresh,
        cache_path=AMENITY_CACHE_PATH,
        amenity_backend=args.amenity_backend,
        osm_pbf_path=osm_pbf_path,
        osm_pbf_url=args.osm_pbf_url,
        download_osm_pbf=args.download_osm_pbf,
        pbf_progress_every=args.pbf_progress_every,
    )
    log_info(
        "Amenity enrichment done: "
        f"queries_used={amenity_stats['queries_used']}, "
        f"cache_hits={amenity_stats['cache_hits']}, "
        f"deferred={amenity_stats['deferred']}, errors={amenity_stats['lookup_errors']}"
    )

    enriched_df = enriched_df.sort_values(
        by=["amenities_total", "max_power_kw"],
        ascending=[False, False],
    ).reset_index(drop=True)

    log_info("Stage 5/6: Writing data artifacts")
    FAST_CSV_PATH.write_text(enriched_df.to_csv(index=False), encoding="utf-8")

    geojson = dataframe_to_geojson(enriched_df, source_meta)
    FAST_GEOJSON_PATH.write_text(
        json.dumps(geojson, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    operators_payload = build_operator_list(
        enriched_df,
        min_stations=args.operator_min_stations,
    )
    OPERATORS_JSON_PATH.write_text(
        json.dumps(operators_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    stations_with_amenities = int((enriched_df["amenities_total"] > 0).sum())

    summary = {
        "run": {
            "started_at": started_at,
            "finished_at": utc_now().replace(microsecond=0).isoformat(),
        },
        "source": source_meta,
        "params": {
            "min_power_kw": args.min_power_kw,
            "radius_m": args.radius_m,
            "amenity_backend": args.amenity_backend,
            "query_budget": args.query_budget,
            "refresh_days": args.refresh_days,
            "max_stations": args.max_stations,
            "operator_min_stations": args.operator_min_stations,
        },
        "records": {
            "raw_rows": int(len(raw_df)),
            "fast_chargers_total": int(len(enriched_df)),
            "stations_with_amenities": stations_with_amenities,
        },
        "amenity_lookup": amenity_stats,
        "operators": {
            "min_stations": int(args.operator_min_stations),
            "listed_operators": int(operators_payload["total_operators"]),
        },
    }

    SUMMARY_JSON_PATH.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    log_info("Stage 6/6: Updating run history and README status")
    write_run_history(RUN_HISTORY_PATH, summary)
    update_readme_status(README_PATH, summary)
    log_info(f"Pipeline completed in {format_duration(time.monotonic() - pipeline_started)}")

    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
