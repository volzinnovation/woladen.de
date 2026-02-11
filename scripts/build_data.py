#!/usr/bin/env python3
"""Build dataset for woladen.de.

Pipeline steps:
1. Fetch latest BNetzA charging registry CSV (with local cache fallback).
2. Filter to active fast chargers (>= min power).
3. Enrich chargers with nearby OSM amenities via Overpass (with local cache).
4. Write derived CSV + GeoJSON + summary artifacts and update README status block.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import html
import json
import re
import time
import unicodedata
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

DATA_DIR = Path("data")
README_PATH = Path("README.md")

RAW_CACHE_PATH = DATA_DIR / "bnetza_cache.csv"
RAW_META_PATH = DATA_DIR / "bnetza_source.json"
AMENITY_CACHE_PATH = DATA_DIR / "osm_amenity_cache.json"
FAST_CSV_PATH = DATA_DIR / "chargers_fast.csv"
FAST_GEOJSON_PATH = DATA_DIR / "chargers_fast.geojson"
SUMMARY_JSON_PATH = DATA_DIR / "summary.json"
RUN_HISTORY_PATH = DATA_DIR / "run_history.csv"

README_START = "<!-- DATA_STATUS_START -->"
README_END = "<!-- DATA_STATUS_END -->"

AMENITY_SCHEMA_VERSION = 1
CSV_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")


@dataclass(frozen=True)
class AmenityRule:
    key: str
    selectors: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class DownloadCandidate:
    url: str
    filetype: str
    date_token: str


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build woladen.de data artifacts")
    parser.add_argument("--min-power-kw", type=float, default=50.0)
    parser.add_argument("--radius-m", type=int, default=100)
    parser.add_argument(
        "--query-budget",
        type=int,
        default=500,
        help="Maximum new Overpass lookups per run (cache hits are free)",
    )
    parser.add_argument(
        "--refresh-days",
        type=int,
        default=30,
        help="Refresh cached amenity lookups older than this many days",
    )
    parser.add_argument(
        "--max-stations",
        type=int,
        default=0,
        help="Optional cap for processed chargers (0 = no cap)",
    )
    parser.add_argument("--overpass-delay-ms", type=int, default=250)
    parser.add_argument("--force-refresh", action="store_true")
    return parser.parse_args()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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

    errors: list[str] = []

    for url in candidate_urls:
        try:
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
            return metadata
        except requests.RequestException as exc:
            errors.append(f"{url}: {exc}")

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

    power_cols = [
        col
        for col in df.columns
        if "nennleistung" in normalize_text(col) or "leistung" in normalize_text(col)
    ]
    if not power_cols:
        raise RuntimeError("Could not identify power columns in BNetzA CSV")

    numeric_power = pd.DataFrame({col: to_float(df[col]) for col in power_cols})
    df["max_power_kw"] = numeric_power.max(axis=1, skipna=True)

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

    df = df.drop_duplicates(subset=["lat", "lon", "operator"]).reset_index(drop=True)

    def station_id(row: pd.Series) -> str:
        raw = f"{row['lat']:.5f}|{row['lon']:.5f}|{row['operator']}|{row['address']}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]

    df["station_id"] = df.apply(station_id, axis=1)

    selected_columns = [
        "station_id",
        "operator",
        "status",
        "max_power_kw",
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


def lookup_amenities(
    session: requests.Session,
    *,
    lat: float,
    lon: float,
    radius_m: int,
) -> dict[str, int]:
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
        for cat in categories:
            marker = (elem_type, elem_id, cat)
            if marker in seen:
                continue
            seen.add(marker)
            counts[f"amenity_{cat}"] += 1

    return counts


def enrich_with_amenities(
    df: pd.DataFrame,
    *,
    session: requests.Session,
    radius_m: int,
    query_budget: int,
    refresh_days: int,
    overpass_delay_ms: int,
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
            cache_hits += 1
            source = "cache"
        else:
            if queries_used >= query_budget:
                cache_misses += 1
                deferred += 1
                source = "deferred"
                counts = {col: 0 for col in amenity_columns}
            else:
                try:
                    counts = lookup_amenities(
                        session,
                        lat=float(row["lat"]),
                        lon=float(row["lon"]),
                        radius_m=radius_m,
                    )
                    source = "live"
                except Exception:
                    counts = {col: 0 for col in amenity_columns}
                    source = "error"
                    lookup_errors += 1

                queries_used += 1
                cache_misses += 1

                entries[key] = {
                    "checked_at": now.isoformat(),
                    "radius_m": radius_m,
                    "counts": counts,
                }

                if overpass_delay_ms > 0:
                    time.sleep(overpass_delay_ms / 1000.0)

        for col in amenity_columns:
            value = int(counts.get(col, 0))
            df.at[idx, col] = value

        total = int(sum(int(counts.get(col, 0)) for col in amenity_columns))
        df.at[idx, "amenities_total"] = total
        df.at[idx, "amenities_source"] = source

    cache["meta"] = {
        "schema_version": AMENITY_SCHEMA_VERSION,
        "radius_m": radius_m,
        "updated_at": now.isoformat(),
    }
    cache["entries"] = entries

    cache_path.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

    stats = {
        "queries_used": queries_used,
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "deferred": deferred,
        "lookup_errors": lookup_errors,
        "cache_entries": len(entries),
    }
    return df, stats


def dataframe_to_geojson(df: pd.DataFrame, source_meta: dict[str, Any]) -> dict[str, Any]:
    features: list[dict[str, Any]] = []

    for _, row in df.iterrows():
        properties: dict[str, Any] = {
            "station_id": row["station_id"],
            "operator": row["operator"],
            "status": row["status"],
            "max_power_kw": float(row["max_power_kw"]),
            "postcode": row["postcode"],
            "city": row["city"],
            "address": row["address"],
            "amenities_total": int(row["amenities_total"]),
            "amenities_source": row["amenities_source"],
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


def write_run_history(path: Path, summary: dict[str, Any]) -> None:
    fieldnames = [
        "timestamp",
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
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


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
            f"- Overpass queries this run: `{lookup['queries_used']}` (cache hits: `{lookup['cache_hits']}`, deferred: `{lookup['deferred']}`)",
            "",
            "Generated files:",
            "- `data/bnetza_cache.csv`",
            "- `data/chargers_fast.csv`",
            "- `data/chargers_fast.geojson`",
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

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    started_at = utc_now().replace(microsecond=0).isoformat()

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "woladen.de-data-pipeline/1.0 (+https://woladen.de)",
            "Accept": "application/json,text/plain,*/*",
        }
    )

    source_meta = fetch_bnetza_csv(session, RAW_CACHE_PATH, RAW_META_PATH)

    raw_df = load_raw_dataframe(RAW_CACHE_PATH)
    fast_df = build_fast_charger_frame(raw_df, min_power_kw=args.min_power_kw)

    if args.max_stations and args.max_stations > 0:
        fast_df = fast_df.head(args.max_stations).reset_index(drop=True)

    enriched_df, amenity_stats = enrich_with_amenities(
        fast_df,
        session=session,
        radius_m=args.radius_m,
        query_budget=args.query_budget,
        refresh_days=args.refresh_days,
        overpass_delay_ms=args.overpass_delay_ms,
        force_refresh=args.force_refresh,
        cache_path=AMENITY_CACHE_PATH,
    )

    enriched_df = enriched_df.sort_values(
        by=["amenities_total", "max_power_kw"],
        ascending=[False, False],
    ).reset_index(drop=True)

    FAST_CSV_PATH.write_text(enriched_df.to_csv(index=False), encoding="utf-8")

    geojson = dataframe_to_geojson(enriched_df, source_meta)
    FAST_GEOJSON_PATH.write_text(
        json.dumps(geojson, ensure_ascii=False, indent=2),
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
            "query_budget": args.query_budget,
            "refresh_days": args.refresh_days,
            "max_stations": args.max_stations,
        },
        "records": {
            "raw_rows": int(len(raw_df)),
            "fast_chargers_total": int(len(enriched_df)),
            "stations_with_amenities": stations_with_amenities,
        },
        "amenity_lookup": amenity_stats,
    }

    SUMMARY_JSON_PATH.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    write_run_history(RUN_HISTORY_PATH, summary)
    update_readme_status(README_PATH, summary)

    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
