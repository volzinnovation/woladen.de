#!/usr/bin/env python3
"""Build dataset for woladen.de.

Pipeline steps:
1. Fetch latest BNetzA charging registry CSV (with local cache fallback).
2. Filter to active fast chargers (>= min power).
3. Match stations with live occupancy from MobiData BW OCPI feeds where possible.
4. Enrich chargers with nearby OSM amenities via local Germany PBF or Overpass.
5. Write derived CSV + GeoJSON + summary artifacts and update README status block.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import html
import importlib.util
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import unicodedata
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import pandas as pd
import requests
import urllib3

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
BNETZA_API_LADESTATION_URL = (
    "https://d1269bxe5ubfat.cloudfront.net/bnetza-api/data/bnetza_api_ladestation000.csv?v=1"
)
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OSM_GERMANY_PBF_URL = "https://download.geofabrik.de/europe/germany-latest.osm.pbf"
MOBIDATA_SOURCES_URL = "https://api.mobidata-bw.de/ocpdb/api/public/v1/sources"
MOBIDATA_OCPI_LOCATIONS_URL = "https://api.mobidata-bw.de/ocpdb/api/ocpi/3.0/locations"
MOBILITHEK_TOKEN_URL = "https://mobilithek.info/auth/realms/MDP/protocol/openid-connect/token"
MOBILITHEK_PUBLICATION_FILE_URL = (
    "https://mobilithek.info/mdp-api/mdp-conn-server/v1/publication/{publication_id}/file"
)
MOBILITHEK_PUBLICATION_PUBLIC_FILE_URL = (
    "https://mobilithek.info/mdp-api/mdp-conn-server/v1/publication/{publication_id}/file/noauth"
)
DATEX_V3_SUBSCRIPTION_URL = (
    "https://mobilithek.info:8443/mobilithek/api/v1.0/subscription/datexv3?subscriptionID={subscription_id}"
)
MOBILITHEK_PUBLICATION_FILE_ACCESS_URL = (
    "https://mobilithek.info/mdp-api/mdp-conn-server/v1/publication/{publication_id}/file/access"
)
MOBILITHEK_METADATA_SEARCH_URL = "https://mobilithek.info/mdp-api/mdp-msa-metadata/v2/offers/search"
MOBILITHEK_METADATA_OFFER_URL = "https://mobilithek.info/mdp-api/mdp-msa-metadata/v2/offers/{publication_id}"
MOBILITHEK_USERNAME_ENV = "MOBILITHEK_USERNAME"
MOBILITHEK_PASSWORD_ENV = "MOBILITHEK_PASSWORD"
MOBILITHEK_AFIR_SEARCH_TERM = "AFIR"
CHARGING_DATA_CATEGORY = "https://w3id.org/mdp/schema/data_categories#FILLING_AND_CHARGING_STATIONS"
DATEX_V3_DATA_MODEL = "https://w3id.org/mdp/schema/data_model#DATEX_2_V3"

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path("data")
README_PATH = Path("README.md")
MOBILITHEK_AFIR_PROVIDER_CONFIG_PATH = REPO_ROOT / "data" / "mobilithek_afir_provider_configs.json"
MOBILITHEK_SUBSCRIPTION_REGISTRY_PATH = REPO_ROOT / "secret" / "mobilithek_subscriptions.json"
MOBILITHEK_USERNAME_FILE = REPO_ROOT / "secret" / "mobilithek_user.txt"
MOBILITHEK_PASSWORD_FILE = REPO_ROOT / "secret" / "mobilithek_pwd.txt"
MOBILITHEK_MACHINE_CERT_P12 = REPO_ROOT / "secret" / "certificate.p12"
MOBILITHEK_MACHINE_CERT_PASSWORD_FILE = REPO_ROOT / "secret" / "pwd.txt"

RAW_CACHE_PATH = DATA_DIR / "bnetza_cache.csv"
RAW_META_PATH = DATA_DIR / "bnetza_source.json"
BNETZA_API_LADESTATION_CACHE_PATH = DATA_DIR / "bnetza_api_ladestation_cache.csv"
BNETZA_API_LADESTATION_META_PATH = DATA_DIR / "bnetza_api_ladestation_source.json"
AMENITY_CACHE_PATH = DATA_DIR / "osm_amenity_cache.json"
FULL_CSV_PATH = DATA_DIR / "chargers_full.csv"
FAST_CSV_PATH = DATA_DIR / "chargers_fast.csv"
FAST_GEOJSON_PATH = DATA_DIR / "chargers_fast.geojson"
UNDER_50_GEOJSON_PATH = DATA_DIR / "chargers_under_50.geojson"
SUMMARY_JSON_PATH = DATA_DIR / "summary.json"
OPERATORS_JSON_PATH = DATA_DIR / "operators.json"
RUN_HISTORY_PATH = DATA_DIR / "run_history.csv"

README_START = "<!-- DATA_STATUS_START -->"
README_END = "<!-- DATA_STATUS_END -->"

AMENITY_SCHEMA_VERSION = 1
CSV_ENCODINGS = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
BNETZA_POWER_COLUMN_INDEX = 6  # 7th column in source file
EARTH_RADIUS_M = 6_371_000.0
MAX_REASONABLE_DISPLAY_POWER_KW = 400.0
NUMERIC_TOKEN_RE = re.compile(r"-?\d+(?:[.,]\d+)?")
MOBIDATA_PAGE_LIMIT = 500
MOBIDATA_TIMEOUT_SECONDS = 60
MOBIDATA_IGNORED_SOURCE_UIDS = {"opendata_swiss"}
OCCUPANCY_AVAILABLE_STATUSES = {"AVAILABLE"}
OCCUPANCY_OCCUPIED_STATUSES = {"OCCUPIED", "CHARGING", "BLOCKED", "RESERVED", "INUSE"}
OCCUPANCY_OUT_OF_ORDER_STATUSES = {
    "OUTOFORDER",
    "OUTOFSERVICE",
    "INOPERATIVE",
    "FAULTED",
    "CLOSED",
    "OFFLINE",
}
OCCUPANCY_UNKNOWN_STATUSES = {"UNKNOWN", "STATIC", "PLANNED", "REMOVED"}
DATEX_MAX_MATCH_DISTANCE_M = 200.0
DATEX_TLS_VERIFY = False
DATEX_REQUEST_TIMEOUT_SECONDS = 60
MOBILITHEK_SEARCH_PAGE_SIZE = 200
DETAIL_MATCH_MAX_DISTANCE_M = 200.0
STATIC_DETAIL_FIELDS = (
    "detail_source_uid",
    "detail_source_name",
    "detail_last_updated",
    "datex_site_id",
    "datex_station_ids",
    "datex_charge_point_ids",
    "price_display",
    "price_energy_eur_kwh_min",
    "price_energy_eur_kwh_max",
    "price_currency",
    "price_quality",
    "opening_hours_display",
    "opening_hours_is_24_7",
    "helpdesk_phone",
    "payment_methods_display",
    "auth_methods_display",
    "connector_types_display",
    "current_types_display",
    "connector_count",
    "green_energy",
    "service_types_display",
    "details_json",
)
TRIMMED_GEOJSON_TOP_LEVEL_KEYS = frozenset({"source"})
TRIMMED_GEOJSON_PROPERTY_KEYS = frozenset(
    {
        "status",
        "detail_source_uid",
        "datex_site_id",
        "datex_station_ids",
        "datex_charge_point_ids",
        "details_json",
        "amenities_source",
    }
)
GENERIC_OPERATOR_WORDS = {
    "afir",
    "recharging",
    "charging",
    "infrastructure",
    "realtime",
    "dynamic",
    "static",
    "dynamisch",
    "statisch",
    "dyn",
    "stat",
    "json",
    "data",
    "station",
    "stations",
    "point",
    "points",
    "deutschland",
    "gmbh",
    "mbh",
    "ag",
    "kg",
    "co",
    "inc",
    "ltd",
    "aps",
    "mobility",
    "plus",
    "group",
    "und",
    "public",
    "gesellschaft",
}
CONNECTOR_TYPE_LABELS = {
    "iec62196t2combo": "CCS",
    "combo2ccsdc": "CCS",
    "iec62196t1combo": "CCS Typ 1",
    "iec62196t2": "Typ 2",
    "type2ac": "Typ 2",
    "type2": "Typ 2",
    "chademo": "CHAdeMO",
    "domesticf": "Schuko",
    "other": "Sonstiger Stecker",
}
CURRENT_TYPE_LABELS = {
    "ac": "AC",
    "dc": "DC",
}
AUTH_METHOD_LABELS = {
    "creditcard": "Kreditkarte",
    "debitcard": "Debitkarte",
    "apps": "App",
    "rfid": "RFID",
    "activerfidchip": "RFID",
    "mifareclassic": "RFID",
    "mifaredesfire": "RFID",
    "nfc": "NFC",
    "website": "Web",
    "overtheair": "Automatische Freischaltung",
    "plugncharge": "Plug & Charge",
    "paymentcardreader": "Kartenterminal",
    "paymentcardcontactless": "Kontaktlos",
}
PAYMENT_METHOD_LABELS = {
    "creditcard": "Kreditkarte",
    "debitcard": "Debitkarte",
    "nfc": "NFC",
    "website": "Web",
    "paymentcardreader": "Kartenterminal",
    "paymentcardcontactless": "Kontaktlos",
    "otheradhocpaymentoption": "Ad-hoc-Zahlung",
    "contractbasedpaymentoption": "Vertragsbasiert",
}
SERVICE_TYPE_LABELS = {
    "unattended": "Selbstbedient",
    "physicalattendance": "Mit Personal",
}
BOOLEAN_YES_VALUES = {"1", "true", "yes", "ja", "y"}
MOBILITHEK_DATEX_PUBLICATIONS: tuple[dict[str, Any], ...] = (
    {
        "uid": "mobilithek_tesla_datex",
        "name": "Tesla DATEX II",
        "operator_patterns": ("tesla",),
        "static_publication_id": "953828817873125376",
        "dynamic_publication_id": "953843379766972416",
        "requires_auth": False,
    },
    {
        "uid": "mobilithek_enbw_datex",
        "name": "EnBW DATEX II",
        "operator_patterns": ("enbw",),
        "static_publication_id": "907574882292453376",
        "dynamic_publication_id": "907575401287241728",
        "requires_auth": True,
    },
)

if not DATEX_TLS_VERIFY:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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


@dataclass(frozen=True)
class DatexStaticSite:
    site_id: str
    station_ids: tuple[str, ...]
    lat: float
    lon: float
    postcode: str
    city: str
    address: str
    operator_name: str
    total_evses: int
    evse_ids: tuple[str, ...]


@dataclass(frozen=True)
class AfirStaticPublication:
    uid: str
    publication_id: str
    title: str
    publisher: str
    access_mode: str
    data_model: str
    access_url: str = ""


@dataclass(frozen=True)
class DirectDatexSource:
    provider_uid: str
    display_name: str
    publisher: str
    dynamic_url: str
    static_url: str
    dynamic_title: str
    static_title: str


@dataclass(frozen=True)
class ElisoStaticSite:
    site_id: str
    station_ids: tuple[str, ...]
    lat: float
    lon: float
    postcode: str
    city: str
    address: str
    operator_name: str
    total_evses: int
    evse_ids: tuple[str, ...]


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
    parser.add_argument("--radius-m", type=int, default=250)
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
        help="Download or refresh the Germany osm-pbf file when osm-pbf backend is selected",
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


def parse_numeric_tokens(value: Any) -> list[float]:
    if value is None or pd.isna(value):
        return []

    if isinstance(value, (int, float)) and not pd.isna(value):
        return [float(value)]

    text = str(value).strip()
    if not text:
        return []

    tokens: list[float] = []
    for token in NUMERIC_TOKEN_RE.findall(text):
        normalized = token.replace(",", ".")
        try:
            tokens.append(float(normalized))
        except ValueError:
            continue
    return tokens


def max_numeric_token(value: Any, *, clamp_max: float | None = None) -> float:
    tokens = [token for token in parse_numeric_tokens(value) if token > 0]
    if not tokens:
        return math.nan
    if clamp_max is not None:
        tokens = [min(token, clamp_max) for token in tokens]
    return max(tokens)


def to_max_numeric_token_float(
    series: pd.Series,
    *,
    clamp_max: float | None = None,
) -> pd.Series:
    return pd.to_numeric(
        series.apply(lambda value: max_numeric_token(value, clamp_max=clamp_max)),
        errors="coerce",
    )


def normalize_evse_id(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return "".join(ch for ch in str(value).upper() if ch.isalnum())


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        normalized = value
        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError:
        return None


def choose_latest_timestamp(values: list[str]) -> str:
    parsed: list[tuple[datetime, str]] = []
    for value in values:
        dt = parse_iso_datetime(value)
        if dt is not None:
            parsed.append((dt, value))
    if parsed:
        parsed.sort(key=lambda item: item[0])
        return parsed[-1][1]
    return values[-1] if values else ""


def get_env_text(name: str) -> str:
    return str(os.environ.get(name, "")).strip()


def _xml_local_name(value: str) -> str:
    return str(value or "").split("}", 1)[-1]


def _xml_scalar_value(text: str) -> Any:
    normalized = str(text or "").strip()
    if not normalized:
        return ""
    lowered = normalized.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    return normalized


def _merge_xml_child_value(mapping: dict[str, Any], key: str, value: Any) -> None:
    existing = mapping.get(key)
    if existing is None:
        mapping[key] = value
        return
    if isinstance(existing, list):
        existing.append(value)
        return
    mapping[key] = [existing, value]


def _parse_xml_element(element: ET.Element) -> Any:
    tag = _xml_local_name(element.tag)
    attributes = {
        _xml_local_name(key): str(value).strip()
        for key, value in element.attrib.items()
        if str(value).strip()
    }
    children = list(element)
    text = str(element.text or "").strip()

    if tag == "reference":
        payload: dict[str, Any] = {}
        reference_id = attributes.pop("id", "")
        if reference_id:
            payload["idG"] = reference_id
        payload.update(attributes)
        if text and "idG" not in payload:
            payload["idG"] = text
        return payload

    if not children:
        if attributes:
            payload = dict(attributes)
            if text:
                payload["value"] = _xml_scalar_value(text)
            return payload
        return _xml_scalar_value(text)

    payload: dict[str, Any] = {}
    for child in children:
        _merge_xml_child_value(payload, _xml_local_name(child.tag), _parse_xml_element(child))

    payload.update(attributes)
    if text:
        payload["value"] = _xml_scalar_value(text)
    return payload


def _decode_xml_bytes(raw: bytes) -> dict[str, Any]:
    root = ET.fromstring(raw.decode("utf-8"))
    decoded = _parse_xml_element(root)
    if isinstance(decoded, dict):
        return decoded
    return {_xml_local_name(root.tag): decoded}


def _iter_dict_items(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _iter_values(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, "", (), {}):
        return []
    return [value]


def _value_field(value: Any) -> Any:
    if isinstance(value, dict) and "value" in value:
        return value.get("value")
    return value


def _reference_id(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("idG", "id", "externalIdentifier"):
            text = str(value.get(key) or "").strip()
            if text:
                return text
    return str(value or "").strip()


def _datex_publication_root(payload: dict[str, Any]) -> dict[str, Any]:
    message_container = payload.get("messageContainer")
    message_payload = message_container.get("payload") if isinstance(message_container, dict) else None
    candidates: list[Any] = [
        ((message_payload or {}).get("aegiEnergyInfrastructureTablePublication"))
        if isinstance(message_payload, dict)
        else None,
        ((payload.get("payload") or {}).get("aegiEnergyInfrastructureTablePublication")),
        payload.get("aegiEnergyInfrastructureTablePublication"),
        message_payload,
        payload.get("payload"),
        payload,
    ]
    if isinstance(message_payload, list):
        for item in message_payload:
            if isinstance(item, dict):
                candidates.append(item.get("aegiEnergyInfrastructureTablePublication"))
                candidates.append(item)
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate.get("energyInfrastructureTable"):
            return candidate
    return {}


def decode_json_bytes(content: bytes) -> dict[str, Any] | list[Any]:
    raw = content
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    stripped = raw.lstrip()
    if stripped.startswith(b"<"):
        return _decode_xml_bytes(raw)
    return json.loads(raw.decode("utf-8"))


def load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def read_optional_text(path: Path) -> str:
    path = Path(path).expanduser()
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def parse_http_status_code(header_text: str) -> int:
    for line in header_text.splitlines():
        if line.startswith("HTTP/"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                return int(parts[1])
    return 0


def load_static_subscription_ids(path: Path = MOBILITHEK_SUBSCRIPTION_REGISTRY_PATH) -> dict[str, str]:
    registry = load_json_object(path)
    static_subscription_ids: dict[str, str] = {}
    for provider_uid, entry in registry.items():
        if not isinstance(entry, dict):
            continue
        subscription_id = str(entry.get("static_subscription_id") or "").strip()
        if subscription_id:
            static_subscription_ids[str(provider_uid).strip()] = subscription_id
    return static_subscription_ids


def load_dynamic_subscription_ids(path: Path = MOBILITHEK_SUBSCRIPTION_REGISTRY_PATH) -> dict[str, str]:
    registry = load_json_object(path)
    dynamic_subscription_ids: dict[str, str] = {}
    for provider_uid, entry in registry.items():
        if not isinstance(entry, dict):
            continue
        subscription_id = str(entry.get("subscription_id") or "").strip()
        if subscription_id:
            dynamic_subscription_ids[str(provider_uid).strip()] = subscription_id
    return dynamic_subscription_ids


def load_direct_datex_sources(path: Path = MOBILITHEK_SUBSCRIPTION_REGISTRY_PATH) -> list[DirectDatexSource]:
    registry = load_json_object(path)
    sources: list[DirectDatexSource] = []
    for provider_uid, entry in registry.items():
        if not isinstance(entry, dict):
            continue
        if "enabled" in entry and not bool(entry.get("enabled")):
            continue
        fetch_kind = str(entry.get("fetch_kind") or "").strip()
        dynamic_url = str(entry.get("fetch_url") or "").strip()
        static_url = str(entry.get("static_fetch_url") or "").strip()
        if fetch_kind != "direct_url" or not dynamic_url or not static_url:
            continue
        display_name = str(entry.get("display_name") or provider_uid or "").strip() or str(provider_uid).strip()
        publisher = str(entry.get("publisher") or display_name or provider_uid or "").strip()
        dynamic_title = str(entry.get("offer_title") or display_name or provider_uid or "").strip()
        static_title = str(entry.get("static_offer_title") or display_name or provider_uid or "").strip()
        sources.append(
            DirectDatexSource(
                provider_uid=str(provider_uid).strip(),
                display_name=display_name,
                publisher=publisher,
                dynamic_url=dynamic_url,
                static_url=static_url,
                dynamic_title=dynamic_title,
                static_title=static_title,
            )
        )
    return sorted(sources, key=lambda item: item.provider_uid)


def load_registry_datex_publications(
    subscription_path: Path = MOBILITHEK_SUBSCRIPTION_REGISTRY_PATH,
    config_path: Path = MOBILITHEK_AFIR_PROVIDER_CONFIG_PATH,
) -> list[dict[str, Any]]:
    registry = load_json_object(subscription_path)
    config_payload = load_json_object(config_path)
    providers_by_uid: dict[str, dict[str, Any]] = {}

    for provider in config_payload.get("providers") or []:
        if not isinstance(provider, dict):
            continue
        provider_uid = str(provider.get("uid") or "").strip()
        if provider_uid:
            providers_by_uid[provider_uid] = provider

    publications: list[dict[str, Any]] = []
    for provider_uid, entry in registry.items():
        if not isinstance(entry, dict):
            continue
        if "enabled" in entry and not bool(entry.get("enabled")):
            continue

        provider_uid = str(provider_uid).strip()
        provider = providers_by_uid.get(provider_uid) or {}
        feeds = provider.get("feeds") or {}
        static_feed = feeds.get("static") or {}
        dynamic_feed = feeds.get("dynamic") or {}
        if (
            str(static_feed.get("data_model") or "").strip() != DATEX_V3_DATA_MODEL
            or str(dynamic_feed.get("data_model") or "").strip() != DATEX_V3_DATA_MODEL
        ):
            continue

        static_publication_id = str(
            entry.get("static_publication_id") or static_feed.get("publication_id") or ""
        ).strip()
        dynamic_publication_id = str(
            entry.get("publication_id") or dynamic_feed.get("publication_id") or ""
        ).strip()
        if not static_publication_id or not dynamic_publication_id:
            continue

        display_name = str(
            entry.get("display_name") or provider.get("display_name") or provider_uid or ""
        ).strip()
        publisher = str(entry.get("publisher") or provider.get("publisher") or display_name or provider_uid).strip()
        publications.append(
            {
                "uid": provider_uid,
                "name": display_name or publisher or provider_uid,
                "operator_patterns": tuple(sorted(operator_tokens(provider_uid, display_name, publisher))),
                "static_publication_id": static_publication_id,
                "dynamic_publication_id": dynamic_publication_id,
                "static_access_mode": str(
                    entry.get("static_access_mode") or static_feed.get("access_mode") or ""
                ).strip(),
                "dynamic_access_mode": str(
                    entry.get("access_mode") or dynamic_feed.get("access_mode") or ""
                ).strip(),
                "static_subscription_id": str(entry.get("static_subscription_id") or "").strip(),
                "dynamic_subscription_id": str(entry.get("subscription_id") or "").strip(),
                "static_access_url": resolve_content_access_url(static_feed.get("content_data") or {}),
                "dynamic_access_url": resolve_content_access_url(dynamic_feed.get("content_data") or {}),
            }
        )

    return sorted(publications, key=lambda item: item["uid"])


def load_provider_uid_by_static_publication(
    path: Path = MOBILITHEK_AFIR_PROVIDER_CONFIG_PATH,
) -> dict[str, str]:
    provider_context = load_provider_context_by_static_publication(path)
    return {
        publication_id: str(context.get("uid") or "").strip()
        for publication_id, context in provider_context.items()
        if str(context.get("uid") or "").strip()
    }


def load_provider_context_by_static_publication(
    path: Path = MOBILITHEK_AFIR_PROVIDER_CONFIG_PATH,
) -> dict[str, dict[str, str]]:
    payload = load_json_object(path)
    provider_context_by_publication: dict[str, dict[str, str]] = {}
    for provider in payload.get("providers") or []:
        if not isinstance(provider, dict):
            continue
        provider_uid = str(provider.get("uid") or "").strip()
        display_name = str(provider.get("display_name") or "").strip()
        publisher = str(provider.get("publisher") or "").strip()
        static_publication_id = str((((provider.get("feeds") or {}).get("static")) or {}).get("publication_id") or "").strip()
        if provider_uid and static_publication_id:
            provider_context_by_publication[static_publication_id] = {
                "uid": provider_uid,
                "display_name": display_name,
                "publisher": publisher,
            }
    return provider_context_by_publication


def extract_first_lang_value(payload: Any) -> str:
    if isinstance(payload, dict):
        for key in ("values", "value", "text", "name", "legalName"):
            if key not in payload:
                continue
            value = extract_first_lang_value(payload.get(key))
            if value:
                return value
        for item in payload.values():
            value = extract_first_lang_value(item)
            if value:
                return value
    if isinstance(payload, list):
        for item in payload:
            value = extract_first_lang_value(item)
            if value:
                return value
    if payload is None:
        return ""
    return str(payload).strip()


def extract_datex_site_coordinates(site: dict[str, Any]) -> tuple[float | None, float | None]:
    candidate_locations = [site.get("locationReference") or {}]
    for station in _iter_dict_items(site.get("energyInfrastructureStation")):
        if station:
            candidate_locations.append(station.get("locationReference") or {})
        for refill_point in _iter_dict_items(station.get("refillPoint")):
            candidate_locations.append(refill_point.get("locationReference") or {})

    for location_reference in candidate_locations:
        point = location_reference.get("locPointLocation") or {}
        area = location_reference.get("locAreaLocation") or {}
        direct_point = location_reference

        for candidate in (
            point.get("coordinatesForDisplay"),
            ((point.get("pointByCoordinates") or {}).get("pointCoordinates") or {}),
            direct_point.get("coordinatesForDisplay"),
            ((direct_point.get("pointByCoordinates") or {}).get("pointCoordinates") or {}),
            area.get("coordinatesForDisplay"),
        ):
            if not isinstance(candidate, dict):
                continue
            lat = _value_field(candidate.get("latitude"))
            lon = _value_field(candidate.get("longitude"))
            if lat is None or lon is None:
                continue
            try:
                return float(lat), float(lon)
            except (TypeError, ValueError):
                continue

    return None, None


def extract_datex_site_address(site: dict[str, Any]) -> tuple[str, str, str]:
    candidate_locations = [site.get("locationReference") or {}]
    for station in _iter_dict_items(site.get("energyInfrastructureStation")):
        if station:
            candidate_locations.append(station.get("locationReference") or {})
        for refill_point in _iter_dict_items(station.get("refillPoint")):
            candidate_locations.append(refill_point.get("locationReference") or {})

    for location_reference in candidate_locations:
        point = location_reference.get("locPointLocation") or location_reference
        extension = (
            point.get("locLocationExtensionG")
            or location_reference.get("_locationReferenceExtension")
            or location_reference.get("locLocationExtensionG")
            or {}
        )
        facility = extension.get("facilityLocation") or extension.get("FacilityLocation") or {}
        address = facility.get("address") or {}
        if not isinstance(address, dict):
            continue

        postcode = str(_value_field(address.get("postcode")) or "").strip()
        city = extract_first_lang_value(address.get("city"))

        address_line_parts: list[str] = []
        for line in _iter_values(address.get("addressLine")):
            text = extract_first_lang_value((line or {}).get("text")) if isinstance(line, dict) else ""
            if not text:
                text = extract_first_lang_value(line)
            if text:
                address_line_parts.append(text)
        full_address = " ".join(address_line_parts).strip()
        if postcode or city or full_address:
            return postcode, city, full_address

    return "", "", ""


def extract_datex_operator_name(site: dict[str, Any]) -> str:
    candidate_nodes: list[dict[str, Any]] = [site]
    for station in _iter_dict_items(site.get("energyInfrastructureStation")):
        candidate_nodes.append(station)
        for refill_point in _iter_dict_items(station.get("refillPoint")):
            candidate_nodes.append(refill_point)

    for candidate in candidate_nodes:
        for key in ("operator", "owner", "energyProvider"):
            node = candidate.get(key) or {}
            organisation = node.get("afacAnOrganisation") or node
            name = extract_first_lang_value(organisation.get("name"))
            if not name:
                name = extract_first_lang_value(organisation.get("legalName"))
            if name:
                return name
    return ""


def fetch_mobilithek_access_token(
    session: requests.Session,
    *,
    username_file: Path = MOBILITHEK_USERNAME_FILE,
    password_file: Path = MOBILITHEK_PASSWORD_FILE,
) -> str | None:
    username = get_env_text(MOBILITHEK_USERNAME_ENV)
    password = get_env_text(MOBILITHEK_PASSWORD_ENV)
    if not username or not password:
        file_username = read_optional_text(username_file)
        file_password = read_optional_text(password_file)
        if file_username and file_password:
            username = file_username
            password = file_password
    if not username or not password:
        return None

    response = request_with_retries(
        "POST",
        MOBILITHEK_TOKEN_URL,
        session,
        timeout=DATEX_REQUEST_TIMEOUT_SECONDS,
        verify=DATEX_TLS_VERIFY,
        data={
            "grant_type": "password",
            "client_id": "Platform",
            "username": username,
            "password": password,
        },
    )
    payload = response.json()
    token = str(payload.get("access_token") or "").strip()
    return token or None


def fetch_mobilithek_subscription_payload_with_mtls(
    *,
    subscription_id: str,
) -> dict[str, Any] | list[Any]:
    subscription_id = str(subscription_id or "").strip()
    if not subscription_id:
        raise RuntimeError("missing_subscription_id")
    if not MOBILITHEK_MACHINE_CERT_P12.exists():
        raise RuntimeError("missing_machine_certificate")
    if not MOBILITHEK_MACHINE_CERT_PASSWORD_FILE.exists():
        raise RuntimeError("missing_machine_certificate_password")

    password = MOBILITHEK_MACHINE_CERT_PASSWORD_FILE.read_text(encoding="utf-8").strip()
    if not password:
        raise RuntimeError("missing_machine_certificate_password")

    curl_path = shutil.which("curl") or "/usr/bin/curl"
    if not Path(curl_path).exists():
        raise RuntimeError("curl_not_found")

    url = DATEX_V3_SUBSCRIPTION_URL.format(subscription_id=subscription_id)
    with tempfile.TemporaryDirectory(prefix="mobilithek-static-subscription-") as temp_dir:
        header_path = Path(temp_dir) / "headers.txt"
        body_path = Path(temp_dir) / "body.bin"
        command = [
            curl_path,
            "-sS",
            "-L",
            "--max-time",
            str(DATEX_REQUEST_TIMEOUT_SECONDS),
            "-D",
            str(header_path),
            "-o",
            str(body_path),
            "-H",
            "Accept: application/json, application/octet-stream",
            "-H",
            "Accept-Encoding: gzip",
            "--cert-type",
            "P12",
            "--cert",
            f"{MOBILITHEK_MACHINE_CERT_P12}:{password}",
            url,
        ]

        try:
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                timeout=DATEX_REQUEST_TIMEOUT_SECONDS + 2,
            )
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError(f"{subscription_id}: timeout") from exc

        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            if "Operation timed out" in stderr or result.returncode == 28:
                raise TimeoutError(f"{subscription_id}: timeout")
            raise RuntimeError(stderr or f"curl_exit_{result.returncode}")

        header_text = header_path.read_text(encoding="utf-8", errors="replace") if header_path.exists() else ""
        status_code = parse_http_status_code(header_text)
        body = body_path.read_bytes() if body_path.exists() else b""
        if status_code >= 400:
            body_excerpt = body[:200].decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"http_{status_code}: {body_excerpt or 'request_failed'}")

        return decode_json_bytes(body)


def fetch_mobilithek_publication_payload(
    session: requests.Session,
    *,
    publication_id: str,
    requires_auth: bool,
    access_token: str | None,
) -> dict[str, Any]:
    endpoint_template = (
        MOBILITHEK_PUBLICATION_FILE_URL if requires_auth else MOBILITHEK_PUBLICATION_PUBLIC_FILE_URL
    )
    url = endpoint_template.format(publication_id=publication_id)
    payload = fetch_json_payload_from_url(
        session,
        url=url,
        access_token=access_token if requires_auth else None,
    )
    if not isinstance(payload, dict):
        raise ValueError("expected_json_object_payload")
    return payload


def fetch_json_payload_from_url(
    session: requests.Session,
    *,
    url: str,
    access_token: str | None = None,
) -> dict[str, Any] | list[Any]:
    headers = {"Accept": "application/json, application/octet-stream"}
    if access_token:
        headers["Authorization"] = f"Bearer {access_token}"
    response = request_with_retries(
        "GET",
        url,
        session,
        timeout=DATEX_REQUEST_TIMEOUT_SECONDS,
        verify=DATEX_TLS_VERIFY,
        headers=headers,
    )
    content = response.content
    return decode_json_bytes(content)


def parse_datex_static_sites(payload: dict[str, Any]) -> list[DatexStaticSite]:
    publication = _datex_publication_root(payload)
    sites: list[DatexStaticSite] = []

    for table in _iter_dict_items(publication.get("energyInfrastructureTable")):
        for site in _iter_dict_items(table.get("energyInfrastructureSite")):
            site_id = _reference_id(site)
            if not site_id:
                continue

            lat, lon = extract_datex_site_coordinates(site)
            if lat is None or lon is None:
                continue

            postcode, city, address = extract_datex_site_address(site)
            operator_name = extract_datex_operator_name(site)
            evse_ids: list[str] = []
            station_ids: list[str] = []
            total_evses = 0

            for station in _iter_dict_items(site.get("energyInfrastructureStation")):
                station_id = _reference_id(station)
                if station_id:
                    station_ids.append(station_id)

                refill_points = _iter_dict_items(station.get("refillPoint"))
                try:
                    station_count = int(float(_value_field(station.get("numberOfRefillPoints")) or 0))
                except (TypeError, ValueError):
                    station_count = 0
                if station_count <= 0:
                    station_count = len(refill_points)
                total_evses += max(0, station_count)

                for refill_point in refill_points:
                    charging_point = (
                        refill_point.get("aegiElectricChargingPoint")
                        or refill_point.get("aegiRefillPoint")
                        or refill_point
                    )
                    evse_id = normalize_evse_id(_reference_id(charging_point))
                    if evse_id:
                        evse_ids.append(evse_id)

            unique_station_ids = tuple(dict.fromkeys(item for item in station_ids if item))
            unique_evse_ids = tuple(dict.fromkeys(item for item in evse_ids if item))
            if total_evses <= 0:
                total_evses = len(unique_evse_ids)

            sites.append(
                DatexStaticSite(
                    site_id=site_id,
                    station_ids=unique_station_ids,
                    lat=lat,
                    lon=lon,
                    postcode=postcode,
                    city=city,
                    address=address,
                    operator_name=operator_name,
                    total_evses=max(0, total_evses),
                    evse_ids=unique_evse_ids,
                )
            )

    return sites


def normalize_datex_occupancy_status(
    status_value: Any,
    *,
    opening_status: Any = None,
    operation_status: Any = None,
    status_description: Any = None,
) -> str:
    candidates = [
        normalize_occupancy_status(status_value),
        normalize_occupancy_status(opening_status),
        normalize_occupancy_status(operation_status),
        normalize_occupancy_status(extract_first_lang_value(status_description)),
    ]

    available = {"AVAILABLE", "FREE", "OPEN"}
    occupied = {"OCCUPIED", "CHARGING", "BLOCKED", "RESERVED", "INUSE"}
    out_of_order = {
        "OUTOFORDER",
        "OUTOFSERVICE",
        "INOPERATIVE",
        "FAULTED",
        "CLOSED",
        "OFFLINE",
    }

    for candidate in candidates:
        if not candidate:
            continue
        if candidate in available:
            return "AVAILABLE"
        if candidate in occupied:
            return "OCCUPIED"
        if candidate in out_of_order:
            return "OUTOFORDER"
        if "AVAILABLE" in candidate:
            return "AVAILABLE"
        if "OCCUP" in candidate or "CHARG" in candidate:
            return "OCCUPIED"
        if "OUTOF" in candidate or "FAULT" in candidate or "CLOSED" in candidate:
            return "OUTOFORDER"

    return "UNKNOWN"


def parse_datex_dynamic_states(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    site_states: dict[str, dict[str, Any]] = {}

    candidate_containers: list[dict[str, Any]] = [payload]
    message_container = payload.get("messageContainer")
    if isinstance(message_container, dict):
        message_payload = message_container.get("payload")
        if isinstance(message_payload, dict):
            candidate_containers.append(message_payload)
        elif isinstance(message_payload, list):
            candidate_containers.extend(item for item in message_payload if isinstance(item, dict))

    direct_payload = payload.get("payload")
    if isinstance(direct_payload, dict):
        candidate_containers.append(direct_payload)
    elif isinstance(direct_payload, list):
        candidate_containers.extend(item for item in direct_payload if isinstance(item, dict))

    seen_container_ids: set[int] = set()
    for container in candidate_containers:
        container_id = id(container)
        if container_id in seen_container_ids:
            continue
        seen_container_ids.add(container_id)

        publications: list[dict[str, Any]] = []
        publication = container.get("aegiEnergyInfrastructureStatusPublication")
        if isinstance(publication, dict):
            publications.append(publication)
        dynamic_information = container.get("dynamicInformation")
        if isinstance(dynamic_information, dict) and dynamic_information.get("energyInfrastructureSiteStatus"):
            publications.append({"energyInfrastructureSiteStatus": dynamic_information.get("energyInfrastructureSiteStatus")})
        if container.get("energyInfrastructureSiteStatus"):
            publications.append({"energyInfrastructureSiteStatus": container.get("energyInfrastructureSiteStatus")})

        for publication in publications:
            for site_status in _iter_dict_items(publication.get("energyInfrastructureSiteStatus")):
                site_reference = _reference_id(site_status.get("reference"))
                if not site_reference:
                    continue

                state = site_states.setdefault(
                    site_reference,
                    {
                        "station_refs": set(),
                        "evses": {},
                        "last_updated_values": [],
                    },
                )

                site_last_updated = str(site_status.get("lastUpdated") or "").strip()
                if site_last_updated:
                    state["last_updated_values"].append(site_last_updated)

                for station_status in _iter_dict_items(site_status.get("energyInfrastructureStationStatus")):
                    station_reference = _reference_id(station_status.get("reference"))
                    if station_reference:
                        state["station_refs"].add(station_reference)

                    station_last_updated = str(station_status.get("lastUpdated") or "").strip()
                    if station_last_updated:
                        state["last_updated_values"].append(station_last_updated)

                    for refill_point_status in _iter_dict_items(station_status.get("refillPointStatus")):
                        charging_point_status = (
                            refill_point_status.get("aegiElectricChargingPointStatus")
                            or refill_point_status.get("aegiRefillPointStatus")
                            or refill_point_status
                        )
                        if not isinstance(charging_point_status, dict):
                            continue
                        evse_reference = normalize_evse_id(_reference_id(charging_point_status.get("reference")))
                        if not evse_reference:
                            continue

                        status = normalize_datex_occupancy_status(
                            _value_field(charging_point_status.get("status")),
                            opening_status=_value_field(charging_point_status.get("openingStatus")),
                            operation_status=_value_field(charging_point_status.get("operationStatus")),
                            status_description=charging_point_status.get("statusDescription"),
                        )
                        last_updated = str(charging_point_status.get("lastUpdated") or "").strip()
                        if last_updated:
                            state["last_updated_values"].append(last_updated)

                        state["evses"][evse_reference] = {
                            "status": status,
                            "last_updated": last_updated or station_last_updated or site_last_updated,
                        }

    return site_states


def build_datex_match_candidates(
    df: pd.DataFrame,
    *,
    operator_patterns: tuple[str, ...],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    normalized_patterns = tuple(pattern.lower() for pattern in operator_patterns if pattern)

    for _, row in df.iterrows():
        operator_name = str(row.get("operator", ""))
        operator_normalized = normalize_text(operator_name)
        if normalized_patterns and not any(pattern in operator_normalized for pattern in normalized_patterns):
            continue
        candidates.append(
            {
                "station_id": str(row["station_id"]),
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
                "postcode": str(row.get("postcode", "")).strip(),
                "city": str(row.get("city", "")).strip(),
                "address": str(row.get("address", "")).strip(),
                "charging_points_count": int(row.get("charging_points_count", 0) or 0),
            }
        )

    return candidates


def match_datex_sites_to_stations(
    df: pd.DataFrame,
    sites: list[DatexStaticSite],
    *,
    operator_patterns: tuple[str, ...],
    max_distance_m: float = DATEX_MAX_MATCH_DISTANCE_M,
) -> dict[str, str]:
    station_candidates = build_datex_match_candidates(df, operator_patterns=operator_patterns)
    scored_pairs: list[tuple[float, float, str, str]] = []

    for site in sites:
        for candidate in station_candidates:
            distance_m = haversine_distance_m(site.lat, site.lon, candidate["lat"], candidate["lon"])
            if distance_m > max_distance_m:
                continue

            score = distance_m
            if site.postcode and site.postcode == candidate["postcode"]:
                score -= 50.0
            if site.city and normalize_text(site.city) == normalize_text(candidate["city"]):
                score -= 15.0
            if site.address and candidate["address"]:
                site_address_norm = normalize_text(site.address)
                candidate_address_norm = normalize_text(candidate["address"])
                if site_address_norm and candidate_address_norm and (
                    site_address_norm in candidate_address_norm
                    or candidate_address_norm in site_address_norm
                ):
                    score -= 10.0
            if site.total_evses > 0 and candidate["charging_points_count"] > 0:
                score += abs(site.total_evses - candidate["charging_points_count"]) * 3.0

            scored_pairs.append((score, distance_m, site.site_id, candidate["station_id"]))

    scored_pairs.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
    matches: dict[str, str] = {}
    used_station_ids: set[str] = set()

    for _, _, site_id, station_id in scored_pairs:
        if site_id in matches or station_id in used_station_ids:
            continue
        matches[site_id] = station_id
        used_station_ids.add(station_id)

    return matches


def known_occupancy_evses(summary: dict[str, Any]) -> int:
    total = int(summary.get("occupancy_total_evses", 0) or 0)
    unknown = int(summary.get("occupancy_unknown_evses", 0) or 0)
    return max(0, total - unknown)


def should_replace_occupancy(existing: dict[str, Any], candidate: dict[str, Any]) -> bool:
    existing_total = int(existing.get("occupancy_total_evses", 0) or 0)
    candidate_total = int(candidate.get("occupancy_total_evses", 0) or 0)
    if candidate_total <= 0:
        return False
    if existing_total <= 0:
        return True

    existing_known = known_occupancy_evses(existing)
    candidate_known = known_occupancy_evses(candidate)
    if candidate_known != existing_known:
        return candidate_known > existing_known
    if candidate_total != existing_total:
        return candidate_total > existing_total
    return False


def combine_occupancy_stats(*stats_objects: dict[str, Any]) -> dict[str, Any]:
    combined: dict[str, Any] = {
        "sources_discovered": 0,
        "sources_used": 0,
        "locations_scanned": 0,
        "matched_locations": 0,
        "matched_stations": 0,
        "matched_evses": 0,
        "errors": [],
        "sources": [],
    }

    for stats in stats_objects:
        combined["sources_discovered"] += int(stats.get("sources_discovered", 0) or 0)
        combined["sources_used"] += int(stats.get("sources_used", 0) or 0)
        combined["locations_scanned"] += int(stats.get("locations_scanned", 0) or 0)
        combined["matched_locations"] += int(stats.get("matched_locations", 0) or 0)
        combined["matched_stations"] += int(stats.get("matched_stations", 0) or 0)
        combined["matched_evses"] += int(stats.get("matched_evses", 0) or 0)
        combined["errors"].extend(stats.get("errors", []))
        combined["sources"].extend(stats.get("sources", []))

    return combined


def merge_unique_text_lists(series: pd.Series) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()

    for value in series:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue

        if isinstance(value, (list, tuple, set)):
            candidates = value
        else:
            candidates = [value]

        for candidate in candidates:
            text = str(candidate).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            merged.append(text)

    return merged


def join_unique_display_values(values: list[str], *, separator: str = " | ") -> str:
    merged = merge_unique_text_lists(pd.Series(values, dtype="object"))
    return separator.join(merged)


def split_structured_text(value: Any) -> list[str]:
    text = normalize_optional_text(value)
    if not text:
        return []
    parts = [part.strip() for part in re.split(r"[;|,]", text) if part.strip()]
    return list(dict.fromkeys(parts))


def normalize_bnetza_opening_hours(
    opening_hours: Any,
    opening_days: Any,
    opening_times: Any,
) -> str:
    opening_text = normalize_optional_text(opening_hours)
    normalized_opening = normalize_text(opening_text)
    if normalized_opening in {"247", "24h", "24std", "24stunden", "24hours", "24x7"}:
        return "24/7"
    if opening_text and normalized_opening not in {"keineangabe", "nan", "nichtbekannt"}:
        return opening_text

    days_text = normalize_optional_text(opening_days)
    times_text = normalize_optional_text(opening_times)
    if days_text and times_text:
        return f"{days_text}: {times_text}"
    if times_text:
        return times_text
    if days_text:
        return days_text
    return ""


def slugify(value: str) -> str:
    folded = unicodedata.normalize("NFKD", value)
    ascii_only = "".join(ch for ch in folded if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "_", ascii_only.lower()).strip("_")


def stem_words(value: str) -> list[str]:
    words = re.findall(r"[A-Za-z0-9]+", value)
    return [word.lower() for word in words if word]


def operator_tokens(*values: str) -> set[str]:
    tokens: set[str] = set()
    for value in values:
        for word in stem_words(value):
            if len(word) < 4 or word in GENERIC_OPERATOR_WORDS:
                continue
            tokens.add(word)
            compact = normalize_text(word)
            if compact:
                tokens.add(compact)
        compact_value = normalize_text(value)
        if compact_value and len(compact_value) >= 4 and compact_value not in GENERIC_OPERATOR_WORDS:
            tokens.add(compact_value)
    return tokens


def address_similarity(site_address: str, candidate_address: str) -> bool:
    left = normalize_text(site_address)
    right = normalize_text(candidate_address)
    if not left or not right:
        return False
    return left in right or right in left


def operator_similarity(*, site_operator: str, publisher: str, candidate_operator: str) -> float:
    if not candidate_operator:
        return 0.0

    candidate_norm = normalize_text(candidate_operator)
    if not candidate_norm:
        return 0.0

    for source in (site_operator, publisher):
        source_norm = normalize_text(source)
        if not source_norm:
            continue
        if source_norm in candidate_norm or candidate_norm in source_norm:
            return 1.0

    source_tokens = operator_tokens(site_operator, publisher)
    candidate_tokens = operator_tokens(candidate_operator)
    if not source_tokens or not candidate_tokens:
        return 0.0

    overlap = source_tokens & candidate_tokens
    if not overlap:
        return 0.0

    return len(overlap) / max(1, min(len(source_tokens), len(candidate_tokens)))


def station_operator_candidates(station_row: dict[str, Any]) -> list[str]:
    return merge_unique_text_lists(
        pd.Series(
            [
                station_row.get("operator", ""),
                station_row.get("bnetza_display_name", ""),
                *(station_row.get("operator_aliases") or []),
            ],
            dtype="object",
        )
    )


def normalize_occupancy_status(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return re.sub(r"[^A-Z]", "", str(value).upper())


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
            meta_path.write_text(dumps_pretty_json(metadata), encoding="utf-8")
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


def fetch_optional_auxiliary_csv(
    session: requests.Session,
    *,
    url: str,
    cache_path: Path,
    meta_path: Path,
    label: str,
) -> dict[str, Any]:
    try:
        response = request_with_retries("GET", url, session, timeout=60)
        content = response.content
        cache_path.write_bytes(content)
        metadata = {
            "source_url": url,
            "label": label,
            "fetched_at": utc_now().isoformat(),
            "bytes": len(content),
            "content_type": response.headers.get("content-type", ""),
        }
        meta_path.write_text(dumps_pretty_json(metadata), encoding="utf-8")
        return metadata
    except requests.RequestException as exc:
        fallback_meta: dict[str, Any] = {
            "source_url": url,
            "label": label,
            "fetched_at": utc_now().isoformat(),
            "error": str(exc),
        }
        if cache_path.exists():
            fallback_meta["cache_only"] = True
            if meta_path.exists():
                try:
                    previous_meta = json.loads(meta_path.read_text(encoding="utf-8"))
                    fallback_meta["previous_source_url"] = previous_meta.get("source_url")
                except json.JSONDecodeError:
                    pass
            return fallback_meta
        fallback_meta["unavailable"] = True
        return fallback_meta


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


def load_bnetza_api_station_aliases(path: Path) -> dict[str, list[str]]:
    if not path.exists():
        return {}

    encoding = detect_csv_encoding(path)
    df = pd.read_csv(
        path,
        sep=";",
        dtype=str,
        encoding=encoding,
        engine="python",
        on_bad_lines="skip",
    ).dropna(axis=1, how="all")

    station_id_col = find_column(df, ["ladestationid"], contains=True)
    if not station_id_col:
        return {}

    alias_columns = [
        find_column(df, ["betreiber"], contains=False),
        find_column(df, ["betreiberanzeigename"], contains=True),
        find_column(df, ["betreiberbereinigt"], contains=True),
    ]
    alias_columns = [column for column in alias_columns if column]

    aliases_by_station_id: dict[str, list[str]] = {}
    for row in df.to_dict("records"):
        station_id = normalize_optional_text(row.get(station_id_col))
        if not station_id:
            continue
        aliases = merge_unique_text_lists(
            pd.Series([row.get(column, "") for column in alias_columns], dtype="object")
        )
        if aliases:
            aliases_by_station_id[station_id] = aliases

    return aliases_by_station_id


def build_fast_charger_frame(
    raw_df: pd.DataFrame,
    min_power_kw: float,
    *,
    bnetza_api_station_aliases: dict[str, list[str]] | None = None,
) -> pd.DataFrame:
    return _build_grouped_bnetza_station_frame(
        raw_df,
        bnetza_api_station_aliases=bnetza_api_station_aliases,
        active_only=True,
        min_power_kw=min_power_kw,
    )


def _station_group_key(lat: Any, lon: Any, operator: Any) -> tuple[str, str, str]:
    return (
        f"{float(lat):.7f}",
        f"{float(lon):.7f}",
        str(operator or "").strip(),
    )


def _build_grouped_bnetza_station_frame(
    raw_df: pd.DataFrame,
    *,
    bnetza_api_station_aliases: dict[str, list[str]] | None = None,
    active_only: bool,
    min_power_kw: float | None = None,
    legacy_station_ids_by_group_key: dict[tuple[str, str, str], str] | None = None,
    include_has_active_record: bool = False,
) -> pd.DataFrame:
    df = raw_df.copy()
    bnetza_api_station_aliases = bnetza_api_station_aliases or {}
    legacy_station_ids_by_group_key = legacy_station_ids_by_group_key or {}

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

    power_col = find_column(
        df,
        [
            "nennleistung ladeeinrichtung",
            "nennleistungladeeinrichtung",
            "leistung kw",
            "max power",
            "leistung",
        ],
        contains=True,
    )
    if not power_col:
        if len(df.columns) <= BNETZA_POWER_COLUMN_INDEX:
            raise RuntimeError(
                "BNetzA CSV does not have a 7th column for power extraction "
                f"(found {len(df.columns)} columns)"
            )
        power_col = df.columns[BNETZA_POWER_COLUMN_INDEX]
    raw_max_power_kw = to_max_numeric_token_float(df[power_col])
    capped_max_power_kw = to_max_numeric_token_float(
        df[power_col],
        clamp_max=MAX_REASONABLE_DISPLAY_POWER_KW,
    )

    charging_points_col = find_column(
        df,
        ["anzahl ladepunkte", "anzahlladepunkte", "ladepunkte"],
        contains=True,
    )
    ladestation_id_col = find_column(
        df,
        ["ladeeinrichtungsid", "ladestationid"],
        contains=True,
    )
    if charging_points_col:
        df["charging_points_count_row"] = to_float(df[charging_points_col]).fillna(1.0)
    else:
        df["charging_points_count_row"] = 1.0
    df["charging_points_count_row"] = df["charging_points_count_row"].clip(lower=1.0)
    if ladestation_id_col:
        df["bnetza_ladestation_ids_row"] = df[ladestation_id_col].apply(
            lambda value: merge_unique_text_lists(
                pd.Series([normalize_optional_text(value)], dtype="object")
            )
        )
    else:
        df["bnetza_ladestation_ids_row"] = [[] for _ in range(len(df))]

    connector_power_cols = [
        col for col in df.columns if normalize_text(col).startswith("nennleistungstecker")
    ]
    connector_type_cols = [col for col in df.columns if normalize_text(col).startswith("steckertypen")]
    evse_id_cols = [col for col in df.columns if normalize_text(col).startswith("evseid")]
    if connector_power_cols:
        connector_power_frame = pd.DataFrame(
            {
                col: to_max_numeric_token_float(
                    df[col],
                    clamp_max=MAX_REASONABLE_DISPLAY_POWER_KW,
                )
                for col in connector_power_cols
            },
            index=df.index,
        )
        connector_max = connector_power_frame.max(axis=1, skipna=True)
    else:
        connector_max = pd.Series(index=df.index, dtype="float64")

    per_point_fallback = raw_max_power_kw / df["charging_points_count_row"].replace(0, pd.NA)
    capped_per_point_fallback = per_point_fallback.clip(
        upper=MAX_REASONABLE_DISPLAY_POWER_KW
    )
    df["max_individual_power_kw_row"] = (
        connector_max
        .fillna(capped_per_point_fallback)
        .fillna(capped_max_power_kw)
    )
    # User-facing "max kW" should represent plausible stall power, not
    # summed cabinet/site power from source rows.
    df["max_power_kw"] = df["max_individual_power_kw_row"]
    if evse_id_cols:
        df["evse_ids_row"] = df[evse_id_cols].apply(
            lambda row: merge_unique_text_lists(
                pd.Series([normalize_evse_id(value) for value in row if normalize_evse_id(value)])
            ),
            axis=1,
        )
    else:
        df["evse_ids_row"] = [[] for _ in range(len(df))]

    if connector_type_cols:
        df["connector_types_row"] = df[connector_type_cols].apply(
            lambda row: merge_unique_text_lists(
                pd.Series(
                    [item for value in row for item in split_structured_text(value)],
                    dtype="object",
                )
            ),
            axis=1,
        )
    else:
        df["connector_types_row"] = [[] for _ in range(len(df))]

    df["lat"] = to_float(df[lat_col])
    df["lon"] = to_float(df[lon_col])

    # If columns appear swapped, correct them.
    if df["lat"].abs().max(skipna=True) > 90 and df["lon"].abs().max(skipna=True) <= 90:
        df[["lat", "lon"]] = df[["lon", "lat"]]

    if status_col:
        status_norm = df[status_col].fillna("").str.lower().str.strip()
        row_is_active = status_norm.isin({"in betrieb", "inbetrieb", "in betriebnahme"})
        row_is_active |= status_norm.str.contains("in betrieb", regex=False)
    else:
        row_is_active = pd.Series([True] * len(df), index=df.index)

    df["row_is_active"] = row_is_active.astype(bool)

    # Geographic sanity filter for Germany (+small buffer).
    df = df[(df["lat"].between(46.0, 56.5)) & (df["lon"].between(5.0, 16.5))]
    if active_only:
        df = df[df["row_is_active"]]
    if min_power_kw is not None:
        df = df[df["max_power_kw"] >= min_power_kw]

    address_parts: list[str] = []
    for hint in ["strasse", "straße", "hausnummer", "postleitzahl", "ort", "stadt"]:
        col = find_column(df, [hint], contains=True)
        if col and col not in address_parts:
            address_parts.append(col)

    display_name_col = find_column(df, ["anzeigename", "kartenname"], contains=True)
    location_name_col = find_column(df, ["standortbezeichnung"], contains=True)
    parking_info_col = find_column(df, ["parkraum"], contains=True)
    payment_systems_col = find_column(df, ["bezahlsysteme"], contains=True)
    opening_hours_col = find_column(df, ["oeffnungszeiten"], contains=False)
    opening_days_col = find_column(df, ["oeffnungszeitenwochentage"], contains=True)
    opening_times_col = find_column(df, ["oeffnungszeitentageszeiten"], contains=True)
    commissioned_col = find_column(df, ["inbetriebnahmedatum"], contains=True)

    if operator_col:
        df["operator"] = df[operator_col].fillna("").str.strip()
    else:
        df["operator"] = "Unbekannt"

    if address_parts:
        if df.empty:
            df["address"] = pd.Series(index=df.index, dtype="object")
        else:
            df["address"] = (
                df[address_parts]
                .fillna("")
                .astype(str)
                .apply(lambda row: " ".join(row), axis=1)
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

    df["bnetza_display_name"] = (
        df[display_name_col].fillna("").astype(str).str.strip()
        if display_name_col
        else ""
    )
    df["bnetza_location_name"] = (
        df[location_name_col].fillna("").astype(str).str.strip()
        if location_name_col
        else ""
    )
    df["bnetza_parking_info"] = (
        df[parking_info_col].fillna("").astype(str).str.strip()
        if parking_info_col
        else ""
    )
    df["bnetza_payment_systems"] = (
        df[payment_systems_col].fillna("").astype(str).str.strip()
        if payment_systems_col
        else ""
    )
    df["bnetza_opening_hours_raw"] = (
        df[opening_hours_col].fillna("").astype(str).str.strip()
        if opening_hours_col
        else ""
    )
    df["bnetza_opening_days"] = (
        df[opening_days_col].fillna("").astype(str).str.strip()
        if opening_days_col
        else ""
    )
    df["bnetza_opening_times"] = (
        df[opening_times_col].fillna("").astype(str).str.strip()
        if opening_times_col
        else ""
    )
    if df.empty:
        df["bnetza_opening_hours"] = pd.Series(index=df.index, dtype="object")
    else:
        df["bnetza_opening_hours"] = df.apply(
            lambda row: normalize_bnetza_opening_hours(
                row.get("bnetza_opening_hours_raw", ""),
                row.get("bnetza_opening_days", ""),
                row.get("bnetza_opening_times", ""),
            ),
            axis=1,
        )
    df["bnetza_commissioned_at"] = (
        df[commissioned_col].fillna("").astype(str).str.strip()
        if commissioned_col
        else ""
    )
    if df.empty:
        df["operator_aliases_row"] = pd.Series(index=df.index, dtype="object")
    else:
        df["operator_aliases_row"] = df.apply(
            lambda row: merge_unique_text_lists(
                pd.Series(
                    [
                        row.get("operator", ""),
                        row.get("bnetza_display_name", ""),
                        *(
                            alias
                            for ladestation_id in row.get("bnetza_ladestation_ids_row", [])
                            for alias in bnetza_api_station_aliases.get(str(ladestation_id), [])
                        ),
                    ],
                    dtype="object",
                )
            ),
            axis=1,
        )

    def first_nonempty(series: pd.Series) -> str:
        for value in series:
            text = str(value).strip()
            if text:
                return text
        return ""

    df = df.sort_values(
        by=["row_is_active", "max_power_kw"],
        ascending=[False, False],
        kind="mergesort",
    )
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
            evse_ids=("evse_ids_row", merge_unique_text_lists),
            connector_types=("connector_types_row", merge_unique_text_lists),
            bnetza_ladestation_ids=("bnetza_ladestation_ids_row", merge_unique_text_lists),
            operator_aliases=("operator_aliases_row", merge_unique_text_lists),
            bnetza_display_name=("bnetza_display_name", first_nonempty),
            bnetza_location_name=("bnetza_location_name", first_nonempty),
            bnetza_parking_info=("bnetza_parking_info", first_nonempty),
            bnetza_payment_systems=("bnetza_payment_systems", first_nonempty),
            bnetza_opening_hours=("bnetza_opening_hours", first_nonempty),
            bnetza_opening_days=("bnetza_opening_days", first_nonempty),
            bnetza_opening_times=("bnetza_opening_times", first_nonempty),
            bnetza_commissioned_at=("bnetza_commissioned_at", first_nonempty),
            has_active_record=("row_is_active", "max"),
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

    impossible_display_power = (
        (df["max_power_kw"] > MAX_REASONABLE_DISPLAY_POWER_KW)
        | (df["max_individual_power_kw"] > MAX_REASONABLE_DISPLAY_POWER_KW)
    )
    if impossible_display_power.any():
        sample = df.loc[
            impossible_display_power,
            ["operator", "postcode", "city", "address", "max_power_kw", "max_individual_power_kw"],
        ].head(10)
        raise RuntimeError(
            "Detected impossible output power values above "
            f"{MAX_REASONABLE_DISPLAY_POWER_KW:.0f} kW after normalization. "
            f"Sample rows:\n{sample.to_string(index=False)}"
        )

    def station_id(row: pd.Series) -> str:
        legacy_station_id = legacy_station_ids_by_group_key.get(
            _station_group_key(row["lat"], row["lon"], row["operator"])
        )
        if legacy_station_id:
            return legacy_station_id
        raw = f"{row['lat']:.7f}|{row['lon']:.7f}|{row['operator']}|{row['address']}"
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
        "connector_types",
        "bnetza_ladestation_ids",
        "operator_aliases",
        "bnetza_display_name",
        "bnetza_location_name",
        "bnetza_parking_info",
        "bnetza_payment_systems",
        "bnetza_opening_hours",
        "bnetza_opening_days",
        "bnetza_opening_times",
        "bnetza_commissioned_at",
        "evse_ids",
    ]
    if include_has_active_record:
        selected_columns.append("has_active_record")

    return df[selected_columns].copy()


def build_full_registry_station_frame(
    raw_df: pd.DataFrame,
    *,
    bnetza_api_station_aliases: dict[str, list[str]] | None = None,
    legacy_station_ids_by_group_key: dict[tuple[str, str, str], str] | None = None,
) -> pd.DataFrame:
    return _build_grouped_bnetza_station_frame(
        raw_df,
        bnetza_api_station_aliases=bnetza_api_station_aliases,
        active_only=False,
        min_power_kw=None,
        legacy_station_ids_by_group_key=legacy_station_ids_by_group_key,
        include_has_active_record=True,
    )


def build_fast_projection_from_full_registry(
    full_df: pd.DataFrame,
    *,
    min_power_kw: float,
) -> pd.DataFrame:
    projected = full_df[
        full_df["has_active_record"].fillna(False).astype(bool)
        & (full_df["max_power_kw"].fillna(0.0) >= min_power_kw)
    ].copy()
    return projected.drop(columns=["has_active_record"], errors="ignore").reset_index(drop=True)


def build_under_power_projection_from_full_registry(
    full_df: pd.DataFrame,
    *,
    max_power_kw: float,
) -> pd.DataFrame:
    projected = full_df[
        full_df["has_active_record"].fillna(False).astype(bool)
        & (full_df["max_power_kw"].fillna(0.0) < max_power_kw)
    ].copy()
    return projected.drop(columns=["has_active_record"], errors="ignore").reset_index(drop=True)


def attach_empty_amenity_columns(df: pd.DataFrame, *, source: str = "not_enriched") -> pd.DataFrame:
    prepared = df.copy()
    for rule in AMENITY_RULES:
        prepared[f"amenity_{rule.key}"] = 0
    prepared["amenities_total"] = 0
    prepared["amenities_source"] = source
    prepared["amenity_examples"] = "[]"
    return prepared


def filter_fast_chargers_with_amenities(df: pd.DataFrame) -> pd.DataFrame:
    if "amenities_total" not in df.columns:
        return df.copy().reset_index(drop=True)

    amenity_counts = pd.to_numeric(df["amenities_total"], errors="coerce").fillna(0)
    return df[amenity_counts > 0].copy().reset_index(drop=True)


def fetch_live_occupancy_sources(session: requests.Session) -> list[dict[str, str]]:
    response = request_with_retries(
        "GET",
        MOBIDATA_SOURCES_URL,
        session,
        timeout=MOBIDATA_TIMEOUT_SECONDS,
    )
    payload = response.json()
    items = payload.get("items", [])
    sources: list[dict[str, str]] = []

    for item in items:
        uid = str(item.get("uid", "")).strip()
        if not uid or uid in MOBIDATA_IGNORED_SOURCE_UIDS:
            continue
        if str(item.get("realtime_status", "")).upper() != "ACTIVE":
            continue

        name = str(item.get("name", "")).strip() or uid
        sources.append({"uid": uid, "name": name})

    sources.sort(key=lambda item: item["uid"])
    return sources


def merge_location_occupancy(
    location: dict[str, Any],
    *,
    source_uid: str,
    source_name: str,
    evse_to_station: dict[str, str],
    station_occupancy: dict[str, dict[str, Any]],
) -> tuple[set[str], set[str]]:
    matched_station_ids: set[str] = set()
    matched_evse_ids: set[str] = set()

    for pool in location.get("charging_pool", []) or []:
        for evse in pool.get("evses", []) or []:
            evse_id = normalize_evse_id(evse.get("evse_id") or evse.get("original_uid"))
            if not evse_id:
                continue

            station_id = evse_to_station.get(evse_id)
            if not station_id:
                continue

            matched_station_ids.add(station_id)
            matched_evse_ids.add(evse_id)

            station_state = station_occupancy.setdefault(
                station_id,
                {
                    "source_uids": set(),
                    "source_names": set(),
                    "evses": {},
                },
            )
            station_state["source_uids"].add(source_uid)
            station_state["source_names"].add(source_name)

            status = normalize_occupancy_status(evse.get("status")) or "UNKNOWN"
            last_updated = (
                str(evse.get("last_updated") or pool.get("last_updated") or location.get("last_updated") or "")
                .strip()
            )
            existing = station_state["evses"].get(evse_id)
            candidate_dt = parse_iso_datetime(last_updated)
            existing_dt = parse_iso_datetime(existing.get("last_updated")) if existing else None

            should_replace = existing is None
            if existing is not None and candidate_dt is not None:
                should_replace = existing_dt is None or candidate_dt >= existing_dt
            elif existing is not None and existing_dt is None and candidate_dt is None:
                should_replace = bool(status)

            if should_replace:
                station_state["evses"][evse_id] = {
                    "status": status,
                    "last_updated": last_updated,
                }

    return matched_station_ids, matched_evse_ids


def summarize_station_occupancy(
    station_state: dict[str, Any],
    *,
    total_evses: int | None = None,
) -> dict[str, Any]:
    status_counts: defaultdict[str, int] = defaultdict(int)
    timestamps: list[str] = []

    for evse in station_state.get("evses", {}).values():
        status = normalize_occupancy_status(evse.get("status")) or "UNKNOWN"
        status_counts[status] += 1
        last_updated = str(evse.get("last_updated", "")).strip()
        if last_updated:
            timestamps.append(last_updated)

    derived_total = int(sum(status_counts.values()))
    available = int(sum(status_counts[status] for status in OCCUPANCY_AVAILABLE_STATUSES))
    occupied = int(sum(status_counts[status] for status in OCCUPANCY_OCCUPIED_STATUSES))
    charging = int(status_counts["CHARGING"])
    out_of_order = int(sum(status_counts[status] for status in OCCUPANCY_OUT_OF_ORDER_STATUSES))
    reported_unknown = int(sum(status_counts[status] for status in OCCUPANCY_UNKNOWN_STATUSES))
    total = int(max(derived_total, total_evses or 0))
    unknown = int(max(reported_unknown, total - available - occupied - out_of_order))

    summary_status = ""
    if total > 0:
        if available > 0:
            summary_status = "AVAILABLE"
        elif occupied > 0:
            summary_status = "OCCUPIED"
        elif out_of_order > 0 and unknown == 0 and out_of_order == total:
            summary_status = "OUT_OF_ORDER"
        else:
            summary_status = "UNKNOWN"

    return {
        "occupancy_source_uid": "|".join(sorted(station_state.get("source_uids", set()))),
        "occupancy_source_name": " / ".join(sorted(station_state.get("source_names", set()))),
        "occupancy_status": summary_status,
        "occupancy_last_updated": choose_latest_timestamp(timestamps),
        "occupancy_total_evses": total,
        "occupancy_available_evses": available,
        "occupancy_occupied_evses": occupied,
        "occupancy_charging_evses": charging,
        "occupancy_out_of_order_evses": out_of_order,
        "occupancy_unknown_evses": unknown,
    }


def enrich_with_live_occupancy(
    df: pd.DataFrame,
    *,
    session: requests.Session,
    progress_every: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    enriched = df.copy()
    default_columns: dict[str, Any] = {
        "occupancy_source_uid": "",
        "occupancy_source_name": "",
        "occupancy_status": "",
        "occupancy_last_updated": "",
        "occupancy_total_evses": 0,
        "occupancy_available_evses": 0,
        "occupancy_occupied_evses": 0,
        "occupancy_charging_evses": 0,
        "occupancy_out_of_order_evses": 0,
        "occupancy_unknown_evses": 0,
    }
    for column, default in default_columns.items():
        enriched[column] = default

    evse_to_station: dict[str, str] = {}
    station_row_lookup: dict[str, int] = {}
    for row_index, row in enriched.reset_index(drop=True).iterrows():
        station_id = str(row["station_id"])
        station_row_lookup[station_id] = row_index
        for evse_id in row.get("evse_ids", []):
            normalized = normalize_evse_id(evse_id)
            if normalized and normalized not in evse_to_station:
                evse_to_station[normalized] = station_id

    stats: dict[str, Any] = {
        "sources_discovered": 0,
        "sources_used": 0,
        "locations_scanned": 0,
        "matched_locations": 0,
        "matched_stations": 0,
        "matched_evses": 0,
        "errors": [],
        "sources": [],
    }

    if not evse_to_station:
        return enriched, stats

    try:
        sources = fetch_live_occupancy_sources(session)
    except (requests.RequestException, ValueError) as exc:
        stats["errors"].append(f"source_discovery_failed: {exc}")
        log_info(f"Live occupancy source discovery failed: {exc}")
        return enriched, stats

    stats["sources_discovered"] = len(sources)
    if not sources:
        return enriched, stats

    station_occupancy: dict[str, dict[str, Any]] = {}
    matched_station_ids: set[str] = set()
    matched_evses: set[str] = set()

    for source in sources:
        source_uid = source["uid"]
        source_name = source["name"]
        source_locations_scanned = 0
        source_matched_locations = 0
        source_matched_stations: set[str] = set()
        source_matched_evses: set[str] = set()
        offset = 0
        total_count = 0

        try:
            while True:
                response = request_with_retries(
                    "GET",
                    MOBIDATA_OCPI_LOCATIONS_URL,
                    session,
                    timeout=MOBIDATA_TIMEOUT_SECONDS,
                    params={
                        "source_uid": source_uid,
                        "limit": MOBIDATA_PAGE_LIMIT,
                        "offset": offset,
                    },
                )
                payload = response.json()
                items = payload.get("items", [])
                total_count = int(payload.get("total_count", source_locations_scanned + len(items)))
                if not items:
                    break

                source_locations_scanned += len(items)
                stats["locations_scanned"] += len(items)

                for location in items:
                    location_station_ids, location_evse_ids = merge_location_occupancy(
                        location,
                        source_uid=source_uid,
                        source_name=source_name,
                        evse_to_station=evse_to_station,
                        station_occupancy=station_occupancy,
                    )
                    if location_station_ids:
                        source_matched_locations += 1
                        stats["matched_locations"] += 1
                        source_matched_stations.update(location_station_ids)
                        source_matched_evses.update(location_evse_ids)

                if progress_every > 0 and (
                    source_locations_scanned % progress_every == 0
                    or source_locations_scanned >= total_count
                ):
                    log_info(
                        "Live occupancy source progress: "
                        f"{source_uid} {source_locations_scanned}/{total_count or '?'} "
                        f"(matched stations: {len(source_matched_stations)})"
                    )

                offset += len(items)
                if offset >= total_count:
                    break
        except (requests.RequestException, ValueError) as exc:
            stats["errors"].append(f"{source_uid}: {exc}")
            log_info(f"Live occupancy fetch failed for {source_uid}: {exc}")
            continue

        stats["sources"].append(
            {
                "uid": source_uid,
                "name": source_name,
                "locations_scanned": int(source_locations_scanned),
                "matched_locations": int(source_matched_locations),
                "matched_stations": int(len(source_matched_stations)),
                "matched_evses": int(len(source_matched_evses)),
            }
        )
        matched_station_ids.update(source_matched_stations)
        matched_evses.update(source_matched_evses)

    stats["sources_used"] = len(stats["sources"])

    for station_id, station_state in station_occupancy.items():
        row_index = station_row_lookup.get(station_id)
        if row_index is None:
            continue
        summary = summarize_station_occupancy(station_state)
        for column, value in summary.items():
            enriched.at[row_index, column] = value

    stats["matched_stations"] = int(len(matched_station_ids))
    stats["matched_evses"] = int(len(matched_evses))
    return enriched, stats


def extract_occupancy_summary_from_row(row: pd.Series) -> dict[str, Any]:
    return {
        "occupancy_source_uid": row.get("occupancy_source_uid", ""),
        "occupancy_source_name": row.get("occupancy_source_name", ""),
        "occupancy_status": row.get("occupancy_status", ""),
        "occupancy_last_updated": row.get("occupancy_last_updated", ""),
        "occupancy_total_evses": int(row.get("occupancy_total_evses", 0) or 0),
        "occupancy_available_evses": int(row.get("occupancy_available_evses", 0) or 0),
        "occupancy_occupied_evses": int(row.get("occupancy_occupied_evses", 0) or 0),
        "occupancy_charging_evses": int(row.get("occupancy_charging_evses", 0) or 0),
        "occupancy_out_of_order_evses": int(row.get("occupancy_out_of_order_evses", 0) or 0),
        "occupancy_unknown_evses": int(row.get("occupancy_unknown_evses", 0) or 0),
    }


def enrich_with_mobilithek_datex(
    df: pd.DataFrame,
    *,
    session: requests.Session,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    try:
        datex_publications = load_registry_datex_publications()
    except (OSError, ValueError, TypeError) as exc:
        datex_publications = []
        registry_source_error = f"registry_datex_publications_failed: {exc}"
    else:
        registry_source_error = ""

    if not datex_publications:
        datex_publications = list(MOBILITHEK_DATEX_PUBLICATIONS)

    try:
        direct_datex_sources = load_direct_datex_sources()
    except (OSError, ValueError, TypeError) as exc:
        direct_datex_sources = []
        direct_source_error = f"direct_datex_registry_failed: {exc}"
    else:
        direct_source_error = ""
    for source in direct_datex_sources:
        datex_publications.append(
            {
                "uid": source.provider_uid,
                "name": source.display_name or source.publisher or source.provider_uid,
                "operator_patterns": (),
                "dynamic_url": source.dynamic_url,
                "static_url": source.static_url,
                "requires_auth": False,
            }
        )

    enriched = df.copy()
    stats: dict[str, Any] = {
        "sources_discovered": len(datex_publications),
        "sources_used": 0,
        "locations_scanned": 0,
        "matched_locations": 0,
        "matched_stations": 0,
        "matched_evses": 0,
        "errors": [],
        "sources": [],
    }
    if registry_source_error:
        stats["errors"].append(registry_source_error)
    if direct_source_error:
        stats["errors"].append(direct_source_error)

    access_token: str | None = None
    if any(
        publication.get("requires_auth")
        or str(publication.get("static_access_mode") or "").strip() == "auth"
        or str(publication.get("dynamic_access_mode") or "").strip() == "auth"
        for publication in datex_publications
    ):
        try:
            access_token = fetch_mobilithek_access_token(session)
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            stats["errors"].append(f"mobilithek_auth_failed: {exc}")

    updated_station_ids: set[str] = set()
    updated_evse_ids: set[str] = set()

    for publication in datex_publications:
        publication_uid = str(publication["uid"])
        publication_name = str(publication["name"])
        requires_auth = bool(publication.get("requires_auth"))
        static_url = str(publication.get("static_url") or "").strip()
        dynamic_url = str(publication.get("dynamic_url") or "").strip()
        static_access_mode = str(publication.get("static_access_mode") or "").strip()
        dynamic_access_mode = str(publication.get("dynamic_access_mode") or "").strip()
        static_subscription_id = str(publication.get("static_subscription_id") or "").strip()
        dynamic_subscription_id = str(publication.get("dynamic_subscription_id") or "").strip()
        static_access_url = str(publication.get("static_access_url") or "").strip()
        dynamic_access_url = str(publication.get("dynamic_access_url") or "").strip()

        if (
            (requires_auth or static_access_mode == "auth" or dynamic_access_mode == "auth")
            and not access_token
            and not static_subscription_id
            and not dynamic_subscription_id
        ):
            stats["errors"].append(f"{publication_uid}: missing_mobilithek_access_token")
            continue

        try:
            if static_url and dynamic_url:
                static_payload = fetch_json_payload_from_url(session, url=static_url)
                dynamic_payload = fetch_json_payload_from_url(session, url=dynamic_url)
            else:
                static_payload, _, static_error = fetch_mobilithek_static_payload_with_probe(
                    session,
                    publication_id=str(publication["static_publication_id"]),
                    preferred_access_mode=static_access_mode or ("auth" if requires_auth else "noauth"),
                    access_token=access_token,
                    subscription_id=static_subscription_id,
                    fallback_url=static_access_url,
                )
                if static_payload is None:
                    raise RuntimeError(static_error or "static_fetch_failed")
                dynamic_payload, _, dynamic_error = fetch_mobilithek_static_payload_with_probe(
                    session,
                    publication_id=str(publication["dynamic_publication_id"]),
                    preferred_access_mode=dynamic_access_mode or ("auth" if requires_auth else "noauth"),
                    access_token=access_token,
                    subscription_id=dynamic_subscription_id,
                    fallback_url=dynamic_access_url,
                )
                if dynamic_payload is None:
                    raise RuntimeError(dynamic_error or "dynamic_fetch_failed")
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            stats["errors"].append(f"{publication_uid}: {exc}")
            continue

        static_sites = parse_datex_static_sites(static_payload)
        dynamic_states = parse_datex_dynamic_states(dynamic_payload)
        site_to_station = match_datex_sites_to_stations(
            enriched,
            static_sites,
            operator_patterns=tuple(publication.get("operator_patterns") or ()),
        )

        site_index: dict[str, DatexStaticSite] = {site.site_id: site for site in static_sites}
        station_reference_to_site_id: dict[str, str] = {}
        for site in static_sites:
            station_reference_to_site_id[site.site_id] = site.site_id
            for station_id in site.station_ids:
                if station_id:
                    station_reference_to_site_id[station_id] = site.site_id

        publication_matched_locations = 0
        publication_matched_stations: set[str] = set()
        publication_matched_evses: set[str] = set()

        stats["locations_scanned"] += len(static_sites)

        for site_reference, state in dynamic_states.items():
            site_id = station_reference_to_site_id.get(site_reference)
            if not site_id:
                for station_reference in state.get("station_refs", set()):
                    site_id = station_reference_to_site_id.get(station_reference)
                    if site_id:
                        break
            if not site_id:
                continue

            db_station_id = site_to_station.get(site_id)
            if not db_station_id:
                continue

            publication_matched_locations += 1
            publication_matched_stations.add(db_station_id)
            publication_matched_evses.update(state.get("evses", {}).keys())

            site = site_index.get(site_id)
            source_state = {
                "source_uids": {publication_uid},
                "source_names": {publication_name},
                "evses": state.get("evses", {}),
            }
            summary = summarize_station_occupancy(
                source_state,
                total_evses=site.total_evses if site else None,
            )

            row_index = enriched.index[enriched["station_id"] == db_station_id]
            if len(row_index) != 1:
                continue
            target_index = row_index[0]
            existing_summary = extract_occupancy_summary_from_row(enriched.loc[target_index])
            if not should_replace_occupancy(existing_summary, summary):
                continue

            for column, value in summary.items():
                enriched.at[target_index, column] = value
            updated_station_ids.add(db_station_id)
            updated_evse_ids.update(state.get("evses", {}).keys())

        stats["sources"].append(
            {
                "uid": publication_uid,
                "name": publication_name,
                "locations_scanned": int(len(static_sites)),
                "matched_locations": int(publication_matched_locations),
                "matched_stations": int(len(publication_matched_stations)),
                "matched_evses": int(len(publication_matched_evses)),
            }
        )

    stats["sources_used"] = len(stats["sources"])
    stats["matched_locations"] = int(sum(item["matched_locations"] for item in stats["sources"]))
    stats["matched_stations"] = int(len(updated_station_ids))
    stats["matched_evses"] = int(len(updated_evse_ids))
    return enriched, stats


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
    if text.lower() in {"nan", "nat"}:
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
    local_exists = osm_pbf_path.exists() and osm_pbf_path.stat().st_size > 0
    remote_size = 0
    remote_last_modified: datetime | None = None
    should_download = not local_exists

    if download_if_missing:
        try:
            head_response = request_with_retries(
                "HEAD",
                osm_pbf_url,
                session,
                timeout=DATEX_REQUEST_TIMEOUT_SECONDS,
                verify=True,
                allow_redirects=True,
            )
            remote_size = int(head_response.headers.get("content-length", "0") or 0)
            last_modified_text = str(head_response.headers.get("last-modified") or "").strip()
            if last_modified_text:
                remote_last_modified = parsedate_to_datetime(last_modified_text)
                if remote_last_modified.tzinfo is None:
                    remote_last_modified = remote_last_modified.replace(tzinfo=timezone.utc)
        except Exception as exc:
            log_info(f"OSM PBF HEAD check failed, using local file metadata only: {exc}")

    if local_exists and not should_download:
        local_stat = osm_pbf_path.stat()
        if download_if_missing:
            local_mtime = datetime.fromtimestamp(local_stat.st_mtime, tz=timezone.utc)
            size_differs = remote_size > 0 and int(local_stat.st_size) != remote_size
            newer_remote = remote_last_modified is not None and remote_last_modified > local_mtime + timedelta(minutes=5)
            should_download = size_differs or newer_remote
        if not should_download:
            return {
                "downloaded": False,
                "path": str(osm_pbf_path),
                "bytes": int(local_stat.st_size),
                "remote_last_modified": remote_last_modified.isoformat() if remote_last_modified else "",
            }

    if not local_exists and not download_if_missing:
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
    if remote_last_modified is not None:
        timestamp = remote_last_modified.timestamp()
        os.utime(osm_pbf_path, (timestamp, timestamp))
    file_size = int(osm_pbf_path.stat().st_size)
    log_info(f"OSM PBF ready: {osm_pbf_path} ({file_size / 1_048_576:.1f} MiB)")
    return {
        "downloaded": True,
        "path": str(osm_pbf_path),
        "bytes": file_size,
        "source_url": osm_pbf_url,
        "remote_last_modified": remote_last_modified.isoformat() if remote_last_modified else "",
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


def derive_provider_stem(title: str, publisher: str) -> str:
    words = [word for word in stem_words(title) if word not in GENERIC_OPERATOR_WORDS]
    if words:
        return "_".join(words[:4])

    publisher_words = [word for word in stem_words(publisher) if word not in GENERIC_OPERATOR_WORDS]
    if publisher_words:
        return "_".join(publisher_words[:4])

    return slugify(title or publisher or "mobilithek_offer")


def content_data_entry(metadata: dict[str, Any]) -> dict[str, Any]:
    values = metadata.get("contentData") or []
    return values[0] if values else {}


def resolve_content_access_url(content_data: dict[str, Any]) -> str:
    access_url = normalize_optional_text(content_data.get("accessUrl"))
    if not access_url:
        return ""

    parsed = urlparse(access_url)
    if parsed.query:
        return access_url

    description = normalize_optional_text(content_data.get("description"))
    if description:
        full_url_match = re.search(r"https?://[^\s)]+", description)
        if full_url_match:
            hinted_url = full_url_match.group(0).rstrip(".,")
            hinted_parsed = urlparse(hinted_url)
            if hinted_parsed.scheme and hinted_parsed.netloc and hinted_parsed.path == parsed.path and hinted_parsed.query:
                return hinted_url

        route_match = re.search(r"GET\s+([^\s?]+)\?([^\s]+)", description)
        if route_match:
            hinted_path = route_match.group(1).strip()
            hinted_query = route_match.group(2).strip().rstrip(".,)")
            if hinted_path == parsed.path and hinted_query:
                return urlunparse(parsed._replace(query=hinted_query))

    return access_url


def classify_mobilithek_feed_kind(metadata: dict[str, Any], *, fallback_title: str = "") -> str:
    content_data = content_data_entry(metadata)
    schema_profile = str(content_data.get("schemaProfileName") or "")
    title = str(metadata.get("title") or fallback_title or "")
    title_lower = title.lower()
    schema_lower = schema_profile.lower()
    media_type = str(content_data.get("mediaType") or "").lower()
    access_url = resolve_content_access_url(content_data)
    delta_delivery = content_data.get("deltaDelivery")

    if (
        "dynamic" in schema_lower
        or "realtime" in title_lower
        or "dynam" in title_lower
        or "dyn" in title_lower
    ):
        return "dynamic"
    if "static" in schema_lower or "statisch" in title_lower or "stat" in title_lower:
        return "static"
    if delta_delivery is True:
        return "dynamic"
    if access_url or "csv" in media_type or "json" in media_type:
        return "static"
    return "unknown"


def mobilithek_offer_access_mode(metadata: dict[str, Any]) -> str:
    contract = metadata.get("contractOffer") or {}
    anonymous = contract.get("providerApprovalAnonymousAccessRequired")
    return "noauth" if anonymous else "auth"


def is_charging_related_offer(metadata: dict[str, Any]) -> bool:
    categories = metadata.get("dataCategories") or []
    if CHARGING_DATA_CATEGORY in categories:
        return True

    content_data = content_data_entry(metadata)
    haystack = " ".join(
        [
            normalize_optional_text(metadata.get("title")),
            normalize_optional_text(metadata.get("publisher")),
            normalize_optional_text(content_data.get("description")),
            normalize_optional_text(content_data.get("schemaProfileName")),
            resolve_content_access_url(content_data),
        ]
    ).lower()

    return any(
        token in haystack
        for token in (
            "recharging",
            "charging",
            "energy-infrastructure",
            "ladesäule",
            "ladestation",
            "ladepunkt",
            "chargecloud",
            "tesla",
            "enbw",
            "qwello",
            "pump",
            "wirelane",
        )
    )


def search_mobilithek_offers(
    session: requests.Session,
    *,
    search_term: str,
    page: int,
    size: int,
) -> dict[str, Any]:
    response = request_with_retries(
        "POST",
        MOBILITHEK_METADATA_SEARCH_URL,
        session,
        timeout=DATEX_REQUEST_TIMEOUT_SECONDS,
        verify=DATEX_TLS_VERIFY,
        params={"page": page, "size": size, "sort": "latest,desc"},
        json={"searchString": search_term},
    )
    return response.json().get("dataOffers", {})


def fetch_mobilithek_offer_metadata(
    session: requests.Session,
    *,
    publication_id: str,
) -> dict[str, Any]:
    response = request_with_retries(
        "GET",
        MOBILITHEK_METADATA_OFFER_URL.format(publication_id=publication_id),
        session,
        timeout=DATEX_REQUEST_TIMEOUT_SECONDS,
        verify=DATEX_TLS_VERIFY,
    )
    return response.json()


def probe_mobilithek_file_access(
    session: requests.Session,
    *,
    access_token: str | None,
    publication_id: str,
) -> dict[str, Any]:
    if not access_token:
        return {"status": "missing_credentials", "is_accessible": None}

    response = request_with_retries(
        "GET",
        MOBILITHEK_PUBLICATION_FILE_ACCESS_URL.format(publication_id=publication_id),
        session,
        timeout=DATEX_REQUEST_TIMEOUT_SECONDS,
        verify=DATEX_TLS_VERIFY,
        headers={"Authorization": f"Bearer {access_token}"},
    )
    payload: dict[str, Any]
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    return {
        "status": "ok",
        "status_code": response.status_code,
        "is_accessible": payload.get("isAccessible"),
        "response_excerpt": response.text[:200],
    }


def should_attempt_static_payload_fetch(
    access_probe: dict[str, Any],
    *,
    subscription_id: str = "",
    fallback_url: str = "",
) -> bool:
    if access_probe.get("is_accessible") is True:
        return True
    if subscription_id:
        return True
    return bool(fallback_url)


def fetch_mobilithek_static_payload_with_probe(
    session: requests.Session,
    *,
    publication_id: str,
    preferred_access_mode: str,
    access_token: str | None,
    subscription_id: str = "",
    fallback_url: str = "",
) -> tuple[dict[str, Any] | list[Any] | None, str, str | None]:
    attempts: list[tuple[str, bool]] = []
    if preferred_access_mode == "auth":
        attempts.append(("auth", True))
        attempts.append(("noauth", False))
    else:
        attempts.append(("noauth", False))
        attempts.append(("auth", True))

    last_error: str | None = None
    for mode_name, requires_auth in attempts:
        try:
            if requires_auth and not access_token:
                raise RuntimeError("missing_mobilithek_access_token")
            url = (
                MOBILITHEK_PUBLICATION_FILE_URL.format(publication_id=publication_id)
                if requires_auth
                else MOBILITHEK_PUBLICATION_PUBLIC_FILE_URL.format(publication_id=publication_id)
            )
            payload = fetch_json_payload_from_url(
                session,
                url=url,
                access_token=access_token if requires_auth else None,
            )
            return payload, mode_name, None
        except Exception as exc:
            last_error = f"{mode_name}: {exc}"

    if fallback_url:
        try:
            payload = fetch_json_payload_from_url(session, url=fallback_url)
            return payload, "direct_access_url", None
        except Exception as exc:
            last_error = f"direct_access_url: {exc}"

    if subscription_id:
        try:
            payload = fetch_mobilithek_subscription_payload_with_mtls(subscription_id=subscription_id)
            return payload, "mtls_subscription", None
        except Exception as exc:
            last_error = f"mtls_subscription: {exc}"
            return None, "mtls_subscription", last_error

    return None, preferred_access_mode, last_error


def derive_eliso_static_site_id(site: dict[str, Any]) -> str:
    address = normalize_optional_text(site.get("address"))
    postcode = normalize_optional_text(site.get("postalCode"))
    city = normalize_optional_text(site.get("city"))
    location_key = " | ".join(part for part in (address, postcode, city) if part)
    if location_key:
        return location_key

    coordinates = site.get("coordinates") or {}
    try:
        lat = float(coordinates.get("latitude"))
        lon = float(coordinates.get("longitude"))
        return f"{lat:.6f},{lon:.6f}"
    except (TypeError, ValueError):
        pass

    return normalize_optional_text(site.get("operator_name") or site.get("operator"))


def parse_eliso_static_sites(payload: list[dict[str, Any]]) -> list[ElisoStaticSite]:
    sites: list[ElisoStaticSite] = []
    for site in payload:
        if not isinstance(site, dict):
            continue
        coordinates = site.get("coordinates") or {}
        try:
            lat = float(coordinates.get("latitude"))
            lon = float(coordinates.get("longitude"))
        except (TypeError, ValueError):
            continue

        evse_ids: list[str] = []
        station_ids: list[str] = []
        total_evses = int(site.get("chargepoints_count") or 0)
        for evse in site.get("evses") or []:
            if not isinstance(evse, dict):
                continue
            evse_id = normalize_evse_id(evse.get("evseId"))
            if evse_id:
                evse_ids.append(evse_id)
                station_ids.append(evse_id)
        if total_evses <= 0:
            total_evses = len(evse_ids)

        site_id = derive_eliso_static_site_id(site)
        if not site_id:
            site_id = f"{lat:.6f},{lon:.6f}"

        sites.append(
            ElisoStaticSite(
                site_id=site_id,
                station_ids=tuple(dict.fromkeys(item for item in station_ids if item)),
                lat=lat,
                lon=lon,
                postcode=normalize_optional_text(site.get("postalCode")),
                city=normalize_optional_text(site.get("city")),
                address=normalize_optional_text(site.get("address")),
                operator_name=normalize_optional_text(site.get("operator_name")),
                total_evses=max(0, total_evses),
                evse_ids=tuple(dict.fromkeys(item for item in evse_ids if item)),
            )
        )
    return sites


def static_grid_key(lat: float, lon: float) -> tuple[int, int]:
    return (math.floor(lat / 0.01), math.floor(lon / 0.01))


def build_static_station_indexes(
    df: pd.DataFrame,
) -> tuple[dict[tuple[int, int], list[dict[str, Any]]], dict[str, dict[str, Any]], dict[str, set[str]]]:
    grid: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    station_by_id: dict[str, dict[str, Any]] = {}
    evse_to_station_ids: dict[str, set[str]] = defaultdict(set)

    for record in df.to_dict("records"):
        station_id = str(record.get("station_id") or "")
        if not station_id:
            continue
        station_by_id[station_id] = record
        grid[static_grid_key(float(record["lat"]), float(record["lon"]))].append(record)
        for evse_id in record.get("evse_ids") or []:
            normalized = normalize_evse_id(evse_id)
            if normalized:
                evse_to_station_ids[normalized].add(station_id)

    return grid, station_by_id, evse_to_station_ids


def score_static_site_to_station(
    site: DatexStaticSite | ElisoStaticSite,
    station_row: dict[str, Any],
    *,
    publisher: str,
    max_distance_m: float,
) -> tuple[bool, float, float, dict[str, Any]]:
    distance_m = haversine_distance_m(
        float(site.lat),
        float(site.lon),
        float(station_row["lat"]),
        float(station_row["lon"]),
    )
    candidate_evse_ids = {
        normalize_evse_id(item) for item in (station_row.get("evse_ids") or []) if normalize_evse_id(item)
    }
    evse_overlap = len(set(site.evse_ids) & candidate_evse_ids)
    if distance_m > max_distance_m and evse_overlap <= 0:
        return False, distance_m, distance_m, {}

    station_postcode = str(station_row.get("postcode") or "").strip()
    postcode_match = bool(site.postcode and station_postcode == site.postcode)
    postcode_conflict = bool(site.postcode and station_postcode and station_postcode != site.postcode)
    city_match = bool(
        site.city and normalize_text(str(station_row.get("city") or "")) == normalize_text(site.city)
    )
    address_match = address_similarity(site.address, str(station_row.get("address") or ""))
    op_similarity = max(
        (
            operator_similarity(
                site_operator=site.operator_name,
                publisher=publisher,
                candidate_operator=candidate_operator,
            )
            for candidate_operator in station_operator_candidates(station_row)
        ),
        default=0.0,
    )

    score = distance_m
    if postcode_match:
        score -= 45.0
    if city_match:
        score -= 10.0
    if address_match:
        score -= 15.0
    if op_similarity >= 1.0:
        score -= 30.0
    elif op_similarity > 0.0:
        score -= 12.0 * op_similarity
    if evse_overlap > 0:
        score -= 120.0 + (evse_overlap * 20.0)

    site_evses = int(site.total_evses or 0)
    station_points = int(station_row.get("charging_points_count", 0) or 0)
    if site_evses > 0 and station_points > 0:
        score += abs(site_evses - station_points) * 2.5

    accepted = False
    if evse_overlap > 0:
        accepted = True
    elif distance_m <= 30.0:
        if postcode_conflict:
            accepted = bool(address_match or op_similarity > 0.0)
        else:
            accepted = bool(postcode_match or city_match or address_match or op_similarity > 0.0)
    elif postcode_match and (address_match or op_similarity > 0.0 or distance_m <= 80.0):
        accepted = True
    elif address_match and distance_m <= 120.0:
        accepted = True
    elif op_similarity >= 1.0 and distance_m <= 120.0:
        accepted = True

    details = {
        "distance_m": round(distance_m, 1),
        "postcode_match": postcode_match,
        "city_match": city_match,
        "address_match": address_match,
        "operator_similarity": round(op_similarity, 3),
        "evse_overlap": int(evse_overlap),
    }
    return accepted, score, distance_m, details


def match_static_sites_to_bnetza(
    sites: list[DatexStaticSite | ElisoStaticSite],
    *,
    publisher: str,
    station_grid: dict[tuple[int, int], list[dict[str, Any]]],
    station_by_id: dict[str, dict[str, Any]],
    evse_to_station_ids: dict[str, set[str]],
    max_distance_m: float = DETAIL_MATCH_MAX_DISTANCE_M,
) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    scored_pairs: list[tuple[float, float, str, str, dict[str, Any]]] = []

    for site in sites:
        candidate_station_ids: set[str] = set()
        for evse_id in site.evse_ids:
            candidate_station_ids.update(evse_to_station_ids.get(evse_id, set()))

        if candidate_station_ids:
            candidate_rows = [
                station_by_id[station_id]
                for station_id in candidate_station_ids
                if station_id in station_by_id
            ]
        else:
            site_key = static_grid_key(float(site.lat), float(site.lon))
            candidate_rows: list[dict[str, Any]] = []
            for lat_offset in (-1, 0, 1):
                for lon_offset in (-1, 0, 1):
                    candidate_rows.extend(
                        station_grid.get((site_key[0] + lat_offset, site_key[1] + lon_offset), [])
                    )

        for station_row in candidate_rows:
            accepted, score, distance_m, details = score_static_site_to_station(
                site,
                station_row,
                publisher=publisher,
                max_distance_m=max_distance_m,
            )
            if not accepted:
                continue
            station_id = str(station_row.get("station_id") or "")
            if not station_id:
                continue
            scored_pairs.append(
                (
                    score,
                    distance_m,
                    site.site_id,
                    station_id,
                    {
                        **details,
                        "site_id": site.site_id,
                        "station_id": station_id,
                    },
                )
            )

    scored_pairs.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
    matches: dict[str, str] = {}
    match_meta: dict[str, dict[str, Any]] = {}
    used_station_ids: set[str] = set()

    for _, _, site_id, station_id, details in scored_pairs:
        if site_id in matches or station_id in used_station_ids:
            continue
        matches[site_id] = station_id
        used_station_ids.add(station_id)
        match_meta[site_id] = details

    return matches, match_meta


def iter_walk_nodes(value: Any) -> Any:
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from iter_walk_nodes(item)
    elif isinstance(value, list):
        for item in value:
            yield from iter_walk_nodes(item)


def normalize_code_value(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def humanize_code(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    spaced = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", spaced)
    spaced = spaced.replace("_", " ").replace("-", " ").replace("/", " / ")
    spaced = re.sub(r"\s+", " ", spaced).strip()
    return spaced[:1].upper() + spaced[1:] if spaced else ""


def map_display_value(value: Any, mapping: dict[str, str]) -> str:
    code = normalize_code_value(value)
    if not code:
        return ""
    return mapping.get(code, humanize_code(value))


def join_display_list(values: list[str]) -> str:
    items = merge_unique_text_lists(pd.Series([item for item in values if item], dtype="object"))
    return " | ".join(items)


def parse_boolish(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    normalized = normalize_code_value(value)
    if not normalized:
        return None
    if normalized in BOOLEAN_YES_VALUES:
        return True
    if normalized in {"0", "false", "no", "nein", "n"}:
        return False
    return None


def extract_phone_numbers(value: Any) -> list[str]:
    values: list[str] = []
    for node in iter_walk_nodes(value):
        if not isinstance(node, dict):
            continue
        for key in ("telephoneNumber", "phoneNumber", "hotline_number"):
            phone = normalize_optional_text(node.get(key))
            if phone:
                values.append(phone)
    return merge_unique_text_lists(pd.Series(values, dtype="object"))


def summarize_operating_hours(value: Any) -> str:
    if isinstance(value, str):
        text = normalize_optional_text(value)
        if not text:
            return ""
        if normalize_text(text) in {"247", "24h", "24std", "24stunden", "24hours", "24x7"}:
            return "24/7"
        return text

    if isinstance(value, dict):
        normalized_keys = {normalize_code_value(key) for key in value.keys()}
        if "afacopenallhours" in normalized_keys or "openallhours" in normalized_keys:
            return "24/7"

    texts: list[str] = []
    for node in iter_walk_nodes(value):
        if not isinstance(node, dict):
            continue
        if any(normalize_code_value(key) in {"afacopenallhours", "openallhours"} for key in node.keys()):
            return "24/7"
        for key, candidate in node.items():
            key_norm = normalize_code_value(key)
            if key_norm not in {"openingtime", "openingtimes", "openingtimevalue", "periodname", "text"}:
                continue
            text = extract_first_lang_value(candidate) if not isinstance(candidate, str) else normalize_optional_text(candidate)
            if text:
                texts.append(text)
    return join_display_list(texts[:6])


def extract_latest_detail_timestamp(value: Any) -> str:
    candidates: list[str] = []
    for node in iter_walk_nodes(value):
        if not isinstance(node, dict):
            continue
        for key in ("lastUpdated", "mobilithek_last_updated_dts", "versionG"):
            candidate = normalize_optional_text(node.get(key))
            if candidate and parse_iso_datetime(candidate) is not None:
                candidates.append(candidate)
    return choose_latest_timestamp(candidates)


def collect_datex_price_components(value: Any) -> dict[str, Any]:
    kwh_values: list[float] = []
    minute_values: list[float] = []
    currencies: list[str] = []
    payment_methods: list[str] = []
    complex_tariff = False

    for node in iter_walk_nodes(value):
        if not isinstance(node, dict):
            continue
        energy_rates = node.get("energyRate") or []
        if not isinstance(energy_rates, list):
            continue
        for rate in energy_rates:
            if not isinstance(rate, dict):
                continue
            currencies.extend(
                [
                    normalize_optional_text(item)
                    for item in (rate.get("applicableCurrency") or [])
                    if normalize_optional_text(item)
                ]
            )
            payment = (rate.get("payment") or {}).get("paymentMeans") or []
            for method in payment:
                method_value = map_display_value((method or {}).get("value"), PAYMENT_METHOD_LABELS)
                if method_value:
                    payment_methods.append(method_value)
            for price in rate.get("energyPrice") or []:
                if not isinstance(price, dict):
                    continue
                try:
                    numeric_value = float(price.get("value"))
                except (TypeError, ValueError):
                    continue
                price_type = normalize_code_value(((price.get("priceType") or {}).get("value")) or "")
                if price_type == "priceperkwh":
                    kwh_values.append(numeric_value)
                elif price_type == "priceperminute":
                    minute_values.append(numeric_value)
                else:
                    complex_tariff = True
                if price.get("timeBasedApplicability"):
                    complex_tariff = True

    return {
        "kwh_values": kwh_values,
        "minute_values": minute_values,
        "currencies": merge_unique_text_lists(pd.Series(currencies, dtype="object")),
        "payment_methods": merge_unique_text_lists(pd.Series(payment_methods, dtype="object")),
        "complex_tariff": complex_tariff,
    }


def format_euro_amount(value: float) -> str:
    rounded = round(float(value) + 1e-9, 2)
    return f"{rounded:.2f}".replace(".", ",")


def summarize_price_display(price_components: dict[str, Any]) -> dict[str, Any]:
    kwh_values = [float(item) for item in price_components.get("kwh_values", [])]
    minute_values = [float(item) for item in price_components.get("minute_values", [])]
    currency_values = [item for item in price_components.get("currencies", []) if item]
    currency = currency_values[0] if currency_values else ("EUR" if kwh_values or minute_values else "")
    complex_tariff = bool(price_components.get("complex_tariff"))

    if not kwh_values and not minute_values:
        return {
            "price_display": "",
            "price_energy_eur_kwh_min": "",
            "price_energy_eur_kwh_max": "",
            "price_currency": currency,
            "price_quality": "",
        }

    display = ""
    quality = ""
    kwh_min = min(kwh_values) if kwh_values else None
    kwh_max = max(kwh_values) if kwh_values else None

    if kwh_values and currency == "EUR":
        if complex_tariff or minute_values:
            display = f"ab {format_euro_amount(kwh_min)} €/kWh"
            quality = "from"
        elif abs(kwh_min - kwh_max) < 0.0001:
            display = f"{format_euro_amount(kwh_min)} €/kWh"
            quality = "exact"
        else:
            display = f"{format_euro_amount(kwh_min)}–{format_euro_amount(kwh_max)} €/kWh"
            quality = "range"
    elif minute_values and currency == "EUR":
        minute_min = min(minute_values)
        minute_max = max(minute_values)
        if complex_tariff or abs(minute_min - minute_max) >= 0.0001:
            display = f"ab {format_euro_amount(minute_min)} €/min"
            quality = "from"
        else:
            display = f"{format_euro_amount(minute_min)} €/min"
            quality = "exact"

    return {
        "price_display": display,
        "price_energy_eur_kwh_min": round(kwh_min, 6) if kwh_min is not None and currency == "EUR" else "",
        "price_energy_eur_kwh_max": round(kwh_max, 6) if kwh_max is not None and currency == "EUR" else "",
        "price_currency": currency,
        "price_quality": quality,
    }


def compact_details_json(payload: dict[str, Any]) -> str:
    filtered = {key: value for key, value in payload.items() if value not in ("", [], {}, None)}
    if not filtered:
        return ""
    return json.dumps(filtered, ensure_ascii=False, separators=(",", ":"))


def static_source_display_name(publication: AfirStaticPublication) -> str:
    title = normalize_optional_text(publication.title)
    lowered = title.lower()
    for prefix in ("afir-recharging-stat-", "afir recharging stat "):
        if lowered.startswith(prefix):
            return title[len(prefix):].strip() or publication.publisher or title
    return publication.publisher or title or publication.uid


def extract_datex_static_details(
    site_record: DatexStaticSite,
    site_payload: dict[str, Any],
    *,
    publication: AfirStaticPublication,
    match_meta: dict[str, Any],
) -> dict[str, Any]:
    connector_types: list[str] = []
    current_types: list[str] = []
    auth_methods: list[str] = []
    service_types: list[str] = []
    green_values: list[bool] = []
    connector_count = 0
    opening_hours = summarize_operating_hours(site_payload.get("operatingHours"))

    for station in site_payload.get("energyInfrastructureStation") or []:
        if not isinstance(station, dict):
            continue
        if not opening_hours:
            opening_hours = summarize_operating_hours(station.get("operatingHours"))

        for method in station.get("authenticationAndIdentificationMethods") or []:
            value = map_display_value((method or {}).get("value"), AUTH_METHOD_LABELS)
            if value:
                auth_methods.append(value)

        for service in station.get("serviceType") or []:
            service_value = (
                (service or {}).get("serviceType", {}).get("value")
                or (service or {}).get("value")
            )
            value = map_display_value(service_value, SERVICE_TYPE_LABELS)
            if value:
                service_types.append(value)

        for refill_point in station.get("refillPoint") or []:
            charging_point = (refill_point or {}).get("aegiElectricChargingPoint") or {}
            connector_count += int(
                charging_point.get("numberOfConnectors")
                or len(charging_point.get("connector") or [])
                or 0
            )

            current_value = map_display_value(
                (charging_point.get("currentType") or {}).get("value"),
                CURRENT_TYPE_LABELS,
            )
            if current_value:
                current_types.append(current_value)

            for connector in charging_point.get("connector") or []:
                connector_value = map_display_value(
                    ((connector or {}).get("connectorType") or {}).get("value"),
                    CONNECTOR_TYPE_LABELS,
                )
                if connector_value:
                    connector_types.append(connector_value)

            for electric_energy in charging_point.get("electricEnergy") or []:
                if not isinstance(electric_energy, dict):
                    continue
                green = parse_boolish(electric_energy.get("isGreenEnergy"))
                if green is not None:
                    green_values.append(green)

    price_components = collect_datex_price_components(site_payload)
    price_summary = summarize_price_display(price_components)
    payment_methods = price_components.get("payment_methods", [])
    helpdesk_numbers = extract_phone_numbers(site_payload.get("helpdesk") or site_payload)
    detail_last_updated = extract_latest_detail_timestamp(site_payload)

    green_energy: str | bool = ""
    if green_values:
        green_energy = all(green_values)

    details_json = compact_details_json(
        {
            "source_name": static_source_display_name(publication),
            "publication_id": publication.publication_id,
            "site_id": site_record.site_id,
            "station_ids": list(site_record.station_ids),
            "charge_point_ids": list(site_record.evse_ids),
            "distance_m": match_meta.get("distance_m"),
            "evse_overlap": match_meta.get("evse_overlap"),
            "operator_similarity": match_meta.get("operator_similarity"),
            "price_display": price_summary["price_display"],
            "opening_hours": opening_hours,
            "helpdesk_phone": helpdesk_numbers[:1],
            "payment_methods": payment_methods,
            "auth_methods": merge_unique_text_lists(pd.Series(auth_methods, dtype="object")),
            "connector_types": merge_unique_text_lists(pd.Series(connector_types, dtype="object")),
            "current_types": merge_unique_text_lists(pd.Series(current_types, dtype="object")),
            "service_types": merge_unique_text_lists(pd.Series(service_types, dtype="object")),
            "green_energy": green_energy,
            "last_updated": detail_last_updated,
        }
    )

    return {
        "detail_source_uid": publication.uid,
        "detail_source_name": static_source_display_name(publication),
        "detail_last_updated": detail_last_updated,
        "datex_site_id": site_record.site_id,
        "datex_station_ids": "|".join(site_record.station_ids),
        "datex_charge_point_ids": "|".join(site_record.evse_ids),
        **price_summary,
        "opening_hours_display": opening_hours,
        "opening_hours_is_24_7": opening_hours == "24/7",
        "helpdesk_phone": helpdesk_numbers[0] if helpdesk_numbers else "",
        "payment_methods_display": join_display_list(payment_methods),
        "auth_methods_display": join_display_list(auth_methods),
        "connector_types_display": join_display_list(connector_types),
        "current_types_display": join_display_list(current_types),
        "connector_count": connector_count or "",
        "green_energy": green_energy,
        "service_types_display": join_display_list(service_types),
        "details_json": details_json,
    }


def extract_eliso_static_details(
    site_record: ElisoStaticSite,
    site_payload: dict[str, Any],
    *,
    publication: AfirStaticPublication,
    match_meta: dict[str, Any],
) -> dict[str, Any]:
    connector_types: list[str] = []
    current_types: list[str] = []
    auth_methods: list[str] = []
    payment_methods: list[str] = []
    connector_count = 0

    if parse_boolish(site_payload.get("contract_based_payment_option")):
        payment_methods.append("Vertragsbasiert")
    if parse_boolish(site_payload.get("other_adhoc_payment_option")):
        payment_methods.append("Ad-hoc-Zahlung")

    for evse in site_payload.get("evses") or []:
        if not isinstance(evse, dict):
            continue
        connector_count += int(evse.get("connector_count") or len(evse.get("connectors") or []) or 0)
        current_value = map_display_value(
            evse.get("charge_points_type") or ((evse.get("connectors") or [{}])[0].get("powerType")),
            CURRENT_TYPE_LABELS,
        )
        if current_value:
            current_types.append(current_value)

        for connector in evse.get("connectors") or []:
            if not isinstance(connector, dict):
                continue
            connector_value = map_display_value(
                connector.get("type_of_connector"),
                CONNECTOR_TYPE_LABELS,
            )
            if connector_value:
                connector_types.append(connector_value)

        if parse_boolish(evse.get("plug_n_charge")):
            auth_methods.append("Plug & Charge")
        if parse_boolish(evse.get("payment_card_reader")):
            payment_methods.append("Kartenterminal")
        if parse_boolish(evse.get("payment_card_contactless")):
            payment_methods.append("Kontaktlos")

    opening_hours = summarize_operating_hours(site_payload.get("opening_time"))
    helpdesk_numbers = extract_phone_numbers(site_payload)
    detail_last_updated = extract_latest_detail_timestamp(site_payload)
    service_types = [
        map_display_value(site_payload.get("physical_support"), SERVICE_TYPE_LABELS)
    ]
    green_energy = parse_boolish(site_payload.get("electricity_supplied_is_100_percent_renewable"))

    details_json = compact_details_json(
        {
            "source_name": static_source_display_name(publication),
            "publication_id": publication.publication_id,
            "site_id": site_record.site_id,
            "station_ids": list(site_record.station_ids),
            "charge_point_ids": list(site_record.evse_ids),
            "distance_m": match_meta.get("distance_m"),
            "evse_overlap": match_meta.get("evse_overlap"),
            "operator_similarity": match_meta.get("operator_similarity"),
            "opening_hours": opening_hours,
            "helpdesk_phone": helpdesk_numbers[:1],
            "payment_methods": merge_unique_text_lists(pd.Series(payment_methods, dtype="object")),
            "auth_methods": merge_unique_text_lists(pd.Series(auth_methods, dtype="object")),
            "connector_types": merge_unique_text_lists(pd.Series(connector_types, dtype="object")),
            "current_types": merge_unique_text_lists(pd.Series(current_types, dtype="object")),
            "service_types": merge_unique_text_lists(pd.Series(service_types, dtype="object")),
            "green_energy": green_energy,
            "last_updated": detail_last_updated,
        }
    )

    return {
        "detail_source_uid": publication.uid,
        "detail_source_name": static_source_display_name(publication),
        "detail_last_updated": detail_last_updated,
        "datex_site_id": site_record.site_id,
        "datex_station_ids": "|".join(site_record.station_ids),
        "datex_charge_point_ids": "|".join(site_record.evse_ids),
        "price_display": "",
        "price_energy_eur_kwh_min": "",
        "price_energy_eur_kwh_max": "",
        "price_currency": "",
        "price_quality": "",
        "opening_hours_display": opening_hours,
        "opening_hours_is_24_7": opening_hours == "24/7",
        "helpdesk_phone": helpdesk_numbers[0] if helpdesk_numbers else "",
        "payment_methods_display": join_display_list(payment_methods),
        "auth_methods_display": join_display_list(auth_methods),
        "connector_types_display": join_display_list(connector_types),
        "current_types_display": join_display_list(current_types),
        "connector_count": connector_count or "",
        "green_energy": green_energy if green_energy is not None else "",
        "service_types_display": join_display_list(service_types),
        "details_json": details_json,
    }


def has_detail_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def detail_nonempty_score(details: dict[str, Any]) -> int:
    score = 0
    if has_detail_value(details.get("price_display")):
        score += 8
    if has_detail_value(details.get("opening_hours_display")):
        score += 5
    if has_detail_value(details.get("helpdesk_phone")):
        score += 4
    for field in (
        "payment_methods_display",
        "auth_methods_display",
        "connector_types_display",
        "current_types_display",
        "service_types_display",
    ):
        if has_detail_value(details.get(field)):
            score += 2
    if has_detail_value(details.get("connector_count")):
        score += 1
    if isinstance(details.get("green_energy"), bool):
        score += 1
    if has_detail_value(details.get("detail_last_updated")):
        score += 1
    return score


def build_bnetza_fallback_details(row: pd.Series) -> dict[str, Any]:
    connector_values = row.get("connector_types", [])
    connector_display = ""
    if isinstance(connector_values, list):
        connector_display = join_display_list(
            [map_display_value(item, CONNECTOR_TYPE_LABELS) for item in connector_values]
        )

    return {
        "detail_source_uid": "",
        "detail_source_name": "",
        "detail_last_updated": "",
        "datex_site_id": "",
        "datex_station_ids": "",
        "datex_charge_point_ids": "",
        "price_display": "",
        "price_energy_eur_kwh_min": "",
        "price_energy_eur_kwh_max": "",
        "price_currency": "",
        "price_quality": "",
        "opening_hours_display": normalize_optional_text(row.get("bnetza_opening_hours")),
        "opening_hours_is_24_7": normalize_optional_text(row.get("bnetza_opening_hours")) == "24/7",
        "helpdesk_phone": "",
        "payment_methods_display": normalize_optional_text(row.get("bnetza_payment_systems")),
        "auth_methods_display": "",
        "connector_types_display": connector_display,
        "current_types_display": "",
        "connector_count": "",
        "green_energy": "",
        "service_types_display": "",
        "details_json": "",
    }


def apply_static_publication_payload(
    enriched: pd.DataFrame,
    *,
    publication: AfirStaticPublication,
    payload: dict[str, Any] | list[Any],
    access_mode_used: str,
    row_lookup: dict[str, int],
    detail_scores: dict[str, int],
    station_grid: dict[tuple[int, int], list[dict[str, Any]]],
    station_by_id: dict[str, dict[str, Any]],
    evse_to_station_ids: dict[str, set[str]],
    stats: dict[str, Any],
) -> None:
    site_records: list[DatexStaticSite | ElisoStaticSite] = []
    site_payload_index: dict[str, Any] = {}
    payload_kind = "unsupported"

    if isinstance(payload, dict):
        publication_root = _datex_publication_root(payload)
        if isinstance(publication_root, dict):
            payload_kind = "datex"
            site_records = parse_datex_static_sites(payload)
            for table in _iter_dict_items(publication_root.get("energyInfrastructureTable")):
                for site in _iter_dict_items(table.get("energyInfrastructureSite")):
                    site_id = _reference_id(site)
                    if site_id:
                        site_payload_index[site_id] = site
    elif isinstance(payload, list) and payload and isinstance(payload[0], dict):
        if "coordinates" in payload[0] and "evses" in payload[0]:
            payload_kind = "eliso"
            site_records = parse_eliso_static_sites(payload)
            for site in payload:
                site_id = derive_eliso_static_site_id(site)
                if not site_id:
                    coords = site.get("coordinates") or {}
                    lat = coords.get("latitude")
                    lon = coords.get("longitude")
                    if lat is not None and lon is not None:
                        site_id = f"{float(lat):.6f},{float(lon):.6f}"
                if site_id:
                    site_payload_index[site_id] = site

    if not site_records:
        stats["sources"].append(
            {
                "uid": publication.uid,
                "publication_id": publication.publication_id,
                "title": publication.title,
                "publisher": publication.publisher,
                "access_mode": access_mode_used,
                "status": "unsupported_payload",
                "payload_kind": payload_kind,
                "matched_sites": 0,
                "matched_stations": 0,
            }
        )
        return

    matches, match_meta = match_static_sites_to_bnetza(
        site_records,
        publisher=publication.publisher,
        station_grid=station_grid,
        station_by_id=station_by_id,
        evse_to_station_ids=evse_to_station_ids,
    )

    matched_station_ids: set[str] = set()
    matched_sites = 0
    for site_record in site_records:
        station_id = matches.get(site_record.site_id)
        if not station_id:
            continue
        row_index = row_lookup.get(station_id)
        site_payload = site_payload_index.get(site_record.site_id)
        if row_index is None or not isinstance(site_payload, dict):
            continue

        if payload_kind == "datex":
            candidate = extract_datex_static_details(
                site_record,
                site_payload,
                publication=publication,
                match_meta=match_meta.get(site_record.site_id, {}),
            )
        else:
            candidate = extract_eliso_static_details(
                site_record,
                site_payload,
                publication=publication,
                match_meta=match_meta.get(site_record.site_id, {}),
            )

        current_score = detail_scores.get(station_id, 0)
        candidate_score = detail_nonempty_score(candidate)
        if candidate_score <= 0:
            continue

        for field, value in candidate.items():
            if field == "detail_source_uid" or field == "detail_source_name":
                continue
            if not has_detail_value(value):
                continue
            enriched.at[row_index, field] = value

        if candidate_score >= current_score or not normalize_optional_text(
            enriched.at[row_index, "detail_source_uid"]
        ):
            enriched.at[row_index, "detail_source_uid"] = candidate["detail_source_uid"]
            enriched.at[row_index, "detail_source_name"] = candidate["detail_source_name"]
            if has_detail_value(candidate.get("detail_last_updated")):
                enriched.at[row_index, "detail_last_updated"] = candidate["detail_last_updated"]
            detail_scores[station_id] = max(current_score, candidate_score)

        matched_sites += 1
        matched_station_ids.add(station_id)

    if matched_station_ids:
        stats["sources_used"] += 1
    stats["matched_sites"] += matched_sites
    stats["sources"].append(
        {
            "uid": publication.uid,
            "publication_id": publication.publication_id,
            "title": publication.title,
            "publisher": publication.publisher,
            "access_mode": access_mode_used,
            "status": "ok",
            "payload_kind": payload_kind,
            "sites_scanned": len(site_records),
            "matched_sites": matched_sites,
            "matched_stations": len(matched_station_ids),
        }
    )


def enrich_with_static_details(
    df: pd.DataFrame,
    *,
    session: requests.Session,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    enriched = df.copy()
    for field in STATIC_DETAIL_FIELDS:
        if field in {"opening_hours_is_24_7"}:
            enriched[field] = False
        else:
            enriched[field] = pd.Series([""] * len(enriched), index=enriched.index, dtype="object")

    row_lookup: dict[str, int] = {}
    detail_scores: dict[str, int] = {}
    station_grid, station_by_id, evse_to_station_ids = build_static_station_indexes(enriched)

    for row_index, row in enriched.reset_index(drop=True).iterrows():
        station_id = str(row.get("station_id") or "")
        row_lookup[station_id] = row_index
        fallback = build_bnetza_fallback_details(row)
        for field, value in fallback.items():
            enriched.at[row_index, field] = value
        detail_scores[station_id] = detail_nonempty_score(fallback)

    stats: dict[str, Any] = {
        "offers_discovered": 0,
        "static_offers_considered": 0,
        "sources_used": 0,
        "matched_sites": 0,
        "matched_stations": 0,
        "stations_with_price": 0,
        "stations_with_opening_hours": 0,
        "stations_with_helpdesk": 0,
        "errors": [],
        "sources": [],
    }

    try:
        access_token = fetch_mobilithek_access_token(session)
    except (requests.RequestException, ValueError, RuntimeError) as exc:
        access_token = None
        stats["errors"].append(f"mobilithek_auth_failed: {exc}")

    try:
        static_subscription_ids = load_static_subscription_ids()
    except (OSError, ValueError, TypeError) as exc:
        static_subscription_ids = {}
        stats["errors"].append(f"static_subscription_registry_failed: {exc}")

    try:
        provider_context_by_static_publication = load_provider_context_by_static_publication()
    except (OSError, ValueError, TypeError) as exc:
        provider_context_by_static_publication = {}
        stats["errors"].append(f"provider_config_load_failed: {exc}")

    try:
        direct_datex_sources = load_direct_datex_sources()
    except (OSError, ValueError, TypeError) as exc:
        direct_datex_sources = []
        stats["errors"].append(f"direct_datex_registry_failed: {exc}")

    for source in direct_datex_sources:
        publication = AfirStaticPublication(
            uid=f"mobilithek_{source.provider_uid}_static",
            publication_id=source.static_url,
            title=source.static_title,
            publisher=source.publisher or source.display_name or source.provider_uid,
            access_mode="noauth",
            data_model=DATEX_V3_DATA_MODEL,
            access_url=source.static_url,
        )
        stats["offers_discovered"] += 1
        stats["static_offers_considered"] += 1
        try:
            payload = fetch_json_payload_from_url(session, url=source.static_url)
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            stats["errors"].append(f"{source.static_url}: {exc}")
            stats["sources"].append(
                {
                    "uid": publication.uid,
                    "publication_id": publication.publication_id,
                    "title": publication.title,
                    "publisher": publication.publisher,
                    "access_mode": "direct_url",
                    "status": "fetch_failed",
                    "error": str(exc),
                    "matched_sites": 0,
                    "matched_stations": 0,
                }
            )
            continue

        apply_static_publication_payload(
            enriched,
            publication=publication,
            payload=payload,
            access_mode_used="direct_url",
            row_lookup=row_lookup,
            detail_scores=detail_scores,
            station_grid=station_grid,
            station_by_id=station_by_id,
            evse_to_station_ids=evse_to_station_ids,
            stats=stats,
        )

    seen_publication_ids: set[str] = set()
    page = 0
    total_pages = 1

    while page < total_pages:
        try:
            search_payload = search_mobilithek_offers(
                session,
                search_term=MOBILITHEK_AFIR_SEARCH_TERM,
                page=page,
                size=MOBILITHEK_SEARCH_PAGE_SIZE,
            )
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            stats["errors"].append(f"offer_search_page_{page}: {exc}")
            break

        total_pages = int(search_payload.get("totalPages") or 1)
        for item in search_payload.get("content", []) or []:
            publication_id = str(item.get("id") or item.get("publicationId") or "").strip()
            if not publication_id or publication_id in seen_publication_ids:
                continue
            seen_publication_ids.add(publication_id)
            stats["offers_discovered"] += 1

            try:
                metadata = fetch_mobilithek_offer_metadata(session, publication_id=publication_id)
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                stats["errors"].append(f"{publication_id}: metadata_failed: {exc}")
                continue

            if not is_charging_related_offer(metadata):
                continue

            if classify_mobilithek_feed_kind(metadata) != "static":
                continue
            stats["static_offers_considered"] += 1

            access_probe: dict[str, Any]
            try:
                access_probe = probe_mobilithek_file_access(
                    session,
                    access_token=access_token,
                    publication_id=publication_id,
                )
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                access_probe = {"status": "error", "response_excerpt": str(exc), "is_accessible": None}

            publisher = normalize_optional_text(metadata.get("publisher"))
            title = normalize_optional_text(metadata.get("title"))
            provider_context = provider_context_by_static_publication.get(publication_id) or {}
            provider_uid = str(provider_context.get("uid") or "").strip() or derive_provider_stem(title, publisher)
            if not publisher:
                publisher = normalize_optional_text(provider_context.get("publisher"))
            if not publisher:
                publisher = normalize_optional_text(provider_context.get("display_name"))
            static_subscription_id = static_subscription_ids.get(provider_uid, "")
            publication = AfirStaticPublication(
                uid=f"mobilithek_{provider_uid}_static",
                publication_id=publication_id,
                title=title,
                publisher=publisher,
                access_mode=mobilithek_offer_access_mode(metadata),
                data_model=normalize_optional_text(content_data_entry(metadata).get("dataModel")),
                access_url=resolve_content_access_url(content_data_entry(metadata)),
            )

            if not should_attempt_static_payload_fetch(
                access_probe,
                subscription_id=static_subscription_id,
                fallback_url=publication.access_url,
            ):
                stats["sources"].append(
                    {
                        "uid": publication.uid,
                        "publication_id": publication.publication_id,
                        "title": publication.title,
                        "publisher": publication.publisher,
                        "access_mode": publication.access_mode,
                        "status": "not_accessible",
                        "matched_sites": 0,
                        "matched_stations": 0,
                    }
                )
                continue

            payload, access_mode_used, fetch_error = fetch_mobilithek_static_payload_with_probe(
                session,
                publication_id=publication_id,
                preferred_access_mode=publication.access_mode,
                access_token=access_token,
                subscription_id=static_subscription_id,
                fallback_url=publication.access_url,
            )
            if payload is None:
                stats["errors"].append(f"{publication_id}: {fetch_error}")
                stats["sources"].append(
                    {
                        "uid": publication.uid,
                        "publication_id": publication.publication_id,
                        "title": publication.title,
                        "publisher": publication.publisher,
                        "access_mode": access_mode_used,
                        "status": "fetch_failed",
                        "error": fetch_error,
                        "matched_sites": 0,
                        "matched_stations": 0,
                    }
                )
                continue

            apply_static_publication_payload(
                enriched,
                publication=publication,
                payload=payload,
                access_mode_used=access_mode_used,
                row_lookup=row_lookup,
                detail_scores=detail_scores,
                station_grid=station_grid,
                station_by_id=station_by_id,
                evse_to_station_ids=evse_to_station_ids,
                stats=stats,
            )

        page += 1

    stats["matched_stations"] = int(
        enriched["detail_source_uid"].fillna("").astype(str).str.strip().ne("").sum()
    )
    stats["stations_with_price"] = int(
        enriched["price_display"].fillna("").astype(str).str.strip().ne("").sum()
    )
    stats["stations_with_opening_hours"] = int(
        enriched["opening_hours_display"].fillna("").astype(str).str.strip().ne("").sum()
    )
    stats["stations_with_helpdesk"] = int(
        enriched["helpdesk_phone"].fillna("").astype(str).str.strip().ne("").sum()
    )
    return enriched, stats


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

    cache_path.write_text(dumps_pretty_json(cache), encoding="utf-8")

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


def geojson_display_text(*values: Any) -> str:
    for value in values:
        if isinstance(value, (list, tuple, set)):
            parts = [normalize_optional_text(item) for item in value]
            text = " | ".join(part for part in parts if part)
        else:
            text = normalize_optional_text(value)
        if text:
            return text
    return ""


def geojson_int(value: Any, default: int = 0) -> int:
    text = normalize_optional_text(value)
    if not text:
        return default
    try:
        return int(float(text))
    except (TypeError, ValueError):
        return default


def dataframe_to_geojson(df: pd.DataFrame, source_meta: dict[str, Any]) -> dict[str, Any]:
    features: list[dict[str, Any]] = []

    for _, row in df.iterrows():
        green_energy = row.get("green_energy", "")
        opening_hours_display = geojson_display_text(
            row.get("opening_hours_display", ""),
            row.get("bnetza_opening_hours", ""),
        )
        payment_methods_display = geojson_display_text(
            row.get("payment_methods_display", ""),
            row.get("bnetza_payment_systems", ""),
        )
        connector_types_display = geojson_display_text(
            row.get("connector_types_display", ""),
            row.get("connector_types", ""),
        )
        connector_count = geojson_int(
            row.get("connector_count", ""),
            default=int(row.get("charging_points_count", 0) or 0),
        )
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
            "occupancy_source_uid": row.get("occupancy_source_uid", ""),
            "occupancy_source_name": row.get("occupancy_source_name", ""),
            "occupancy_status": row.get("occupancy_status", ""),
            "occupancy_last_updated": row.get("occupancy_last_updated", ""),
            "occupancy_total_evses": int(row.get("occupancy_total_evses", 0)),
            "occupancy_available_evses": int(row.get("occupancy_available_evses", 0)),
            "occupancy_occupied_evses": int(row.get("occupancy_occupied_evses", 0)),
            "occupancy_charging_evses": int(row.get("occupancy_charging_evses", 0)),
            "occupancy_out_of_order_evses": int(row.get("occupancy_out_of_order_evses", 0)),
            "occupancy_unknown_evses": int(row.get("occupancy_unknown_evses", 0)),
            "detail_source_uid": row.get("detail_source_uid", ""),
            "detail_source_name": row.get("detail_source_name", ""),
            "detail_last_updated": row.get("detail_last_updated", ""),
            "datex_site_id": row.get("datex_site_id", ""),
            "datex_station_ids": row.get("datex_station_ids", ""),
            "datex_charge_point_ids": row.get("datex_charge_point_ids", ""),
            "price_display": row.get("price_display", ""),
            "price_energy_eur_kwh_min": row.get("price_energy_eur_kwh_min", ""),
            "price_energy_eur_kwh_max": row.get("price_energy_eur_kwh_max", ""),
            "price_currency": row.get("price_currency", ""),
            "price_quality": row.get("price_quality", ""),
            "opening_hours_display": opening_hours_display,
            "opening_hours_is_24_7": bool(row.get("opening_hours_is_24_7", False)),
            "helpdesk_phone": row.get("helpdesk_phone", ""),
            "payment_methods_display": payment_methods_display,
            "auth_methods_display": row.get("auth_methods_display", ""),
            "connector_types_display": connector_types_display,
            "current_types_display": row.get("current_types_display", ""),
            "connector_count": connector_count,
            "green_energy": green_energy if isinstance(green_energy, bool) else row.get("green_energy", ""),
            "service_types_display": row.get("service_types_display", ""),
            "details_json": row.get("details_json", ""),
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


def finalize_bundle_geojson(feature_collection: dict[str, Any]) -> dict[str, Any]:
    trimmed = {
        key: value
        for key, value in feature_collection.items()
        if key not in TRIMMED_GEOJSON_TOP_LEVEL_KEYS
    }
    trimmed["features"] = [
        {
            **feature,
            "properties": {
                key: value
                for key, value in feature.get("properties", {}).items()
                if key not in TRIMMED_GEOJSON_PROPERTY_KEYS
            },
        }
        for feature in feature_collection.get("features", [])
    ]
    return trimmed


def sanitize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: sanitize_json_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [sanitize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_json_value(item) for item in value]
    if isinstance(value, set):
        return [sanitize_json_value(item) for item in sorted(value)]
    if value is None or isinstance(value, (bool, int)):
        return value
    if isinstance(value, str):
        return "" if value.strip().lower() in {"nan", "nat"} else value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item") and callable(value.item):
        return sanitize_json_value(value.item())
    if pd.isna(value):
        return None
    return value


def dumps_minified_json(payload: Any) -> str:
    return json.dumps(
        sanitize_json_value(payload),
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def dumps_pretty_json(payload: Any) -> str:
    return json.dumps(
        sanitize_json_value(payload),
        ensure_ascii=False,
        indent=2,
        allow_nan=False,
    )


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
    occupancy = summary.get("occupancy_lookup", {})
    static_details = summary.get("static_detail_lookup", {})

    block = "\n".join(
        [
            README_START,
            "## Data Build Status",
            "",
            f"- Last build (UTC): `{run['started_at']}`",
            f"- Source: `{source.get('source_url', 'unknown')}`",
            f"- Full registry stations: `{records.get('full_registry_stations_total', 0)}`",
            f"- Fast chargers (>= {summary['params']['min_power_kw']} kW): `{records['fast_chargers_total']}`",
            f"- Fast chargers with live occupancy: `{records.get('stations_with_live_occupancy', 0)}`",
            (
                f"- Fast chargers with static AFIR details: `{records.get('stations_with_static_details', 0)}` "
                f"(price: `{records.get('stations_with_price', 0)}`, "
                f"opening hours: `{records.get('stations_with_opening_hours', 0)}`)"
            ),
            f"- Chargers with >=1 nearby amenity: `{records['stations_with_amenities']}`",
            (
                f"- Occupancy sources scanned: `{occupancy.get('sources_used', 0)}` "
                f"(matched EVSEs: `{occupancy.get('matched_evses', 0)}`)"
            ),
            (
                f"- Static AFIR sources used: `{static_details.get('sources_used', 0)}` "
                f"(helpdesk phones: `{records.get('stations_with_helpdesk', 0)}`)"
            ),
            f"- Amenity backend: `{lookup.get('backend', 'overpass')}`",
            (
                f"- Live amenity lookups this run: `{lookup['queries_used']}` "
                f"(cache hits: `{lookup['cache_hits']}`, deferred: `{lookup['deferred']}`)"
            ),
            "",
            "Generated files:",
            "- `data/bnetza_cache.csv`",
            "- `data/chargers_full.csv`",
            "- `data/chargers_fast.csv`",
            "- `data/chargers_fast.geojson`",
            "- `data/chargers_under_50.geojson`",
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

    log_info("Stage 1/8: Fetching BNetzA source")
    source_meta = fetch_bnetza_csv(session, RAW_CACHE_PATH, RAW_META_PATH)
    log_info(f"Source ready: {source_meta.get('source_url', 'unknown')}")
    bnetza_api_station_meta = fetch_optional_auxiliary_csv(
        session,
        url=BNETZA_API_LADESTATION_URL,
        cache_path=BNETZA_API_LADESTATION_CACHE_PATH,
        meta_path=BNETZA_API_LADESTATION_META_PATH,
        label="bnetza_api_ladestation",
    )
    if bnetza_api_station_meta.get("unavailable"):
        log_info(
            "BNetzA API station source unavailable; continuing without auxiliary aliases: "
            f"{bnetza_api_station_meta.get('error', 'unknown_error')}"
        )
    else:
        log_info(
            "BNetzA API station source ready: "
            f"{bnetza_api_station_meta.get('source_url', 'unknown')}"
            + (" (cache fallback)" if bnetza_api_station_meta.get("cache_only") else "")
        )
    source_meta["bnetza_api_ladestation_source"] = bnetza_api_station_meta

    log_info("Stage 2/8: Loading and normalizing raw source")
    raw_df = load_raw_dataframe(RAW_CACHE_PATH)
    bnetza_api_station_aliases = load_bnetza_api_station_aliases(BNETZA_API_LADESTATION_CACHE_PATH)
    log_info(f"Raw rows loaded: {len(raw_df)}")

    log_info("Stage 3/8: Building canonical full station frame and filtered fast charger frame")
    legacy_fast_df = build_fast_charger_frame(
        raw_df,
        min_power_kw=args.min_power_kw,
        bnetza_api_station_aliases=bnetza_api_station_aliases,
    )
    legacy_station_ids_by_group_key = {
        _station_group_key(row["lat"], row["lon"], row["operator"]): str(row["station_id"])
        for _, row in legacy_fast_df.iterrows()
    }
    full_df = build_full_registry_station_frame(
        raw_df,
        bnetza_api_station_aliases=bnetza_api_station_aliases,
        legacy_station_ids_by_group_key=legacy_station_ids_by_group_key,
    )
    fast_df = build_fast_projection_from_full_registry(
        full_df,
        min_power_kw=args.min_power_kw,
    )
    under_50_df = build_under_power_projection_from_full_registry(
        full_df,
        max_power_kw=args.min_power_kw,
    )
    log_info(f"Full registry stations: {len(full_df)}")
    log_info(f"Fast chargers after filter: {len(fast_df)}")
    log_info(f"Active chargers under {args.min_power_kw:g} kW: {len(under_50_df)}")

    if args.max_stations and args.max_stations > 0:
        fast_df = fast_df.head(args.max_stations).reset_index(drop=True)
        log_info(f"Applied max_stations cap: {len(fast_df)} rows")

    log_info("Stage 4/8: Matching live occupancy from MobiData BW and Mobilithek DATEX II")
    occupied_df, ocpi_occupancy_stats = enrich_with_live_occupancy(
        fast_df,
        session=session,
        progress_every=args.progress_every,
    )
    occupied_df, datex_occupancy_stats = enrich_with_mobilithek_datex(
        occupied_df,
        session=session,
    )
    occupancy_stats = combine_occupancy_stats(ocpi_occupancy_stats, datex_occupancy_stats)
    occupancy_stats["matched_stations"] = int((occupied_df["occupancy_total_evses"] > 0).sum())
    log_info(
        "Live occupancy enrichment done: "
        f"sources={occupancy_stats['sources_used']}, "
        f"matched_stations={occupancy_stats['matched_stations']}, "
        f"matched_evses={occupancy_stats['matched_evses']}, "
        f"errors={len(occupancy_stats['errors'])}"
    )

    log_info("Stage 5/8: Enriching chargers with static AFIR details")
    detailed_df, static_detail_stats = enrich_with_static_details(
        occupied_df,
        session=session,
    )
    log_info(
        "Static detail enrichment done: "
        f"sources={static_detail_stats['sources_used']}, "
        f"matched_stations={static_detail_stats['matched_stations']}, "
        f"price={static_detail_stats['stations_with_price']}, "
        f"opening_hours={static_detail_stats['stations_with_opening_hours']}, "
        f"helpdesk={static_detail_stats['stations_with_helpdesk']}, "
        f"errors={len(static_detail_stats['errors'])}"
    )

    log_info("Stage 6/8: Enriching chargers with nearby amenities")
    enriched_df, amenity_stats = enrich_with_amenities(
        detailed_df,
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

    pre_amenity_filter_count = len(enriched_df)
    enriched_df = filter_fast_chargers_with_amenities(enriched_df)
    removed_without_amenities = pre_amenity_filter_count - len(enriched_df)
    log_info(f"Excluded fast chargers without nearby amenities: {removed_without_amenities}")

    enriched_df = enriched_df.sort_values(
        by=["amenities_total", "max_power_kw"],
        ascending=[False, False],
    ).reset_index(drop=True)

    full_export_df = full_df.drop(
        columns=["evse_ids", "operator_aliases", "bnetza_ladestation_ids"],
        errors="ignore",
    ).copy()
    export_df = enriched_df.drop(
        columns=["evse_ids", "operator_aliases", "bnetza_ladestation_ids"],
        errors="ignore",
    ).copy()

    log_info("Stage 7/8: Writing data artifacts")
    FULL_CSV_PATH.write_text(full_export_df.to_csv(index=False), encoding="utf-8")
    FAST_CSV_PATH.write_text(export_df.to_csv(index=False), encoding="utf-8")

    geojson = finalize_bundle_geojson(dataframe_to_geojson(export_df, source_meta))
    FAST_GEOJSON_PATH.write_text(
        dumps_minified_json(geojson),
        encoding="utf-8",
    )

    under_50_geojson = finalize_bundle_geojson(
        dataframe_to_geojson(
            attach_empty_amenity_columns(under_50_df, source="not_enriched"),
            source_meta,
        )
    )
    UNDER_50_GEOJSON_PATH.write_text(
        dumps_minified_json(under_50_geojson),
        encoding="utf-8",
    )

    operators_payload = build_operator_list(
        export_df,
        min_stations=args.operator_min_stations,
    )
    OPERATORS_JSON_PATH.write_text(
        dumps_minified_json(operators_payload),
        encoding="utf-8",
    )

    stations_with_amenities = int((export_df["amenities_total"] > 0).sum())
    stations_with_live_occupancy = int((export_df["occupancy_total_evses"] > 0).sum())
    stations_with_static_details = int(
        export_df["detail_source_uid"].fillna("").astype(str).str.strip().ne("").sum()
    )

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
            "full_registry_stations_total": int(len(full_export_df)),
            "full_registry_active_stations_total": int(
                full_df["has_active_record"].fillna(False).astype(bool).sum()
            ),
            "fast_chargers_total": int(len(export_df)),
            "chargers_under_50_total": int(len(under_50_df)),
            "chargers_all_active_total": int(len(export_df) + len(under_50_df)),
            "stations_with_live_occupancy": stations_with_live_occupancy,
            "stations_with_static_details": stations_with_static_details,
            "stations_with_price": int(
                export_df["price_display"].fillna("").astype(str).str.strip().ne("").sum()
            ),
            "stations_with_opening_hours": int(
                export_df["opening_hours_display"].fillna("").astype(str).str.strip().ne("").sum()
            ),
            "stations_with_helpdesk": int(
                export_df["helpdesk_phone"].fillna("").astype(str).str.strip().ne("").sum()
            ),
            "stations_with_amenities": stations_with_amenities,
        },
        "occupancy_lookup": occupancy_stats,
        "static_detail_lookup": static_detail_stats,
        "amenity_lookup": amenity_stats,
        "operators": {
            "min_stations": int(args.operator_min_stations),
            "listed_operators": int(operators_payload["total_operators"]),
        },
    }

    SUMMARY_JSON_PATH.write_text(dumps_pretty_json(summary), encoding="utf-8")

    log_info("Stage 8/8: Updating run history and README status")
    write_run_history(RUN_HISTORY_PATH, summary)
    update_readme_status(README_PATH, summary)
    log_info(f"Pipeline completed in {format_duration(time.monotonic() - pipeline_started)}")

    print(dumps_pretty_json(summary))


if __name__ == "__main__":
    main()
