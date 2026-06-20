#!/usr/bin/env python3
"""Build static site folder from web assets + generated data."""

from __future__ import annotations

import html
import ast
import csv
import json
import math
import os
import re
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT / "web"
DATA_DIR = ROOT / "data"
SITE_DIR = ROOT / "site"
SITE_DATA_DIR = SITE_DIR / "data"
STATION_DIR = SITE_DIR / "station"
SITE_ORIGIN = "https://woladen.de"
SITEMAP_MAX_URLS = 45_000
SOCIAL_IMAGE_VERSION = "20260620-eu"
SOCIAL_IMAGE_PATH = f"img/social-card-home.png?v={SOCIAL_IMAGE_VERSION}"
SOCIAL_IMAGE_WIDTH = "1200"
SOCIAL_IMAGE_HEIGHT = "630"
SOCIAL_IMAGE_ALT = (
    "woladen.de preview with a Europe map and the slogan: Zuverlässig laden. Europaweit. Ohne Ladeweile."
)
IOS_APP_LINK = "https://apps.apple.com/de/app/wo-laden/id6759499459"
ANDROID_APP_LINK = "https://play.google.com/store/apps/details?id=de.woladen.android"
STATION_ID_NAMESPACE = "DE:"
STATIC_BUNDLE_ENV = "WOLADEN_STATIC_BUNDLE_PATH"
STATIC_BUNDLE_CANDIDATES = (
    DATA_DIR / "eu27_ch_static" / "open_static.sqlite3",
    ROOT.parent / "Woladen.de-analytics" / "data" / "eu27_ch_static" / "open_static.sqlite3",
    ROOT.parent / "woladen.de-analytics" / "data" / "eu27_ch_static" / "open_static.sqlite3",
)
LIVE_STATE_ENV = "WOLADEN_LIVE_STATE_PATH"
LIVE_STATE_CANDIDATES = (
    ROOT.parent / "Woladen.de-analytics" / "data" / "live_state.sqlite3",
    ROOT.parent / "woladen.de-analytics" / "data" / "live_state.sqlite3",
    DATA_DIR / "live_state.sqlite3",
)
LIVE_MATCH_ENV = "WOLADEN_LIVE_MATCH_PATH"
LIVE_MATCH_CANDIDATES = (
    DATA_DIR / "mobilithek_afir_static_matches.csv",
    ROOT.parent / "Woladen.de-analytics" / "data" / "mobilithek_afir_static_matches.csv",
    ROOT.parent / "woladen.de-analytics" / "data" / "mobilithek_afir_static_matches.csv",
)
MANAGEMENT_INDEX_ENV = "WOLADEN_MANAGEMENT_INDEX_PATH"
MANAGEMENT_INDEX_CANDIDATES = (
    DATA_DIR / "management" / "index.json",
    ROOT.parent / "Woladen.de-analytics" / "commercial_web" / "data" / "management" / "index.json",
    ROOT.parent / "woladen.de-analytics" / "commercial_web" / "data" / "management" / "index.json",
)
MANAGEMENT_SNAPSHOT_ENV = "WOLADEN_MANAGEMENT_SNAPSHOT_PATH"
COUNTRY_STATION_ID_RE = re.compile(r"^([A-Za-z]{2}):(.*)$")

REQUIRED_DATA = [
    "chargers_fast.geojson",
    "chargers_under_50.geojson",
    "operators.json",
    "open_static_summary.json",
    "station_ratings.json",
    "summary.json",
]

ROOT_URLS = [
    "",
    "management.html",
    "privacy.html",
    "imprint.html",
]

SEO_LANGUAGES = ("en", "de", "fr", "nl")
SEO_REQUIRED_KEYS = (
    "brandName",
    "seoName",
    "primaryTagline",
    "humanHook",
    "timeLine",
    "productMessage",
    "homeTitle",
    "homeDescription",
    "homeIntro",
    "homeCountryListTitle",
    "coveragePath",
    "coverageTitle",
    "coverageH1",
    "coverageDescription",
    "coverageIntro",
    "coverageLinkLabel",
    "primaryCtaLabel",
    "appDownloadLabel",
    "appStoreAlt",
    "googlePlayAlt",
    "seoCtaTitle",
    "seoCtaBody",
    "countryTitle",
    "countryH1",
    "countryDescription",
    "countryIntro",
    "countriesLabel",
    "stationsLabel",
    "chargingPointsLabel",
    "fastStationsLabel",
    "countryDataBoxKicker",
    "countryDataBoxTitle",
    "countryDataBoxBody",
    "staticStationsMetric",
    "liveInfoStationsMetric",
    "staticStationsDescription",
    "liveInfoStationsDescription",
    "dataFreshness",
    "openApp",
    "exploreCoverage",
    "topStopsTitle",
    "topStopsIntro",
    "majorOperatorsTitle",
    "backToCoverage",
    "stationChargingPoints",
    "stationNearbyPlaces",
)
COUNTRY_SEO = {
    "AT": {
        "en": ("Austria", "austria"),
        "de": ("Österreich", "oesterreich"),
        "fr": ("Autriche", "autriche"),
        "nl": ("Oostenrijk", "oostenrijk"),
    },
    "BE": {
        "en": ("Belgium", "belgium"),
        "de": ("Belgien", "belgien"),
        "fr": ("Belgique", "belgique"),
        "nl": ("België", "belgie"),
    },
    "CH": {
        "en": ("Switzerland", "switzerland"),
        "de": ("Schweiz", "schweiz"),
        "fr": ("Suisse", "suisse"),
        "nl": ("Zwitserland", "zwitserland"),
    },
    "CY": {
        "en": ("Cyprus", "cyprus"),
        "de": ("Zypern", "zypern"),
        "fr": ("Chypre", "chypre"),
        "nl": ("Cyprus", "cyprus"),
    },
    "CZ": {
        "en": ("Czechia", "czechia"),
        "de": ("Tschechien", "tschechien"),
        "fr": ("Tchéquie", "tchequie"),
        "nl": ("Tsjechië", "tsjechie"),
    },
    "DE": {
        "en": ("Germany", "germany"),
        "de": ("Deutschland", "deutschland"),
        "fr": ("Allemagne", "allemagne"),
        "nl": ("Duitsland", "duitsland"),
    },
    "DK": {
        "en": ("Denmark", "denmark"),
        "de": ("Dänemark", "daenemark"),
        "fr": ("Danemark", "danemark"),
        "nl": ("Denemarken", "denemarken"),
    },
    "ES": {
        "en": ("Spain", "spain"),
        "de": ("Spanien", "spanien"),
        "fr": ("Espagne", "espagne"),
        "nl": ("Spanje", "spanje"),
    },
    "FI": {
        "en": ("Finland", "finland"),
        "de": ("Finnland", "finnland"),
        "fr": ("Finlande", "finlande"),
        "nl": ("Finland", "finland"),
    },
    "FR": {
        "en": ("France", "france"),
        "de": ("Frankreich", "frankreich"),
        "fr": ("France", "france"),
        "nl": ("Frankrijk", "frankrijk"),
    },
    "GR": {
        "en": ("Greece", "greece"),
        "de": ("Griechenland", "griechenland"),
        "fr": ("Grèce", "grece"),
        "nl": ("Griekenland", "griekenland"),
    },
    "HU": {
        "en": ("Hungary", "hungary"),
        "de": ("Ungarn", "ungarn"),
        "fr": ("Hongrie", "hongrie"),
        "nl": ("Hongarije", "hongarije"),
    },
    "LT": {
        "en": ("Lithuania", "lithuania"),
        "de": ("Litauen", "litauen"),
        "fr": ("Lituanie", "lituanie"),
        "nl": ("Litouwen", "litouwen"),
    },
    "LU": {
        "en": ("Luxembourg", "luxembourg"),
        "de": ("Luxemburg", "luxemburg"),
        "fr": ("Luxembourg", "luxembourg"),
        "nl": ("Luxemburg", "luxemburg"),
    },
    "LV": {
        "en": ("Latvia", "latvia"),
        "de": ("Lettland", "lettland"),
        "fr": ("Lettonie", "lettonie"),
        "nl": ("Letland", "letland"),
    },
    "MT": {
        "en": ("Malta", "malta"),
        "de": ("Malta", "malta"),
        "fr": ("Malte", "malte"),
        "nl": ("Malta", "malta"),
    },
    "NL": {
        "en": ("Netherlands", "netherlands"),
        "de": ("Niederlande", "niederlande"),
        "fr": ("Pays-Bas", "pays-bas"),
        "nl": ("Nederland", "nederland"),
    },
    "NO": {
        "en": ("Norway", "norway"),
        "de": ("Norwegen", "norwegen"),
        "fr": ("Norvège", "norvege"),
        "nl": ("Noorwegen", "noorwegen"),
    },
    "PL": {
        "en": ("Poland", "poland"),
        "de": ("Polen", "polen"),
        "fr": ("Pologne", "pologne"),
        "nl": ("Polen", "polen"),
    },
    "PT": {
        "en": ("Portugal", "portugal"),
        "de": ("Portugal", "portugal"),
        "fr": ("Portugal", "portugal"),
        "nl": ("Portugal", "portugal"),
    },
    "SE": {
        "en": ("Sweden", "sweden"),
        "de": ("Schweden", "schweden"),
        "fr": ("Suède", "suede"),
        "nl": ("Zweden", "zweden"),
    },
    "SI": {
        "en": ("Slovenia", "slovenia"),
        "de": ("Slowenien", "slowenien"),
        "fr": ("Slovénie", "slovenie"),
        "nl": ("Slovenië", "slovenie"),
    },
}

AMENITY_LABELS = {
    "bakery": "Bakery",
    "cafe": "Cafe",
    "convenience": "Convenience store",
    "fast_food": "Fast Food",
    "hotel": "Hotel",
    "ice_cream": "Ice cream",
    "museum": "Museum",
    "park": "Park",
    "pharmacy": "Pharmacy",
    "playground": "Playground",
    "restaurant": "Restaurant",
    "supermarket": "Supermarket",
    "toilets": "Toilets",
}

AMENITY_GROUPS = (
    ("Food & drink", ("restaurant", "cafe", "fast_food", "ice_cream", "bakery")),
    ("Shopping", ("supermarket", "convenience", "pharmacy")),
    ("Leisure & nature", ("museum", "playground", "park")),
    ("Accommodation", ("hotel",)),
    ("Other", ()),
)
AMENITY_GROUP_BY_CATEGORY = {
    category: label
    for label, categories in AMENITY_GROUPS
    for category in categories
}


@dataclass(frozen=True)
class SeoCountry:
    code: str
    station_count: int
    charger_count: int
    fast_station_count: int
    source_name: str
    live_station_count: int = 0


@dataclass(frozen=True)
class SeoStationSummary:
    country_code: str
    station_id: str
    operator: str
    city: str
    max_power_kw: float
    charger_count: int
    amenities_total: int


@dataclass(frozen=True)
class SeoPage:
    identity: str
    language: str
    path: str
    title: str
    description: str
    h1: str
    alternate_paths: dict[str, str]
    body_html: str
    sitemap_group: str


def format_text(value: object) -> str:
    return html.escape(str(value or "").strip())


def format_opening_hours_display(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    day_keys = ["Mo", "Tu", "We", "Th", "Fr", "Sa", "Su"]
    replacements = {
        "Mo": "Mo",
        "Tu": "Tu",
        "We": "We",
        "Th": "Th",
        "Fr": "Fr",
        "Sa": "Sa",
        "Su": "Su",
    }
    holiday_states: set[str] = set()
    fallback_clauses: list[str] = []
    day_clauses_by_body: dict[str, set[str]] = {}
    day_token = r"(?:Mo|Tu|We|Th|Fr|Sa|Su|PH)"
    day_selector_re = re.compile(
        rf"^({day_token}(?:\s*-\s*(?:Mo|Tu|We|Th|Fr|Sa|Su))?(?:\s*,\s*{day_token}(?:\s*-\s*(?:Mo|Tu|We|Th|Fr|Sa|Su))?)*)\s+(.+)$"
    )

    def expand_day_range(start: str, end: str) -> list[str]:
        if start not in day_keys or end not in day_keys:
            return []
        days: list[str] = []
        start_index = day_keys.index(start)
        end_index = day_keys.index(end)
        for offset in range(len(day_keys)):
            index = (start_index + offset) % len(day_keys)
            days.append(day_keys[index])
            if index == end_index:
                break
        return days

    def parse_day_selector(selector: str) -> tuple[set[str], bool]:
        selected_days: set[str] = set()
        matches_public_holiday = False
        for part in selector.split(","):
            token = part.strip()
            if not token:
                continue
            if token == "PH":
                matches_public_holiday = True
                continue
            range_match = re.match(r"^([A-Z][a-z])\s*-\s*([A-Z][a-z])$", token)
            if range_match:
                selected_days.update(expand_day_range(range_match.group(1), range_match.group(2)))
            elif token in day_keys:
                selected_days.add(token)
        return selected_days, matches_public_holiday

    def first_day_index(days: set[str]) -> int:
        indexes = [index for index, day in enumerate(day_keys) if day in days]
        return min(indexes) if indexes else len(day_keys)

    def format_days(days: set[str]) -> str:
        ordered = [day for day in day_keys if day in days]
        ranges: list[str] = []
        index = 0
        while index < len(ordered):
            start = ordered[index]
            end = start
            while index + 1 < len(ordered) and day_keys.index(ordered[index + 1]) == day_keys.index(end) + 1:
                index += 1
                end = ordered[index]
            if start == end:
                ranges.append(replacements[start])
            else:
                ranges.append(f"{replacements[start]}-{replacements[end]}")
            index += 1
        return ", ".join(ranges)

    def format_clause(clause: str) -> str:
        clause = re.sub(r"\b(\d{1,2}:\d{2})\s*-\s*\d{1,2}:\d{2}\+", r"from \1", clause.strip())
        clause = re.sub(r"\b(\d{1,2}:\d{2})\+", r"from \1", clause)
        formatted = re.sub(
            r"\b(Mo|Tu|We|Th|Fr|Sa|Su)\b",
            lambda match: replacements[match.group(1)],
            clause,
        )
        formatted = re.sub(r"\boff\b", "closed", formatted, flags=re.IGNORECASE)
        formatted = re.sub(r"\bclosed\b", "closed", formatted, flags=re.IGNORECASE)
        formatted = re.sub(r"\bopen\b", "open", formatted, flags=re.IGNORECASE)
        return re.sub(r",\s*", ", ", formatted)

    for clause in text.split(";"):
        trimmed = clause.strip()
        match = day_selector_re.match(trimmed)
        if not match:
            formatted = format_clause(trimmed)
            if formatted:
                fallback_clauses.append(formatted)
            continue

        body = match.group(2).strip()
        is_closed_clause = re.match(r"^(?:off|closed)$", body, flags=re.IGNORECASE) is not None
        selected_days, matches_public_holiday = parse_day_selector(match.group(1))
        if matches_public_holiday and not is_closed_clause:
            holiday_states.add("open")
        if not selected_days or is_closed_clause:
            continue

        body_display = format_clause(body)
        day_clauses_by_body.setdefault(body_display, set()).update(selected_days)

    clauses = [
        f"{format_days(days)} {body_display}"
        for body_display, days in sorted(
            day_clauses_by_body.items(),
            key=lambda item: first_day_index(item[1]),
        )
    ]
    clauses.extend(clause for clause in fallback_clauses if clause)

    if "open" in holiday_states:
        clauses.append("open on public holidays")

    return "; ".join(clauses)


def format_power_kw(value: object) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "0"
    rounded = round(numeric)
    if abs(numeric - rounded) < 0.05:
        return str(int(rounded))
    return f"{numeric:.1f}".rstrip("0").rstrip(".")


def to_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def is_truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def format_amenity_count(count: int) -> str:
    label = "nearby amenity" if count == 1 else "nearby amenities"
    return f"{count} {label}"


def sanitize_json_value(value: object) -> object:
    if isinstance(value, dict):
        return {key: sanitize_json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [sanitize_json_value(item) for item in value]
    if isinstance(value, str):
        return "" if value.strip().lower() in {"nan", "nat"} else value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def public_station_id(value: object) -> str:
    station_id = str(value or "").strip()
    if not station_id:
        return ""
    country_match = COUNTRY_STATION_ID_RE.match(station_id)
    if country_match:
        return f"{country_match.group(1).upper()}:{country_match.group(2)}"
    return f"{STATION_ID_NAMESPACE}{station_id}"


def station_country_code(value: object, assume_default_namespace: bool = False) -> str:
    station_id = str(value or "").strip()
    if not station_id:
        return ""
    country_match = COUNTRY_STATION_ID_RE.match(station_id)
    if country_match:
        code = country_match.group(1).upper()
        return code if code in COUNTRY_SEO else ""
    if assume_default_namespace:
        code = STATION_ID_NAMESPACE.rstrip(":").upper()
        return code if code in COUNTRY_SEO else ""
    return ""


def grouped_station_key(value: object, assume_default_namespace: bool = False) -> tuple[str, str]:
    code = station_country_code(value, assume_default_namespace)
    if not code:
        return "", ""
    return code, public_station_id(value)


def station_url_query(query: str) -> str:
    parts: list[str] = []
    for key, value in parse_qsl(query, keep_blank_values=True):
        next_value = public_station_id(value) if key == "station" else value
        parts.append(f"{quote(key)}={quote(str(next_value), safe=':')}")
    return "&".join(parts)


def public_station_url(value: object) -> str:
    station_url = str(value or "").strip()
    if not station_url:
        return ""
    split = urlsplit(station_url)
    if not split.query:
        return station_url
    return urlunsplit(
        (
            split.scheme,
            split.netloc,
            split.path,
            station_url_query(split.query),
            split.fragment,
        )
    )


def public_bundle_value(value: object, key: str = "") -> object:
    if key == "station_id":
        return public_station_id(value)
    if key == "station_url":
        return public_station_url(value)
    if isinstance(value, dict):
        return {item_key: public_bundle_value(item_value, item_key) for item_key, item_value in value.items()}
    if isinstance(value, list):
        return [public_bundle_value(item) for item in value]
    return value


def resolve_static_bundle_path() -> Path:
    configured = os.environ.get(STATIC_BUNDLE_ENV)
    candidates = [Path(configured).expanduser()] if configured else []
    candidates.extend(STATIC_BUNDLE_CANDIDATES)
    for candidate in candidates:
        if candidate.exists():
            return candidate
    searched = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(
        f"Europe static bundle not found. Set {STATIC_BUNDLE_ENV} or place open_static.sqlite3 at one of: {searched}"
    )


def existing_source_paths(candidates: tuple[Path, ...], env_var: str, explicit_path: Path | None = None) -> list[Path]:
    raw_candidates: list[Path] = []
    if explicit_path is not None:
        raw_candidates.append(explicit_path)
    else:
        configured = os.environ.get(env_var)
        if configured:
            raw_candidates.append(Path(configured).expanduser())
        raw_candidates.extend(candidates)

    paths: list[Path] = []
    seen: set[str] = set()
    for candidate in raw_candidates:
        if not candidate.exists():
            continue
        resolved = str(candidate.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        paths.append(candidate)
    return paths


def parse_json_object(value: object) -> dict[str, object]:
    text = str(value or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def parse_json_list(value: object) -> list[object]:
    text = str(value or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []
    return payload if isinstance(payload, list) else []


def parse_boolean(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def format_static_list_value(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    items: list[str] = []
    if text.startswith("["):
        try:
            literal = ast.literal_eval(text)
        except (SyntaxError, ValueError):
            literal = None
        if isinstance(literal, list):
            items = [str(item or "").strip() for item in literal]
    if not items:
        items = re.split(r"\s*[;|]\s*", text)
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        normalized = re.sub(r"\s+", " ", str(item or "").strip())
        if not normalized or normalized.lower() in seen:
            continue
        seen.add(normalized.lower())
        deduped.append(normalized)
    return "; ".join(deduped[:12])


def source_name_from_attribution(value: object, fallback: object = "") -> str:
    attribution = parse_json_object(value)
    for key in ("source_name", "display_name", "attribution", "source_uid"):
        text = str(attribution.get(key) or "").strip()
        if text:
            return text
    return str(fallback or "").strip()


def static_station_feature(row: sqlite3.Row) -> dict[str, object]:
    amenity_counts = parse_json_object(row["amenity_category_counts_json"])
    amenity_examples = parse_json_list(row["amenity_examples_json"])
    source_name = source_name_from_attribution(row["source_attribution_json"], row["source_uid"])
    station_name = str(row["station_name"] or "").strip()
    operator_name = str(row["operator_name"] or "").strip()
    operator = operator_name or station_name or "Unknown operator"
    connector_types = format_static_list_value(row["connector_types"])
    max_power_kw = row["max_power_kw"]
    charger_count = to_int(row["charger_count"], default=1)
    green_energy = parse_boolean(row["green_energy"])
    properties: dict[str, object] = {
        "country_code": str(row["country_code"] or "").strip().upper(),
        "station_id": public_station_id(row["station_id"]),
        "source_station_id": str(row["source_station_id"] or "").strip(),
        "operator": operator,
        "station_name": station_name,
        "address": str(row["address"] or "").strip(),
        "postcode": str(row["postal_code"] or "").strip(),
        "city": str(row["city"] or "").strip(),
        "max_power_kw": max_power_kw,
        "charging_points_count": charger_count,
        "connector_count": charger_count,
        "connector_types_display": connector_types,
        "payment_methods_display": format_static_list_value(row["payment_methods"]),
        "auth_methods_display": format_static_list_value(row["auth_methods"]),
        "opening_hours_display": str(row["opening_hours"] or "").strip(),
        "price_display": str(row["price_display"] or "").strip(),
        "helpdesk_phone": str(row["helpdesk_phone"] or "").strip(),
        "amenities_total": to_int(row["amenities_total"], default=0),
        "amenity_examples": amenity_examples,
        "detail_source_name": source_name,
        "detail_source_url": str(row["station_source_url"] or row["source_url"] or "").strip(),
        "detail_last_updated": str(row["detail_last_updated"] or "").strip(),
    }
    if green_energy is not None:
        properties["green_energy"] = green_energy
    for category, count in amenity_counts.items():
        properties[f"amenity_{category}"] = count
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [row["longitude"], row["latitude"]],
        },
        "properties": properties,
    }


def iter_static_station_features() -> object:
    bundle_path = resolve_static_bundle_path()
    conn = sqlite3.connect(bundle_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
              s.country_code,
              s.station_id,
              s.source_uid,
              s.source_station_id,
              s.operator_name,
              s.station_name,
              s.address,
              s.postal_code,
              s.city,
              s.latitude,
              s.longitude,
              s.charger_count,
              s.max_power_kw,
              s.connector_types,
              s.source_url AS station_source_url,
              s.opening_hours,
              s.payment_methods,
              s.auth_methods,
              s.green_energy,
              s.helpdesk_phone,
              s.price_display,
              s.detail_last_updated,
              COALESCE(a.amenities_total, 0) AS amenities_total,
              COALESCE(a.amenity_category_counts_json, '{}') AS amenity_category_counts_json,
              COALESCE(a.amenity_examples_json, '[]') AS amenity_examples_json,
              src.source_url,
              src.attribution_json AS source_attribution_json
            FROM stations s
            LEFT JOIN station_amenities a ON a.station_uid = s.station_uid
            LEFT JOIN sources src ON src.source_uid = s.source_uid
            WHERE s.station_id IS NOT NULL
              AND TRIM(s.station_id) != ''
              AND s.latitude IS NOT NULL
              AND s.longitude IS NOT NULL
            ORDER BY s.country_code, s.station_id
            """
        )
        for row in rows:
            yield static_station_feature(row)
    finally:
        conn.close()


def station_page_path(station_id: str) -> str:
    public_id = public_station_id(station_id)
    if ":" in public_id:
        namespace, local_id = public_id.split(":", 1)
        return f"station/{quote(namespace, safe='')}/{quote(local_id, safe='')}.html"
    return f"station/{quote(public_id, safe='')}.html"


def station_query_url(station_id: str) -> str:
    return f"/?station={quote(public_station_id(station_id), safe=':')}"


def absolute_url(path: str) -> str:
    clean = path.lstrip("/")
    if not clean:
        return f"{SITE_ORIGIN}/"
    return f"{SITE_ORIGIN}/{clean}"


def load_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def load_seo_bundles() -> dict[str, dict[str, str]]:
    bundles: dict[str, dict[str, str]] = {}
    missing: list[str] = []
    for language in SEO_LANGUAGES:
        path = WEB_DIR / "i18n" / f"{language}.json"
        if not path.exists():
            missing.append(f"{language}: missing {path.relative_to(ROOT)}")
            continue
        bundle = load_json(path)
        seo = bundle.get("seo")
        if not isinstance(seo, dict):
            missing.append(f"{language}: missing seo namespace")
            continue
        values: dict[str, str] = {}
        for key in SEO_REQUIRED_KEYS:
            value = str(seo.get(key) or "").strip()
            if not value:
                missing.append(f"{language}: missing seo.{key}")
            values[key] = value
        bundles[language] = values
    if missing:
        raise ValueError("Incomplete SEO translations:\n" + "\n".join(f"- {item}" for item in missing))
    return bundles


def interpolate(template: str, values: dict[str, object]) -> str:
    result = template
    for key, value in values.items():
        result = result.replace("{" + key + "}", str(value))
    return result


def country_display_name(country_code: str, language: str) -> str:
    try:
        return COUNTRY_SEO[country_code][language][0]
    except KeyError as exc:
        raise ValueError(f"Missing SEO country name for {country_code}/{language}") from exc


def country_slug(country_code: str, language: str) -> str:
    try:
        return COUNTRY_SEO[country_code][language][1]
    except KeyError as exc:
        raise ValueError(f"Missing SEO country slug for {country_code}/{language}") from exc


def seo_home_path(language: str) -> str:
    return f"{language}/index.html"


def seo_coverage_path(language: str, bundles: dict[str, dict[str, str]]) -> str:
    return f"{language}/{bundles[language]['coveragePath'].strip('/')}/index.html"


def seo_country_path(country_code: str, language: str) -> str:
    return f"{language}/{country_slug(country_code, language)}/index.html"


def public_path(path: str) -> str:
    if path.endswith("index.html"):
        return path.removesuffix("index.html")
    return path


def page_url(path: str) -> str:
    return absolute_url(public_path(path))


def load_live_state_station_ids_by_country(live_state_path: Path | None = None) -> dict[str, set[str]]:
    station_ids: dict[str, set[str]] = {}
    paths = existing_source_paths(LIVE_STATE_CANDIDATES, LIVE_STATE_ENV, live_state_path)
    for path in paths:
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
            conn.execute("PRAGMA query_only=ON")
            rows = conn.execute(
                """
                SELECT station_id
                FROM station_current_state
                WHERE station_id IS NOT NULL
                  AND TRIM(station_id) != ''
                """
            )
            for row in rows:
                country_code, station_id = grouped_station_key(row[0])
                if country_code and station_id:
                    station_ids.setdefault(country_code, set()).add(station_id)
        except sqlite3.DatabaseError:
            if live_state_path is not None or os.environ.get(LIVE_STATE_ENV):
                raise
            continue
        finally:
            if conn is not None:
                conn.close()
    return station_ids


def load_reviewed_match_station_ids_by_country(match_path: Path | None = None) -> dict[str, set[str]]:
    station_ids: dict[str, set[str]] = {}
    paths = existing_source_paths(LIVE_MATCH_CANDIDATES, LIVE_MATCH_ENV, match_path)
    for path in paths:
        with path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if not is_truthy(row.get("station_in_bundle")):
                    continue
                country_code, station_id = grouped_station_key(row.get("station_id"), assume_default_namespace=True)
                if country_code and station_id:
                    station_ids.setdefault(country_code, set()).add(station_id)
    return station_ids


def latest_management_snapshot_path(index_path: Path) -> Path | None:
    payload = load_json(index_path)
    latest_date = str(payload.get("latest_date") or "").strip()
    if not latest_date:
        available_dates = payload.get("available_dates")
        if isinstance(available_dates, list):
            latest_date = max((str(item).strip() for item in available_dates if str(item).strip()), default="")
    if not latest_date:
        return None

    snapshot_paths = payload.get("snapshot_paths")
    if isinstance(snapshot_paths, dict):
        snapshot_path = str(snapshot_paths.get(latest_date) or "").strip()
        if snapshot_path:
            return index_path.parent / snapshot_path

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", latest_date):
        year, month, day = latest_date.split("-")
        return index_path.parent / "days" / year / month / day / "snapshot.json"
    return None


def station_count_from_management_summary(value: object) -> int | None:
    if not isinstance(value, dict) or "afir_stations_observed" not in value:
        return None
    return max(0, to_int(value.get("afir_stations_observed")))


def load_management_station_counts_by_country(
    index_path: Path | None = None,
    snapshot_path: Path | None = None,
) -> dict[str, int]:
    snapshot_paths = existing_source_paths((), MANAGEMENT_SNAPSHOT_ENV, snapshot_path)
    payloads: list[dict[str, object]] = []
    if snapshot_paths:
        payloads = [load_json(path) for path in snapshot_paths]
    else:
        snapshot_entries: list[tuple[str, int, dict[str, object]]] = []
        for sequence, path in enumerate(existing_source_paths(MANAGEMENT_INDEX_CANDIDATES, MANAGEMENT_INDEX_ENV, index_path)):
            candidate = latest_management_snapshot_path(path)
            if candidate and candidate.exists():
                payload = load_json(candidate)
                snapshot_date = str(payload.get("snapshot_date") or "").strip()
                snapshot_entries.append((snapshot_date, sequence, payload))
        if snapshot_entries:
            latest_date = max(entry[0] for entry in snapshot_entries)
            payloads = [payload for date, _, payload in snapshot_entries if date == latest_date]

    counts: dict[str, int] = {}
    for payload in payloads:
        country_summaries = payload.get("country_summaries")
        if isinstance(country_summaries, dict):
            for raw_code, summary in country_summaries.items():
                code = str(raw_code or "").strip().upper()
                count = station_count_from_management_summary(summary)
                if code in COUNTRY_SEO and count is not None:
                    counts[code] = count
            continue

        count = station_count_from_management_summary(payload.get("summary"))
        if count is not None:
            counts["DE"] = count
    return counts


def load_dynamic_station_counts_by_country(
    live_state_path: Path | None = None,
    match_path: Path | None = None,
    management_index_path: Path | None = None,
    management_snapshot_path: Path | None = None,
) -> dict[str, int]:
    station_ids = load_live_state_station_ids_by_country(live_state_path)
    for country_code, ids in load_reviewed_match_station_ids_by_country(match_path).items():
        station_ids.setdefault(country_code, set()).update(ids)
    counts = {country_code: len(ids) for country_code, ids in station_ids.items()}
    counts.update(load_management_station_counts_by_country(management_index_path, management_snapshot_path))
    return counts


def load_seo_countries(
    summary_path: Path | None = None,
    live_counts_by_country: dict[str, int] | None = None,
) -> tuple[list[SeoCountry], str]:
    summary_path = summary_path or DATA_DIR / "open_static_summary.json"
    live_counts_by_country = live_counts_by_country or {}
    payload = load_json(summary_path)
    generated_at = str(payload.get("generated_at") or "").strip()
    rows = payload.get("countries")
    if not isinstance(rows, list):
        raise ValueError(f"{summary_path} does not contain a countries array")

    countries: list[SeoCountry] = []
    missing_codes: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = str(row.get("code") or "").strip().upper()
        if not code:
            continue
        if code not in COUNTRY_SEO:
            missing_codes.append(code)
            continue
        station_count = to_int(row.get("station_count"))
        countries.append(
            SeoCountry(
                code=code,
                station_count=station_count,
                charger_count=to_int(row.get("charger_count")),
                fast_station_count=to_int(row.get("fast_station_count")),
                source_name=str(row.get("name") or "").strip(),
                live_station_count=min(to_int(live_counts_by_country.get(code)), station_count),
            )
        )
    if missing_codes:
        raise ValueError("Missing SEO country metadata for: " + ", ".join(sorted(set(missing_codes))))
    countries.sort(key=lambda country: country.code)
    return countries, generated_at


def format_count(value: int, language: str) -> str:
    separator = "." if language in {"de", "nl"} else "\u202f" if language == "fr" else ","
    return f"{value:,}".replace(",", separator)


def render_hreflang_links(alternate_paths: dict[str, str]) -> str:
    lines = [
        f'<link rel="alternate" hreflang="{html.escape(language)}" href="{html.escape(page_url(path))}" />'
        for language, path in sorted(alternate_paths.items())
    ]
    if "en" in alternate_paths:
        lines.insert(0, f'<link rel="alternate" hreflang="x-default" href="{html.escape(page_url(alternate_paths["en"]))}" />')
    return "\n    ".join(lines)


def render_json_ld(payload: dict[str, object]) -> str:
    return html.escape(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), quote=False)


def render_seo_shell(page: SeoPage, bundles: dict[str, dict[str, str]], structured_data: list[dict[str, object]] | None = None) -> str:
    bundle = bundles[page.language]
    canonical_url = page_url(page.path)
    alternate_links = render_hreflang_links(page.alternate_paths)
    structured_blocks = "\n    ".join(
        f'<script type="application/ld+json">{render_json_ld(payload)}</script>'
        for payload in structured_data or []
    )
    return f"""<!doctype html>
<html lang="{html.escape(page.language)}">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{format_text(page.title)}</title>
    <meta name="description" content="{format_text(page.description)}" />
    <link rel="canonical" href="{html.escape(canonical_url)}" />
    {alternate_links}
    <meta property="og:type" content="website" />
    <meta property="og:title" content="{format_text(page.title)}" />
    <meta property="og:description" content="{format_text(page.description)}" />
    <meta property="og:url" content="{html.escape(canonical_url)}" />
    <meta property="og:site_name" content="woladen" />
    <meta property="og:image" content="{html.escape(absolute_url(SOCIAL_IMAGE_PATH))}" />
    <meta property="og:image:width" content="{SOCIAL_IMAGE_WIDTH}" />
    <meta property="og:image:height" content="{SOCIAL_IMAGE_HEIGHT}" />
    <meta property="og:image:alt" content="{format_text(bundle['seoName'])}" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="{format_text(page.title)}" />
    <meta name="twitter:description" content="{format_text(page.description)}" />
    <meta name="twitter:image" content="{html.escape(absolute_url(SOCIAL_IMAGE_PATH))}" />
    <link rel="icon" href="/favicon.ico?v=20260411" sizes="any" />
    <link rel="icon" type="image/png" sizes="32x32" href="/favicon-32x32.png?v=20260411" />
    <link rel="icon" type="image/png" sizes="16x16" href="/favicon-16x16.png?v=20260411" />
    <link rel="apple-touch-icon" sizes="180x180" href="/img/touch-icon.png?v=20260411" />
    <link rel="stylesheet" href="/styles.css?v=20260620-seo-cta-fullwidth1" />
    {structured_blocks}
  </head>
  <body class="seo-page legal-page">
    <main class="legal-shell seo-shell">
      <a href="/?lang={html.escape(page.language)}" class="legal-back">{format_text(bundle['openApp'])}</a>
      <section class="legal-card seo-hero">
        <p class="legal-kicker">{format_text(bundle['primaryTagline'])}</p>
        <h1>{format_text(page.h1)}</h1>
        <p class="legal-intro">{format_text(bundle['humanHook'])} {format_text(bundle['timeLine'])}</p>
        {page.body_html}
      </section>
    </main>
  </body>
</html>
"""


def country_stat_items(country: SeoCountry, language: str, bundle: dict[str, str]) -> str:
    stats = (
        (bundle["stationsLabel"], country.station_count),
        (bundle["chargingPointsLabel"], country.charger_count),
        (bundle["fastStationsLabel"], country.fast_station_count),
    )
    return "".join(
        '<li>'
        f'<strong>{html.escape(format_count(value, language))}</strong>'
        f'{format_text(label)}'
        '</li>'
        for label, value in stats
    )


def seo_app_href(language: str, station_id: str = "") -> str:
    query = f"lang={quote(language)}"
    if station_id:
        query = f"{query}&station={quote(public_station_id(station_id), safe=':')}"
    return f"/?{query}"


def render_seo_cta_text(label: str) -> str:
    return (
        '<span class="seo-cta-pointer" aria-hidden="true">👉</span>'
        f'<span class="seo-cta-text">{format_text(label)}</span>'
        '<span class="seo-cta-pointer" aria-hidden="true">👈</span>'
    )


def render_seo_cta(language: str, bundle: dict[str, str], station_id: str = "") -> str:
    app_href = seo_app_href(language, station_id)
    cta_label = bundle["primaryCtaLabel"]
    return (
        '<div class="seo-action-panel">'
        '<div class="seo-action-copy">'
        f'<h2>{format_text(bundle["seoCtaTitle"])}</h2>'
        f'<p>{format_text(bundle["seoCtaBody"])}</p>'
        "</div>"
        '<div class="seo-action-links">'
        f'<a class="link-btn seo-primary-cta" href="{html.escape(app_href)}" aria-label="{format_text(cta_label)}">{render_seo_cta_text(cta_label)}</a>'
        f'<span>{format_text(bundle["appDownloadLabel"])}</span>'
        '<div class="seo-store-links">'
        f'<a class="app-install-link" href="{html.escape(IOS_APP_LINK)}" aria-label="{format_text(bundle["appStoreAlt"])}">'
        f'<img class="app-install-store-badge app-install-store-badge--apple" src="/img/app-store-badge.svg" alt="{format_text(bundle["appStoreAlt"])}" width="250" height="83" decoding="async" />'
        "</a>"
        f'<a class="app-install-link" href="{html.escape(ANDROID_APP_LINK)}" aria-label="{format_text(bundle["googlePlayAlt"])}">'
        f'<img class="app-install-store-badge app-install-store-badge--google" src="/img/google-play-badge.png" alt="{format_text(bundle["googlePlayAlt"])}" width="646" height="250" decoding="async" />'
        "</a>"
        "</div>"
        "</div>"
        "</div>"
    )


def render_seo_section_cta(language: str, bundle: dict[str, str]) -> str:
    cta_label = bundle["primaryCtaLabel"]
    return (
        '<p class="station-note seo-section-cta">'
        f'<a class="link-btn seo-secondary-cta" href="{html.escape(seo_app_href(language))}" aria-label="{format_text(cta_label)}">'
        f'{render_seo_cta_text(cta_label)}</a>'
        "</p>"
    )


def render_country_data_box(country: SeoCountry, language: str, bundle: dict[str, str]) -> str:
    metrics = (
        (
            country.station_count,
            bundle["staticStationsMetric"],
            bundle["staticStationsDescription"],
        ),
        (
            country.live_station_count,
            bundle["liveInfoStationsMetric"],
            bundle["liveInfoStationsDescription"],
        ),
    )
    metric_cards = "".join(
        '<div class="seo-data-metric">'
        f"<strong>{html.escape(format_count(value, language))}</strong>"
        f"<span>{format_text(label)}</span>"
        f"<small>{format_text(description)}</small>"
        "</div>"
        for value, label, description in metrics
    )
    return (
        '<aside class="app-install-promo seo-data-box">'
        '<div class="app-install-head">'
        '<div class="app-install-copy">'
        f'<p class="app-install-kicker">{format_text(bundle["countryDataBoxKicker"])}</p>'
        f'<h2>{format_text(bundle["countryDataBoxTitle"])}</h2>'
        f'<p>{format_text(bundle["countryDataBoxBody"])}</p>'
        "</div>"
        f'<div class="app-install-links seo-data-metrics">{metric_cards}</div>'
        "</div>"
        "</aside>"
    )


def country_links(countries: list[SeoCountry], language: str) -> str:
    items = []
    for country in countries:
        path = "/" + public_path(seo_country_path(country.code, language))
        items.append(
            "<li>"
            f'<a href="{html.escape(path)}">{format_text(country_display_name(country.code, language))}</a>'
            "</li>"
        )
    return "".join(items)


def coverage_country_rows(countries: list[SeoCountry], language: str, bundle: dict[str, str]) -> str:
    rows: list[str] = []
    for country in countries:
        path = "/" + public_path(seo_country_path(country.code, language))
        rows.append(
            "<tr>"
            f'<td><a href="{html.escape(path)}">{format_text(country_display_name(country.code, language))}</a></td>'
            f'<td class="country-code">{html.escape(country.code)}</td>'
            f'<td class="station-count">{html.escape(format_count(country.station_count, language))}</td>'
            f'<td class="station-count">{html.escape(format_count(country.charger_count, language))}</td>'
            f'<td class="station-count">{html.escape(format_count(country.fast_station_count, language))}</td>'
            "</tr>"
        )
    return "".join(rows)


def load_seo_station_summaries(limit_per_country: int = 6) -> tuple[dict[str, list[SeoStationSummary]], dict[str, list[tuple[str, int]]]]:
    bundle_path = resolve_static_bundle_path()
    conn = sqlite3.connect(bundle_path)
    conn.row_factory = sqlite3.Row
    top_stations: dict[str, list[SeoStationSummary]] = {}
    operators: dict[str, list[tuple[str, int]]] = {}
    try:
        station_rows = conn.execute(
            """
            WITH ranked AS (
              SELECT
                s.country_code,
                s.station_id,
                COALESCE(NULLIF(TRIM(s.operator_name), ''), NULLIF(TRIM(s.station_name), ''), 'Unknown operator') AS operator,
                COALESCE(NULLIF(TRIM(s.city), ''), NULLIF(TRIM(s.postal_code), ''), '') AS city,
                COALESCE(s.max_power_kw, 0) AS max_power_kw,
                COALESCE(s.charger_count, 0) AS charger_count,
                COALESCE(a.amenities_total, 0) AS amenities_total,
                ROW_NUMBER() OVER (
                  PARTITION BY s.country_code
                  ORDER BY COALESCE(a.amenities_total, 0) DESC,
                           COALESCE(s.max_power_kw, 0) DESC,
                           COALESCE(s.charger_count, 0) DESC,
                           s.station_id ASC
                ) AS rank
              FROM stations s
              LEFT JOIN station_amenities a ON a.station_uid = s.station_uid
              WHERE s.station_id IS NOT NULL
                AND TRIM(s.station_id) != ''
                AND s.latitude IS NOT NULL
                AND s.longitude IS NOT NULL
                AND COALESCE(s.max_power_kw, 0) >= 50
            )
            SELECT *
            FROM ranked
            WHERE rank <= ?
            ORDER BY country_code, rank
            """,
            (limit_per_country,),
        )
        for row in station_rows:
            country_code = str(row["country_code"] or "").strip().upper()
            top_stations.setdefault(country_code, []).append(
                SeoStationSummary(
                    country_code=country_code,
                    station_id=public_station_id(row["station_id"]),
                    operator=str(row["operator"] or "").strip(),
                    city=str(row["city"] or "").strip(),
                    max_power_kw=float(row["max_power_kw"] or 0),
                    charger_count=to_int(row["charger_count"]),
                    amenities_total=to_int(row["amenities_total"]),
                )
            )

        operator_rows = conn.execute(
            """
            SELECT
              country_code,
              COALESCE(NULLIF(TRIM(operator_name), ''), NULLIF(TRIM(station_name), ''), 'Unknown operator') AS operator,
              COUNT(*) AS station_count
            FROM stations
            WHERE station_id IS NOT NULL
              AND TRIM(station_id) != ''
              AND COALESCE(max_power_kw, 0) >= 50
            GROUP BY country_code, operator
            ORDER BY country_code, station_count DESC, operator ASC
            """
        )
        for row in operator_rows:
            country_code = str(row["country_code"] or "").strip().upper()
            entries = operators.setdefault(country_code, [])
            if len(entries) < 5:
                entries.append((str(row["operator"] or "").strip(), to_int(row["station_count"])))
    finally:
        conn.close()
    return top_stations, operators


def render_station_summary_list(stations: list[SeoStationSummary], language: str, bundle: dict[str, str]) -> str:
    if not stations:
        return ""
    items: list[str] = []
    for station in stations:
        href = "/" + station_page_path(station.station_id)
        meta = [
            f"{format_power_kw(station.max_power_kw)} kW",
            f"{format_count(station.charger_count, language)} {bundle['stationChargingPoints']}",
            f"{format_count(station.amenities_total, language)} {bundle['stationNearbyPlaces']}",
        ]
        if station.city:
            meta.insert(0, station.city)
        items.append(
            "<li>"
            f'<a href="{html.escape(href)}"><strong>{format_text(station.operator)}</strong></a>'
            f"{format_text(' · '.join(meta))}"
            "</li>"
        )
    return "".join(items)


def render_operator_list(operators: list[tuple[str, int]], language: str) -> str:
    return "".join(
        "<li>"
        f"<strong>{format_text(operator)}</strong>"
        f"{format_text(format_count(count, language))}"
        "</li>"
        for operator, count in operators
        if operator
    )


def build_seo_pages(
    countries: list[SeoCountry],
    generated_at: str,
    bundles: dict[str, dict[str, str]],
    top_stations: dict[str, list[SeoStationSummary]] | None = None,
    operators: dict[str, list[tuple[str, int]]] | None = None,
) -> list[SeoPage]:
    top_stations = top_stations or {}
    operators = operators or {}
    pages: list[SeoPage] = []

    home_alternates = {language: seo_home_path(language) for language in SEO_LANGUAGES}
    coverage_alternates = {language: seo_coverage_path(language, bundles) for language in SEO_LANGUAGES}

    for language in SEO_LANGUAGES:
        bundle = bundles[language]
        coverage_href = "/" + public_path(seo_coverage_path(language, bundles))
        home_body = (
            f'<p class="legal-intro">{format_text(bundle["homeIntro"])}</p>'
            f'{render_seo_cta(language, bundle)}'
            f'<p class="station-note"><a href="{html.escape(coverage_href)}">{format_text(bundle["exploreCoverage"])}</a></p>'
            f'<h2>{format_text(bundle["homeCountryListTitle"])}</h2>'
            f'<ul class="station-list seo-country-list">{country_links(countries, language)}</ul>'
            f'{render_seo_section_cta(language, bundle)}'
        )
        pages.append(
            SeoPage(
                identity="home",
                language=language,
                path=seo_home_path(language),
                title=bundle["homeTitle"],
                description=bundle["homeDescription"],
                h1=bundle["brandName"],
                alternate_paths=home_alternates,
                body_html=home_body,
                sitemap_group="sitemap-seo-home.xml",
            )
        )

        coverage_body = (
            f'<p class="legal-intro">{format_text(bundle["coverageIntro"])}</p>'
            f'{render_seo_cta(language, bundle)}'
            '<table class="country-table seo-country-table"><thead><tr>'
            f'<th scope="col">{format_text(bundle["countriesLabel"])}</th>'
            '<th scope="col">ISO</th>'
            f'<th scope="col" class="station-count">{format_text(bundle["stationsLabel"])}</th>'
            f'<th scope="col" class="station-count">{format_text(bundle["chargingPointsLabel"])}</th>'
            f'<th scope="col" class="station-count">{format_text(bundle["fastStationsLabel"])}</th>'
            '</tr></thead><tbody>'
            f'{coverage_country_rows(countries, language, bundle)}'
            '</tbody></table>'
            f'<p class="station-note">{format_text(interpolate(bundle["dataFreshness"], {"date": generated_at or "unknown"}))}</p>'
            f'{render_seo_section_cta(language, bundle)}'
        )
        pages.append(
            SeoPage(
                identity="coverage",
                language=language,
                path=seo_coverage_path(language, bundles),
                title=bundle["coverageTitle"],
                description=bundle["coverageDescription"],
                h1=bundle["coverageH1"],
                alternate_paths=coverage_alternates,
                body_html=coverage_body,
                sitemap_group="sitemap-seo-coverage.xml",
            )
        )

    for country in countries:
        country_alternates = {
            language: seo_country_path(country.code, language)
            for language in SEO_LANGUAGES
        }
        for language in SEO_LANGUAGES:
            bundle = bundles[language]
            country_name = country_display_name(country.code, language)
            coverage_href = "/" + public_path(seo_coverage_path(language, bundles))
            body = (
                f'<p class="legal-intro">{format_text(interpolate(bundle["countryIntro"], {"country": country_name}))}</p>'
                f'{render_seo_cta(language, bundle)}'
                f'{render_country_data_box(country, language, bundle)}'
                f'<ul class="station-list seo-stat-list">{country_stat_items(country, language, bundle)}</ul>'
                f'{render_seo_section_cta(language, bundle)}'
                f'<h2>{format_text(bundle["topStopsTitle"])}</h2>'
                f'<p>{format_text(bundle["topStopsIntro"])}</p>'
                f'<ul class="station-list">{render_station_summary_list(top_stations.get(country.code, []), language, bundle)}</ul>'
                f'{render_seo_section_cta(language, bundle)}'
                f'<h2>{format_text(bundle["majorOperatorsTitle"])}</h2>'
                f'<ul class="station-list">{render_operator_list(operators.get(country.code, []), language)}</ul>'
                f'{render_seo_section_cta(language, bundle)}'
                f'<p class="station-note"><a href="{html.escape(coverage_href)}">{format_text(bundle["backToCoverage"])}</a></p>'
            )
            pages.append(
                SeoPage(
                    identity=f"country:{country.code}",
                    language=language,
                    path=seo_country_path(country.code, language),
                    title=interpolate(bundle["countryTitle"], {"country": country_name}),
                    description=interpolate(bundle["countryDescription"], {"country": country_name}),
                    h1=interpolate(bundle["countryH1"], {"country": country_name}),
                    alternate_paths=country_alternates,
                    body_html=body,
                    sitemap_group="sitemap-seo-countries.xml",
                )
            )

    return pages


def structured_data_for_page(page: SeoPage) -> list[dict[str, object]]:
    payload: dict[str, object] = {
        "@context": "https://schema.org",
        "@type": "WebPage" if page.identity != "home" else "WebSite",
        "name": page.title,
        "description": page.description,
        "url": page_url(page.path),
    }
    if page.identity != "home":
        payload["isPartOf"] = {
            "@type": "WebSite",
            "name": "woladen",
            "url": SITE_ORIGIN + "/",
        }
    return [payload]


def write_seo_pages() -> dict[str, list[str]]:
    bundles = load_seo_bundles()
    live_counts_by_country = load_dynamic_station_counts_by_country()
    countries, generated_at = load_seo_countries(live_counts_by_country=live_counts_by_country)
    top_stations, operators = load_seo_station_summaries()
    pages = build_seo_pages(countries, generated_at, bundles, top_stations, operators)
    groups: dict[str, list[str]] = {}
    for page in pages:
        target = SITE_DIR / page.path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(render_seo_shell(page, bundles, structured_data_for_page(page)), encoding="utf-8")
        groups.setdefault(page.sitemap_group, []).append(page.path)
    for paths in groups.values():
        paths.sort()
    return groups


def amenity_summary(properties: dict[str, object]) -> list[str]:
    counts: list[tuple[int, str]] = []
    for key, value in properties.items():
        if not key.startswith("amenity_"):
            continue
        count = to_int(value)
        if count <= 0:
            continue
        category = key.removeprefix("amenity_")
        label = AMENITY_LABELS.get(category, category.replace("_", " ").title())
        counts.append((count, label))
    counts.sort(key=lambda item: (-item[0], item[1]))
    return [label for _, label in counts[:6]]


def amenity_group_label(category: str) -> str:
    return AMENITY_GROUP_BY_CATEGORY.get(category, "Other")


def amenity_example_sort_key(example: dict[str, object]) -> tuple[float, str, str]:
    distance = example.get("distance_m")
    try:
        distance_value = float(distance)
    except (TypeError, ValueError):
        distance_value = math.inf
    category = str(example.get("category") or "")
    name = str(example.get("name") or "").lower()
    return distance_value, category, name


def render_amenity_example_item(example: dict[str, object]) -> str:
    category = str(example.get("category") or "").strip()
    label = AMENITY_LABELS.get(category, category.replace("_", " ").title() or "Nearby amenity")
    name = str(example.get("name") or "").strip() or label
    meta_parts = [label]
    distance = example.get("distance_m")
    if distance not in (None, ""):
        try:
            meta_parts.append(f"{round(float(distance))} m away")
        except (TypeError, ValueError):
            pass
    return (
        '<div class="station-amenity-item">'
        f"<strong>{html.escape(name)}</strong>"
        f"{html.escape(' • '.join(meta_parts))}"
        "</div>"
    )


def render_amenity_items(properties: dict[str, object]) -> str:
    examples = properties.get("amenity_examples")
    if isinstance(examples, list):
        grouped_examples: dict[str, list[dict[str, object]]] = {
            label: [] for label, _ in AMENITY_GROUPS
        }
        for example in examples[:8]:
            if not isinstance(example, dict):
                continue
            category = str(example.get("category") or "").strip()
            grouped_examples[amenity_group_label(category)].append(example)

        groups: list[str] = []
        for label, _ in AMENITY_GROUPS:
            group_items = sorted(grouped_examples[label], key=amenity_example_sort_key)
            if not group_items:
                continue
            rendered_items = "".join(render_amenity_example_item(example) for example in group_items)
            groups.append(
                '<li class="station-amenity-group">'
                f'<strong class="station-amenity-group-title">{html.escape(label)}</strong>'
                f'<div class="station-amenity-group-items">{rendered_items}</div>'
                "</li>"
            )
        if groups:
            return "".join(groups)

    summary = amenity_summary(properties)
    if not summary:
        return (
            "<li>"
            "<strong>No nearby details yet</strong>"
            "This station does not have mapped amenity examples yet."
            "</li>"
        )
    return "".join(
        "<li>"
        f"<strong>{html.escape(label)}</strong>"
        "Available near this station."
        "</li>"
        for label in summary
    )


def build_static_detail_rows(properties: dict[str, object]) -> list[tuple[str, str]]:
    rows: list[tuple[str, str]] = []

    def add_row(label: str, key: str) -> None:
        value = str(properties.get(key) or "").strip()
        if value:
            rows.append((label, value))

    add_row("Payment", "payment_methods_display")
    add_row("Access", "auth_methods_display")
    add_row("Connectors", "connector_types_display")
    add_row("Current type", "current_types_display")
    connector_count = to_int(properties.get("connector_count"), default=0)
    if connector_count > 0:
        rows.append(("Connectors", f"{connector_count} sockets"))
    add_row("Service", "service_types_display")

    green_energy = properties.get("green_energy")
    if isinstance(green_energy, bool):
        rows.append(("Energy", "100% renewable" if green_energy else "Not marked as renewable"))

    return rows


def build_station_description(properties: dict[str, object]) -> str:
    operator = str(properties.get("operator") or "Unknown operator").strip()
    address = str(properties.get("address") or "").strip()
    city = str(properties.get("city") or "").strip()
    power = format_power_kw(properties.get("max_power_kw"))
    amenities_total = to_int(properties.get("amenities_total"))
    summary = amenity_summary(properties)
    amenity_count = format_amenity_count(amenities_total)
    place = city or address or "Europe"
    station_label = "fast charging station" if to_int(properties.get("max_power_kw")) >= 50 else "charging station"
    if summary:
        amenity_text = ", ".join(summary[:3])
        return (
            f"{station_label.title()} by {operator} in {place}. "
            f"Up to {power} kW, {amenity_count}, including {amenity_text}."
        )
    return (
        f"{station_label.title()} by {operator} in {place}. "
        f"Up to {power} kW and {amenity_count}."
    )


def build_station_page(feature: dict[str, object]) -> tuple[str, str]:
    geometry = feature.get("geometry")
    if not isinstance(geometry, dict):
        geometry = {}
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list):
        coordinates = [0.0, 0.0]
    lon = float(coordinates[0]) if len(coordinates) > 0 else 0.0
    lat = float(coordinates[1]) if len(coordinates) > 1 else 0.0

    properties = feature.get("properties")
    if not isinstance(properties, dict):
        properties = {}

    station_id = public_station_id(properties.get("station_id"))
    operator = str(properties.get("operator") or "Unknown operator").strip()
    address = str(properties.get("address") or "").strip()
    postcode = str(properties.get("postcode") or "").strip()
    city = str(properties.get("city") or "").strip()
    country_code = str(properties.get("country_code") or "").strip().upper()
    title_city = city or postcode or country_code or "Europe"
    max_power = format_power_kw(properties.get("max_power_kw"))
    charging_points = to_int(properties.get("charging_points_count"), default=1)
    amenities_total = to_int(properties.get("amenities_total"))
    description = build_station_description(properties)
    amenity_text = ", ".join(amenity_summary(properties)[:4])
    station_type = "Fast charger" if to_int(properties.get("max_power_kw")) >= 50 else "Charging station"
    social_title = f"{operator} in {title_city} | {max_power} kW {station_type.lower()} | woladen.de"
    social_image_url = absolute_url(SOCIAL_IMAGE_PATH)

    page_path = station_page_path(station_id)
    canonical_url = absolute_url(page_path)
    app_url = station_query_url(station_id)
    google_maps_url = f"https://www.google.com/maps/dir/?api=1&destination={lat},{lon}"
    amenity_items = render_amenity_items(properties)
    static_detail_rows = build_static_detail_rows(properties)
    static_detail_items = "".join(
        "<li>"
        f"<strong>{html.escape(label)}</strong>"
        f"{html.escape(value)}"
        "</li>"
        for label, value in static_detail_rows
    )
    detail_source_name = str(properties.get("detail_source_name") or "").strip()
    detail_last_updated = str(properties.get("detail_last_updated") or "").strip()
    detail_source_text = ""
    if detail_source_name and detail_last_updated:
        detail_source_text = f"Details via {html.escape(detail_source_name)} • updated {html.escape(detail_last_updated)}"
    elif detail_source_name:
        detail_source_text = f"Details via {html.escape(detail_source_name)}"
    elif detail_last_updated:
        detail_source_text = f"Updated {html.escape(detail_last_updated)}"
    amenity_paragraph = (
        f"Nearby you can find {html.escape(amenity_text)}."
        if amenity_text
        else "This station is available as a direct link in the woladen.de web app."
    )
    price_chip = str(properties.get("price_display") or "").strip()
    opening_hours_chip = format_opening_hours_display(properties.get("opening_hours_display"))

    page_html = f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>{format_text(operator)} in {format_text(title_city)} | {format_text(max_power)} kW {format_text(station_type)} | woladen.de</title>
    <meta name="description" content="{format_text(description)}" />
    <link rel="canonical" href="{canonical_url}" />
    <meta property="og:type" content="website" />
    <meta property="og:title" content="{format_text(social_title)}" />
    <meta property="og:description" content="{format_text(description)}" />
    <meta property="og:url" content="{canonical_url}" />
    <meta property="og:site_name" content="woladen.de" />
    <meta property="og:locale" content="en_US" />
    <meta property="og:image" content="{social_image_url}" />
    <meta property="og:image:width" content="{SOCIAL_IMAGE_WIDTH}" />
    <meta property="og:image:height" content="{SOCIAL_IMAGE_HEIGHT}" />
    <meta property="og:image:alt" content="{SOCIAL_IMAGE_ALT}" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:title" content="{format_text(social_title)}" />
    <meta name="twitter:description" content="{format_text(description)}" />
    <meta name="twitter:image" content="{social_image_url}" />
    <meta name="twitter:image:alt" content="{SOCIAL_IMAGE_ALT}" />
    <link rel="icon" href="/favicon.ico?v=20260411" sizes="any" />
    <link rel="icon" type="image/png" sizes="32x32" href="/favicon-32x32.png?v=20260411" />
    <link rel="icon" type="image/png" sizes="16x16" href="/favicon-16x16.png?v=20260411" />
    <link rel="apple-touch-icon" sizes="180x180" href="/img/touch-icon.png?v=20260411" />
    <link rel="stylesheet" href="/styles.css" />
  </head>
  <body class="station-page">
    <main class="station-shell">
      <a href="{app_url}" class="legal-back">Open web app</a>
      <section class="station-hero">
        <p class="legal-kicker">{format_text(station_type)} direct link</p>
        <h1>{format_text(operator)}</h1>
        <p class="station-summary">{format_text(address)}<br />{format_text(postcode)} {format_text(city)}</p>
        <div class="station-chip-row">
          <span class="station-chip">⚡ {format_text(max_power)} kW max</span>
          <span class="station-chip">🔌 {charging_points} charging points</span>
          <span class="station-chip">🏪 {format_amenity_count(amenities_total)}</span>
          {f'<span class="station-chip">€ {format_text(price_chip)}</span>' if price_chip else ''}
          {f'<span class="station-chip">🕒 {format_text(opening_hours_chip)}</span>' if opening_hours_chip else ''}
        </div>
        <p class="station-summary">{amenity_paragraph}</p>
        <div class="station-actions">
          <a href="{app_url}" class="link-btn">Open in web app</a>
          <a href="{google_maps_url}" target="_blank" rel="noopener noreferrer" class="link-btn secondary-link">Navigate</a>
        </div>
      </section>

      <section class="legal-card">
        <h2>Station overview</h2>
        <p class="legal-intro">
          woladen.de helps you find reliable charging stations across Europe with useful nearby places, static registry data, and live data where available.
        </p>
        <h3>Address</h3>
        <p>{format_text(address)}<br />{format_text(postcode)} {format_text(city)}</p>
        <h3>Nearby amenities</h3>
        <ul class="station-list">
          {amenity_items}
        </ul>
        {f'<h3>Details</h3><ul class="station-list">{static_detail_items}</ul>' if static_detail_items else ''}
        {f'<p class="station-note">{detail_source_text}</p>' if detail_source_text else ''}
        <p class="station-note">
          Data sources: European open static charging registry bundle and OpenStreetMap. Map and POI data © OpenStreetMap contributors.
        </p>
      </section>
    </main>
  </body>
</html>
"""
    return page_path, page_html


def write_station_pages() -> list[str]:
    STATION_DIR.mkdir(parents=True, exist_ok=True)
    page_paths: list[str] = []
    for feature in iter_static_station_features():
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties")
        if not isinstance(properties, dict):
            continue
        station_id = public_station_id(properties.get("station_id"))
        if not station_id:
            continue
        page_path, page_html = build_station_page(feature)
        target = SITE_DIR / page_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(page_html, encoding="utf-8")
        page_paths.append(page_path)
    return page_paths


def write_urlset_sitemap(relative_path: str, paths: list[str]) -> None:
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for path in paths:
        lines.append("  <url>")
        lines.append(f"    <loc>{html.escape(page_url(path))}</loc>")
        lines.append("  </url>")
    lines.append("</urlset>")
    (SITE_DIR / relative_path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def chunk_paths(paths: list[str], chunk_size: int | None = None) -> list[list[str]]:
    chunk_size = chunk_size or SITEMAP_MAX_URLS
    return [paths[index:index + chunk_size] for index in range(0, len(paths), chunk_size)]


def write_sitemap(page_paths: list[str], seo_groups: dict[str, list[str]] | None = None) -> None:
    sitemap_paths: list[str] = []
    write_urlset_sitemap("sitemap-pages.xml", ROOT_URLS)
    sitemap_paths.append("sitemap-pages.xml")

    for relative_path, paths in sorted((seo_groups or {}).items()):
        write_urlset_sitemap(relative_path, paths)
        sitemap_paths.append(relative_path)

    for index, chunk in enumerate(chunk_paths(page_paths), start=1):
        relative_path = f"sitemap-stations-{index}.xml"
        write_urlset_sitemap(relative_path, chunk)
        sitemap_paths.append(relative_path)

    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
    ]
    for path in sitemap_paths:
        lines.append("  <sitemap>")
        lines.append(f"    <loc>{html.escape(absolute_url(path))}</loc>")
        lines.append("  </sitemap>")
    lines.append("</sitemapindex>")
    (SITE_DIR / "sitemap.xml").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_robots_txt() -> None:
    (SITE_DIR / "robots.txt").write_text(
        "\n".join([
            "User-agent: *",
            "Allow: /",
            f"Sitemap: {absolute_url('sitemap.xml')}",
            "",
        ]),
        encoding="utf-8",
    )


def copy_management_data_tree() -> None:
    source_root = DATA_DIR / "management"
    if not source_root.exists():
        return
    target_root = SITE_DATA_DIR / "management"
    for source_path in sorted(source_root.rglob("*")):
        if source_path.is_dir():
            continue
        relative_path = source_path.relative_to(source_root)
        target_path = target_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if source_path.suffix.lower() == ".json":
            payload = public_bundle_value(sanitize_json_value(json.loads(source_path.read_text(encoding="utf-8"))))
            target_path.write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False),
                encoding="utf-8",
            )
        else:
            shutil.copy2(source_path, target_path)


def copy_station_occupancy_tree() -> None:
    source_root = DATA_DIR / "station-occupancy"
    target_root = SITE_DATA_DIR / "station-occupancy"
    if target_root.is_dir():
        shutil.rmtree(target_root)
    elif target_root.exists():
        target_root.unlink()
    if not source_root.exists():
        return
    target_root.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(source_root, target_root)


def write_station_occupancy_index() -> None:
    source_root = DATA_DIR / "station-occupancy"
    if not source_root.exists():
        return
    station_ids = sorted(
        path.stem
        for path in source_root.rglob("*.json")
        if path.name != "index.json"
    )
    (source_root / "index.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "station_ids": station_ids,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )


def refresh_open_static_summary() -> None:
    script = ROOT / "scripts" / "build_open_static_summary.py"
    subprocess.run([sys.executable, str(script)], check=True)


def main() -> None:
    refresh_open_static_summary()
    write_station_occupancy_index()

    if SITE_DIR.exists():
        shutil.rmtree(SITE_DIR)
    SITE_DIR.mkdir(parents=True, exist_ok=True)

    for src in WEB_DIR.glob("*"):
        target = SITE_DIR / src.name
        if src.is_dir():
            shutil.copytree(src, target)
        else:
            shutil.copy2(src, target)

    SITE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    for filename in REQUIRED_DATA:
        source = DATA_DIR / filename
        if source.exists():
            payload = public_bundle_value(sanitize_json_value(json.loads(source.read_text(encoding="utf-8"))))
            (SITE_DATA_DIR / filename).write_text(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False),
                encoding="utf-8",
            )
    copy_management_data_tree()
    copy_station_occupancy_tree()

    seo_groups = write_seo_pages()
    station_page_paths = write_station_pages()
    write_sitemap(station_page_paths, seo_groups)
    write_robots_txt()


if __name__ == "__main__":
    main()
