#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import urllib3

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_data import (
    fetch_mobilithek_access_token,
    fetch_mobilithek_static_payload_with_probe as fetch_datex_payload_with_probe,
    haversine_distance_m,
    load_static_subscription_ids,
    normalize_text,
    parse_datex_static_sites,
    parse_datex_dynamic_states,
    parse_eliso_static_sites,
)

DATA_DIR = REPO_ROOT / "data"
SUBSCRIPTION_REGISTRY_PATH = REPO_ROOT / "secret" / "mobilithek_subscriptions.json"
CHARGING_DATA_CATEGORY = "https://w3id.org/mdp/schema/data_categories#FILLING_AND_CHARGING_STATIONS"
METADATA_SEARCH_URL = "https://mobilithek.info/mdp-api/mdp-msa-metadata/v2/offers/search"
METADATA_OFFER_URL = "https://mobilithek.info/mdp-api/mdp-msa-metadata/v2/offers/{publication_id}"
CONTRACT_CREATE_URL = "https://mobilithek.info/mdp-api/mdp-msa-contracts/v1/contracts"
PUBLICATION_FILE_URL = "https://mobilithek.info/mdp-api/mdp-conn-server/v1/publication/{publication_id}/file"
PUBLICATION_PUBLIC_FILE_URL = "https://mobilithek.info/mdp-api/mdp-conn-server/v1/publication/{publication_id}/file/noauth"
PUBLICATION_FILE_ACCESS_URL = (
    "https://mobilithek.info/mdp-api/mdp-conn-server/v1/publication/{publication_id}/file/access"
)
DATEX_V3_SUBSCRIPTION_URL = (
    "https://mobilithek.info:8443/mobilithek/api/v1.0/subscription/datexv3?subscriptionID={subscription_id}"
)
MAX_MATCH_DISTANCE_M = 200.0
SEARCH_PAGE_SIZE = 200
PUBLICATION_TIMEOUT_SECONDS = 15
GRID_CELL_DEGREES = 0.01
DATEX_V3_DATA_MODEL = "https://w3id.org/mdp/schema/data_model#DATEX_2_V3"
MODEL_OTHER_DATA_MODEL = "https://w3id.org/mdp/schema/data_model#MODEL_OTHER"

GENERIC_STEM_WORDS = {
    "afir",
    "recharging",
    "charging",
    "infrastructure",
    "realtime",
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
    "v2",
    "v3",
}

GENERIC_OPERATOR_WORDS = GENERIC_STEM_WORDS | {
    "group",
    "mobility",
    "mobilitat",
    "mobiliy",
    "plus",
    "und",
    "amp",
    "public",
    "gesellschaft",
    "innovationsgesellschaft",
    "de",
}


@dataclass(frozen=True)
class StaticSiteRecord:
    site_id: str
    station_ids: tuple[str, ...]
    evse_ids: tuple[str, ...]
    lat: float
    lon: float
    postcode: str
    city: str
    address: str
    total_evses: int
    operator_name: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Mobilithek AFIR provider configs and static coverage from canonical BNetzA station catalogs"
    )
    parser.add_argument("--search-term", default="AFIR")
    parser.add_argument(
        "--chargers-csv",
        type=Path,
        default=DATA_DIR / "chargers_full.csv",
        help="Canonical full-registry station catalog used for provider matching.",
    )
    parser.add_argument(
        "--bundle-chargers-csv",
        type=Path,
        default=DATA_DIR / "chargers_fast.csv",
        help="Filtered frontend/product bundle used only for secondary coverage counters.",
    )
    parser.add_argument(
        "--output-config",
        type=Path,
        default=DATA_DIR / "mobilithek_afir_provider_configs.json",
    )
    parser.add_argument(
        "--output-coverage",
        type=Path,
        default=DATA_DIR / "mobilithek_afir_static_coverage.json",
    )
    parser.add_argument(
        "--output-matches",
        type=Path,
        default=DATA_DIR / "mobilithek_afir_static_matches.csv",
    )
    parser.add_argument(
        "--machine-cert-p12",
        type=Path,
        default=None,
        help="Optional Mobilithek machine certificate (PKCS#12) for mTLS probing",
    )
    parser.add_argument(
        "--machine-cert-password-file",
        type=Path,
        default=None,
        help="Optional text file containing the PKCS#12 password",
    )
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def slugify(value: str) -> str:
    folded = unicodedata.normalize("NFKD", value)
    ascii_only = "".join(ch for ch in folded if not unicodedata.combining(ch))
    return re.sub(r"[^a-z0-9]+", "_", ascii_only.lower()).strip("_")


def stem_words(value: str) -> list[str]:
    words = re.findall(r"[A-Za-z0-9]+", value)
    return [word.lower() for word in words if word]


def derive_provider_stem(title: str, publisher: str) -> str:
    words = stem_words(title)
    filtered = [word for word in words if word not in GENERIC_STEM_WORDS]
    if filtered:
        return "_".join(filtered)

    publisher_words = [word for word in stem_words(publisher) if word not in GENERIC_STEM_WORDS]
    if publisher_words:
        return "_".join(publisher_words)

    return slugify(title or publisher or "mobilithek_offer")


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

    comparisons = [site_operator, publisher]
    for value in comparisons:
        source_norm = normalize_text(value)
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


def classify_feed_kind(metadata: dict[str, Any], *, fallback_title: str = "") -> str:
    content_data = ((metadata.get("contentData") or [{}])[0]) if metadata.get("contentData") else {}
    schema_profile = str(content_data.get("schemaProfileName") or "")
    title = str(metadata.get("title") or fallback_title or "")
    title_lower = title.lower()
    schema_lower = schema_profile.lower()
    media_type = str(content_data.get("mediaType") or "").lower()
    access_url = str(content_data.get("accessUrl") or "").strip()
    delta_delivery = content_data.get("deltaDelivery")

    if "dynamic" in schema_lower or "realtime" in title_lower or "dynam" in title_lower or "dyn" in title_lower:
        return "dynamic"
    if "static" in schema_lower or "statisch" in title_lower or "stat" in title_lower:
        return "static"
    if delta_delivery is True:
        return "dynamic"
    if access_url or "csv" in media_type or "json" in media_type:
        return "static"
    return "unknown"


def content_data_entry(metadata: dict[str, Any]) -> dict[str, Any]:
    values = metadata.get("contentData") or []
    return values[0] if values else {}


def offer_access_mode(metadata: dict[str, Any]) -> str:
    contract = metadata.get("contractOffer") or {}
    anonymous = contract.get("providerApprovalAnonymousAccessRequired")
    return "noauth" if anonymous else "auth"


def is_test_offer(metadata: dict[str, Any], *, fallback_title: str = "") -> bool:
    title = str(metadata.get("title") or fallback_title or "").strip().lower()
    return bool(
        re.search(r"(^|[^a-z0-9])test([^a-z0-9]|$)", title)
        or title.endswith("test")
        or "smatricstest" in title
    )


def _category_id(category: Any) -> str:
    if isinstance(category, dict):
        return str(category.get("id") or category.get("value") or category.get("uri") or "").strip()
    return str(category or "").strip()


def is_charging_related_offer(metadata: dict[str, Any], *, search_offer: dict[str, Any] | None = None) -> bool:
    search_offer = search_offer or {}
    if str(search_offer.get("dataCategory") or "").strip() == CHARGING_DATA_CATEGORY:
        return True

    for category in metadata.get("dataCategories") or []:
        if _category_id(category) == CHARGING_DATA_CATEGORY:
            return True

    content = content_data_entry(metadata)
    haystack = " ".join(
        [
            str(metadata.get("title") or search_offer.get("title") or ""),
            str(metadata.get("description") or search_offer.get("description") or ""),
            str((((metadata.get("agents") or {}).get("publisher") or {}).get("name")) or ""),
            str(content.get("description") or ""),
            str(content.get("schemaProfileName") or ""),
            str(content.get("accessUrl") or ""),
        ]
    ).lower()
    return any(token in haystack for token in ("recharging", "charging", "energy-infrastructure", "ladepunkt"))


def auth_headers(access_token: str | None) -> dict[str, str] | None:
    token = str(access_token or "").strip()
    if not token:
        return None
    return {"Authorization": f"Bearer {token}"}


def search_mobilithek_offers(
    session: requests.Session, *, search_term: str, page: int, size: int, access_token: str | None = None
) -> dict[str, Any]:
    response = session.post(
        METADATA_SEARCH_URL,
        params={"page": page, "size": size, "sort": "latest,desc"},
        json={"searchString": search_term},
        headers=auth_headers(access_token),
        timeout=60,
        verify=False,
    )
    response.raise_for_status()
    return response.json()["dataOffers"]


def fetch_offer_metadata(
    session: requests.Session, publication_id: str, *, access_token: str | None = None
) -> dict[str, Any]:
    response = session.get(
        METADATA_OFFER_URL.format(publication_id=publication_id),
        headers=auth_headers(access_token),
        timeout=60,
        verify=False,
    )
    response.raise_for_status()
    return response.json()


def create_subscription(
    session: requests.Session, *, access_token: str | None, publication_id: str
) -> dict[str, Any]:
    if not access_token:
        return {"attempted": False, "status": "missing_credentials"}

    response = session.post(
        CONTRACT_CREATE_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        json={"dataOfferId": publication_id, "relevantToMDVPBefG": False},
        timeout=20,
        verify=False,
    )

    body = response.text[:500]
    result: dict[str, Any] = {
        "attempted": True,
        "status_code": response.status_code,
        "response_excerpt": body,
    }
    if response.status_code in (200, 201):
        result["status"] = "created"
    elif response.status_code == 409:
        result["status"] = "already_exists"
    elif response.status_code == 403:
        result["status"] = "forbidden"
    else:
        result["status"] = "error"
    return result


def supports_eliso_generic_json_feed(provider_uid: str, feed: dict[str, Any] | None) -> bool:
    if provider_uid != "eliso" or not isinstance(feed, dict):
        return False
    if str(feed.get("data_model") or "").strip() != MODEL_OTHER_DATA_MODEL:
        return False
    media_type = str((feed.get("content_data") or {}).get("mediaType") or "").strip().lower()
    return media_type == "application/json" or media_type.endswith("+json") or "json" in media_type


def parse_static_sites_with_operator(
    payload: dict[str, Any] | list[Any],
    *,
    provider_uid: str = "",
) -> list[StaticSiteRecord]:
    sites: list[StaticSiteRecord] = []
    if provider_uid == "eliso" and isinstance(payload, list):
        for site in parse_eliso_static_sites(payload):
            sites.append(
                StaticSiteRecord(
                    site_id=site.site_id,
                    station_ids=site.station_ids,
                    evse_ids=site.evse_ids,
                    lat=float(site.lat),
                    lon=float(site.lon),
                    postcode=site.postcode,
                    city=site.city,
                    address=site.address,
                    total_evses=max(0, int(site.total_evses or 0)),
                    operator_name=site.operator_name,
                )
            )
        return sites

    if not isinstance(payload, dict):
        return sites

    for site in parse_datex_static_sites(payload):
        sites.append(
            StaticSiteRecord(
                site_id=site.site_id,
                station_ids=site.station_ids,
                evse_ids=site.evse_ids,
                lat=float(site.lat),
                lon=float(site.lon),
                postcode=site.postcode,
                city=site.city,
                address=site.address,
                total_evses=max(0, int(site.total_evses or 0)),
                operator_name=site.operator_name,
            )
        )
    return sites


def load_chargers(chargers_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(chargers_csv)
    required = [
        "station_id",
        "operator",
        "charging_points_count",
        "lat",
        "lon",
        "postcode",
        "city",
        "address",
    ]
    for column in required:
        if column not in df.columns:
            raise RuntimeError(f"missing required column: {column}")

    return df[required].copy()


def empty_static_coverage(*, fetch_status: str, access_mode: str) -> dict[str, Any]:
    return {
        "fetch_status": fetch_status,
        "access_mode": access_mode,
        "locations_scanned": 0,
        "matched_locations": 0,
        "matched_stations": 0,
        "matched_charging_points": 0,
        "station_coverage_ratio": 0.0,
        "charging_point_coverage_ratio": 0.0,
        "bundle_matched_stations": 0,
        "bundle_matched_charging_points": 0,
        "bundle_station_coverage_ratio": 0.0,
        "bundle_charging_point_coverage_ratio": 0.0,
        "site_operator_samples": [],
    }


def grid_key(lat: float, lon: float) -> tuple[int, int]:
    return (math.floor(lat / GRID_CELL_DEGREES), math.floor(lon / GRID_CELL_DEGREES))


def build_station_spatial_index(df: pd.DataFrame) -> dict[tuple[int, int], list[dict[str, Any]]]:
    index: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for row in df.to_dict("records"):
        key = grid_key(float(row["lat"]), float(row["lon"]))
        index.setdefault(key, []).append(row)
    return index


def score_site_to_station(
    site: StaticSiteRecord,
    station_row: pd.Series,
    *,
    publisher: str,
) -> tuple[bool, float, float, dict[str, Any]]:
    distance_m = haversine_distance_m(site.lat, site.lon, float(station_row["lat"]), float(station_row["lon"]))
    if distance_m > MAX_MATCH_DISTANCE_M:
        return False, distance_m, distance_m, {}

    station_postcode = str(station_row.get("postcode") or "").strip()
    postcode_match = bool(site.postcode and station_postcode == site.postcode)
    postcode_conflict = bool(site.postcode and station_postcode and station_postcode != site.postcode)
    city_match = bool(
        site.city and normalize_text(str(station_row.get("city") or "")) == normalize_text(site.city)
    )
    address_match = address_similarity(site.address, str(station_row.get("address") or ""))
    op_similarity = operator_similarity(
        site_operator=site.operator_name,
        publisher=publisher,
        candidate_operator=str(station_row.get("operator") or ""),
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

    site_evses = int(site.total_evses or 0)
    station_points = int(station_row.get("charging_points_count", 0) or 0)
    if site_evses > 0 and station_points > 0:
        score += abs(site_evses - station_points) * 2.5

    accepted = False
    if distance_m <= 30.0:
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
    }
    return accepted, score, distance_m, details


def match_static_sites(
    df: pd.DataFrame,
    station_index: dict[tuple[int, int], list[dict[str, Any]]],
    *,
    sites: list[StaticSiteRecord],
    publisher: str,
    provider_uid: str,
    static_publication_id: str,
) -> tuple[dict[str, str], list[dict[str, Any]]]:
    scored_pairs: list[tuple[float, float, str, str, dict[str, Any]]] = []

    for site in sites:
        site_key = grid_key(site.lat, site.lon)
        candidate_rows: list[dict[str, Any]] = []
        for lat_offset in (-1, 0, 1):
            for lon_offset in (-1, 0, 1):
                candidate_rows.extend(
                    station_index.get((site_key[0] + lat_offset, site_key[1] + lon_offset), [])
                )

        for station_row_dict in candidate_rows:
            station_row = pd.Series(station_row_dict)
            accepted, score, distance_m, details = score_site_to_station(site, station_row, publisher=publisher)
            if not accepted:
                continue
            scored_pairs.append(
                (
                    score,
                    distance_m,
                    site.site_id,
                    str(station_row["station_id"]),
                    {
                        **details,
                        "provider_uid": provider_uid,
                        "static_publication_id": static_publication_id,
                        "site_id": site.site_id,
                        "site_operator": site.operator_name,
                        "site_address": site.address,
                        "site_postcode": site.postcode,
                        "site_city": site.city,
                        "site_total_evses": site.total_evses,
                        "station_id": str(station_row["station_id"]),
                        "datex_station_ids": "|".join(site.station_ids),
                        "datex_charge_point_ids": "|".join(site.evse_ids),
                        "station_operator": str(station_row.get("operator") or ""),
                        "station_address": str(station_row.get("address") or ""),
                        "station_postcode": str(station_row.get("postcode") or ""),
                        "station_city": str(station_row.get("city") or ""),
                        "station_charging_points_count": int(
                            station_row.get("charging_points_count", 0) or 0
                        ),
                        "station_in_bundle": 1 if bool(station_row.get("in_bundle")) else 0,
                        "score": round(score, 2),
                    },
                )
            )

    scored_pairs.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
    matches: dict[str, str] = {}
    used_stations: set[str] = set()
    rows: list[dict[str, Any]] = []

    for _, _, site_id, station_id, row in scored_pairs:
        if site_id in matches or station_id in used_stations:
            continue
        matches[site_id] = station_id
        used_stations.add(station_id)
        rows.append(row)

    return matches, rows


def summarize_static_coverage(
    df: pd.DataFrame,
    bundle_df: pd.DataFrame,
    *,
    matches: dict[str, str],
    total_sites: int,
    fetch_status: str,
    access_mode: str,
    site_operator_samples: list[str],
) -> dict[str, Any]:
    matched_station_ids = sorted(set(matches.values()))
    matched_station_count = len(matched_station_ids)
    total_station_count = int(len(df))
    matched_points = int(
        df[df["station_id"].isin(matched_station_ids)]["charging_points_count"].fillna(0).astype(int).sum()
    )
    total_points = int(df["charging_points_count"].fillna(0).astype(int).sum())
    bundle_station_ids = set(bundle_df["station_id"].astype(str)) if "station_id" in bundle_df.columns else set()
    matched_bundle_station_ids = sorted(set(matched_station_ids) & bundle_station_ids)
    matched_bundle_points = int(
        bundle_df[bundle_df["station_id"].isin(matched_bundle_station_ids)]["charging_points_count"]
        .fillna(0)
        .astype(int)
        .sum()
    )
    total_bundle_station_count = int(len(bundle_df))
    total_bundle_points = int(bundle_df["charging_points_count"].fillna(0).astype(int).sum())

    return {
        "fetch_status": fetch_status,
        "access_mode": access_mode,
        "locations_scanned": int(total_sites),
        "matched_locations": int(len(matches)),
        "matched_stations": matched_station_count,
        "matched_charging_points": matched_points,
        "station_coverage_ratio": round(matched_station_count / total_station_count, 6)
        if total_station_count
        else 0.0,
        "charging_point_coverage_ratio": round(matched_points / total_points, 6) if total_points else 0.0,
        "bundle_matched_stations": int(len(matched_bundle_station_ids)),
        "bundle_matched_charging_points": matched_bundle_points,
        "bundle_station_coverage_ratio": round(len(matched_bundle_station_ids) / total_bundle_station_count, 6)
        if total_bundle_station_count
        else 0.0,
        "bundle_charging_point_coverage_ratio": round(matched_bundle_points / total_bundle_points, 6)
        if total_bundle_points
        else 0.0,
        "site_operator_samples": site_operator_samples[:10],
    }


def fetch_static_payload_with_probe(
    session: requests.Session,
    *,
    publication_id: str,
    preferred_access_mode: str,
    access_token: str | None,
    subscription_id: str = "",
) -> tuple[dict[str, Any] | list[Any] | None, str, str | None]:
    return fetch_datex_payload_with_probe(
        session,
        publication_id=publication_id,
        preferred_access_mode=preferred_access_mode,
        access_token=access_token,
        subscription_id=subscription_id,
    )


def read_optional_text(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    value = path.read_text(encoding="utf-8").strip()
    return value or None


def load_dynamic_subscription_ids(
    path: Path = SUBSCRIPTION_REGISTRY_PATH,
) -> dict[str, str]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    subscription_ids: dict[str, str] = {}
    for provider_uid, entry in payload.items():
        if not isinstance(entry, dict):
            continue
        subscription_id = str(entry.get("subscription_id") or "").strip()
        if subscription_id:
            subscription_ids[str(provider_uid).strip()] = subscription_id
    return subscription_ids


def probe_publication_file_access(
    session: requests.Session,
    *,
    access_token: str | None,
    publication_id: str,
) -> dict[str, Any]:
    if not access_token:
        return {"status": "missing_credentials", "is_accessible": None}

    response = session.get(
        PUBLICATION_FILE_ACCESS_URL.format(publication_id=publication_id),
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
        verify=False,
    )
    payload: dict[str, Any] = {}
    try:
        payload = response.json()
    except Exception:
        payload = {}
    return {
        "status": "ok" if response.ok else "error",
        "status_code": response.status_code,
        "is_accessible": payload.get("isAccessible"),
        "response_excerpt": response.text[:200],
    }


def probe_machine_certificate(
    *,
    cert_p12: Path | None,
    password: str | None,
) -> dict[str, Any]:
    if cert_p12 is None or not cert_p12.exists():
        return {"configured": False, "status": "missing_certificate"}
    if not password:
        return {"configured": True, "status": "missing_password", "certificate_path": str(cert_p12)}

    curl_path = shutil.which("curl") or "/usr/bin/curl"
    if not Path(curl_path).exists():
        return {
            "configured": True,
            "status": "missing_curl",
            "certificate_path": str(cert_p12),
        }

    probe_url = DATEX_V3_SUBSCRIPTION_URL.format(subscription_id="0")
    with tempfile.TemporaryDirectory(prefix="mobilithek-mtls-") as temp_dir:
        header_path = Path(temp_dir) / "headers.txt"
        body_path = Path(temp_dir) / "body.bin"
        command = [
            curl_path,
            "-sS",
            "-D",
            str(header_path),
            "--cert-type",
            "P12",
            "--cert",
            f"{cert_p12}:{password}",
            "-H",
            "Accept-Encoding: gzip",
            probe_url,
            "-o",
            str(body_path),
        ]

        try:
            result = subprocess.run(
                command,
                check=False,
                capture_output=True,
                text=True,
                timeout=20,
            )
        except subprocess.TimeoutExpired:
            return {
                "configured": True,
                "status": "timeout",
                "certificate_path": str(cert_p12),
                "probe_url": probe_url,
            }
        except Exception as exc:
            return {
                "configured": True,
                "status": "error",
                "certificate_path": str(cert_p12),
                "probe_url": probe_url,
                "error": str(exc),
            }

        header_text = header_path.read_text(encoding="utf-8", errors="replace") if header_path.exists() else ""
        body_text = body_path.read_text(encoding="utf-8", errors="replace") if body_path.exists() else ""
        status_code = None
        for line in header_text.splitlines():
            if line.startswith("HTTP/"):
                parts = line.split()
                if len(parts) >= 2 and parts[1].isdigit():
                    status_code = int(parts[1])
                break

        probe_status = "ok"
        if result.returncode != 0:
            probe_status = "error"
        elif status_code == 404:
            probe_status = "mtls_ok_no_subscription"
        elif status_code == 403:
            probe_status = "mtls_ok_forbidden"
        elif status_code == 400 and "SSL certificate" in body_text:
            probe_status = "certificate_rejected"

        return {
            "configured": True,
            "status": probe_status,
            "certificate_path": str(cert_p12),
            "probe_url": probe_url,
            "http_status": status_code,
            "stderr_excerpt": result.stderr[:200],
            "body_excerpt": body_text[:200],
        }


def summarize_dynamic_probe(
    *,
    payload: dict[str, Any] | list[Any] | None,
    fetch_status: str,
    access_mode: str,
    delta_delivery: bool,
    provider_uid: str = "",
) -> dict[str, Any]:
    if payload is None:
        return {
            "fetch_status": fetch_status,
            "access_mode": access_mode,
            "delta_delivery": bool(delta_delivery),
            "site_status_count": 0,
            "station_status_count": 0,
            "evse_status_count": 0,
            "available_evses": 0,
            "occupied_evses": 0,
            "out_of_order_evses": 0,
            "unknown_evses": 0,
            "latest_last_updated": None,
        }

    generic_evses = payload.get("evses") if isinstance(payload, dict) else None
    if provider_uid == "eliso" and isinstance(generic_evses, list):
        status_counter: Counter[str] = Counter()
        last_updated_values: list[str] = []

        for item in generic_evses:
            if not isinstance(item, dict):
                continue
            availability_value = normalize_text(str(item.get("availability_status") or ""))
            operational_value = normalize_text(str(item.get("operational_status") or ""))
            if operational_value == "nonoperational":
                status_counter["OUTOFORDER"] += 1
            elif availability_value == "notinuse":
                status_counter["AVAILABLE"] += 1
            elif availability_value == "inuse":
                status_counter["OCCUPIED"] += 1
            else:
                status_counter["UNKNOWN"] += 1

            last_updated = str(item.get("mobilithek_last_updated_dts") or "").strip()
            if last_updated:
                last_updated_values.append(last_updated)

        return {
            "fetch_status": fetch_status,
            "access_mode": access_mode,
            "delta_delivery": bool(delta_delivery),
            "site_status_count": 0,
            "station_status_count": 0,
            "evse_status_count": int(sum(status_counter.values())),
            "available_evses": int(status_counter.get("AVAILABLE", 0)),
            "occupied_evses": int(status_counter.get("OCCUPIED", 0)),
            "out_of_order_evses": int(status_counter.get("OUTOFORDER", 0)),
            "unknown_evses": int(status_counter.get("UNKNOWN", 0)),
            "latest_last_updated": max(last_updated_values) if last_updated_values else None,
        }

    site_states = parse_datex_dynamic_states(payload)
    station_refs: set[str] = set()
    status_counter: Counter[str] = Counter()
    last_updated_values: list[str] = []

    for state in site_states.values():
        station_refs.update(item for item in state.get("station_refs", set()) if item)
        for evse_state in (state.get("evses") or {}).values():
            status = str(evse_state.get("status") or "UNKNOWN")
            status_counter[status] += 1
            last_updated = str(evse_state.get("last_updated") or "").strip()
            if last_updated:
                last_updated_values.append(last_updated)
        for value in state.get("last_updated_values") or []:
            value_text = str(value).strip()
            if value_text:
                last_updated_values.append(value_text)

    return {
        "fetch_status": fetch_status,
        "access_mode": access_mode,
        "delta_delivery": bool(delta_delivery),
        "site_status_count": len(site_states),
        "station_status_count": len(station_refs),
        "evse_status_count": int(sum(status_counter.values())),
        "available_evses": int(status_counter.get("AVAILABLE", 0)),
        "occupied_evses": int(status_counter.get("OCCUPIED", 0)),
        "out_of_order_evses": int(status_counter.get("OUTOFORDER", 0)),
        "unknown_evses": int(status_counter.get("UNKNOWN", 0)),
        "latest_last_updated": max(last_updated_values) if last_updated_values else None,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_matches_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    args = parse_args()

    session = requests.Session()
    access_token: str | None = None
    try:
        access_token = fetch_mobilithek_access_token(session)
    except Exception:
        access_token = None

    chargers_df = load_chargers(args.chargers_csv)
    bundle_chargers_df = load_chargers(args.bundle_chargers_csv)
    bundle_station_ids = set(bundle_chargers_df["station_id"].astype(str))
    chargers_df = chargers_df.copy()
    chargers_df["in_bundle"] = chargers_df["station_id"].astype(str).isin(bundle_station_ids)
    station_index = build_station_spatial_index(chargers_df)
    machine_cert_password = read_optional_text(args.machine_cert_password_file)
    machine_cert_probe = probe_machine_certificate(
        cert_p12=args.machine_cert_p12,
        password=machine_cert_password,
    )

    all_offers: list[dict[str, Any]] = []
    page = 0
    total_pages = 1
    while page < total_pages:
        offers_page = search_mobilithek_offers(
            session,
            search_term=args.search_term,
            page=page,
            size=SEARCH_PAGE_SIZE,
            access_token=access_token,
        )
        all_offers.extend(offers_page.get("content") or [])
        total_pages = int(offers_page.get("totalPages") or 1)
        page += 1

    try:
        dynamic_subscription_ids = load_dynamic_subscription_ids()
    except Exception:
        dynamic_subscription_ids = {}
    try:
        static_subscription_ids = load_static_subscription_ids()
    except Exception:
        static_subscription_ids = {}

    detailed_offers: list[dict[str, Any]] = []
    for offer in all_offers:
        publication_id = str(offer.get("publicationId") or "").strip()
        if not publication_id:
            continue

        metadata = fetch_offer_metadata(session, publication_id, access_token=access_token)
        title = str(metadata.get("title") or offer.get("title") or "")
        if is_test_offer(metadata, fallback_title=title):
            continue
        if not is_charging_related_offer(metadata, search_offer=offer):
            continue
        content = content_data_entry(metadata)
        detailed_offers.append(
            {
                "publication_id": publication_id,
                "title": title,
                "publisher": str((((metadata.get("agents") or {}).get("publisher") or {}).get("name")) or ""),
                "metadata": metadata,
                "mdp_brokering": bool(metadata.get("mdpBrokering")),
                "feed_kind": classify_feed_kind(
                    metadata,
                    fallback_title=str(offer.get("title") or ""),
                ),
                "provider_stem": derive_provider_stem(
                    str(metadata.get("title") or offer.get("title") or ""),
                    str((((metadata.get("agents") or {}).get("publisher") or {}).get("name")) or ""),
                ),
                "access_mode": offer_access_mode(metadata),
                "schema_profile_name": str(content.get("schemaProfileName") or ""),
                "data_model": str(content.get("dataModel") or ""),
                "delta_delivery": bool(content.get("deltaDelivery")),
            }
        )

    provider_map: dict[str, dict[str, Any]] = {}
    for offer in detailed_offers:
        provider_uid = slugify(offer["provider_stem"]) or slugify(offer["publisher"]) or offer["publication_id"]
        entry = provider_map.setdefault(
            provider_uid,
            {
                "uid": provider_uid,
                "display_name": offer["provider_stem"].replace("_", " ").strip() or offer["title"],
                "publisher": offer["publisher"],
                "feeds": {"static": None, "dynamic": None, "other": []},
                "subscription": {"static": None, "dynamic": None, "other": []},
                "coverage": None,
            },
        )

        feed_payload = {
            "publication_id": offer["publication_id"],
            "title": offer["title"],
            "publisher": offer["publisher"],
            "access_mode": offer["access_mode"],
            "mdp_brokering": offer["mdp_brokering"],
            "schema_profile_name": offer["schema_profile_name"],
            "data_model": offer["data_model"],
            "delta_delivery": offer["delta_delivery"],
            "contract_offer": offer["metadata"].get("contractOffer") or {},
            "content_data": content_data_entry(offer["metadata"]),
            "file_access_probe": probe_publication_file_access(
                session,
                access_token=access_token,
                publication_id=offer["publication_id"],
            ),
            "subscription_status": create_subscription(
                session, access_token=access_token, publication_id=offer["publication_id"]
            ),
        }

        kind = offer["feed_kind"]
        if kind in ("static", "dynamic") and entry["feeds"][kind] is None:
            entry["feeds"][kind] = feed_payload
            entry["subscription"][kind] = feed_payload["subscription_status"]
        else:
            entry["feeds"]["other"].append(feed_payload)
            entry["subscription"]["other"].append(feed_payload["subscription_status"])

    detailed_match_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []

    for provider_uid, provider in sorted(provider_map.items()):
        static_feed = provider["feeds"]["static"]
        dynamic_feed = provider["feeds"]["dynamic"]
        print(f"[static] {provider_uid}", flush=True)
        if not static_feed:
            static_coverage = empty_static_coverage(fetch_status="no_static_feed", access_mode="")
        elif static_feed.get("data_model") != DATEX_V3_DATA_MODEL and not supports_eliso_generic_json_feed(
            provider_uid, static_feed
        ):
            media_type = str((static_feed.get("content_data") or {}).get("mediaType") or "")
            static_coverage = empty_static_coverage(
                fetch_status=f"unsupported_static_format: {media_type or 'unknown'}",
                access_mode=str(static_feed.get("access_mode") or ""),
            )
        else:
            payload, resolved_access_mode, fetch_error = fetch_static_payload_with_probe(
                session,
                publication_id=str(static_feed["publication_id"]),
                preferred_access_mode=str(static_feed["access_mode"]),
                access_token=access_token,
                subscription_id=static_subscription_ids.get(provider_uid, ""),
            )

            if payload is None:
                static_coverage = empty_static_coverage(
                    fetch_status=fetch_error or "fetch_failed",
                    access_mode=resolved_access_mode,
                )
            else:
                sites = parse_static_sites_with_operator(payload, provider_uid=provider_uid)
                site_operator_samples = list(
                    dict.fromkeys(site.operator_name for site in sites if site.operator_name)
                )
                matches, match_rows = match_static_sites(
                    chargers_df,
                    station_index,
                    sites=sites,
                    publisher=str(provider["publisher"]),
                    provider_uid=provider_uid,
                    static_publication_id=str(static_feed["publication_id"]),
                )
                detailed_match_rows.extend(match_rows)
                static_coverage = summarize_static_coverage(
                    chargers_df,
                    bundle_chargers_df,
                    matches=matches,
                    total_sites=len(sites),
                    fetch_status="ok",
                    access_mode=resolved_access_mode,
                    site_operator_samples=site_operator_samples,
                )

        print(f"[dynamic] {provider_uid}", flush=True)
        if not dynamic_feed:
            dynamic_coverage = summarize_dynamic_probe(
                payload=None,
                fetch_status="no_dynamic_feed",
                access_mode="",
                delta_delivery=False,
                provider_uid=provider_uid,
            )
        elif dynamic_feed.get("data_model") != DATEX_V3_DATA_MODEL and not supports_eliso_generic_json_feed(
            provider_uid, dynamic_feed
        ):
            media_type = str((dynamic_feed.get("content_data") or {}).get("mediaType") or "")
            dynamic_coverage = summarize_dynamic_probe(
                payload=None,
                fetch_status=f"unsupported_dynamic_format: {media_type or 'unknown'}",
                access_mode=str(dynamic_feed.get("access_mode") or ""),
                delta_delivery=bool(dynamic_feed.get("delta_delivery")),
                provider_uid=provider_uid,
            )
        else:
            payload, resolved_access_mode, fetch_error = fetch_static_payload_with_probe(
                session,
                publication_id=str(dynamic_feed["publication_id"]),
                preferred_access_mode=str(dynamic_feed["access_mode"]),
                access_token=access_token,
                subscription_id=dynamic_subscription_ids.get(provider_uid, ""),
            )
            dynamic_coverage = summarize_dynamic_probe(
                payload=payload,
                fetch_status="ok" if payload is not None else (fetch_error or "fetch_failed"),
                access_mode=resolved_access_mode,
                delta_delivery=bool(dynamic_feed.get("delta_delivery")),
                provider_uid=provider_uid,
            )

        provider["coverage"] = {
            "static": static_coverage,
            "dynamic": dynamic_coverage,
        }
        coverage_rows.append(
            {
                "provider_uid": provider_uid,
                "display_name": provider["display_name"],
                "publisher": provider["publisher"],
                "static": static_coverage,
                "dynamic": dynamic_coverage,
            }
        )

    total_station_count = int(len(chargers_df))
    total_charging_points = int(chargers_df["charging_points_count"].fillna(0).astype(int).sum())
    total_bundle_station_count = int(len(bundle_chargers_df))
    total_bundle_charging_points = int(bundle_chargers_df["charging_points_count"].fillna(0).astype(int).sum())

    config_payload = {
        "generated_at": utc_now_iso(),
        "search_term": args.search_term,
        "search_total_elements": int(offers_page.get("totalElements") or len(all_offers)),
        "charging_offer_count": len(detailed_offers),
        "provider_config_count": len(provider_map),
        "source_chargers_csv": str(args.chargers_csv),
        "source_bundle_chargers_csv": str(args.bundle_chargers_csv),
        "machine_certificate_probe": machine_cert_probe,
        "totals": {
            "stations": total_station_count,
            "charging_points": total_charging_points,
            "bundle_stations": total_bundle_station_count,
            "bundle_charging_points": total_bundle_charging_points,
        },
        "providers": [provider_map[key] for key in sorted(provider_map.keys())],
    }
    coverage_payload = {
        "generated_at": config_payload["generated_at"],
        "source_chargers_csv": str(args.chargers_csv),
        "source_bundle_chargers_csv": str(args.bundle_chargers_csv),
        "machine_certificate_probe": machine_cert_probe,
        "totals": config_payload["totals"],
        "providers": coverage_rows,
    }

    write_json(args.output_config, config_payload)
    write_json(args.output_coverage, coverage_payload)
    write_matches_csv(args.output_matches, detailed_match_rows)

    print(json.dumps(
        {
            "generated_at": config_payload["generated_at"],
            "search_total_elements": config_payload["search_total_elements"],
            "charging_offer_count": config_payload["charging_offer_count"],
            "provider_config_count": config_payload["provider_config_count"],
            "matches_written": len(detailed_match_rows),
            "config_path": str(args.output_config),
            "coverage_path": str(args.output_coverage),
            "matches_path": str(args.output_matches),
        },
        ensure_ascii=False,
        indent=2,
    ))


if __name__ == "__main__":
    main()
