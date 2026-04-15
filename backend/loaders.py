from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .models import ProviderTarget, SiteMatch, StationRecord

PUBLICATION_FILE_URL = "https://mobilithek.info/mdp-api/mdp-conn-server/v1/publication/{publication_id}/file"
PUBLICATION_PUBLIC_FILE_URL = (
    "https://mobilithek.info/mdp-api/mdp-conn-server/v1/publication/{publication_id}/file/noauth"
)
DATEX_V3_SUBSCRIPTION_URL = (
    "https://mobilithek.info:8443/mobilithek/api/v1.0/subscription/datexv3?subscriptionID={subscription_id}"
)


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _to_optional_int(value: Any) -> int | None:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _load_json_object(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict):
        return payload
    raise ValueError(f"expected_json_object:{path}")


def _provider_uid_from_detail_source_uid(value: Any) -> str:
    text = str(value or "").strip()
    prefix = "mobilithek_"
    suffix = "_static"
    if text.startswith(prefix) and text.endswith(suffix):
        return text[len(prefix) : -len(suffix)]
    return ""


def load_provider_targets(
    config_path: Path,
    override_path: Path | None = None,
    subscription_registry_path: Path | None = None,
) -> list[ProviderTarget]:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    override_payload = _load_json_object(override_path)
    subscription_registry = _load_json_object(subscription_registry_path)

    providers: list[ProviderTarget] = []
    seen_provider_uids: set[str] = set()
    for provider in payload.get("providers", []):
        dynamic = ((provider.get("feeds") or {}).get("dynamic")) or {}
        publication_id = str(dynamic.get("publication_id") or "").strip()
        if not publication_id:
            continue

        access_mode = str(dynamic.get("access_mode") or "").strip()
        content_data = dynamic.get("content_data") or {}
        delta_delivery = bool(dynamic.get("delta_delivery") or content_data.get("deltaDelivery"))
        retention_period_minutes = _to_optional_int(content_data.get("retentionPeriod"))
        fetch_kind = "publication_file_noauth" if access_mode == "noauth" else "publication_file_auth"
        fetch_url = (
            PUBLICATION_PUBLIC_FILE_URL.format(publication_id=publication_id)
            if access_mode == "noauth"
            else PUBLICATION_FILE_URL.format(publication_id=publication_id)
        )
        subscription_id = ""
        enabled = access_mode == "noauth"

        provider_uid = str(provider.get("uid") or "").strip()
        if not provider_uid:
            continue
        seen_provider_uids.add(provider_uid)
        override = override_payload.get(provider_uid) or {}
        subscription_override = subscription_registry.get(provider_uid) or {}
        merged_override = {**subscription_override, **override}
        override_kind = str(merged_override.get("fetch_kind") or "").strip()
        if override_kind:
            fetch_kind = override_kind
        override_url = str(merged_override.get("fetch_url") or "").strip()
        if override_url:
            fetch_url = override_url
        subscription_id = str(merged_override.get("subscription_id") or "").strip()
        if "delta_delivery" in merged_override:
            delta_delivery = bool(merged_override.get("delta_delivery"))
        if "retention_period_minutes" in merged_override:
            retention_period_minutes = _to_optional_int(merged_override.get("retention_period_minutes"))
        if fetch_kind == "mtls_subscription" and subscription_id:
            fetch_url = DATEX_V3_SUBSCRIPTION_URL.format(subscription_id=subscription_id)
        elif fetch_kind == "direct_url":
            access_url = str(content_data.get("accessUrl") or "").strip()
            if access_url and not override_url:
                fetch_url = access_url
        enabled = bool(merged_override.get("enabled", enabled))

        providers.append(
            ProviderTarget(
                provider_uid=provider_uid,
                display_name=str(provider.get("display_name") or "").strip(),
                publisher=str(provider.get("publisher") or "").strip(),
                publication_id=publication_id,
                access_mode=access_mode,
                fetch_kind=fetch_kind,
                fetch_url=fetch_url,
                subscription_id=subscription_id,
                enabled=enabled,
                delta_delivery=delta_delivery,
                retention_period_minutes=retention_period_minutes,
            )
        )

    for provider_uid in sorted(set(subscription_registry) | set(override_payload)):
        provider_uid = str(provider_uid or "").strip()
        if not provider_uid or provider_uid in seen_provider_uids:
            continue

        subscription_override = subscription_registry.get(provider_uid) or {}
        override = override_payload.get(provider_uid) or {}
        merged_override = {**subscription_override, **override}
        fetch_url = str(merged_override.get("fetch_url") or "").strip()
        fetch_kind = str(merged_override.get("fetch_kind") or "").strip()
        if not fetch_url or not fetch_kind:
            continue

        publication_id = str(merged_override.get("publication_id") or fetch_url).strip()
        access_mode = str(merged_override.get("access_mode") or "").strip()
        subscription_id = str(merged_override.get("subscription_id") or "").strip()
        delta_delivery = bool(merged_override.get("delta_delivery"))
        retention_period_minutes = _to_optional_int(merged_override.get("retention_period_minutes"))
        enabled = bool(merged_override.get("enabled", True))
        if fetch_kind == "mtls_subscription" and subscription_id:
            fetch_url = DATEX_V3_SUBSCRIPTION_URL.format(subscription_id=subscription_id)

        providers.append(
            ProviderTarget(
                provider_uid=provider_uid,
                display_name=str(merged_override.get("display_name") or provider_uid).strip(),
                publisher=str(merged_override.get("publisher") or "").strip(),
                publication_id=publication_id,
                access_mode=access_mode,
                fetch_kind=fetch_kind,
                fetch_url=fetch_url,
                subscription_id=subscription_id,
                enabled=enabled,
                delta_delivery=delta_delivery,
                retention_period_minutes=retention_period_minutes,
            )
        )

    return sorted(providers, key=lambda item: item.provider_uid)


def load_site_matches(site_match_path: Path, chargers_csv_path: Path | None = None) -> list[SiteMatch]:
    rows: dict[tuple[str, str], SiteMatch] = {}

    if chargers_csv_path is not None and chargers_csv_path.exists():
        with chargers_csv_path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                provider_uid = _provider_uid_from_detail_source_uid(row.get("detail_source_uid"))
                site_id = str(row.get("datex_site_id") or "").strip()
                station_id = str(row.get("station_id") or "").strip()
                if not provider_uid or not site_id or not station_id:
                    continue
                rows[(provider_uid, site_id)] = SiteMatch(
                    provider_uid=provider_uid,
                    site_id=site_id,
                    station_id=station_id,
                    score=0.0,
                )

    with site_match_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            provider_uid = str(row.get("provider_uid") or "").strip()
            site_id = str(row.get("site_id") or "").strip()
            station_id = str(row.get("station_id") or "").strip()
            if not provider_uid or not site_id or not station_id:
                continue
            rows[(provider_uid, site_id)] = SiteMatch(
                provider_uid=provider_uid,
                site_id=site_id,
                station_id=station_id,
                score=_to_float(row.get("score")),
            )
    return sorted(rows.values(), key=lambda item: (item.provider_uid, item.site_id, item.station_id))


def load_station_records(chargers_csv_path: Path) -> list[StationRecord]:
    records: list[StationRecord] = []
    with chargers_csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            station_id = str(row.get("station_id") or "").strip()
            if not station_id:
                continue
            records.append(
                StationRecord(
                    station_id=station_id,
                    operator=str(row.get("operator") or "").strip(),
                    address=str(row.get("address") or "").strip(),
                    postcode=str(row.get("postcode") or "").strip(),
                    city=str(row.get("city") or "").strip(),
                    lat=_to_float(row.get("lat")),
                    lon=_to_float(row.get("lon")),
                    charging_points_count=_to_int(row.get("charging_points_count")),
                    max_power_kw=_to_float(row.get("max_power_kw")),
                )
            )
    return records
