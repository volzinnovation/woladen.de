from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig
from .store import LiveStore

LATEST_ATTRIBUTE_FIELDS = (
    "availability_status",
    "operational_status",
    "price_display",
    "price_currency",
    "price_energy_eur_kwh_min",
    "price_energy_eur_kwh_max",
    "price_time_eur_min_min",
    "price_time_eur_min_max",
    "next_available_charging_slots",
    "supplemental_facility_status",
)


def load_bundle_station_summary(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    features = payload.get("features")
    if not isinstance(features, list):
        raise ValueError(f"{path} does not contain a GeoJSON feature list")

    station_ids: set[str] = set()
    for feature in features:
        if not isinstance(feature, dict):
            continue
        properties = feature.get("properties")
        if not isinstance(properties, dict):
            continue
        station_id = str(properties.get("station_id") or "").strip()
        if station_id:
            station_ids.add(station_id)

    return {
        "feature_count": len(features),
        "station_ids": station_ids,
        "unique_station_count": len(station_ids),
        "duplicate_station_id_count": len(features) - len(station_ids),
    }


def _parse_iso(value: str | None) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _max_timestamp(values: Iterable[str]) -> str | None:
    best_text = ""
    best_dt: datetime | None = None
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        parsed = _parse_iso(text)
        if parsed is None:
            if not best_text:
                best_text = text
            continue
        if best_dt is None or parsed > best_dt:
            best_dt = parsed
            best_text = text
    return best_text or None


def _pick_newer_timestamp(
    current_timestamp: str | None,
    current_station_id: str | None,
    candidate_timestamp: str | None,
    candidate_station_id: str | None,
) -> tuple[str | None, str | None]:
    current_text = str(current_timestamp or "").strip()
    candidate_text = str(candidate_timestamp or "").strip()
    candidate_station = str(candidate_station_id or "").strip() or None
    current_station = str(current_station_id or "").strip() or None

    if not candidate_text:
        return current_text or None, current_station
    if not current_text:
        return candidate_text, candidate_station

    current_dt = _parse_iso(current_text)
    candidate_dt = _parse_iso(candidate_text)
    if current_dt is not None and candidate_dt is not None:
        if candidate_dt > current_dt:
            return candidate_text, candidate_station
        if candidate_dt < current_dt:
            return current_text, current_station
        if candidate_station and (not current_station or candidate_station < current_station):
            return candidate_text, candidate_station
        return current_text, current_station

    if candidate_text > current_text:
        return candidate_text, candidate_station
    if candidate_text < current_text:
        return current_text, current_station
    if candidate_station and (not current_station or candidate_station < current_station):
        return candidate_text, candidate_station
    return current_text, current_station


def _fetch_distinct_station_ids(conn, table_name: str) -> set[str]:
    rows = conn.execute(f"SELECT DISTINCT station_id FROM {table_name} WHERE station_id <> ''").fetchall()
    return {str(row["station_id"]) for row in rows if str(row["station_id"]).strip()}


def _ensure_provider_aggregate(provider_aggregates: dict[str, dict[str, Any]], provider_uid: str) -> dict[str, Any]:
    return provider_aggregates.setdefault(
        provider_uid,
        {
            "station_ids": set(),
            "observation_rows": 0,
            "last_received_update_at_values": [],
            "last_source_update_at_values": [],
            "latest_updated_station_id": None,
            "latest_updated_station_timestamp": None,
            "latest_attribute_updates": {},
        },
    )


def _has_attribute_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _decode_live_json_field(value: Any) -> Any:
    if isinstance(value, list):
        return value
    text = str(value or "").strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return value
    if parsed in (None, "", [], {}):
        return []
    return parsed


def _update_latest_attribute(
    aggregate: dict[str, Any],
    *,
    attribute_name: str,
    value: Any,
    station_id: str,
    fetched_at: str,
    source_observed_at: str,
) -> None:
    if not _has_attribute_value(value):
        return

    current = aggregate["latest_attribute_updates"].get(attribute_name)
    next_timestamp, next_station_id = _pick_newer_timestamp(
        current.get("fetched_at") if current else None,
        current.get("station_id") if current else None,
        fetched_at,
        station_id,
    )
    if current and next_timestamp == current.get("fetched_at") and next_station_id == current.get("station_id"):
        return

    aggregate["latest_attribute_updates"][attribute_name] = {
        "station_id": next_station_id,
        "fetched_at": next_timestamp,
        "source_observed_at": source_observed_at or None,
        "value": value,
    }


def build_bundle_live_status_report(
    *,
    store: LiveStore,
    geojson_path: Path,
) -> dict[str, Any]:
    bundle_summary = load_bundle_station_summary(geojson_path)
    bundle_station_ids = bundle_summary["station_ids"]

    provider_aggregates: dict[str, dict[str, Any]] = {}
    observed_bundle_station_ids: set[str] = set()
    observed_station_ids_not_in_bundle: set[str] = set()
    latest_received_update_at: str | None = None
    latest_updated_station_id: str | None = None

    with store.connection() as conn:
        current_bundle_station_ids = _fetch_distinct_station_ids(conn, "station_current_state") & bundle_station_ids
        current_state_station_ids_not_in_bundle = _fetch_distinct_station_ids(conn, "station_current_state") - bundle_station_ids

        current_rows = conn.execute(
            """
            SELECT
                provider_uid,
                provider_evse_id,
                station_id,
                fetched_at,
                source_observed_at,
                availability_status,
                operational_status,
                price_display,
                price_currency,
                price_energy_eur_kwh_min,
                price_energy_eur_kwh_max,
                price_time_eur_min_min,
                price_time_eur_min_max,
                next_available_charging_slots,
                supplemental_facility_status
            FROM evse_current_state
            WHERE station_id <> ''
            ORDER BY provider_uid, fetched_at DESC, station_id, provider_evse_id
            """
        ).fetchall()

        providers = [dict(row) for row in conn.execute("SELECT * FROM providers ORDER BY provider_uid").fetchall()]

    for row in current_rows:
        provider_uid = str(row["provider_uid"])
        station_id = str(row["station_id"])
        if station_id not in bundle_station_ids:
            observed_station_ids_not_in_bundle.add(station_id)
            continue

        observed_bundle_station_ids.add(station_id)
        aggregate = _ensure_provider_aggregate(provider_aggregates, provider_uid)
        aggregate["station_ids"].add(station_id)
        aggregate["observation_rows"] += 1
        last_received_update_at_value = str(row["fetched_at"] or "").strip()
        if last_received_update_at_value:
            aggregate["last_received_update_at_values"].append(last_received_update_at_value)
            (
                aggregate["latest_updated_station_timestamp"],
                aggregate["latest_updated_station_id"],
            ) = _pick_newer_timestamp(
                aggregate.get("latest_updated_station_timestamp"),
                aggregate.get("latest_updated_station_id"),
                last_received_update_at_value,
                station_id,
            )
            latest_received_update_at, latest_updated_station_id = _pick_newer_timestamp(
                latest_received_update_at,
                latest_updated_station_id,
                last_received_update_at_value,
                station_id,
            )
        source_observed_at = str(row["source_observed_at"] or "").strip()
        if source_observed_at:
            aggregate["last_source_update_at_values"].append(source_observed_at)
        fetched_at = last_received_update_at_value
        for attribute_name in LATEST_ATTRIBUTE_FIELDS:
            value = row[attribute_name]
            if attribute_name in {"next_available_charging_slots", "supplemental_facility_status"}:
                value = _decode_live_json_field(value)
            _update_latest_attribute(
                aggregate,
                attribute_name=attribute_name,
                value=value,
                station_id=station_id,
                fetched_at=fetched_at,
                source_observed_at=source_observed_at,
            )

    bundle_station_count = int(bundle_summary["unique_station_count"])
    provider_station_count_sum = int(sum(len(item["station_ids"]) for item in provider_aggregates.values()))

    providers_by_uid = {str(provider["provider_uid"]): provider for provider in providers}
    provider_items: list[dict[str, Any]] = []
    for provider_uid in sorted(set(providers_by_uid) | set(provider_aggregates)):
        provider = providers_by_uid.get(provider_uid, {})
        aggregate = provider_aggregates.get(provider_uid, {})
        station_ids = aggregate.get("station_ids", set())
        last_received_update_at = _max_timestamp(aggregate.get("last_received_update_at_values", []))
        last_source_update_at = _max_timestamp(aggregate.get("last_source_update_at_values", []))
        provider_items.append(
            {
                "provider_uid": provider_uid,
                "display_name": str(provider.get("display_name") or ""),
                "publisher": str(provider.get("publisher") or ""),
                "enabled": bool(provider.get("enabled")) if provider else False,
                "fetch_kind": str(provider.get("fetch_kind") or ""),
                "delta_delivery": bool(provider.get("delta_delivery")) if provider else False,
                "stations_with_any_live_observation": len(station_ids),
                "observation_rows": int(aggregate.get("observation_rows", 0) or 0),
                "coverage_ratio": (len(station_ids) / bundle_station_count) if bundle_station_count else 0.0,
                "last_received_update_at": last_received_update_at,
                "last_source_update_at": last_source_update_at,
                "latest_updated_station_id": aggregate.get("latest_updated_station_id"),
                "latest_attribute_updates": aggregate.get("latest_attribute_updates", {}),
                "last_polled_at": str(provider.get("last_polled_at") or "") or None,
                "last_result": str(provider.get("last_result") or "") or None,
                "last_push_received_at": str(provider.get("last_push_received_at") or "") or None,
                "last_push_result": str(provider.get("last_push_result") or "") or None,
            }
        )

    provider_items.sort(
        key=lambda item: (
            -int(item["stations_with_any_live_observation"]),
            -int(item["observation_rows"]),
            str(item["provider_uid"]),
        )
    )

    return {
        "db_path": str(store.config.db_path.resolve()),
        "geojson_path": str(geojson_path.resolve()),
        "bundle_feature_count": int(bundle_summary["feature_count"]),
        "bundle_station_count": bundle_station_count,
        "bundle_duplicate_station_id_count": int(bundle_summary["duplicate_station_id_count"]),
        "stations_with_any_live_observation": len(observed_bundle_station_ids),
        "stations_with_current_live_state": len(current_bundle_station_ids),
        "coverage_ratio": (len(observed_bundle_station_ids) / bundle_station_count) if bundle_station_count else 0.0,
        "last_received_update_at": latest_received_update_at,
        "latest_updated_station_id": latest_updated_station_id,
        "last_source_update_at": _max_timestamp(
            item["last_source_update_at"]
            for item in provider_items
            if item.get("last_source_update_at")
        ),
        "providers_with_any_live_observation": sum(
            1 for item in provider_items if int(item["stations_with_any_live_observation"]) > 0
        ),
        "observed_station_ids_not_in_bundle": len(observed_station_ids_not_in_bundle),
        "current_state_station_ids_not_in_bundle": len(current_state_station_ids_not_in_bundle),
        "provider_station_count_sum": provider_station_count_sum,
        "provider_station_overlap_excess": provider_station_count_sum - len(observed_bundle_station_ids),
        "providers": provider_items,
    }


def build_status_report(config: AppConfig | None = None, store: LiveStore | None = None) -> dict[str, Any]:
    effective_config = config or AppConfig()
    effective_store = store or LiveStore(effective_config)
    effective_store.initialize()
    return build_bundle_live_status_report(
        store=effective_store,
        geojson_path=effective_config.chargers_geojson_path,
    )
