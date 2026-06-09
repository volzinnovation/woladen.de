#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import os
import shutil
import ssl
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence
import urllib.error
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from commercial_backend.ch_ladestationen import (  # noqa: E402
    STATIC_DATA_URL as CH_STATIC_DATA_URL,
    parse_static_payload as parse_ch_static_payload,
)
from scripts import osm_amenities  # noqa: E402

NL_OCPI_LOCATIONS_URL = "https://opendata.ndw.nu/charging_point_locations_ocpi.json.gz"
NL_OCPI_TARIFFS_URL = "https://opendata.ndw.nu/charging_point_tariffs_ocpi.json.gz"
CH_PROVIDER_UID = "ch_bfe_ladestationen"
CH_STATIC_SOURCE_UID = "ch_bfe_ladestationen_static"
OSM_PBF_URLS = {
    country_code: osm_amenities.COUNTRY_PBF_URLS[country_code]
    for country_code in ("BE", "NL", "CH")
}
USER_AGENT = "woladen.de onboarded static catalog builder"
FAST_POWER_THRESHOLD_KW = 50.0
EARTH_RADIUS_M = 6_371_000.0
AMENITY_TAG_KEYS = ("amenity", "shop", "tourism", "leisure", "public_transport")
AMENITY_BUNDLE_CATEGORIES = (
    "restaurant",
    "cafe",
    "fast_food",
    "toilets",
    "supermarket",
    "bakery",
    "convenience",
    "pharmacy",
    "hotel",
    "museum",
    "playground",
    "park",
    "ice_cream",
)
AMENITY_KIND_TO_BUNDLE_CATEGORY = {
    "amenity:restaurant": "restaurant",
    "amenity:cafe": "cafe",
    "amenity:fast_food": "fast_food",
    "amenity:toilets": "toilets",
    "shop:supermarket": "supermarket",
    "shop:bakery": "bakery",
    "shop:convenience": "convenience",
    "amenity:pharmacy": "pharmacy",
    "shop:pharmacy": "pharmacy",
    "tourism:hotel": "hotel",
    "tourism:museum": "museum",
    "leisure:playground": "playground",
    "leisure:park": "park",
    "amenity:ice_cream": "ice_cream",
    "shop:ice_cream": "ice_cream",
}

STATION_FIELDS = (
    "country_code",
    "station_id",
    "source_uid",
    "source_station_id",
    "operator_name",
    "address",
    "postal_code",
    "city",
    "latitude",
    "longitude",
    "charger_count",
    "max_power_kw",
    "source_url",
    "license",
    "id_rule",
    "helpdesk_phone",
    "price_display",
    "price_energy_eur_kwh_min",
    "price_energy_eur_kwh_max",
    "price_currency",
    "price_quality",
)
CHARGER_FIELDS = (
    "country_code",
    "station_id",
    "charger_id",
    "source_uid",
    "source_station_id",
    "source_evse_id",
    "connector_id",
    "connector_type",
    "current_type",
    "max_power_kw",
    "operator_name",
)
AMENITY_FIELDS = (
    "country_code",
    "station_id",
    "amenity_count",
    "amenity_summary",
    "nearest_amenity_kind",
    "nearest_amenity_name",
    "nearest_amenity_distance_m",
    "osm_pbf_url",
    "osm_extraction_status",
)
DEDUPE_FIELDS = (
    "issue",
    "country_code",
    "station_id",
    "source_uid",
    "details",
)
FULL_BUNDLE_FIELDS = (
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
    "bnetza_display_name",
    "bnetza_location_name",
    "bnetza_parking_info",
    "bnetza_payment_systems",
    "bnetza_opening_hours",
    "bnetza_opening_days",
    "bnetza_opening_times",
    "bnetza_commissioned_at",
    "has_active_record",
)
FAST_BUNDLE_FIELDS = (
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
    "bnetza_display_name",
    "bnetza_location_name",
    "bnetza_parking_info",
    "bnetza_payment_systems",
    "bnetza_opening_hours",
    "bnetza_opening_days",
    "bnetza_opening_times",
    "bnetza_commissioned_at",
    "occupancy_source_uid",
    "occupancy_source_name",
    "occupancy_status",
    "occupancy_last_updated",
    "occupancy_total_evses",
    "occupancy_available_evses",
    "occupancy_occupied_evses",
    "occupancy_charging_evses",
    "occupancy_out_of_order_evses",
    "occupancy_unknown_evses",
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
    *(f"amenity_{category}" for category in AMENITY_BUNDLE_CATEGORIES),
    "amenities_total",
    "amenities_source",
    "amenity_examples",
)
TRIMMED_GEOJSON_PROPERTY_KEYS = {
    "status",
    "detail_source_uid",
    "datex_site_id",
    "datex_station_ids",
    "datex_charge_point_ids",
    "details_json",
    "amenities_source",
}


@dataclass(frozen=True)
class StationRow:
    country_code: str
    station_id: str
    source_uid: str
    source_station_id: str
    operator_name: str
    address: str
    postal_code: str
    city: str
    latitude: float | None
    longitude: float | None
    charger_count: int
    max_power_kw: float | None
    source_url: str
    license: str
    id_rule: str
    display_name: str = ""
    location_name: str = ""
    parking_info: str = ""
    payment_methods: str = ""
    opening_hours: str = ""
    opening_days: str = ""
    opening_times: str = ""
    helpdesk_phone: str = ""
    auth_methods: str = ""
    green_energy: str = ""
    service_types: str = ""
    price_display: str = ""
    price_energy_eur_kwh_min: str = ""
    price_energy_eur_kwh_max: str = ""
    price_currency: str = ""
    price_quality: str = ""
    detail_last_updated: str = ""
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ChargerRow:
    country_code: str
    station_id: str
    charger_id: str
    source_uid: str
    source_station_id: str
    source_evse_id: str
    connector_id: str
    connector_type: str
    current_type: str
    max_power_kw: float | None
    operator_name: str


def _text(value: Any) -> str:
    return str(value or "").strip()


def _compact_json(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _join_unique(values: Iterable[Any], *, separator: str = ";") -> str:
    return separator.join(_unique_texts(values))


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _download_bytes(url: str, *, timeout_seconds: int = 90) -> bytes:
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "*/*"})
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            return response.read()
    except (ssl.SSLError, urllib.error.URLError):
        # Some local Python builds do not know the system CA bundle. GitHub
        # Actions and the Docker image should pass normal TLS verification.
        context = ssl._create_unverified_context()
        with urlopen(request, timeout=timeout_seconds, context=context) as response:
            return response.read()


def _download_json(url: str, *, timeout_seconds: int = 90) -> Any:
    raw = _download_bytes(url, timeout_seconds=timeout_seconds)
    if url.endswith(".gz") or raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw.decode("utf-8"))


def _safe_id(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in _text(value))
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "unknown"


def _station_dict(row: StationRow) -> dict[str, str]:
    return {
        "country_code": row.country_code,
        "station_id": row.station_id,
        "source_uid": row.source_uid,
        "source_station_id": row.source_station_id,
        "operator_name": row.operator_name,
        "address": row.address,
        "postal_code": row.postal_code,
        "city": row.city,
        "latitude": "" if row.latitude is None else f"{row.latitude:.7f}",
        "longitude": "" if row.longitude is None else f"{row.longitude:.7f}",
        "charger_count": str(row.charger_count),
        "max_power_kw": "" if row.max_power_kw is None else f"{row.max_power_kw:.3f}",
        "source_url": row.source_url,
        "license": row.license,
        "id_rule": row.id_rule,
        "helpdesk_phone": row.helpdesk_phone,
        "price_display": row.price_display,
        "price_energy_eur_kwh_min": row.price_energy_eur_kwh_min,
        "price_energy_eur_kwh_max": row.price_energy_eur_kwh_max,
        "price_currency": row.price_currency,
        "price_quality": row.price_quality,
    }


def _charger_dict(row: ChargerRow) -> dict[str, str]:
    return {
        "country_code": row.country_code,
        "station_id": row.station_id,
        "charger_id": row.charger_id,
        "source_uid": row.source_uid,
        "source_station_id": row.source_station_id,
        "source_evse_id": row.source_evse_id,
        "connector_id": row.connector_id,
        "connector_type": row.connector_type,
        "current_type": row.current_type,
        "max_power_kw": "" if row.max_power_kw is None else f"{row.max_power_kw:.3f}",
        "operator_name": row.operator_name,
    }


def _write_csv(path: Path, fieldnames: Sequence[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def _max_power_from_connectors(connectors: list[dict[str, Any]]) -> float | None:
    values: list[float] = []
    for connector in connectors:
        direct_watts = _float_or_none(connector.get("max_electric_power"))
        if direct_watts and direct_watts > 0:
            values.append(direct_watts / 1000.0)
            continue
        voltage = _float_or_none(connector.get("max_voltage"))
        amperage = _float_or_none(connector.get("max_amperage"))
        if voltage and amperage:
            values.append(voltage * amperage / 1000.0)
    return max(values) if values else None


def _is_fast(max_power_kw: float | None, current_type: str) -> bool:
    if max_power_kw is None:
        return False
    if max_power_kw >= FAST_POWER_THRESHOLD_KW:
        return True
    return current_type.upper() == "DC" and max_power_kw >= 22.0


def _ch_coordinate_station_id(row: dict[str, Any]) -> str:
    latitude = _float_or_none(row.get("latitude"))
    longitude = _float_or_none(row.get("longitude"))
    if latitude is None or longitude is None:
        return _text(row["station_id"])
    provider_key = _safe_id(_text(row.get("operator_id")) or _text(row.get("operator_name")) or "unknown")
    return f"ch:coord:{provider_key}:{_coordinate_text(latitude)}:{_coordinate_text(longitude)}"


def _parse_ch_catalog() -> tuple[list[StationRow], list[ChargerRow], list[dict[str, str]], dict[str, Any]]:
    payload = _download_json(CH_STATIC_DATA_URL)
    static_rows = parse_ch_static_payload(payload)
    chargers: list[ChargerRow] = []
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    dedupe: list[dict[str, str]] = []
    for row in static_rows:
        station_id = _ch_coordinate_station_id(row)
        grouped[station_id].append(row)
        chargers.append(
            ChargerRow(
                country_code="CH",
                station_id=station_id,
                charger_id=_text(row["charger_id"]),
                source_uid=CH_STATIC_SOURCE_UID,
                source_station_id=_text(row["source_station_id"]),
                source_evse_id=_text(row["source_evse_id"]),
                connector_id="",
                connector_type=_text(row["plugs"]),
                current_type=_text(row.get("power_types")),
                max_power_kw=_float_or_none(row["max_power_kw"]),
                operator_name=_text(row["operator_name"]),
            )
        )

    stations: list[StationRow] = []
    for station_id, rows in sorted(grouped.items()):
        first = rows[0]
        latitudes = {_text(row["latitude"]) for row in rows if row.get("latitude") is not None}
        longitudes = {_text(row["longitude"]) for row in rows if row.get("longitude") is not None}
        if len(latitudes) > 1 or len(longitudes) > 1:
            dedupe.append(
                {
                    "issue": "conflicting_station_coordinates",
                    "country_code": "CH",
                    "station_id": station_id,
                    "source_uid": CH_STATIC_SOURCE_UID,
                    "details": f"latitudes={sorted(latitudes)} longitudes={sorted(longitudes)}",
                }
            )
        powers = [_float_or_none(row.get("max_power_kw")) for row in rows]
        is_open_24_hours = any(bool(row.get("is_open_24_hours")) for row in rows)
        opening_times = _join_unique(row.get("opening_times") for row in rows)
        source_station_ids = _unique_texts(row.get("source_station_id") for row in rows)
        if len(source_station_ids) > 1:
            dedupe.append(
                {
                    "issue": "ch_coordinate_station_merge",
                    "country_code": "CH",
                    "station_id": station_id,
                    "source_uid": CH_STATIC_SOURCE_UID,
                    "details": f"source_station_ids={source_station_ids}",
                }
            )
        parking_parts = [
            _join_unique(row.get("accessibility") for row in rows),
            _join_unique(row.get("accessibility_location") for row in rows),
            _join_unique(row.get("parking_facility") for row in rows),
            _join_unique(row.get("parking_spot") for row in rows),
        ]
        renewable_values = {row.get("renewable_energy") for row in rows if row.get("renewable_energy") is not None}
        details = {
            "country_code": "CH",
            "operator_id": _join_unique(row.get("operator_id") for row in rows),
            "source_station_ids": source_station_ids,
            "dynamic_info_available": _join_unique(row.get("dynamic_info_available") for row in rows),
            "accessibility": _join_unique(row.get("accessibility") for row in rows),
            "accessibility_location": _join_unique(row.get("accessibility_location") for row in rows),
            "address_country": _join_unique(row.get("address_country") for row in rows),
            "address_floor": _join_unique(row.get("address_floor") for row in rows),
            "address_region": _join_unique(row.get("address_region") for row in rows),
            "parking_spot": _join_unique(row.get("parking_spot") for row in rows),
            "parking_facility": _join_unique(row.get("parking_facility") for row in rows),
            "opening_times_source": opening_times,
            "charging_station_names": _join_unique(row.get("charging_station_names") for row in rows),
            "value_added_services": _join_unique(row.get("value_added_services") for row in rows),
            "calibration_law_data_availability": _join_unique(
                row.get("calibration_law_data_availability") for row in rows
            ),
            "is_hubject_compatible": sorted(str(row.get("is_hubject_compatible")) for row in rows if row.get("is_hubject_compatible") is not None),
            "geo_charging_point_entrance": _join_unique(row.get("geo_charging_point_entrance") for row in rows),
            "charging_station_location_reference": _join_unique(
                row.get("charging_station_location_reference") for row in rows
            ),
            "energy_source": _join_unique(row.get("energy_source") for row in rows),
            "environmental_impact": _join_unique(row.get("environmental_impact") for row in rows),
            "location_image": _join_unique(row.get("location_image") for row in rows),
            "suboperator_name": _join_unique(row.get("suboperator_name") for row in rows),
            "max_capacity": _join_unique(row.get("max_capacity") for row in rows),
            "additional_info": _join_unique(row.get("additional_info") for row in rows),
            "charging_pool_id": _join_unique(row.get("charging_pool_id") for row in rows),
            "dynamic_power_level": _join_unique(row.get("dynamic_power_level") for row in rows),
            "hardware_manufacturer": _join_unique(row.get("hardware_manufacturer") for row in rows),
            "hub_operator_id": _join_unique(row.get("hub_operator_id") for row in rows),
        }
        details = {key: value for key, value in details.items() if value not in ("", [], {}, None)}
        stations.append(
            StationRow(
                country_code="CH",
                station_id=station_id,
                source_uid=CH_STATIC_SOURCE_UID,
                source_station_id="|".join(source_station_ids) if source_station_ids else _text(first["source_station_id"]),
                operator_name=_join_unique(row.get("operator_name") for row in rows) or _text(first["operator_name"]),
                address=" ".join(part for part in (_text(first["street"]), _text(first["postal_code"]), _text(first["city"])) if part),
                postal_code=_text(first["postal_code"]),
                city=_text(first["city"]),
                latitude=_float_or_none(first["latitude"]),
                longitude=_float_or_none(first["longitude"]),
                charger_count=len(rows),
                max_power_kw=max((value for value in powers if value is not None), default=None),
                source_url=CH_STATIC_DATA_URL,
                license="source_terms_pending_review",
                id_rule="coordinate_lat_lon" if station_id.startswith("ch:coord:") else "source_charging_station_id",
                display_name=_join_unique(row.get("charging_station_names") for row in rows),
                location_name=_join_unique(row.get("charging_station_names") for row in rows) or _text(first["source_station_id"]),
                parking_info=";".join(part for part in parking_parts if part),
                payment_methods=_join_unique(row.get("authentication_modes") for row in rows),
                opening_hours="24/7" if is_open_24_hours else opening_times,
                opening_times=opening_times,
                helpdesk_phone=_join_unique(row.get("hotline_phone_number") for row in rows),
                auth_methods=_join_unique(row.get("authentication_modes") for row in rows),
                green_energy="True" if True in renewable_values else ("False" if renewable_values == {False} else ""),
                service_types=_join_unique(row.get("value_added_services") for row in rows),
                details=details,
            )
        )

    summary = {"source": "CH BFE Ladestationen", "station_count": len(stations), "charger_count": len(chargers)}
    return stations, chargers, dedupe, summary


def _iter_nl_locations(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, list):
        yield from (item for item in payload if isinstance(item, dict))
        return
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            yield from (item for item in data if isinstance(item, dict))
            return
        locations = payload.get("locations")
        if isinstance(locations, list):
            yield from (item for item in locations if isinstance(item, dict))


def _load_nl_tariff_lookup() -> dict[str, dict[str, Any]]:
    try:
        payload = _download_json(NL_OCPI_TARIFFS_URL, timeout_seconds=180)
    except Exception:
        return {}
    if not isinstance(payload, list):
        return {}
    return {_text(item.get("id")): item for item in payload if isinstance(item, dict) and _text(item.get("id"))}


def _nl_tariff_energy_prices(tariff_ids: Iterable[str], tariff_lookup: dict[str, dict[str, Any]]) -> tuple[str, str, str, str, str]:
    prices: list[float] = []
    currencies: set[str] = set()
    matched_tariffs = 0
    for tariff_id in tariff_ids:
        tariff = tariff_lookup.get(tariff_id)
        if not tariff:
            continue
        matched_tariffs += 1
        currency = _text(tariff.get("currency"))
        if currency:
            currencies.add(currency)
        for element in tariff.get("elements") or []:
            if not isinstance(element, dict):
                continue
            for component in element.get("price_components") or []:
                if not isinstance(component, dict) or _text(component.get("type")).upper() != "ENERGY":
                    continue
                price = _float_or_none(component.get("price"))
                if price is not None:
                    prices.append(price)
    if not prices:
        return "", "", "", "", ""
    minimum = min(prices)
    maximum = max(prices)
    currency = sorted(currencies)[0] if currencies else "EUR"
    if minimum == maximum:
        display = f"{_decimal_text(minimum)} {currency}/kWh"
    else:
        display = f"{_decimal_text(minimum)}-{_decimal_text(maximum)} {currency}/kWh"
    quality = f"ocpi_tariff_energy_component:{matched_tariffs}_tariffs"
    return display, _decimal_text(minimum), _decimal_text(maximum), currency, quality


def _format_ocpi_opening_times(opening_times: Any, charging_when_closed: Any) -> tuple[str, str, str, str]:
    if not isinstance(opening_times, dict):
        if charging_when_closed is True:
            return "24/7", "", "", "True"
        return "", "", "", "False"
    if opening_times.get("twentyfourseven") is True:
        return "24/7", "", "", "True"
    regular_hours = opening_times.get("regular_hours") or []
    days: list[str] = []
    times: list[str] = []
    for item in regular_hours:
        if not isinstance(item, dict):
            continue
        weekday = _text(item.get("weekday"))
        period_begin = _text(item.get("period_begin"))
        period_end = _text(item.get("period_end"))
        if weekday:
            days.append(weekday)
        if period_begin or period_end:
            times.append(f"{period_begin}-{period_end}".strip("-"))
    display = _compact_json(opening_times)
    return display, "; ".join(days), "; ".join(times), "False"


def _energy_mix_green_value(energy_mix: Any) -> str:
    if not isinstance(energy_mix, dict):
        return ""
    value = energy_mix.get("is_green_energy")
    if value is True:
        return "True"
    if value is False:
        return "False"
    return ""


def _nl_charger_id(source_evse_id: str) -> str:
    safe_evse_id = _safe_id(source_evse_id)
    if not safe_evse_id:
        raise ValueError("missing_source_evse_id")
    return f"nl:ocpi:{safe_evse_id}"


def _nl_unique_charger_id(base_charger_id: str, source_station_id: str, used_charger_ids: set[str]) -> str:
    if base_charger_id not in used_charger_ids:
        return base_charger_id
    suffix_base = _safe_id(source_station_id)
    counter = 2
    while True:
        candidate = f"{base_charger_id}:{suffix_base}:{counter}"
        if candidate not in used_charger_ids:
            return candidate
        counter += 1


def _nl_connector_summary(connectors: list[dict[str, Any]]) -> tuple[str, str, str]:
    connector_id = _join_unique(connector.get("id") for connector in connectors)
    connector_type = _join_unique(connector.get("standard") for connector in connectors)
    current_types = _unique_texts(connector.get("power_type") for connector in connectors)
    current_type = "DC" if any("DC" in value.upper() for value in current_types) else ";".join(current_types)
    return connector_id, connector_type, current_type


def _parse_nl_catalog() -> tuple[list[StationRow], list[ChargerRow], list[dict[str, str]], dict[str, Any]]:
    payload = _download_json(NL_OCPI_LOCATIONS_URL, timeout_seconds=180)
    tariff_lookup = _load_nl_tariff_lookup()
    stations: list[StationRow] = []
    chargers: list[ChargerRow] = []
    dedupe: list[dict[str, str]] = []
    location_ids: Counter[str] = Counter()
    canonical_by_source_evse_id: dict[str, tuple[str, str, str]] = {}
    used_charger_ids: set[str] = set()

    for location in _iter_nl_locations(payload):
        source_station_id = _text(location.get("id"))
        if not source_station_id:
            continue
        location_ids[source_station_id] += 1
        party_id = _text(location.get("party_id")) or _text(location.get("operator", {}).get("name")) or "dot-nl"
        station_id = f"nl:ocpi:{_safe_id(party_id)}:{_safe_id(source_station_id)}"
        coordinates = location.get("coordinates") or {}
        latitude = _float_or_none(coordinates.get("latitude"))
        longitude = _float_or_none(coordinates.get("longitude"))
        evses = [evse for evse in (location.get("evses") or []) if isinstance(evse, dict)]
        operator = location.get("operator") or {}
        operator_name = _text(operator.get("name") if isinstance(operator, dict) else "")
        opening_hours, opening_days, opening_times, opening_is_24_7 = _format_ocpi_opening_times(
            location.get("opening_times"),
            location.get("charging_when_closed"),
        )
        address = " ".join(
            part
            for part in (
                _text(location.get("address")),
                _text(location.get("postal_code")),
                _text(location.get("city")),
            )
            if part
        )
        station_power_values: list[float] = []
        station_tariff_ids: set[str] = set()
        station_capabilities: set[str] = set()
        evse_statuses: Counter[str] = Counter()
        evse_last_updated: list[str] = []
        canonical_charger_count = 0
        source_evse_alias_count = 0
        for evse in evses:
            source_evse_id = _text(evse.get("evse_id")) or _text(evse.get("uid"))
            if not source_evse_id:
                continue
            source_evse_key = source_evse_id.casefold()
            base_charger_id = _nl_charger_id(source_evse_id)
            evse_statuses[_text(evse.get("status")) or "UNKNOWN"] += 1
            if evse.get("last_updated"):
                evse_last_updated.append(_text(evse.get("last_updated")))
            station_capabilities.update(_text(value) for value in evse.get("capabilities") or [] if _text(value))
            connectors = [connector for connector in (evse.get("connectors") or []) if isinstance(connector, dict)]
            evse_power = _max_power_from_connectors(connectors)
            if evse_power is not None:
                station_power_values.append(evse_power)
            for connector in connectors:
                station_tariff_ids.update(_text(value) for value in connector.get("tariff_ids") or [] if _text(value))
            canonical = canonical_by_source_evse_id.get(source_evse_key)
            if canonical is not None:
                source_evse_alias_count += 1
                canonical_charger_id, canonical_station_id, canonical_source_station_id = canonical
                dedupe.append(
                    {
                        "issue": "duplicate_nl_source_evse_location_alias",
                        "country_code": "NL",
                        "station_id": station_id,
                        "source_uid": "nl_ndw_dotnl_ocpi_locations",
                        "details": (
                            f"source_evse_id={source_evse_id} "
                            f"alias_source_station_id={source_station_id} "
                            f"aliases canonical "
                            f"charger_id={canonical_charger_id} "
                            f"station_id={canonical_station_id} "
                            f"canonical_source_station_id={canonical_source_station_id}"
                        ),
                    }
                )
                continue
            charger_id = _nl_unique_charger_id(base_charger_id, source_station_id, used_charger_ids)
            used_charger_ids.add(charger_id)
            canonical_by_source_evse_id[source_evse_key] = (charger_id, station_id, source_station_id)
            connector_id, connector_type, current_type = _nl_connector_summary(connectors)
            chargers.append(
                ChargerRow(
                    country_code="NL",
                    station_id=station_id,
                    charger_id=charger_id,
                    source_uid="nl_ndw_dotnl_ocpi_locations",
                    source_station_id=source_station_id,
                    source_evse_id=source_evse_id,
                    connector_id=connector_id,
                    connector_type=connector_type,
                    current_type=current_type,
                    max_power_kw=evse_power,
                    operator_name=operator_name,
                )
            )
            canonical_charger_count += 1

        price_display, price_min, price_max, price_currency, price_quality = _nl_tariff_energy_prices(
            station_tariff_ids,
            tariff_lookup,
        )
        facilities = location.get("facilities") if isinstance(location.get("facilities"), list) else []
        energy_mix = location.get("energy_mix") if isinstance(location.get("energy_mix"), dict) else None
        details = {
            "country_code": "NL",
            "ocpi_country_code": _text(location.get("country_code")),
            "party_id": _text(location.get("party_id")),
            "name": _text(location.get("name")),
            "publish": location.get("publish"),
            "publish_allowed_to": location.get("publish_allowed_to"),
            "state": _text(location.get("state")),
            "country": _text(location.get("country")),
            "related_locations": location.get("related_locations"),
            "parking_type": _text(location.get("parking_type")),
            "directions": location.get("directions"),
            "operator": operator if isinstance(operator, dict) else None,
            "suboperator": location.get("suboperator"),
            "owner": location.get("owner"),
            "facilities": facilities,
            "time_zone": _text(location.get("time_zone")),
            "opening_times_source": location.get("opening_times"),
            "charging_when_closed": location.get("charging_when_closed"),
            "images": location.get("images"),
            "energy_mix": energy_mix,
            "last_updated": _text(location.get("last_updated")),
            "evse_status_counts": dict(evse_statuses),
            "evse_capabilities": sorted(station_capabilities),
            "evse_last_updated_max": max(evse_last_updated) if evse_last_updated else "",
            "tariff_ids": sorted(station_tariff_ids),
        }
        if source_evse_alias_count:
            details["source_evse_alias_count"] = source_evse_alias_count
        details = {key: value for key, value in details.items() if value not in ("", [], {}, None)}
        if evses and canonical_charger_count == 0:
            continue
        stations.append(
            StationRow(
                country_code="NL",
                station_id=station_id,
                source_uid="nl_ndw_dotnl_ocpi_locations",
                source_station_id=source_station_id,
                operator_name=operator_name,
                address=address,
                postal_code=_text(location.get("postal_code")),
                city=_text(location.get("city")),
                latitude=latitude,
                longitude=longitude,
                charger_count=canonical_charger_count,
                max_power_kw=max(station_power_values, default=None),
                source_url=NL_OCPI_LOCATIONS_URL,
                license="CC0-1.0-pending-final-confirmation",
                id_rule="ocpi_party_location_id",
                display_name=_text(location.get("name")),
                location_name=_text(location.get("name")) or source_station_id,
                parking_info=_text(location.get("parking_type")),
                payment_methods=";".join(sorted(station_capabilities)),
                opening_hours=opening_hours,
                opening_days=opening_days,
                opening_times=opening_times,
                auth_methods=";".join(sorted(station_capabilities)),
                green_energy=_energy_mix_green_value(energy_mix),
                service_types=";".join(_text(value) for value in facilities if _text(value)),
                price_display=price_display,
                price_energy_eur_kwh_min=price_min,
                price_energy_eur_kwh_max=price_max,
                price_currency=price_currency,
                price_quality=price_quality,
                detail_last_updated=_text(location.get("last_updated")),
                details=details,
            )
        )

    for location_id, count in sorted(location_ids.items()):
        if count > 1:
            dedupe.append(
                {
                    "issue": "duplicate_source_location_id",
                    "country_code": "NL",
                    "station_id": "",
                    "source_uid": "nl_ndw_dotnl_ocpi_locations",
                    "details": f"{location_id} occurs {count} times",
                }
            )

    summary = {
        "source": "NL NDW/DOT-NL Openbare laadpunten Nederland",
        "station_count": len(stations),
        "charger_count": len(chargers),
        "tariff_count": len(tariff_lookup),
    }
    return stations, chargers, dedupe, summary


def _be_catalog_gap() -> tuple[list[StationRow], list[ChargerRow], list[dict[str, str]], dict[str, Any]]:
    summary = {
        "source": "BE Transportdata AFIR sources",
        "station_count": 0,
        "charger_count": 0,
        "status": "not_configured_waiting_for_source_access",
    }
    return [], [], [], summary


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    return EARTH_RADIUS_M * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _grid_key(latitude: float, longitude: float, cell_degrees: float) -> tuple[int, int]:
    return (math.floor(latitude / cell_degrees), math.floor(longitude / cell_degrees))


def _download_pbf(country_code: str, cache_dir: Path) -> Path:
    url = OSM_PBF_URLS[country_code]
    target = cache_dir / Path(url).name
    if target.exists() and target.stat().st_size > 0:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(_download_bytes(url, timeout_seconds=600))
    return target


def _run_osmium_extract(country_code: str, pbf_path: Path, work_dir: Path) -> Path | None:
    if not shutil_which("osmium"):
        return None
    filtered_pbf = work_dir / f"{country_code.lower()}-amenities.osm.pbf"
    geojsonseq = work_dir / f"{country_code.lower()}-amenities.geojsonseq"
    filter_args = [f"n/{key}" for key in AMENITY_TAG_KEYS]
    subprocess.run(
        ["osmium", "tags-filter", "--overwrite", "-o", str(filtered_pbf), str(pbf_path), *filter_args],
        check=True,
    )
    subprocess.run(
        ["osmium", "export", "--overwrite", "-f", "geojsonseq", "-o", str(geojsonseq), str(filtered_pbf)],
        check=True,
    )
    return geojsonseq


def shutil_which(name: str) -> str | None:
    for directory in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(directory) / name
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def _amenity_kind_and_category(properties: dict[str, Any]) -> tuple[str, str]:
    fallback = ""
    for key in AMENITY_TAG_KEYS:
        value = _text(properties.get(key))
        if value:
            kind = f"{key}:{value}"
            fallback = fallback or kind
            category = AMENITY_KIND_TO_BUNDLE_CATEGORY.get(kind, "")
            if category:
                return kind, category
    return fallback, ""


def _amenity_kind(properties: dict[str, Any]) -> str:
    return _amenity_kind_and_category(properties)[0]


def _iter_geojson_features(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if line.startswith("\x1e"):
                line = line.lstrip("\x1e").strip()
            if not line:
                continue
            try:
                document = json.loads(line)
            except json.JSONDecodeError:
                document = json.loads(path.read_text(encoding="utf-8"))
                yield from document.get("features", [])
                return
            if document.get("type") == "FeatureCollection":
                yield from document.get("features", [])
            elif document.get("type") == "Feature":
                yield document


def _load_amenity_points(path: Path, cell_degrees: float) -> dict[tuple[int, int], list[dict[str, Any]]]:
    grid: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for feature in _iter_geojson_features(path):
        geometry = feature.get("geometry") or {}
        if geometry.get("type") != "Point":
            continue
        coordinates = geometry.get("coordinates") or []
        if len(coordinates) < 2:
            continue
        longitude = _float_or_none(coordinates[0])
        latitude = _float_or_none(coordinates[1])
        if latitude is None or longitude is None:
            continue
        properties = feature.get("properties") or {}
        kind, category = _amenity_kind_and_category(properties)
        if not kind:
            continue
        grid[_grid_key(latitude, longitude, cell_degrees)].append(
            {
                "latitude": latitude,
                "longitude": longitude,
                "kind": kind,
                "category": category,
                "name": _text(properties.get("name")),
                "opening_hours": _text(properties.get("opening_hours")),
            }
        )
    return grid


def _build_amenity_rows(
    *,
    stations: list[StationRow],
    include_osm: bool,
    download_osm_pbf: bool,
    pbf_cache_dir: Path,
    radius_m: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    by_country: dict[str, list[StationRow]] = defaultdict(list)
    for station in stations:
        by_country[station.country_code].append(station)
    cell_degrees = max(radius_m / 111_320.0, 0.001)

    with tempfile.TemporaryDirectory(prefix="woladen-onboarded-osm-") as temp_dir:
        work_dir = Path(temp_dir)
        for country_code, country_stations in sorted(by_country.items()):
            osm_status = "skipped"
            points: list[osm_amenities.AmenityPoint] = []
            pbf_sha256 = ""
            extracted_at = ""
            if include_osm and country_code in OSM_PBF_URLS:
                pbf_path = _download_pbf(country_code, pbf_cache_dir) if download_osm_pbf else pbf_cache_dir / Path(OSM_PBF_URLS[country_code]).name
                if pbf_path.exists():
                    try:
                        points, _stats = osm_amenities.collect_amenity_points_from_pbf(
                            pbf_path=pbf_path,
                            stations=country_stations,
                            radius_m=radius_m,
                        )
                        pbf_sha256 = osm_amenities.sha256_file(pbf_path)
                        extracted_at = osm_amenities.utc_now_iso()
                        osm_status = "extracted_from_pbf"
                    except RuntimeError:
                        osm_status = "osmium_not_available"
                else:
                    osm_status = "pbf_missing"

            if points:
                rows.extend(
                    osm_amenities.join_station_amenity_rows(
                        stations=country_stations,
                        points=points,
                        radius_m=radius_m,
                        osm_pbf_url=OSM_PBF_URLS.get(country_code, ""),
                        osm_extraction_status=osm_status,
                        osm_pbf_sha256=pbf_sha256,
                        osm_extracted_at=extracted_at,
                    )
                )
                continue

            for station in country_stations:
                rows.append(_empty_amenity_row(station, osm_status))
    return rows


def _empty_amenity_row(station: StationRow, status: str) -> dict[str, Any]:
    return {
        "country_code": station.country_code,
        "station_id": station.station_id,
        "amenity_count": "0",
        "amenity_summary": "",
        "amenity_category_counts": {},
        "amenity_examples": [],
        "nearest_amenity_kind": "",
        "nearest_amenity_name": "",
        "nearest_amenity_distance_m": "",
        "osm_pbf_url": OSM_PBF_URLS.get(station.country_code, ""),
        "osm_extraction_status": status,
    }


def _decimal_text(value: float | int | None) -> str:
    if value is None:
        return ""
    text = f"{float(value):.3f}".rstrip("0").rstrip(".")
    if "." not in text:
        text = f"{text}.0"
    return text


def _coordinate_text(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.7f}".rstrip("0").rstrip(".")


def _unique_texts(values: Iterable[Any]) -> list[str]:
    seen: dict[str, None] = {}
    for value in values:
        text = _text(value)
        if text:
            seen.setdefault(text, None)
    return sorted(seen)


def _chargers_by_station(chargers: Iterable[ChargerRow]) -> dict[str, list[ChargerRow]]:
    grouped: dict[str, list[ChargerRow]] = defaultdict(list)
    for charger in chargers:
        grouped[charger.station_id].append(charger)
    return grouped


def _parse_amenity_summary(summary: str) -> Counter[str]:
    counter: Counter[str] = Counter()
    for item in _text(summary).split(";"):
        if not item or "=" not in item:
            continue
        key, value = item.rsplit("=", 1)
        try:
            counter[AMENITY_KIND_TO_BUNDLE_CATEGORY[key]] += int(value)
        except (KeyError, ValueError):
            continue
    return counter


def _amenity_bundle_values(amenity_row: dict[str, Any] | None) -> dict[str, Any]:
    category_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    source = ""
    if amenity_row is not None:
        raw_counts = amenity_row.get("amenity_category_counts")
        if isinstance(raw_counts, dict):
            for category, count in raw_counts.items():
                if category in AMENITY_BUNDLE_CATEGORIES:
                    category_counts[category] += int(count or 0)
        else:
            category_counts.update(_parse_amenity_summary(str(amenity_row.get("amenity_summary") or "")))
        raw_examples = amenity_row.get("amenity_examples")
        if isinstance(raw_examples, list):
            examples = raw_examples[:12]
        nearest_category = AMENITY_KIND_TO_BUNDLE_CATEGORY.get(str(amenity_row.get("nearest_amenity_kind") or ""))
        if nearest_category and not examples:
            example: dict[str, Any] = {"category": nearest_category}
            if amenity_row.get("nearest_amenity_name"):
                example["name"] = amenity_row["nearest_amenity_name"]
            distance = _float_or_none(amenity_row.get("nearest_amenity_distance_m"))
            if distance is not None:
                example["distance_m"] = round(distance)
            examples = [example]
        source = "osm-pbf" if amenity_row.get("osm_extraction_status") == "extracted_from_pbf" else _text(
            amenity_row.get("osm_extraction_status")
        )

    values: dict[str, Any] = {
        f"amenity_{category}": int(category_counts.get(category, 0))
        for category in AMENITY_BUNDLE_CATEGORIES
    }
    values["amenities_total"] = int(sum(values[f"amenity_{category}"] for category in AMENITY_BUNDLE_CATEGORIES))
    values["amenities_source"] = source
    values["amenity_examples"] = json.dumps(examples, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return values


def _station_detail_source_name(station: StationRow) -> str:
    if station.country_code == "CH":
        return "CH BFE Ladestationen"
    if station.country_code == "NL":
        return "NL NDW/DOT-NL OCPI locations"
    if station.country_code == "BE":
        return "BE Transportdata AFIR"
    return station.source_uid


def _station_full_bundle_row(station: StationRow, station_chargers: list[ChargerRow]) -> dict[str, Any]:
    connector_types = _unique_texts(charger.connector_type for charger in station_chargers)
    powers = [charger.max_power_kw for charger in station_chargers if charger.max_power_kw is not None]
    max_power_kw = station.max_power_kw if station.max_power_kw is not None else max(powers, default=None)
    return {
        "station_id": station.station_id,
        "operator": station.operator_name or "Unbekannt",
        "status": "In Betrieb",
        "max_power_kw": _decimal_text(max_power_kw),
        "charging_points_count": str(station.charger_count),
        "max_individual_power_kw": _decimal_text(max_power_kw),
        "lat": _coordinate_text(station.latitude),
        "lon": _coordinate_text(station.longitude),
        "postcode": station.postal_code,
        "city": station.city,
        "address": station.address,
        "connector_types": str(connector_types),
        "bnetza_display_name": station.display_name or station.operator_name,
        "bnetza_location_name": station.location_name or station.source_station_id,
        "bnetza_parking_info": station.parking_info,
        "bnetza_payment_systems": station.payment_methods,
        "bnetza_opening_hours": station.opening_hours,
        "bnetza_opening_days": station.opening_days,
        "bnetza_opening_times": station.opening_times,
        "bnetza_commissioned_at": "",
        "has_active_record": "True",
    }


def _station_fast_bundle_row(
    station: StationRow,
    station_chargers: list[ChargerRow],
    amenity_row: dict[str, Any] | None,
    generated_at: str,
) -> dict[str, Any]:
    base = _station_full_bundle_row(station, station_chargers)
    base.pop("has_active_record", None)
    source_evse_ids = _unique_texts(charger.source_evse_id for charger in station_chargers)
    connector_types = _unique_texts(charger.connector_type for charger in station_chargers)
    current_types = _unique_texts(charger.current_type for charger in station_chargers)
    details = {
        "country_code": station.country_code,
        "source_uid": station.source_uid,
        "source_station_id": station.source_station_id,
        "source_url": station.source_url,
        "license": station.license,
        "id_rule": station.id_rule,
    }
    details.update(station.details)
    base.update(
        {
            "occupancy_source_uid": "",
            "occupancy_source_name": "",
            "occupancy_status": "",
            "occupancy_last_updated": "",
            "occupancy_total_evses": "0",
            "occupancy_available_evses": "0",
            "occupancy_occupied_evses": "0",
            "occupancy_charging_evses": "0",
            "occupancy_out_of_order_evses": "0",
            "occupancy_unknown_evses": "0",
            "detail_source_uid": station.source_uid,
            "detail_source_name": _station_detail_source_name(station),
            "detail_last_updated": station.detail_last_updated or generated_at,
            "datex_site_id": station.source_station_id,
            "datex_station_ids": station.source_station_id,
            "datex_charge_point_ids": "|".join(source_evse_ids),
            "price_display": station.price_display,
            "price_energy_eur_kwh_min": station.price_energy_eur_kwh_min,
            "price_energy_eur_kwh_max": station.price_energy_eur_kwh_max,
            "price_currency": station.price_currency,
            "price_quality": station.price_quality,
            "opening_hours_display": station.opening_hours,
            "opening_hours_is_24_7": "True" if station.opening_hours == "24/7" else "False",
            "helpdesk_phone": station.helpdesk_phone,
            "payment_methods_display": station.payment_methods,
            "auth_methods_display": station.auth_methods,
            "connector_types_display": " | ".join(connector_types),
            "current_types_display": " | ".join(current_types),
            "connector_count": str(len(station_chargers)),
            "green_energy": station.green_energy,
            "service_types_display": station.service_types,
            "details_json": json.dumps(details, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
        }
    )
    base.update(_amenity_bundle_values(amenity_row))
    return base


def _write_fast_geojson(path: Path, fast_rows: list[dict[str, Any]], generated_at: str) -> None:
    features = []
    for row in fast_rows:
        latitude = _float_or_none(row.get("lat"))
        longitude = _float_or_none(row.get("lon"))
        if latitude is None or longitude is None:
            continue
        properties: dict[str, Any] = {}
        for field in FAST_BUNDLE_FIELDS:
            if field in {"lat", "lon", "connector_types"} or field in TRIMMED_GEOJSON_PROPERTY_KEYS:
                continue
            value: Any = row.get(field, "")
            if field in {
                "max_power_kw",
                "max_individual_power_kw",
                "price_energy_eur_kwh_min",
                "price_energy_eur_kwh_max",
            }:
                parsed = _float_or_none(value)
                value = "" if parsed is None else parsed
            elif field in {
                "charging_points_count",
                "occupancy_total_evses",
                "occupancy_available_evses",
                "occupancy_occupied_evses",
                "occupancy_charging_evses",
                "occupancy_out_of_order_evses",
                "occupancy_unknown_evses",
                "connector_count",
                "amenities_total",
                *(f"amenity_{category}" for category in AMENITY_BUNDLE_CATEGORIES),
            }:
                value = int(value or 0)
            elif field == "opening_hours_is_24_7":
                value = str(value).strip().lower() == "true"
            elif field == "amenity_examples":
                try:
                    value = json.loads(str(value or "[]"))
                except json.JSONDecodeError:
                    value = []
            properties[field] = value
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [longitude, latitude]},
                "properties": properties,
            }
        )
    path.write_text(
        json.dumps(
            {"type": "FeatureCollection", "generated_at": generated_at, "features": features},
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ),
        encoding="utf-8",
    )


def _build_operator_list(fast_rows: list[dict[str, Any]], min_stations: int, generated_at: str) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    for row in fast_rows:
        counts[_text(row.get("operator")) or "Unbekannt"] += 1
    operators = [
        {"name": operator, "stations": int(station_count)}
        for operator, station_count in counts.items()
        if station_count >= min_stations
    ]
    operators.sort(key=lambda item: (-item["stations"], item["name"].lower()))
    return {
        "generated_at": generated_at,
        "min_stations": int(min_stations),
        "total_operators": len(operators),
        "operators": operators,
    }


def _prepare_output_dir(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    generated_names = {
        "chargers_full.csv",
        "chargers_fast.csv",
        "stations.csv",
        "chargers.csv",
        "chargers_fast.geojson",
        "operators.json",
        "summary.json",
        "catalog_summary.json",
        "source_attribution.json",
        "dedupe_report.csv",
        "live_seed",
    }
    for name in generated_names:
        path = output_dir / name
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            path.unlink()


def build_catalog(
    *,
    output_dir: Path,
    countries: Sequence[str],
    include_osm: bool,
    download_osm_pbf: bool,
    pbf_cache_dir: Path,
    amenity_radius_m: float,
    operator_min_stations: int,
) -> dict[str, Any]:
    _prepare_output_dir(output_dir)
    stations: list[StationRow] = []
    chargers: list[ChargerRow] = []
    dedupe_rows: list[dict[str, str]] = []
    source_summaries: dict[str, Any] = {}
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    for country_code in countries:
        normalized = country_code.upper()
        if normalized == "CH":
            country_stations, country_chargers, country_dedupe, summary = _parse_ch_catalog()
        elif normalized == "NL":
            country_stations, country_chargers, country_dedupe, summary = _parse_nl_catalog()
        elif normalized == "BE":
            country_stations, country_chargers, country_dedupe, summary = _be_catalog_gap()
        else:
            raise ValueError(f"unsupported_country:{country_code}")
        stations.extend(country_stations)
        chargers.extend(country_chargers)
        dedupe_rows.extend(country_dedupe)
        source_summaries[normalized] = summary

    station_ids = Counter(station.station_id for station in stations)
    for station_id, count in sorted(station_ids.items()):
        if count > 1:
            dedupe_rows.append(
                {
                    "issue": "duplicate_normalized_station_id",
                    "country_code": station_id.split(":", 1)[0].upper(),
                    "station_id": station_id,
                    "source_uid": "",
                    "details": f"{count} station rows share station_id",
                }
            )

    fast_chargers = [charger for charger in chargers if _is_fast(charger.max_power_kw, charger.current_type)]
    fast_station_ids = {charger.station_id for charger in fast_chargers}
    amenities = _build_amenity_rows(
        stations=stations,
        include_osm=include_osm,
        download_osm_pbf=download_osm_pbf,
        pbf_cache_dir=pbf_cache_dir,
        radius_m=amenity_radius_m,
    )
    amenities_by_station = {row["station_id"]: row for row in amenities}
    station_chargers = _chargers_by_station(chargers)
    full_rows = [
        _station_full_bundle_row(station, station_chargers.get(station.station_id, []))
        for station in stations
    ]
    fast_rows = [
        _station_fast_bundle_row(
            station,
            station_chargers.get(station.station_id, []),
            amenities_by_station.get(station.station_id),
            generated_at,
        )
        for station in stations
        if station.station_id in fast_station_ids
    ]
    fast_rows = [
        row
        for row in fast_rows
        if int(row.get("amenities_total") or 0) > 0
        and _float_or_none(row.get("lat")) is not None
        and _float_or_none(row.get("lon")) is not None
    ]
    full_rows.sort(key=lambda row: (str(row.get("operator", "")).lower(), str(row.get("station_id", ""))))
    fast_rows.sort(
        key=lambda row: (
            -int(row.get("amenities_total") or 0),
            -(_float_or_none(row.get("max_power_kw")) or 0.0),
            str(row.get("operator", "")).lower(),
            str(row.get("station_id", "")),
        )
    )

    _write_csv(output_dir / "stations.csv", STATION_FIELDS, (_station_dict(station) for station in stations))
    _write_csv(output_dir / "chargers.csv", CHARGER_FIELDS, (_charger_dict(charger) for charger in chargers))
    _write_csv(output_dir / "chargers_full.csv", FULL_BUNDLE_FIELDS, full_rows)
    _write_csv(output_dir / "chargers_fast.csv", FAST_BUNDLE_FIELDS, fast_rows)
    _write_csv(output_dir / "dedupe_report.csv", DEDUPE_FIELDS, dedupe_rows)
    _write_fast_geojson(output_dir / "chargers_fast.geojson", fast_rows, generated_at)

    attribution = {
        "generated_at": generated_at,
        "sources": {
            "CH": {
                "url": CH_STATIC_DATA_URL,
                "license": "source_terms_pending_review",
                "source_uid": CH_STATIC_SOURCE_UID,
                "provider_uid": CH_PROVIDER_UID,
            },
            "NL": {
                "url": NL_OCPI_LOCATIONS_URL,
                "license": "CC0-1.0-pending-final-confirmation",
                "source_uid": "nl_ndw_dotnl_ocpi_locations",
            },
            "BE": {"url": "https://transportdata.be/en/dataset/?q=AFIR", "license": "pending_source_access"},
            "OSM": {
                "pbf_urls": OSM_PBF_URLS,
                "license": "ODbL-1.0",
                "attribution": "OpenStreetMap contributors",
                "amenity_radius_m": amenity_radius_m,
            },
        },
    }
    (output_dir / "source_attribution.json").write_text(json.dumps(attribution, indent=2, sort_keys=True), encoding="utf-8")

    operators_payload = _build_operator_list(fast_rows, operator_min_stations, generated_at)
    (output_dir / "operators.json").write_text(
        json.dumps(operators_payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False),
        encoding="utf-8",
    )

    stations_with_static_details = sum(1 for row in fast_rows if _text(row.get("detail_source_uid")))
    stations_with_opening_hours = sum(1 for row in fast_rows if _text(row.get("opening_hours_display")))
    stations_with_price = sum(1 for row in fast_rows if _text(row.get("price_display")))
    stations_with_helpdesk = sum(1 for row in fast_rows if _text(row.get("helpdesk_phone")))
    stations_with_amenities = sum(1 for row in fast_rows if int(row.get("amenities_total") or 0) > 0)
    summary = {
        "run": {
            "started_at": generated_at,
            "finished_at": generated_at,
        },
        "source": {
            "source_url": "onboarded-static-catalog",
            "fetched_at": generated_at,
            "countries": list(countries),
            "sources": attribution["sources"],
        },
        "params": {
            "min_power_kw": FAST_POWER_THRESHOLD_KW,
            "radius_m": amenity_radius_m,
            "amenity_backend": "osm-pbf" if include_osm else "none",
            "query_budget": 0,
            "refresh_days": 0,
            "max_stations": 0,
            "operator_min_stations": operator_min_stations,
        },
        "records": {
            "raw_rows": len(chargers),
            "full_registry_stations_total": len(full_rows),
            "full_registry_active_stations_total": len(full_rows),
            "fast_chargers_total": len(fast_rows),
            "stations_with_live_occupancy": 0,
            "stations_with_static_details": stations_with_static_details,
            "stations_with_price": stations_with_price,
            "stations_with_opening_hours": stations_with_opening_hours,
            "stations_with_helpdesk": stations_with_helpdesk,
            "stations_with_amenities": stations_with_amenities,
        },
        "occupancy_lookup": {
            "sources_discovered": 0,
            "sources_used": 0,
            "locations_scanned": 0,
            "matched_locations": 0,
            "matched_stations": 0,
            "matched_evses": 0,
            "errors": [],
            "sources": [],
        },
        "static_detail_lookup": {
            "sources_discovered": len(source_summaries),
            "sources_used": sum(1 for item in source_summaries.values() if item.get("station_count", 0)),
            "matched_stations": stations_with_static_details,
            "stations_with_price": stations_with_price,
            "stations_with_opening_hours": stations_with_opening_hours,
            "stations_with_helpdesk": stations_with_helpdesk,
            "errors": [],
            "sources": source_summaries,
        },
        "amenity_lookup": {
            "backend": "osm-pbf" if include_osm else "none",
            "queries_used": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "deferred": 0,
            "lookup_errors": 0,
            "cache_entries": 0,
            "amenity_radius_m": amenity_radius_m,
            "amenity_rows": len(amenities),
            "osm_pbf_urls": OSM_PBF_URLS,
            "stations_with_mapped_amenities": stations_with_amenities,
        },
        "operators": {
            "min_stations": operator_min_stations,
            "listed_operators": operators_payload["total_operators"],
        },
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")

    catalog_summary = {
        "generated_at": generated_at,
        "countries": list(countries),
        "station_count": len(stations),
        "charger_count": len(chargers),
        "fast_charger_count": len(fast_rows),
        "fast_candidate_connector_count": len(fast_chargers),
        "fast_candidate_station_count": len(fast_station_ids),
        "fast_station_count": len(fast_rows),
        "dedupe_issue_count": len(dedupe_rows),
        "amenity_rows": len(amenities),
        "source_summaries": source_summaries,
        "outputs": sorted({path.name for path in output_dir.iterdir() if path.is_file()} | {"catalog_summary.json"}),
    }
    (output_dir / "catalog_summary.json").write_text(json.dumps(catalog_summary, indent=2, sort_keys=True), encoding="utf-8")
    return catalog_summary


def _country_list(values: Sequence[str]) -> list[str]:
    if not values:
        return ["BE", "NL", "CH"]
    countries: list[str] = []
    for value in values:
        for part in value.split(","):
            text = part.strip().upper()
            if text:
                countries.append(text)
    return list(dict.fromkeys(countries))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build onboarded-country open static catalog seed files.")
    parser.add_argument("--output-dir", type=Path, default=Path("data/onboarded_static"))
    parser.add_argument("--country", action="append", default=[], help="Country code. Can be repeated or comma-separated.")
    parser.add_argument("--include-osm", action="store_true", help="Extract OSM amenities from country PBFs.")
    parser.add_argument("--download-osm-pbf", action="store_true", help="Download country PBFs into the cache directory.")
    parser.add_argument("--pbf-cache-dir", type=Path, default=Path("data/osm_pbf_cache"))
    parser.add_argument("--amenity-radius-m", type=float, default=250.0)
    parser.add_argument("--operator-min-stations", type=int, default=100)
    args = parser.parse_args(argv)

    summary = build_catalog(
        output_dir=args.output_dir,
        countries=_country_list(args.country),
        include_osm=args.include_osm,
        download_osm_pbf=args.download_osm_pbf,
        pbf_cache_dir=args.pbf_cache_dir,
        amenity_radius_m=args.amenity_radius_m,
        operator_min_stations=args.operator_min_stations,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
