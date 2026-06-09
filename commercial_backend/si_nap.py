from __future__ import annotations

import io
import json
import re
import unicodedata
import xml.etree.ElementTree as ET
from collections import Counter
from typing import Any, Iterable

from .stream_utils import text_stream_from_binary_stream as _text_stream_util

COUNTRY_CODE = "SI"

SI_NAP_DATASET_TABLE_URL = "https://nap.si/en/datasets_details?id=46963663-38dd-eb04-43a9-cca9bdc0e4ba"
SI_NAP_DATASET_STATUS_URL = "https://nap.si/en/datasets_details?id=acc8a643-9dac-ecad-58da-0ce20f88f4bd"
SI_NAP_PROFILE_URL = "https://www.nap.si/_resources/profiles/DatexII3.6_NAP_prometej_profile.zip"
SI_NAP_TOKEN_URL = "https://b2b.nap.si/uc/user/token"
SI_NAP_TABLE_URL = "https://b2b.nap.si/data/b2b.prometej.energyInfrastructureTablePublication"
SI_NAP_STATUS_URL = "https://b2b.nap.si/data/b2b.prometej.energyInfrastructureStatusPublication"

SI_NAP_TABLE_SOURCE_UID = "si_nap_prometej_energy_infrastructure_table"
SI_NAP_STATUS_SOURCE_UID = "si_nap_prometej_energy_infrastructure_status"
SI_NAP_PROVIDER_UID = "si_nap_prometej"

STATUS_MAP = {
    "available": "free",
    "charging": "occupied",
    "occupied": "occupied",
    "reserved": "reserved",
    "inoperative": "out_of_order",
    "unavailable": "out_of_order",
    "outofservice": "out_of_order",
    "out_of_service": "out_of_order",
    "faulted": "out_of_order",
    "unknown": "unknown",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _safe_id(value: Any) -> str:
    text = unicodedata.normalize("NFKD", _text(value)).encode("ascii", "ignore").decode("ascii").lower()
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-", "*"} else "-" for ch in text)
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "unknown"


def _float_or_none(value: Any) -> float | None:
    text = _text(value).replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _price_scalar(value: float) -> str:
    return f"{float(value):.6f}".rstrip("0").rstrip(".")


def _euro_amount(value: float) -> str:
    return f"{round(float(value) + 0.000000001, 2):.2f}".replace(".", ",")


def _local_name(element: ET.Element) -> str:
    return element.tag.split("}", 1)[-1]


def _direct_children(parent: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in parent if _local_name(child) == name]


def _direct_child_text(parent: ET.Element, name: str) -> str:
    for child in parent:
        if _local_name(child) == name:
            return _text(child.text)
    return ""


def _values_text(parent: ET.Element) -> list[str]:
    values: list[str] = []
    for descendant in parent.iter():
        if _local_name(descendant) == "value" and _text(descendant.text):
            values.append(_text(descendant.text))
    return values


def _direct_child_value_text(parent: ET.Element, name: str) -> str:
    for child in parent:
        if _local_name(child) != name:
            continue
        direct = _text(child.text)
        if direct:
            return direct
        values = _values_text(child)
        return values[0] if values else ""
    return ""


def _first_text(parent: ET.Element, name: str) -> str:
    for child in parent.iter():
        if _local_name(child) == name and _text(child.text):
            return _text(child.text)
    return ""


def _join_unique(values: Iterable[Any]) -> str:
    seen: dict[str, None] = {}
    for value in values:
        text = _text(value)
        if text:
            seen.setdefault(text, None)
    return "|".join(seen.keys())


def _compact_json(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _text_stream_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str,
) -> io.TextIOWrapper:
    return _text_stream_util(raw_stream, content_encoding=content_encoding)


def station_id_from_site_id(source_site_id: Any) -> str:
    return f"si:nap:{_safe_id(source_site_id)}"


def charger_id_from_evse_id(source_evse_id: Any) -> str:
    return f"si:nap:evse:{_safe_id(source_evse_id)}"


def _address_parts(site: ET.Element) -> dict[str, str]:
    result = {"address": "", "postal_code": "", "city": ""}
    for location_reference in site.iter():
        if _local_name(location_reference) != "locationReference":
            continue
        result["postal_code"] = _first_text(location_reference, "postcode")
        result["city"] = _direct_child_value_text(_first_address_child(location_reference), "city")
        if not result["city"]:
            result["city"] = _first_text(location_reference, "city")
        address_lines: list[tuple[int, str]] = []
        for address_line in location_reference.iter():
            if _local_name(address_line) != "addressLine":
                continue
            order_text = _text(address_line.attrib.get("order"))
            order = int(order_text) if order_text.isdigit() else 0
            line = _direct_child_value_text(address_line, "text") or _first_text(address_line, "value")
            if line:
                address_lines.append((order, line))
        result["address"] = " ".join(line for _order, line in sorted(address_lines) if line)
        return result
    return result


def _first_address_child(location_reference: ET.Element) -> ET.Element:
    for child in location_reference.iter():
        if _local_name(child) == "address":
            return child
    return location_reference


def _coordinates(site: ET.Element) -> tuple[float | None, float | None]:
    for container_name in ("coordinatesForDisplay", "pointCoordinates"):
        for element in site.iter():
            if _local_name(element) != container_name:
                continue
            latitude = _float_or_none(_direct_child_text(element, "latitude"))
            longitude = _float_or_none(_direct_child_text(element, "longitude"))
            if latitude is not None and longitude is not None:
                return latitude, longitude
    return None, None


def _operator(site: ET.Element) -> tuple[str, str]:
    for child in site:
        if _local_name(child) != "operator":
            continue
        operator_id = _text(child.attrib.get("id"))
        operator_name = _direct_child_value_text(child, "name") or _direct_child_value_text(child, "legalName")
        return operator_id, operator_name or operator_id
    return "", ""


def _opening_hours(site: ET.Element) -> str:
    periods: list[dict[str, str]] = []
    for operating_hours in _direct_children(site, "operatingHours"):
        for valid_period in operating_hours.iter():
            if _local_name(valid_period) != "validPeriod":
                continue
            day = _first_text(valid_period, "applicableDay") or _direct_child_value_text(valid_period, "periodName")
            start = _first_text(valid_period, "startTimeOfPeriod")
            end = _first_text(valid_period, "endTimeOfPeriod")
            if day or start or end:
                periods.append({"day": day, "start": start, "end": end})
    if not periods:
        return ""
    days = {period["day"].casefold() for period in periods if period.get("day")}
    full_week = {"monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"}
    all_day = all(
        _text(period.get("start")).startswith("00:00")
        and (_text(period.get("end")).startswith("23:59") or _text(period.get("end")).startswith("24:00"))
        for period in periods
    )
    if days >= full_week and all_day:
        return "24/7"
    return _compact_json(periods)


def _connector_rows(refill_point: ET.Element) -> list[dict[str, Any]]:
    connectors: list[dict[str, Any]] = []
    for index, connector in enumerate(_direct_children(refill_point, "connector"), start=1):
        connector_type = _direct_child_text(connector, "connectorType")
        charging_mode = _direct_child_text(connector, "chargingMode")
        connector_format = _direct_child_text(connector, "connectorFormat")
        power = _float_or_none(_direct_child_text(connector, "maxPowerAtSocket"))
        if power is not None:
            power = power / 1000.0 if power > 0 else None
        connector_id = _text(connector.attrib.get("id")) or str(index)
        connectors.append(
            {
                "connector_id": connector_id,
                "connector_type": connector_type,
                "charging_mode": charging_mode,
                "connector_format": connector_format,
                "current_type": _current_type((connector_type, charging_mode)),
                "max_power_kw": power,
            }
        )
    return connectors


def _current_type(values: Iterable[str]) -> str:
    normalized = " ".join(_text(value).casefold() for value in values)
    if "dc" in normalized or "chademo" in normalized or "combo" in normalized:
        return "DC"
    if "ac" in normalized or "iec62196t2" in normalized or "domestic" in normalized:
        return "AC"
    return ""


def _rate_price_fields(refill_point: ET.Element) -> dict[str, Any]:
    energy_values: list[float] = []
    minute_values: list[float] = []
    fixed_values: list[float] = []
    currencies: list[str] = []
    policies: list[str] = []
    source_rows: list[dict[str, Any]] = []
    for rates in _direct_children(refill_point, "rates"):
        currency = _direct_child_text(rates, "applicableCurrency").upper()
        if currency:
            currencies.append(currency)
        for policy in rates.iter():
            if _local_name(policy) == "pricingPolicy" and _text(policy.text):
                policies.append(_text(policy.text))
        for rate_line in rates.iter():
            if _local_name(rate_line) != "rateLine":
                continue
            line_type = _direct_child_text(rate_line, "rateLineType").casefold()
            increment = _direct_child_text(rate_line, "incrementPeriod").upper()
            value = _float_or_none(_direct_child_text(rate_line, "value"))
            if value is None:
                continue
            row = {"rateLineType": line_type, "value": value, "currency": currency}
            source_rows.append(row)
            if line_type in {"perunit", "per_unit"}:
                energy_values.append(value)
            elif line_type in {"perminute", "per_minute", "incrementingrate", "incrementing_rate"} or increment == "PT1M":
                minute_values.append(value)
            else:
                fixed_values.append(value)
    free = any(policy.casefold() == "free" for policy in policies)
    if free and not energy_values and not minute_values:
        return {
            "price_display": "free",
            "price_currency": "EUR",
            "price_energy_eur_kwh_min": "0",
            "price_energy_eur_kwh_max": "0",
            "price_time_eur_min_min": None,
            "price_time_eur_min_max": None,
            "price_quality": "source_si_nap_datex_rates_free",
            "price_complex": False,
            "price_source_text": _compact_json(source_rows),
        }
    if not energy_values and not minute_values:
        return {"price_source_text": _compact_json(source_rows)} if source_rows else {}
    currency = next((item for item in currencies if item), "EUR")
    energy_min = min(energy_values) if energy_values else None
    energy_max = max(energy_values) if energy_values else None
    minute_min = min(minute_values) if minute_values else None
    minute_max = max(minute_values) if minute_values else None
    complex_tariff = bool(minute_values or fixed_values)
    display = ""
    if currency == "EUR" and energy_min is not None:
        if complex_tariff:
            display = f"from {_euro_amount(energy_min)} EUR/kWh"
        elif energy_max is not None and abs(energy_min - energy_max) >= 0.000001:
            display = f"{_euro_amount(energy_min)}-{_euro_amount(energy_max)} EUR/kWh"
        else:
            display = f"{_euro_amount(energy_min)} EUR/kWh"
    elif currency == "EUR" and minute_min is not None:
        display = f"from {_euro_amount(minute_min)} EUR/min" if complex_tariff else f"{_euro_amount(minute_min)} EUR/min"
    return {
        "price_display": display,
        "price_currency": currency,
        "price_energy_eur_kwh_min": _price_scalar(energy_min) if energy_min is not None and currency == "EUR" else "",
        "price_energy_eur_kwh_max": _price_scalar(energy_max) if energy_max is not None and currency == "EUR" else "",
        "price_time_eur_min_min": round(minute_min, 6) if minute_min is not None and currency == "EUR" else None,
        "price_time_eur_min_max": round(minute_max, 6) if minute_max is not None and currency == "EUR" else None,
        "price_quality": "source_si_nap_datex_rates_complex" if complex_tariff else "source_si_nap_datex_rates_exact",
        "price_complex": complex_tariff,
        "price_source_text": _compact_json(source_rows),
    }


def _status_from_source(source_status: str, is_available: str) -> str:
    normalized = _text(source_status).casefold() or "unknown"
    if _text(is_available).casefold() == "false" and normalized == "unknown":
        return "out_of_order"
    return STATUS_MAP.get(normalized, "unknown")


def iter_table_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    publication_time = ""
    seen_evse_ids: set[str] = set()
    for _event, element in ET.iterparse(text_stream, events=("end",)):
        local_name = _local_name(element)
        if local_name == "publicationTime" and not publication_time:
            publication_time = _text(element.text)
            continue
        if local_name != "energyInfrastructureSite":
            continue
        source_site_id = _text(element.attrib.get("id"))
        if not source_site_id:
            element.clear()
            continue
        station_id = station_id_from_site_id(source_site_id)
        station_name = _direct_child_value_text(element, "name")
        site_lat, site_lon = _coordinates(element)
        address = _address_parts(element)
        operator_id, operator_name = _operator(element)
        opening_hours = _opening_hours(element)
        vehicle_types = _join_unique(
            child.text
            for child in element.iter()
            if _local_name(child) == "vehicleType" and _text(child.text)
        )
        for station in element.iter():
            if _local_name(station) != "energyInfrastructureStation":
                continue
            source_station_ref = _text(station.attrib.get("id")) or source_site_id
            auth_methods = _join_unique(
                child.text
                for child in _direct_children(station, "authenticationAndIdentificationMethods")
                if _text(child.text)
            )
            for refill_point in _direct_children(station, "refillPoint"):
                source_evse_id = _text(refill_point.attrib.get("id"))
                if not source_evse_id or source_evse_id in seen_evse_ids:
                    continue
                seen_evse_ids.add(source_evse_id)
                connectors = _connector_rows(refill_point)
                connector_types = _join_unique(
                    connector.get("connector_type") for connector in connectors if connector.get("connector_type")
                )
                connector_formats = _join_unique(
                    connector.get("connector_format") for connector in connectors if connector.get("connector_format")
                )
                current_types = _join_unique(
                    connector.get("current_type") for connector in connectors if connector.get("current_type")
                )
                power_values = [
                    connector.get("max_power_kw")
                    for connector in connectors
                    if connector.get("max_power_kw") is not None
                ]
                row = {
                    "country_code": COUNTRY_CODE,
                    "source_uid": SI_NAP_TABLE_SOURCE_UID,
                    "provider_uid": SI_NAP_PROVIDER_UID,
                    "station_id": station_id,
                    "charger_id": charger_id_from_evse_id(source_evse_id),
                    "source_station_id": source_site_id,
                    "source_evse_id": source_evse_id,
                    "source_station_ref": source_station_ref,
                    "connector_id": _join_unique(connector.get("connector_id") for connector in connectors),
                    "operator_name": operator_name or operator_id,
                    "station_name": station_name or source_site_id,
                    "address": address["address"],
                    "city": address["city"],
                    "postal_code": address["postal_code"],
                    "latitude": site_lat,
                    "longitude": site_lon,
                    "connector_count": len(connectors) or 1,
                    "connector_types": connector_types,
                    "connector_formats": connector_formats,
                    "current_type": current_types,
                    "max_power_kw": max(power_values) if power_values else None,
                    "auth_methods": auth_methods,
                    "opening_hours": opening_hours,
                    "vehicle_types": vehicle_types,
                    "date_updated": publication_time,
                }
                row.update(_rate_price_fields(refill_point))
                yield row
        element.clear()


def iter_status_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    publication_time = ""
    for _event, element in ET.iterparse(text_stream, events=("end",)):
        local_name = _local_name(element)
        if local_name == "publicationTime" and not publication_time:
            publication_time = _text(element.text)
            continue
        if local_name != "energyInfrastructureSiteStatus":
            continue
        site_reference = _reference_id(element)
        if not site_reference:
            element.clear()
            continue
        station_id = station_id_from_site_id(site_reference)
        for station_status in element.iter():
            if _local_name(station_status) != "energyInfrastructureStationStatus":
                continue
            station_ref = _reference_id(station_status)
            station_available = _direct_child_text(station_status, "isAvailable")
            for refill_status in _direct_children(station_status, "refillPointStatus"):
                source_evse_id = _reference_id(refill_status)
                if not source_evse_id:
                    continue
                source_status = _direct_child_text(refill_status, "status").casefold() or "unknown"
                observed_at = (
                    _direct_child_text(refill_status, "lastUpdated")
                    or _direct_child_text(station_status, "lastUpdated")
                    or publication_time
                )
                yield {
                    "country_code": COUNTRY_CODE,
                    "source_uid": SI_NAP_STATUS_SOURCE_UID,
                    "provider_uid": SI_NAP_PROVIDER_UID,
                    "station_id": station_id,
                    "charger_id": charger_id_from_evse_id(source_evse_id),
                    "source_station_id": site_reference,
                    "source_station_ref": station_ref,
                    "source_evse_id": source_evse_id,
                    "source_status": source_status,
                    "availability_status": _status_from_source(source_status, station_available),
                    "source_observed_at": observed_at,
                }
        element.clear()


def _reference_id(parent: ET.Element) -> str:
    if _local_name(parent) == "reference":
        return _text(parent.attrib.get("id"))
    for child in parent:
        if _local_name(child) == "reference":
            return _text(child.attrib.get("id"))
    return ""


def count_xml_records_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> dict[str, Any]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    counts: Counter[str] = Counter()
    statuses: Counter[str] = Counter()
    root_name = ""
    publication_time = ""
    for event, element in ET.iterparse(text_stream, events=("start", "end")):
        name = _local_name(element)
        if event == "start":
            counts[name] += 1
            if not root_name:
                root_name = name
            continue
        if name == "publicationTime" and _text(element.text):
            publication_time = _text(element.text)
        elif name == "status" and _text(element.text):
            statuses[_text(element.text).casefold()] += 1
        element.clear()
    return {
        "root": root_name,
        "publication_time": publication_time,
        "site_count": counts.get("energyInfrastructureSite") or counts.get("energyInfrastructureSiteStatus", 0),
        "station_count": counts.get("energyInfrastructureStation") or counts.get("energyInfrastructureStationStatus", 0),
        "refill_point_count": counts.get("refillPoint") or counts.get("refillPointStatus", 0),
        "connector_count": counts.get("connector", 0),
        **({"status_values": dict(sorted(statuses.items()))} if statuses else {}),
    }
