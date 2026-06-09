from __future__ import annotations

import io
import json
import xml.etree.ElementTree as ET
from collections import Counter
from typing import Any, Iterable

from . import si_nap as _datex
from .stream_utils import buffered_stream as _buffered_stream_util
from .stream_utils import text_stream_from_binary_stream as _text_stream_util

COUNTRY_CODE = "LV"

LV_TRANSPORTDATA_REST_DOWNLOAD_URL = "https://www.transportdata.gov.lv/api/v1/get/file/download-file"
LV_TRANSPORTDATA_FILE_INFO_URL = "https://www.transportdata.gov.lv/api/v1/metadata/file/info"
LV_TRANSPORTDATA_MQTT_HOST = "stream.transportdata.gov.lv"
LV_TRANSPORTDATA_MQTT_PORT = 1883

LV_ECO_MOVEMENT_STATUS_PRICE_CARD_URL = (
    "https://www.transportdata.gov.lv/en/card/a377a160-baa1-4b67-b4e8-6612cd289e22"
)
LV_ECO_MOVEMENT_STATIC_CARD_URL = (
    "https://www.transportdata.gov.lv/en/card/d8e419c3-1585-4666-9067-85712befd2c4"
)
LV_LVC_EV_CHARGING_STREAM_CARD_URL = (
    "https://www.transportdata.gov.lv/en/card/978835ee-f55b-481c-9791-ba0395d3619a"
)

LV_ECO_MOVEMENT_STATUS_PRICE_DATASET_ID = "429625"
LV_ECO_MOVEMENT_STATIC_DATASET_ID = "342436"
LV_LVC_EV_CHARGING_STREAM_DATASET_ID = "65"

LV_ECO_MOVEMENT_STATUS_PRICE_SOURCE_UID = "lv_transportdata_eco_movement_status_price"
LV_ECO_MOVEMENT_STATIC_SOURCE_UID = "lv_transportdata_eco_movement_static"
LV_LVC_EV_CHARGING_STREAM_SOURCE_UID = "lv_transportdata_lvc_ev_charging_stream"

LV_ECO_MOVEMENT_PROVIDER_UID = "lv_transportdata_eco_movement"
LV_LVC_PROVIDER_UID = "lv_transportdata_lvc"

STATUS_MAP = {
    "available": "free",
    "charging": "occupied",
    "occupied": "occupied",
    "reserved": "reserved",
    "blocked": "out_of_order",
    "inoperative": "out_of_order",
    "outoforder": "out_of_order",
    "outofservice": "out_of_order",
    "out_of_service": "out_of_order",
    "unavailable": "out_of_order",
    "faulted": "out_of_order",
    "planned": "unknown",
    "removed": "unknown",
    "unknown": "unknown",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _local_name(element: ET.Element) -> str:
    return element.tag.split("}", 1)[-1]


def _direct_children(parent: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in parent if _local_name(child) == name]


def _direct_child_text(parent: ET.Element, name: str) -> str:
    for child in parent:
        if _local_name(child) == name:
            return _text(child.text)
    return ""


def _first_text(parent: ET.Element, name: str) -> str:
    for child in parent.iter():
        if _local_name(child) == name and _text(child.text):
            return _text(child.text)
    return ""


def _float_or_none(value: Any) -> float | None:
    text = _text(value).replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _coordinates(site: ET.Element) -> tuple[float | None, float | None]:
    for container_name in ("coordinatesForDisplay", "pointCoordinates", "openlrCoordinates"):
        for element in site.iter():
            if _local_name(element) != container_name:
                continue
            latitude = _float_or_none(_direct_child_text(element, "latitude"))
            longitude = _float_or_none(_direct_child_text(element, "longitude"))
            if latitude is not None and longitude is not None:
                return latitude, longitude
    return None, None


def _reference_id(element: ET.Element) -> str:
    reference = _text(element.attrib.get("reference") or element.attrib.get("id"))
    if reference:
        return reference
    for child in element:
        if _local_name(child) == "reference":
            return _text(child.attrib.get("id") or child.attrib.get("reference") or child.text)
    return ""


def _normalized_power_kw(value: Any) -> float | None:
    power = _float_or_none(value)
    if power is None:
        return None
    return power / 1000.0 if power > 1000 else power


def _text_stream_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str,
) -> io.TextIOWrapper:
    return _text_stream_util(
        _buffered_stream_util(raw_stream),
        content_encoding=content_encoding,
    )


def _provider_key(provider_uid: str) -> str:
    return provider_uid.removeprefix("lv_transportdata_").replace("_", "-") or "transportdata"


def station_id_from_site_id(provider_uid: str, source_site_id: Any) -> str:
    return f"lv:transportdata:{_provider_key(provider_uid)}:{_datex._safe_id(source_site_id)}"


def charger_id_from_evse_id(provider_uid: str, source_evse_id: Any) -> str:
    return f"lv:transportdata:{_provider_key(provider_uid)}:evse:{_datex._safe_id(source_evse_id)}"


def _source_evse_alias_ids(source_evse_id: str, *aliases: str) -> list[str]:
    result: list[str] = []
    seen: set[str] = {source_evse_id}
    for alias in aliases:
        text = _text(alias)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _compact_json(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _values_text(parent: ET.Element) -> list[str]:
    return [_text(element.text) for element in parent.iter() if _local_name(element) == "value" and _text(element.text)]


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


def _first_direct_or_nested_text(parent: ET.Element, name: str) -> str:
    return _direct_child_text(parent, name) or _first_text(parent, name)


def _address_parts(site: ET.Element) -> dict[str, str]:
    result = {"address": "", "postal_code": "", "city": ""}
    for address in site.iter():
        if _local_name(address) != "address":
            continue
        result["postal_code"] = _first_text(address, "postcode")
        result["city"] = _direct_child_value_text(address, "city") or _first_text(address, "city")
        address_lines: list[tuple[int, str]] = []
        for address_line in address.iter():
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


def _organisation_details(parent: ET.Element) -> tuple[str, str, str]:
    for child in parent:
        if _local_name(child) not in {"operator", "owner", "energyProvider"}:
            continue
        organisation_id = _text(child.attrib.get("id"))
        name = _direct_child_value_text(child, "name") or _direct_child_value_text(child, "legalName")
        phone = _first_text(child, "telephoneNumber")
        return organisation_id, name or organisation_id, phone
    return "", "", ""


def _opening_hours(parent: ET.Element) -> str:
    for operating_hours in _direct_children(parent, "operatingHours"):
        xsi_type = " ".join(str(value) for value in operating_hours.attrib.values())
        if "OpenAllHours" in xsi_type:
            return "24/7"
        all_year = _first_text(operating_hours, "operatingAllYear").casefold() == "true"
        start = _first_text(operating_hours, "startTimeOfPeriod")
        end = _first_text(operating_hours, "endTimeOfPeriod")
        if all_year and start.startswith("00:00") and (end.startswith("24:00") or end.startswith("23:59")):
            return "24/7"
    return _datex._opening_hours(parent)


def _payment_methods(parent: ET.Element) -> str:
    values: list[str] = []
    for element in parent.iter():
        if _local_name(element) in {"paymentMeans", "paymentMethod"} and _text(element.text):
            values.append(_text(element.text))
    return _datex._join_unique(values)


def _green_energy(refill_point: ET.Element) -> str:
    values = {
        _text(child.text).casefold()
        for child in refill_point.iter()
        if _local_name(child) == "isGreenEnergy" and _text(child.text)
    }
    if not values:
        return ""
    if values == {"true"}:
        return "true"
    if values == {"false"}:
        return "false"
    return "|".join(sorted(values))


def _rate_price_fields(refill_point: ET.Element) -> dict[str, Any]:
    result = dict(_datex._rate_price_fields(refill_point))
    quality = _text(result.get("price_quality"))
    if quality:
        result["price_quality"] = quality.replace("source_si_nap", "source_lv_transportdata")
    return result


def _price_scalar(value: float) -> str:
    return f"{float(value):.6f}".rstrip("0").rstrip(".")


def _euro_amount(value: float) -> str:
    return f"{round(float(value) + 0.000000001, 2):.2f}".replace(".", ",")


def _new_rates_price_fields(parent: ET.Element) -> dict[str, Any]:
    energy_values: list[float] = []
    minute_values: list[float] = []
    fixed_values: list[float] = []
    currencies: list[str] = []
    source_rows: list[dict[str, Any]] = []
    for rates in _direct_children(parent, "newRates"):
        currency = _direct_child_text(rates, "applicableCurrency").upper()
        if currency:
            currencies.append(currency)
        for rate_line in rates.iter():
            if _local_name(rate_line) != "rateLine":
                continue
            line_type = _direct_child_text(rate_line, "rateLineType").casefold()
            increment = _direct_child_text(rate_line, "incrementPeriod").upper()
            value = _float_or_none(_direct_child_text(rate_line, "value"))
            if value is None:
                continue
            source_rows.append({"rateLineType": line_type, "value": value, "currency": currency})
            if line_type in {"perunit", "per_unit"}:
                energy_values.append(value)
            elif line_type in {"perminute", "per_minute", "incrementingrate", "incrementing_rate"} or increment == "PT1M":
                minute_values.append(value)
            else:
                fixed_values.append(value)
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
        "price_quality": "source_lv_transportdata_new_rates_complex" if complex_tariff else "source_lv_transportdata_new_rates_exact",
        "price_complex": complex_tariff,
        "price_source_text": _compact_json(source_rows),
    }


def _status_from_source(source_status: str, is_available: str) -> str:
    normalized = _text(source_status).casefold() or "unknown"
    if _text(is_available).casefold() == "false" and normalized == "unknown":
        return "out_of_order"
    return STATUS_MAP.get(normalized, "unknown")


def _iter_static_rows(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
    source_uid: str,
    provider_uid: str,
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    publication_time = ""
    seen_source_evse_ids: set[str] = set()
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
        station_id = station_id_from_site_id(provider_uid, source_site_id)
        station_name = (
            _direct_child_value_text(element, "name")
            or _direct_child_value_text(element, "brand")
            or source_site_id
        )
        site_lat, site_lon = _coordinates(element)
        address = _address_parts(element)
        site_operator_id, site_operator_name, site_helpdesk_phone = _organisation_details(element)
        site_opening_hours = _opening_hours(element)
        site_last_updated = _direct_child_text(element, "lastUpdated")
        for station in element.iter():
            if _local_name(station) != "energyInfrastructureStation":
                continue
            source_station_ref = _text(station.attrib.get("id")) or source_site_id
            station_operator_id, station_operator_name, station_helpdesk_phone = _organisation_details(station)
            auth_methods = _datex._join_unique(
                child.text
                for child in _direct_children(station, "authenticationAndIdentificationMethods")
                if _text(child.text)
            )
            payment_methods = _payment_methods(station)
            opening_hours = site_opening_hours or _opening_hours(station)
            for refill_point in _direct_children(station, "refillPoint"):
                refill_id = _text(refill_point.attrib.get("id"))
                public_evse_id = _direct_child_text(refill_point, "externalIdentifier")
                source_evse_id = public_evse_id or refill_id
                if not source_evse_id or source_evse_id in seen_source_evse_ids:
                    continue
                seen_source_evse_ids.add(source_evse_id)
                connectors = _datex._connector_rows(refill_point)
                connector_types = _datex._join_unique(
                    connector.get("connector_type") for connector in connectors if connector.get("connector_type")
                )
                connector_formats = _datex._join_unique(
                    connector.get("connector_format") for connector in connectors if connector.get("connector_format")
                )
                current_types = _datex._join_unique(
                    connector.get("current_type") for connector in connectors if connector.get("current_type")
                )
                power_values = [
                    connector.get("max_power_kw")
                    for connector in connectors
                    if connector.get("max_power_kw") is not None
                ]
                row = {
                    "country_code": COUNTRY_CODE,
                    "source_uid": source_uid,
                    "provider_uid": provider_uid,
                    "station_id": station_id,
                    "charger_id": charger_id_from_evse_id(provider_uid, source_evse_id),
                    "source_station_id": source_site_id,
                    "source_evse_id": source_evse_id,
                    "source_evse_alias_ids": _source_evse_alias_ids(source_evse_id, refill_id),
                    "public_evse_id": public_evse_id,
                    "source_station_ref": source_station_ref,
                    "connector_id": _datex._join_unique(connector.get("connector_id") for connector in connectors),
                    "operator_name": station_operator_name or site_operator_name or station_operator_id or site_operator_id,
                    "station_name": station_name,
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
                    "payment_methods": payment_methods,
                    "opening_hours": opening_hours,
                    "green_energy": _green_energy(refill_point),
                    "helpdesk_phone": station_helpdesk_phone or site_helpdesk_phone,
                    "date_updated": _first_direct_or_nested_text(refill_point, "lastUpdated")
                    or _direct_child_text(station, "lastUpdated")
                    or site_last_updated
                    or publication_time,
                }
                row.update(_rate_price_fields(refill_point))
                yield row
        element.clear()


def iter_eco_movement_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    yield from _iter_static_rows(
        raw_stream,
        content_encoding=content_encoding,
        source_uid=LV_ECO_MOVEMENT_STATIC_SOURCE_UID,
        provider_uid=LV_ECO_MOVEMENT_PROVIDER_UID,
    )


def iter_lvc_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    yield from _iter_static_rows(
        raw_stream,
        content_encoding=content_encoding,
        source_uid=LV_LVC_EV_CHARGING_STREAM_SOURCE_UID,
        provider_uid=LV_LVC_PROVIDER_UID,
    )


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
        station_id = station_id_from_site_id(LV_ECO_MOVEMENT_PROVIDER_UID, site_reference)
        for station_status in element.iter():
            if _local_name(station_status) != "energyInfrastructureStationStatus":
                continue
            station_ref = _reference_id(station_status)
            station_available = _direct_child_text(station_status, "isAvailable")
            price_fields = _new_rates_price_fields(station_status)
            for refill_status in _direct_children(station_status, "refillPointStatus"):
                source_evse_id = _reference_id(refill_status)
                if not source_evse_id:
                    continue
                source_status = _direct_child_text(refill_status, "status").casefold() or "unknown"
                observed_at = (
                    _direct_child_text(refill_status, "lastUpdated")
                    or _direct_child_text(station_status, "lastUpdated")
                    or _direct_child_text(element, "lastUpdated")
                    or publication_time
                )
                row = {
                    "country_code": COUNTRY_CODE,
                    "source_uid": LV_ECO_MOVEMENT_STATUS_PRICE_SOURCE_UID,
                    "provider_uid": LV_ECO_MOVEMENT_PROVIDER_UID,
                    "station_id": station_id,
                    "charger_id": charger_id_from_evse_id(LV_ECO_MOVEMENT_PROVIDER_UID, source_evse_id),
                    "source_station_id": site_reference,
                    "source_station_ref": station_ref,
                    "source_evse_id": source_evse_id,
                    "source_status": source_status,
                    "availability_status": _status_from_source(source_status, station_available),
                    "source_observed_at": observed_at,
                }
                row.update(price_fields)
                yield row
        element.clear()


def _update_static_summary(element: ET.Element, summary: dict[str, Any]) -> None:
    summary["site_count"] += 1
    latitude, longitude = _coordinates(element)
    if latitude is None or longitude is None:
        summary["missing_coordinate_site_count"] += 1
    elif not (55.0 <= latitude <= 58.5 and 20.0 <= longitude <= 28.8):
        summary["outside_latvia_bbox_site_count"] += 1

    site_fast = False
    for operator in _direct_children(element, "operator"):
        operator_name = _first_text(operator, "value") or _direct_child_text(operator, "name")
        if operator_name:
            summary["_operators"][operator_name] += 1

    for station in element.iter():
        if _local_name(station) != "energyInfrastructureStation":
            continue
        summary["station_count"] += 1
        for refill_point in _direct_children(station, "refillPoint"):
            summary["refill_point_count"] += 1
            refill_id = _text(refill_point.attrib.get("id"))
            if refill_id:
                summary["_refill_point_ids"][refill_id] += 1
            external_identifier = _direct_child_text(refill_point, "externalIdentifier")
            if external_identifier:
                summary["external_evse_id_count"] += 1
                summary["_external_evse_ids"][external_identifier] += 1

            refill_fast = False
            for connector in _direct_children(refill_point, "connector"):
                summary["connector_count"] += 1
                connector_type = (
                    _direct_child_text(connector, "connectorType")
                    or _direct_child_text(connector, "connectorFormat")
                )
                if connector_type:
                    summary["_connector_types"][connector_type] += 1
                power_kw = _normalized_power_kw(_direct_child_text(connector, "maxPowerAtSocket"))
                if power_kw is not None:
                    summary["max_power_kw"] = max(summary["max_power_kw"] or power_kw, power_kw)
                    if power_kw >= 50:
                        refill_fast = True
                        site_fast = True
            if refill_fast:
                summary["fast_refill_point_count_ge_50kw"] += 1
    if site_fast:
        summary["fast_site_count_ge_50kw"] += 1


def _new_summary() -> dict[str, Any]:
    return {
        "publication_time": "",
        "site_count": 0,
        "station_count": 0,
        "refill_point_count": 0,
        "connector_count": 0,
        "fast_refill_point_count_ge_50kw": 0,
        "fast_site_count_ge_50kw": 0,
        "missing_coordinate_site_count": 0,
        "outside_latvia_bbox_site_count": 0,
        "external_evse_id_count": 0,
        "max_power_kw": None,
        "site_status_count": 0,
        "station_status_count": 0,
        "refill_point_status_count": 0,
        "rate_line_count": 0,
        "_operators": Counter(),
        "_connector_types": Counter(),
        "_refill_point_ids": Counter(),
        "_external_evse_ids": Counter(),
        "_status_values": Counter(),
        "_status_refill_point_ids": Counter(),
    }


def _finalize_summary(summary: dict[str, Any]) -> dict[str, Any]:
    refill_point_ids: Counter[str] = summary.pop("_refill_point_ids")
    external_evse_ids: Counter[str] = summary.pop("_external_evse_ids")
    status_refill_point_ids: Counter[str] = summary.pop("_status_refill_point_ids")
    operators: Counter[str] = summary.pop("_operators")
    connector_types: Counter[str] = summary.pop("_connector_types")
    status_values: Counter[str] = summary.pop("_status_values")
    summary["duplicate_refill_point_id_count"] = sum(count - 1 for count in refill_point_ids.values() if count > 1)
    summary["duplicate_external_evse_id_count"] = sum(count - 1 for count in external_evse_ids.values() if count > 1)
    summary["status_refill_point_reference_count"] = sum(status_refill_point_ids.values())
    summary["duplicate_status_refill_point_reference_count"] = sum(
        count - 1 for count in status_refill_point_ids.values() if count > 1
    )
    summary["operator_count"] = len(operators)
    summary["top_operators"] = operators.most_common(10)
    summary["top_connector_types"] = connector_types.most_common(10)
    summary["status_values"] = status_values.most_common()
    return summary


def count_xml_records_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> dict[str, Any]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    summary = _new_summary()
    for _event, element in ET.iterparse(text_stream, events=("end",)):
        local_name = _local_name(element)
        if local_name == "publicationTime" and not summary["publication_time"]:
            summary["publication_time"] = _text(element.text)
            continue
        if local_name == "energyInfrastructureSite":
            _update_static_summary(element, summary)
            element.clear()
            continue
        if local_name == "energyInfrastructureSiteStatus":
            summary["site_status_count"] += 1
            element.clear()
            continue
        if local_name == "energyInfrastructureStationStatus":
            summary["station_status_count"] += 1
            continue
        if local_name == "refillPointStatus":
            summary["refill_point_status_count"] += 1
            refill_reference = _reference_id(element)
            if refill_reference:
                summary["_status_refill_point_ids"][refill_reference] += 1
            status = _direct_child_text(element, "status")
            if status:
                summary["_status_values"][status] += 1
            continue
        if local_name == "rateLine":
            summary["rate_line_count"] += 1
    return _finalize_summary(summary)
