from __future__ import annotations

import io
import xml.etree.ElementTree as ET
from typing import Any, Iterable

from . import si_nap as _datex
from .stream_utils import buffered_stream as _buffered_stream_util

COUNTRY_CODE = "PT"

PT_NAP_STATIC_DETAIL_URL = "https://nap-portugal.imt-ip.pt/nap/multimodalsupplydetail/148"
PT_NAP_STATUS_DETAIL_URL = "https://nap-portugal.imt-ip.pt/nap/multimodalsupplydetail/149"
PT_NAP_EMEL_DETAIL_URL = "https://nap-portugal.imt-ip.pt/nap/multimodalsupplydetail/201"
PT_MOBIE_STATIC_URL = "https://pgm.mobie.pt/integration/nap/evChargingInfra"
PT_MOBIE_STATUS_URL = "https://pgm.mobie.pt/integration/nap/evActualStatus"

PT_MOBIE_STATIC_SOURCE_UID = "pt_mobie_datex_static"
PT_MOBIE_STATUS_SOURCE_UID = "pt_mobie_datex_status"
PT_MOBIE_PROVIDER_UID = "pt_mobie"

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
    "faulted": "out_of_order",
    "planned": "unknown",
    "removed": "unknown",
    "unknown": "unknown",
}


def station_id_from_site_id(source_site_id: Any) -> str:
    return f"pt:mobie:{_datex._safe_id(source_site_id)}"


def charger_id_from_evse_id(source_evse_id: Any) -> str:
    return f"pt:mobie:evse:{_datex._safe_id(source_evse_id)}"


def _status_from_source(source_status: str, is_available: str) -> str:
    normalized = _datex._text(source_status).casefold() or "unknown"
    if _datex._text(is_available).casefold() == "false" and normalized == "unknown":
        return "out_of_order"
    return STATUS_MAP.get(normalized, "unknown")


def _external_identifier(element: ET.Element) -> str:
    return _datex._direct_child_text(element, "externalIdentifier")


def _source_evse_alias_ids(source_evse_id: str, *aliases: str) -> list[str]:
    result: list[str] = []
    seen: set[str] = {source_evse_id}
    for alias in aliases:
        text = _datex._text(alias)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _pt_price_fields(refill_point: ET.Element) -> dict[str, Any]:
    result = dict(_datex._rate_price_fields(refill_point))
    quality = _datex._text(result.get("price_quality"))
    if quality:
        result["price_quality"] = quality.replace("source_si_nap", "source_pt_mobie")
    return result


def _buffered_stream(raw_stream: io.BufferedIOBase) -> io.BufferedIOBase:
    return _buffered_stream_util(raw_stream)


def iter_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _datex._text_stream_from_binary_stream(_buffered_stream(raw_stream), content_encoding=content_encoding)
    publication_time = ""
    seen_refill_ids: set[str] = set()
    for _event, element in ET.iterparse(text_stream, events=("end",)):
        local_name = _datex._local_name(element)
        if local_name == "publicationTime" and not publication_time:
            publication_time = _datex._text(element.text)
            continue
        if local_name != "energyInfrastructureSite":
            continue
        source_site_id = _datex._text(element.attrib.get("id"))
        if not source_site_id:
            element.clear()
            continue
        station_id = station_id_from_site_id(source_site_id)
        station_name = _datex._direct_child_value_text(element, "name")
        site_lat, site_lon = _datex._coordinates(element)
        address = _datex._address_parts(element)
        operator_id, operator_name = _datex._operator(element)
        opening_hours = _datex._opening_hours(element)
        vehicle_types = _datex._join_unique(
            child.text
            for child in element.iter()
            if _datex._local_name(child) == "vehicleType" and _datex._text(child.text)
        )
        for station in element.iter():
            if _datex._local_name(station) != "energyInfrastructureStation":
                continue
            source_station_ref = _datex._text(station.attrib.get("id")) or source_site_id
            auth_methods = _datex._join_unique(
                child.text
                for child in _datex._direct_children(station, "authenticationAndIdentificationMethods")
                if _datex._text(child.text)
            )
            for refill_point in _datex._direct_children(station, "refillPoint"):
                refill_id = _datex._text(refill_point.attrib.get("id"))
                public_evse_id = _external_identifier(refill_point)
                source_evse_id = public_evse_id or refill_id
                if not source_evse_id or not refill_id or refill_id in seen_refill_ids:
                    continue
                seen_refill_ids.add(refill_id)
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
                    "source_uid": PT_MOBIE_STATIC_SOURCE_UID,
                    "provider_uid": PT_MOBIE_PROVIDER_UID,
                    "station_id": station_id,
                    "charger_id": charger_id_from_evse_id(source_evse_id),
                    "source_station_id": source_site_id,
                    "source_evse_id": source_evse_id,
                    "source_evse_alias_ids": _source_evse_alias_ids(source_evse_id, refill_id),
                    "public_evse_id": public_evse_id,
                    "source_station_ref": source_station_ref,
                    "connector_id": _datex._join_unique(connector.get("connector_id") for connector in connectors),
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
                row.update(_pt_price_fields(refill_point))
                yield row
        element.clear()


def iter_status_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _datex._text_stream_from_binary_stream(_buffered_stream(raw_stream), content_encoding=content_encoding)
    publication_time = ""
    for _event, element in ET.iterparse(text_stream, events=("end",)):
        local_name = _datex._local_name(element)
        if local_name == "publicationTime" and not publication_time:
            publication_time = _datex._text(element.text)
            continue
        if local_name != "energyInfrastructureSiteStatus":
            continue
        site_reference = _datex._reference_id(element)
        if not site_reference:
            element.clear()
            continue
        station_id = station_id_from_site_id(site_reference)
        for station_status in element.iter():
            if _datex._local_name(station_status) != "energyInfrastructureStationStatus":
                continue
            station_ref = _datex._reference_id(station_status)
            station_available = _datex._direct_child_text(station_status, "isAvailable")
            for refill_status in _datex._direct_children(station_status, "refillPointStatus"):
                source_evse_id = _datex._reference_id(refill_status)
                if not source_evse_id:
                    continue
                source_status = _datex._direct_child_text(refill_status, "status").casefold() or "unknown"
                observed_at = (
                    _datex._direct_child_text(refill_status, "lastUpdated")
                    or _datex._direct_child_text(station_status, "lastUpdated")
                    or publication_time
                )
                yield {
                    "country_code": COUNTRY_CODE,
                    "source_uid": PT_MOBIE_STATUS_SOURCE_UID,
                    "provider_uid": PT_MOBIE_PROVIDER_UID,
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


def count_xml_records_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> dict[str, Any]:
    return _datex.count_xml_records_from_binary_stream(_buffered_stream(raw_stream), content_encoding=content_encoding)
