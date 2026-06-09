from __future__ import annotations

import io
import json
import xml.etree.ElementTree as ET
from typing import Any, Iterable

from . import si_nap as _datex
from .stream_utils import buffered_stream as _buffered_stream_util

COUNTRY_CODE = "HU"

HU_NAP_PORTAL_URL = "https://napportal.kozut.hu/"
HU_NAP_API_BASE_URL = "https://napportal.kozut.hu/napp-portal-proxy"
HU_NAP_TOKEN_URL = f"{HU_NAP_API_BASE_URL}/api/token"
HU_NAP_CONTRACTED_PROFILE_SUMMARIES_URL = (
    f"{HU_NAP_API_BASE_URL}/api/ServiceProviderProfile/me/GetContractedProfileSummaries"
)

HU_NAP_ECO_MOVEMENT_STATIC_PROFILE_ID = 193
HU_NAP_MOBILITI_STATIC_PROFILE_ID = 199
HU_NAP_AMPECO_TEST_STATIC_PROFILE_ID = 201
HU_NAP_AMPECO_TEST_DYNAMIC_PROFILE_ID = 202
HU_NAP_MAGYAR_KOZUT_PARKING_CHARGING_STATIC_PROFILE_ID = 28

HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID = "hu_nap_eco_movement_static"
HU_NAP_MOBILITI_STATIC_SOURCE_UID = "hu_nap_mobiliti_static"
HU_NAP_AMPECO_TEST_STATIC_SOURCE_UID = "hu_nap_ampeco_test_static"
HU_NAP_AMPECO_TEST_DYNAMIC_SOURCE_UID = "hu_nap_ampeco_test_dynamic"
HU_NAP_MAGYAR_KOZUT_PARKING_CHARGING_STATIC_SOURCE_UID = "hu_nap_magyar_kozut_parking_charging_static"

HU_NAP_ECO_MOVEMENT_PROVIDER_UID = "hu_nap_eco_movement"
HU_NAP_MOBILITI_PROVIDER_UID = "hu_nap_mobiliti"
HU_NAP_AMPECO_TEST_PROVIDER_UID = "hu_nap_ampeco_test"
HU_NAP_MAGYAR_KOZUT_PROVIDER_UID = "hu_nap_magyar_kozut"

STATIC_SOURCE_PROVIDER_UIDS = {
    HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID: HU_NAP_ECO_MOVEMENT_PROVIDER_UID,
    HU_NAP_MOBILITI_STATIC_SOURCE_UID: HU_NAP_MOBILITI_PROVIDER_UID,
    HU_NAP_AMPECO_TEST_STATIC_SOURCE_UID: HU_NAP_AMPECO_TEST_PROVIDER_UID,
    HU_NAP_MAGYAR_KOZUT_PARKING_CHARGING_STATIC_SOURCE_UID: HU_NAP_MAGYAR_KOZUT_PROVIDER_UID,
}

ALL_SOURCE_PROVIDER_UIDS = {
    **STATIC_SOURCE_PROVIDER_UIDS,
    HU_NAP_AMPECO_TEST_DYNAMIC_SOURCE_UID: HU_NAP_AMPECO_TEST_PROVIDER_UID,
}

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


def _provider_key(provider_uid: str) -> str:
    return provider_uid.removeprefix("hu_nap_").replace("_", "-") or "nap"


def station_id_from_site_id(provider_uid: str, source_site_id: Any) -> str:
    return f"hu:nap:{_provider_key(provider_uid)}:{_datex._safe_id(source_site_id)}"


def charger_id_from_evse_id(provider_uid: str, source_evse_id: Any) -> str:
    return f"hu:nap:{_provider_key(provider_uid)}:evse:{_datex._safe_id(source_evse_id)}"


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


def _external_identifier(element: ET.Element) -> str:
    return _datex._direct_child_text(element, "externalIdentifier")


def _first_direct_or_nested_text(parent: ET.Element, name: str) -> str:
    return _datex._direct_child_text(parent, name) or _datex._first_text(parent, name)


def _operator_details(parent: ET.Element) -> tuple[str, str, str]:
    operator_id, operator_name = _datex._operator(parent)
    phone = ""
    for child in parent:
        if _datex._local_name(child) != "operator":
            continue
        phone = _datex._first_text(child, "telephoneNumber")
        break
    return operator_id, operator_name, phone


def _green_energy(refill_point: ET.Element) -> str:
    values = {
        _datex._text(child.text).casefold()
        for child in refill_point.iter()
        if _datex._local_name(child) == "isGreenEnergy" and _datex._text(child.text)
    }
    if not values:
        return ""
    if values == {"true"}:
        return "true"
    if values == {"false"}:
        return "false"
    return "|".join(sorted(values))


def _huf_amount(value: float) -> str:
    text = f"{float(value):.2f}".rstrip("0").rstrip(".")
    return text


def _site_pricing_fields(
    site: ET.Element,
    refill_point: ET.Element,
    connectors: list[dict[str, Any]],
) -> dict[str, Any]:
    result = dict(_datex._rate_price_fields(refill_point))
    quality = _datex._text(result.get("price_quality"))
    if quality:
        result["price_quality"] = quality.replace("source_si_nap", "source_hu_nap")
    if result.get("price_display"):
        return result

    currency = ""
    source_rows: list[dict[str, Any]] = []
    prices: dict[str, float] = {}
    bases: dict[str, str] = {}
    for rates in _datex._direct_children(site, "rates"):
        currency = currency or _datex._direct_child_text(rates, "applicableCurrency").upper()
        for name, key in (
            ("dcChargerPrice", "dc"),
            ("acChargerPrice", "ac"),
            ("rapidChargerPrice", "rapid"),
        ):
            value = _datex._float_or_none(_datex._first_text(rates, name))
            if value is not None:
                prices[key] = value
                source_rows.append({"field": name, "value": value, "currency": currency})
        for name, key in (
            ("dcChargerBaseOfCalculation", "dc"),
            ("acChargerBaseOfCalculation", "ac"),
            ("rapidChargerBaseOfCalculation", "rapid"),
        ):
            value = _datex._first_text(rates, name).upper()
            if value:
                bases[key] = value
                source_rows.append({"field": name, "value": value})
        for name in ("prepayAmount", "registrationCharge", "guestCommission"):
            value = _datex._float_or_none(_datex._first_text(rates, name))
            if value is not None:
                source_rows.append({"field": name, "value": value, "currency": currency})

    if not prices:
        return result

    current_types = {
        _datex._text(connector.get("current_type")).upper()
        for connector in connectors
        if _datex._text(connector.get("current_type"))
    }
    selected: list[float] = []
    if "DC" in current_types:
        selected.extend(prices[key] for key in ("dc", "rapid") if key in prices and bases.get(key, "KWH") == "KWH")
    if "AC" in current_types:
        selected.extend(prices[key] for key in ("ac",) if key in prices and bases.get(key, "KWH") == "KWH")
    if not selected:
        selected = [value for key, value in prices.items() if bases.get(key, "KWH") == "KWH"]
    if not selected:
        return {**result, "price_source_text": _datex._compact_json(source_rows)}

    unique_values = sorted(dict.fromkeys(selected))
    if len(unique_values) == 1:
        display = f"{_huf_amount(unique_values[0])} {currency or 'HUF'}/kWh"
    else:
        display = f"{_huf_amount(unique_values[0])}-{_huf_amount(unique_values[-1])} {currency or 'HUF'}/kWh"
    return {
        **result,
        "price_display": display,
        "price_currency": currency or "HUF",
        "price_energy_eur_kwh_min": "",
        "price_energy_eur_kwh_max": "",
        "price_quality": "source_hu_nap_datex_pricing_details_huf",
        "price_complex": len(source_rows) > len(prices) + len(bases),
        "price_source_text": _datex._compact_json(source_rows),
    }


def _buffered_stream(raw_stream: io.BufferedIOBase) -> io.BufferedIOBase:
    return _buffered_stream_util(raw_stream)


def _status_from_source(source_status: str, is_available: str) -> str:
    normalized = _datex._text(source_status).casefold() or "unknown"
    if _datex._text(is_available).casefold() == "false" and normalized == "unknown":
        return "out_of_order"
    return STATUS_MAP.get(normalized, "unknown")


def _iter_static_rows(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
    source_uid: str,
    provider_uid: str,
) -> Iterable[dict[str, Any]]:
    text_stream = _datex._text_stream_from_binary_stream(_buffered_stream(raw_stream), content_encoding=content_encoding)
    publication_time = ""
    seen_evse_ids: set[str] = set()
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
        station_id = station_id_from_site_id(provider_uid, source_site_id)
        station_name = _datex._direct_child_value_text(element, "name")
        site_lat, site_lon = _datex._coordinates(element)
        address = _datex._address_parts(element)
        site_operator_id, site_operator_name, site_helpdesk_phone = _operator_details(element)
        site_opening_hours = _datex._opening_hours(element)
        site_last_updated = _datex._direct_child_text(element, "lastUpdated")
        vehicle_types = _datex._join_unique(
            child.text
            for child in element.iter()
            if _datex._local_name(child) == "vehicleType" and _datex._text(child.text)
        )
        for station in element.iter():
            if _datex._local_name(station) != "energyInfrastructureStation":
                continue
            source_station_ref = _datex._text(station.attrib.get("id")) or source_site_id
            station_operator_id, station_operator_name, station_helpdesk_phone = _operator_details(station)
            auth_methods = _datex._join_unique(
                child.text
                for child in _datex._direct_children(station, "authenticationAndIdentificationMethods")
                if _datex._text(child.text)
            )
            opening_hours = site_opening_hours or _datex._opening_hours(station)
            for refill_point in _datex._direct_children(station, "refillPoint"):
                refill_id = _datex._text(refill_point.attrib.get("id"))
                public_evse_id = _external_identifier(refill_point)
                source_evse_id = public_evse_id or refill_id
                if not source_evse_id or not refill_id or source_evse_id in seen_evse_ids:
                    continue
                seen_evse_ids.add(source_evse_id)
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
                    "green_energy": _green_energy(refill_point),
                    "helpdesk_phone": station_helpdesk_phone or site_helpdesk_phone,
                    "date_updated": _first_direct_or_nested_text(refill_point, "lastUpdated")
                    or _datex._direct_child_text(station, "lastUpdated")
                    or site_last_updated
                    or publication_time,
                }
                row.update(_site_pricing_fields(element, refill_point, connectors))
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
        source_uid=HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID,
        provider_uid=HU_NAP_ECO_MOVEMENT_PROVIDER_UID,
    )


def iter_mobiliti_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    yield from _iter_static_rows(
        raw_stream,
        content_encoding=content_encoding,
        source_uid=HU_NAP_MOBILITI_STATIC_SOURCE_UID,
        provider_uid=HU_NAP_MOBILITI_PROVIDER_UID,
    )


def iter_ampeco_test_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    yield from _iter_static_rows(
        raw_stream,
        content_encoding=content_encoding,
        source_uid=HU_NAP_AMPECO_TEST_STATIC_SOURCE_UID,
        provider_uid=HU_NAP_AMPECO_TEST_PROVIDER_UID,
    )


def iter_magyar_kozut_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    yield from _iter_static_rows(
        raw_stream,
        content_encoding=content_encoding,
        source_uid=HU_NAP_MAGYAR_KOZUT_PARKING_CHARGING_STATIC_SOURCE_UID,
        provider_uid=HU_NAP_MAGYAR_KOZUT_PROVIDER_UID,
    )


def iter_status_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
    source_uid: str = HU_NAP_AMPECO_TEST_DYNAMIC_SOURCE_UID,
    provider_uid: str = HU_NAP_AMPECO_TEST_PROVIDER_UID,
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
        station_id = station_id_from_site_id(provider_uid, site_reference)
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
                    "source_uid": source_uid,
                    "provider_uid": provider_uid,
                    "station_id": station_id,
                    "charger_id": charger_id_from_evse_id(provider_uid, source_evse_id),
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


def source_uid_for_profile_id(profile_id: int) -> str:
    return {
        HU_NAP_ECO_MOVEMENT_STATIC_PROFILE_ID: HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID,
        HU_NAP_MOBILITI_STATIC_PROFILE_ID: HU_NAP_MOBILITI_STATIC_SOURCE_UID,
        HU_NAP_AMPECO_TEST_STATIC_PROFILE_ID: HU_NAP_AMPECO_TEST_STATIC_SOURCE_UID,
        HU_NAP_AMPECO_TEST_DYNAMIC_PROFILE_ID: HU_NAP_AMPECO_TEST_DYNAMIC_SOURCE_UID,
        HU_NAP_MAGYAR_KOZUT_PARKING_CHARGING_STATIC_PROFILE_ID: HU_NAP_MAGYAR_KOZUT_PARKING_CHARGING_STATIC_SOURCE_UID,
    }.get(int(profile_id), "")


def provider_uid_for_source_uid(source_uid: str) -> str:
    return ALL_SOURCE_PROVIDER_UIDS.get(source_uid, "")


def metadata_summary_from_subscription(profile: dict[str, Any]) -> dict[str, Any]:
    access = (profile.get("dataAccesses") or [{}])[0] if isinstance(profile.get("dataAccesses"), list) else {}
    return {
        "contract_partner_id": profile.get("contractPartnerId"),
        "profile_id": profile.get("serviceProviderUserProfileId"),
        "source_uid": source_uid_for_profile_id(int(profile.get("serviceProviderUserProfileId") or 0)),
        "profile_name": _datex._text(profile.get("profileName")),
        "provider": _datex._text(profile.get("contactCompany")),
        "data_type": _datex._text(profile.get("dataTypeName")),
        "categories": profile.get("dataCategoryNames") or [],
        "format": _datex._text(access.get("profileDataFormatName")),
        "has_url": bool(_datex._text(access.get("url"))),
    }


def compact_count_summary(rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    row_list = list(rows)
    station_ids = {_datex._text(row.get("station_id")) for row in row_list if _datex._text(row.get("station_id"))}
    fast_station_ids = {
        _datex._text(row.get("station_id"))
        for row in row_list
        if (_datex._float_or_none(row.get("max_power_kw")) or 0) >= 50 and _datex._text(row.get("station_id"))
    }
    return {
        "row_count": len(row_list),
        "station_count": len(station_ids),
        "fast_station_count": len(fast_station_ids),
    }
