from __future__ import annotations

import io
import json
from typing import Any, Iterable

from .stream_utils import text_stream_from_binary_stream as _text_stream_util

COUNTRY_CODE = "AT"

MOBILITYDATA_DATASET_URL = (
    "https://mobilitaetsdaten.gv.at/en/daten/"
    "ladestellenverzeichnis-der-e-control-nationales-ladepunkteregister"
)
ECONTROL_TECHNICAL_INFO_URL = "https://www.e-control.at/ladestellenverzeichnis-technische-informationen"
ECONTROL_PUBLIC_API_DOCS_URL = "https://api.e-control.at/charge/1.0/v2/api-docs?group=public-api"
ECONTROL_PUBLIC_API_SEARCH_URL = "https://api.e-control.at/charge/1.0/search?latitude=48&longitude=16"
ECONTROL_PUBLIC_API_DATEX_TABLE_URL = (
    "https://api.e-control.at/charge/1.0/datex2/v3.5/energy-infrastructure-table-publication"
)
ECONTROL_PUBLIC_API_DATEX_STATUS_URL = (
    "https://api.e-control.at/charge/1.0/datex2/v3.5/energy-infrastructure-status-publication"
)

MOBILITYDATA_DATASET_SOURCE_UID = "at_econtrol_mobilitydata_dataset_page"
ECONTROL_TECHNICAL_INFO_SOURCE_UID = "at_econtrol_technical_info_page"
ECONTROL_PUBLIC_API_DOCS_SOURCE_UID = "at_econtrol_public_api_docs"
ECONTROL_PUBLIC_API_SEARCH_SOURCE_UID = "at_econtrol_public_api_search"
ECONTROL_PUBLIC_API_DATEX_TABLE_SOURCE_UID = "at_econtrol_public_api_datex_table"
ECONTROL_PUBLIC_API_DATEX_STATUS_SOURCE_UID = "at_econtrol_public_api_datex_status"
ECONTROL_PROVIDER_UID = "at_econtrol_public_api"

STATUS_MAP = {
    "AVAILABLE": "free",
    "OCCUPIED": "occupied",
    "CHARGING": "occupied",
    "RESERVED": "reserved",
    "INOPERATIVE": "out_of_order",
    "UNAVAILABLE": "out_of_order",
    "OUTOFORDER": "out_of_order",
    "OUT_OF_ORDER": "out_of_order",
    "OUT_OF_SERVICE": "out_of_order",
    "FAULTED": "out_of_order",
    "BLOCKED": "occupied",
    "PLANNED": "unknown",
    "UNKNOWN": "unknown",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _safe_id(value: Any) -> str:
    text = _text(value).lower()
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-", "*"} else "-" for ch in text)
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "unknown"


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _text(value).casefold() in {"1", "true", "yes", "y", "free"}


def _price_scalar(value: float) -> str:
    return f"{float(value):.6f}".rstrip("0").rstrip(".")


def _euro_amount(value: float) -> str:
    return f"{round(float(value) + 0.000000001, 2):.2f}".replace(".", ",")


def _cent_value(value: Any) -> float | None:
    numeric = _float_or_none(value)
    if numeric is None:
        return None
    return numeric / 100.0


def _currency_code(value: Any) -> str:
    text = _text(value)
    normalized = text.casefold()
    if normalized in {"eur", "euro", "euros"}:
        return "EUR"
    return text.upper()


def _enum_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_enum_value(item) for item in value if _enum_value(item)]
    if isinstance(value, dict):
        values = value.get("values")
        if isinstance(values, dict) and isinstance(values.get("value"), list):
            return [_enum_value(item) for item in values["value"] if _enum_value(item)]
        text = _enum_value(value)
        return [text] if text else []
    text = _enum_value(value)
    return [text] if text else []


def _price_fields_from_values(
    *,
    energy_values: list[float],
    minute_values: list[float],
    fixed_values: list[float] | None = None,
    free: bool = False,
    currency: str = "EUR",
    quality: str,
    source_text: str = "",
) -> dict[str, Any]:
    fixed_values = fixed_values or []
    complex_tariff = bool(minute_values or fixed_values)
    if free and not energy_values and not minute_values:
        return {
            "price_display": "gratis",
            "price_currency": "EUR",
            "price_energy_eur_kwh_min": "0",
            "price_energy_eur_kwh_max": "0",
            "price_time_eur_min_min": None,
            "price_time_eur_min_max": None,
            "price_quality": f"{quality}_free",
            "price_complex": False,
            "price_source_text": source_text,
        }
    if not energy_values and not minute_values:
        return {}

    energy_min = min(energy_values) if energy_values else None
    energy_max = max(energy_values) if energy_values else None
    minute_min = min(minute_values) if minute_values else None
    minute_max = max(minute_values) if minute_values else None
    display = ""
    if currency == "EUR" and energy_min is not None:
        if complex_tariff:
            display = f"ab {_euro_amount(energy_min)} €/kWh"
        elif energy_max is not None and abs(energy_min - energy_max) >= 0.000001:
            display = f"{_euro_amount(energy_min)}-{_euro_amount(energy_max)} €/kWh"
        else:
            display = f"{_euro_amount(energy_min)} €/kWh"
    elif currency == "EUR" and minute_min is not None:
        if complex_tariff or (minute_max is not None and abs(minute_min - minute_max) >= 0.000001):
            display = f"ab {_euro_amount(minute_min)} €/min"
        else:
            display = f"{_euro_amount(minute_min)} €/min"

    return {
        "price_display": display,
        "price_currency": currency,
        "price_energy_eur_kwh_min": _price_scalar(energy_min) if energy_min is not None and currency == "EUR" else "",
        "price_energy_eur_kwh_max": _price_scalar(energy_max) if energy_max is not None and currency == "EUR" else "",
        "price_time_eur_min_min": round(minute_min, 6) if minute_min is not None and currency == "EUR" else None,
        "price_time_eur_min_max": round(minute_max, 6) if minute_max is not None and currency == "EUR" else None,
        "price_quality": f"{quality}_complex" if complex_tariff else quality,
        "price_complex": complex_tariff,
        "price_source_text": source_text,
    }


def _api_search_price_fields(point: dict[str, Any]) -> dict[str, Any]:
    energy = _cent_value(point.get("priceCentKwh"))
    minute = _cent_value(point.get("priceCentMin"))
    blocking = _cent_value(point.get("blockingFeeCentMin"))
    start = _cent_value(point.get("startFeeCent"))
    source = {
        key: point.get(key)
        for key in (
            "freeOfCharge",
            "priceCentKwh",
            "priceCentMin",
            "blockingFeeCentMin",
            "blockingFeeFromMinute",
            "startFeeCent",
        )
        if key in point
    }
    return _price_fields_from_values(
        energy_values=[energy] if energy is not None else [],
        minute_values=[value for value in (minute, blocking) if value not in (None, 0.0)],
        fixed_values=[start] if start not in (None, 0.0) else [],
        free=_truthy(point.get("freeOfCharge")),
        quality="source_at_econtrol_api_search",
        source_text=_json_text(source),
    )


def _datex_new_rates_price_fields(value: dict[str, Any]) -> dict[str, Any]:
    rates = value.get("newRates") if isinstance(value.get("newRates"), dict) else value
    if not isinstance(rates, dict):
        return {}
    policies = [
        item.upper()
        for item in _enum_values(_dict_value(rates, "energyPricingPolicy").get("pricingPolicy"))
    ]
    energy_values: list[float] = []
    minute_values: list[float] = []
    currencies = [_currency_code(item) for item in _enum_values(rates.get("applicableCurrency"))]
    complex_tariff = False
    for collection in rates.get("rateLineCollection") or []:
        if not isinstance(collection, dict):
            continue
        currency = _currency_code(collection.get("applicableCurrency"))
        if currency:
            currencies.append(currency)
        for rate_line in collection.get("rateLine") or []:
            if not isinstance(rate_line, dict):
                continue
            numeric = _float_or_none(rate_line.get("value"))
            if numeric is None:
                continue
            line_type = _enum_value(rate_line.get("rateLineType")).upper()
            increment = _text(rate_line.get("incrementPeriod")).upper()
            if line_type == "PER_UNIT":
                energy_values.append(numeric)
            elif line_type in {"INCREMENTING_RATE", "PER_MINUTE"} or increment == "PT1M":
                minute_values.append(numeric)
            else:
                complex_tariff = True
    fields = _price_fields_from_values(
        energy_values=energy_values,
        minute_values=minute_values,
        free="FREE" in policies,
        currency=next((item for item in currencies if item), "EUR" if energy_values or minute_values else ""),
        quality="source_at_econtrol_datex_new_rates",
        source_text=_json_text(rates),
    )
    if fields and complex_tariff:
        fields["price_complex"] = True
        fields["price_quality"] = "source_at_econtrol_datex_new_rates_complex"
    return fields


def _choose_price_fields(*candidates: dict[str, Any]) -> dict[str, Any]:
    for candidate in reversed(candidates):
        if candidate and (
            _text(candidate.get("price_display"))
            or _text(candidate.get("price_energy_eur_kwh_min"))
            or candidate.get("price_time_eur_min_min") is not None
        ):
            return candidate
    return {}


def _json_text(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _phone_text(*parts: Any) -> str:
    cleaned_parts = [_text(part) for part in parts if _text(part)]
    if cleaned_parts and cleaned_parts[0].startswith("+") and len(cleaned_parts) > 1:
        cleaned_parts[1] = cleaned_parts[1].lstrip("0")
    text = "".join(cleaned_parts)
    return text.replace(" ", "")


def _list_value(value: Any, *keys: str) -> list[Any]:
    for key in keys:
        child = value.get(key) if isinstance(value, dict) else None
        if isinstance(child, list):
            return child
    return []


def _dict_value(value: Any, *keys: str) -> dict[str, Any]:
    for key in keys:
        child = value.get(key) if isinstance(value, dict) else None
        if isinstance(child, dict):
            return child
    return {}


def _enum_value(value: Any) -> str:
    if isinstance(value, dict):
        return _text(value.get("value"))
    return _text(value)


def _localized_text(value: Any) -> str:
    if isinstance(value, dict):
        values = value.get("values")
        if isinstance(values, dict):
            for item in values.get("value") or []:
                if isinstance(item, dict):
                    text = _text(item.get("value"))
                    if text:
                        return text
        text = _text(value.get("value"))
        if text:
            return text
    if isinstance(value, list):
        for item in value:
            text = _localized_text(item)
            if text:
                return text
    return _text(value)


def _location(value: Any) -> tuple[float | None, float | None]:
    if not isinstance(value, dict):
        return None, None
    return _float_or_none(value.get("lat")), _float_or_none(value.get("lon"))


def _datex_coordinates(value: Any) -> tuple[float | None, float | None]:
    if not isinstance(value, dict):
        return None, None
    for candidate in (
        value.get("coordinatesForDisplay"),
        _dict_value(value.get("pointByCoordinates"), "pointCoordinates"),
    ):
        if isinstance(candidate, dict):
            latitude = _float_or_none(candidate.get("latitude"))
            longitude = _float_or_none(candidate.get("longitude"))
            if latitude is not None and longitude is not None:
                return latitude, longitude
    return None, None


def _join_unique(values: Iterable[Any]) -> str:
    seen: dict[str, None] = {}
    for value in values:
        text = _text(value)
        if text:
            seen.setdefault(text, None)
    return "|".join(seen.keys())


def station_id_from_source_id(source_station_id: Any) -> str:
    return f"at:econtrol:{_safe_id(source_station_id)}"


def charger_id_from_evse_id(source_evse_id: Any) -> str:
    return f"at:econtrol:evse:{_safe_id(source_evse_id)}"


def _datex_reference_id(value: Any) -> str:
    if isinstance(value, dict):
        return _text(value.get("id"))
    return ""


def _datex_evse_id(*, site_id: str, station_id: str, refill_point_id: str = "") -> str:
    if site_id and station_id.startswith(f"{site_id}-"):
        return station_id[len(site_id) + 1 :]
    refill_without_suffix = refill_point_id[:-3] if refill_point_id.endswith("-rp") else refill_point_id
    if site_id and refill_without_suffix.startswith(f"{site_id}-"):
        return refill_without_suffix[len(site_id) + 1 :]
    return station_id or refill_without_suffix


def _datex_address(location_reference: dict[str, Any]) -> tuple[str, str, str]:
    extension = _dict_value(location_reference, "_LocationReferenceExtension")
    facility_location = _dict_value(extension, "facilityLocation")
    address = _dict_value(facility_location, "address")
    postal_code = _text(address.get("postcode"))
    city = _localized_text(address.get("city"))
    address_lines = []
    for item in facility_location.get("addressLine") or []:
        if not isinstance(item, dict):
            continue
        address_lines.append(
            (
                int(item.get("order") or 0),
                _localized_text(item.get("text")),
            )
        )
    street = " ".join(text for _order, text in sorted(address_lines) if text)
    return street, postal_code, city


def _datex_organisation_name(value: Any) -> str:
    return _localized_text(_dict_value(value, "name")) or _text(value.get("id") if isinstance(value, dict) else "")


def _datex_connector_type(connector: dict[str, Any]) -> str:
    return _enum_value(connector.get("connectorType"))


def _datex_max_power_kw(connectors: Iterable[dict[str, Any]]) -> float | None:
    values = []
    for connector in connectors:
        power = _float_or_none(connector.get("maxPowerAtSocket"))
        if power is None or power <= 0:
            continue
        values.append(power / 1000.0 if power > 1000 else power)
    return max(values) if values else None


def _text_stream_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str,
) -> io.TextIOWrapper:
    return _text_stream_util(raw_stream, content_encoding=content_encoding)


def iter_api_search_station_rows(payload: Any) -> Iterable[dict[str, Any]]:
    stations = payload if isinstance(payload, list) else []
    for station in stations:
        if not isinstance(station, dict):
            continue
        source_station_id = _text(station.get("stationId"))
        if not source_station_id:
            continue
        station_id = station_id_from_source_id(source_station_id)
        station_lat, station_lon = _location(station.get("location"))
        points = [point for point in station.get("points") or [] if isinstance(point, dict)]
        for point in points:
            source_evse_id = _text(point.get("evseId"))
            if not source_evse_id:
                continue
            point_lat, point_lon = _location(point.get("location"))
            connector_types = [
                _text(connector.get("consumerName") or connector.get("key") or connector.get("description"))
                for connector in point.get("connectorType") or []
                if isinstance(connector, dict)
            ]
            row = {
                "country_code": COUNTRY_CODE,
                "source_uid": ECONTROL_PUBLIC_API_SEARCH_SOURCE_UID,
                "provider_uid": ECONTROL_PROVIDER_UID,
                "station_id": station_id,
                "charger_id": charger_id_from_evse_id(source_evse_id),
                "source_station_id": source_station_id,
                "source_evse_id": source_evse_id,
                "operator_name": _text(station.get("operatorName")) or _text(station.get("contactName")),
                "station_name": _text(station.get("label")),
                "address": _join_unique([station.get("street")]),
                "city": _text(station.get("city")),
                "postal_code": _text(station.get("postCode")),
                "latitude": point_lat if point_lat is not None else station_lat,
                "longitude": point_lon if point_lon is not None else station_lon,
                "connector_count": len(connector_types) or 1,
                "connector_types": _join_unique(connector_types),
                "current_type": _join_unique(point.get("electricityType") or []),
                "max_power_kw": _float_or_none(point.get("capacityKw")),
                "payment_methods": _join_unique(point.get("electronicPaymentProvider") or []),
                "auth_methods": _join_unique(point.get("authenticationMode") or []),
                "green_energy": station.get("greenEnergy"),
                "opening_hours": _json_text(station.get("openingHours")),
                "helpdesk_phone": _phone_text(
                    station.get("phoneCountryCode"),
                    station.get("regionCode"),
                    station.get("phoneNumber"),
                ),
            }
            row.update(_api_search_price_fields(point))
            yield row


def iter_api_search_status_rows(payload: Any) -> Iterable[dict[str, Any]]:
    stations = payload if isinstance(payload, list) else []
    for station in stations:
        if not isinstance(station, dict):
            continue
        source_station_id = _text(station.get("stationId"))
        if not source_station_id:
            continue
        station_id = station_id_from_source_id(source_station_id)
        for point in station.get("points") or []:
            if not isinstance(point, dict):
                continue
            source_evse_id = _text(point.get("evseId"))
            if not source_evse_id:
                continue
            source_status = _text(point.get("status")).upper() or _text(station.get("stationStatus")).upper()
            row = {
                "country_code": COUNTRY_CODE,
                "source_uid": ECONTROL_PUBLIC_API_SEARCH_SOURCE_UID,
                "provider_uid": ECONTROL_PROVIDER_UID,
                "station_id": station_id,
                "charger_id": charger_id_from_evse_id(source_evse_id),
                "source_station_id": source_station_id,
                "source_evse_id": source_evse_id,
                "source_status": source_status or "UNKNOWN",
                "availability_status": STATUS_MAP.get(source_status, "unknown"),
                "source_observed_at": "",
            }
            row.update(_api_search_price_fields(point))
            yield row


def iter_datex_table_station_rows(payload: Any) -> Iterable[dict[str, Any]]:
    for table in payload.get("energyInfrastructureTable") or []:
        if not isinstance(table, dict):
            continue
        for site in table.get("energyInfrastructureSite") or []:
            if not isinstance(site, dict):
                continue
            source_station_id = _text(site.get("id"))
            if not source_station_id:
                continue
            site_location = _dict_value(site, "locationReference")
            site_lat, site_lon = _datex_coordinates(site_location)
            site_address, site_postal_code, site_city = _datex_address(site_location)
            operator_name = _datex_organisation_name(site.get("operator")) or _datex_organisation_name(site.get("owner"))
            station_name = _localized_text(site.get("name"))
            site_description = _localized_text(site.get("description"))
            for station in site.get("energyInfrastructureStation") or []:
                if not isinstance(station, dict):
                    continue
                source_station_ref = _text(station.get("id"))
                if not source_station_ref:
                    continue
                refill_points = [item for item in station.get("refillPoint") or [] if isinstance(item, dict)]
                if not refill_points:
                    refill_points = [{}]
                station_location = _dict_value(station, "locationReference")
                station_lat, station_lon = _datex_coordinates(station_location)
                for refill_point in refill_points:
                    refill_point_id = _text(refill_point.get("id"))
                    source_evse_id = _datex_evse_id(
                        site_id=source_station_id,
                        station_id=source_station_ref,
                        refill_point_id=refill_point_id,
                    )
                    if not source_evse_id:
                        continue
                    connectors = [
                        connector
                        for connector in refill_point.get("connector") or []
                        if isinstance(connector, dict)
                    ]
                    connector_types = [
                        _datex_connector_type(connector)
                        for connector in connectors
                        if _datex_connector_type(connector)
                    ]
                    auth_methods = [
                        _enum_value(item)
                        for item in station.get("authenticationAndIdentificationMethods") or []
                        if _enum_value(item)
                    ]
                    yield {
                        "country_code": COUNTRY_CODE,
                        "source_uid": ECONTROL_PUBLIC_API_DATEX_TABLE_SOURCE_UID,
                        "provider_uid": ECONTROL_PROVIDER_UID,
                        "station_id": station_id_from_source_id(source_station_id),
                        "charger_id": charger_id_from_evse_id(source_evse_id),
                        "source_station_id": source_station_id,
                        "source_evse_id": source_evse_id,
                        "source_station_ref": source_station_ref,
                        "connector_id": refill_point_id,
                        "operator_name": operator_name,
                        "station_name": station_name or source_station_id,
                        "description": site_description,
                        "address": site_address,
                        "city": site_city,
                        "postal_code": site_postal_code,
                        "latitude": station_lat if station_lat is not None else site_lat,
                        "longitude": station_lon if station_lon is not None else site_lon,
                        "connector_count": len(connectors) or 1,
                        "connector_types": _join_unique(connector_types),
                        "current_type": _enum_value(refill_point.get("deliveryUnit")),
                        "max_power_kw": _datex_max_power_kw(connectors),
                        "auth_methods": _join_unique(auth_methods),
                        "green_energy": _json_text(refill_point.get("electricEnergyMix")),
                        "opening_hours": _json_text(site.get("openingHours")),
                        "date_updated": _text(station.get("lastUpdated") or site.get("lastUpdated")),
                    }


def iter_datex_status_rows(payload: Any) -> Iterable[dict[str, Any]]:
    publication_time = _text(payload.get("publicationTime"))
    for site in payload.get("energyInfrastructureSiteStatus") or []:
        if not isinstance(site, dict):
            continue
        source_station_id = _datex_reference_id(site.get("reference"))
        if not source_station_id:
            continue
        site_price = _datex_new_rates_price_fields(site)
        for station in site.get("energyInfrastructureStationStatus") or []:
            if not isinstance(station, dict):
                continue
            source_station_ref = _datex_reference_id(station.get("reference"))
            station_price = _choose_price_fields(site_price, _datex_new_rates_price_fields(station))
            for refill_point in station.get("refillPointStatus") or []:
                if not isinstance(refill_point, dict):
                    continue
                source_evse_id = _datex_evse_id(
                    site_id=source_station_id,
                    station_id=source_station_ref,
                    refill_point_id=_datex_reference_id(refill_point.get("reference")),
                )
                if not source_evse_id:
                    continue
                source_status = _enum_value(refill_point.get("status")).upper()
                row = {
                    "country_code": COUNTRY_CODE,
                    "source_uid": ECONTROL_PUBLIC_API_DATEX_STATUS_SOURCE_UID,
                    "provider_uid": ECONTROL_PROVIDER_UID,
                    "station_id": station_id_from_source_id(source_station_id),
                    "charger_id": charger_id_from_evse_id(source_evse_id),
                    "source_station_id": source_station_id,
                    "source_evse_id": source_evse_id,
                    "source_status": source_status or "UNKNOWN",
                    "availability_status": STATUS_MAP.get(source_status, "unknown"),
                    "source_observed_at": _text(
                        refill_point.get("lastUpdated")
                        or station.get("lastUpdated")
                        or site.get("lastUpdated")
                        or publication_time
                    ),
                }
                row.update(_choose_price_fields(station_price, _datex_new_rates_price_fields(refill_point)))
                yield row


def iter_api_search_station_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        yield from iter_api_search_station_rows(json.load(text_stream))
    finally:
        text_stream.detach()


def iter_api_search_status_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        yield from iter_api_search_status_rows(json.load(text_stream))
    finally:
        text_stream.detach()


def iter_datex_table_station_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        yield from iter_datex_table_station_rows(json.load(text_stream))
    finally:
        text_stream.detach()


def iter_datex_status_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        yield from iter_datex_status_rows(json.load(text_stream))
    finally:
        text_stream.detach()
