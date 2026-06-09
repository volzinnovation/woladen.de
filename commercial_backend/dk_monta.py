from __future__ import annotations

import io
import json
from dataclasses import dataclass
from typing import Any, Iterable, TextIO

from .stream_utils import text_stream_from_binary_stream as _text_stream_util

COUNTRY_CODE = "DK"
MONTA_PROVIDER_UID = "dk_monta"
BE_COUNTRY_CODE = "BE"
BE_MONTA_PROVIDER_UID = "be_monta"

MONTA_PUBLIC_AUTH_TOKEN_URL = "https://public-api.monta.com/api/v1/auth/token"
MONTA_AFIR_CHARGE_POINTS_URL = "https://public-api.monta.com/api/v1/afir/charge-points"
MONTA_AFIR_EVSE_STATUS_URL_TEMPLATE = (
    "https://public-api.monta.com/api/v1/afir/charge-points/{evse_id}/status"
)

MONTA_AFIR_CHARGE_POINTS_SOURCE_UID = "dk_monta_afir_charge_points"
MONTA_AFIR_EVSE_STATUS_SOURCE_UID = "dk_monta_afir_evse_status"
BE_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID = "be_monta_afir_charge_points"
BE_MONTA_AFIR_EVSE_STATUS_SOURCE_UID = "be_monta_afir_evse_status"


@dataclass(frozen=True)
class MontaCountryConfig:
    country_code: str
    provider_uid: str
    table_source_uid: str
    status_source_uid: str
    status_price_quality: str


MONTA_COUNTRY_CONFIGS = {
    "DK": MontaCountryConfig(
        country_code="DK",
        provider_uid=MONTA_PROVIDER_UID,
        table_source_uid=MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
        status_source_uid=MONTA_AFIR_EVSE_STATUS_SOURCE_UID,
        status_price_quality="source_dk_monta_afir_status_price",
    ),
    "BE": MontaCountryConfig(
        country_code="BE",
        provider_uid=BE_MONTA_PROVIDER_UID,
        table_source_uid=BE_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
        status_source_uid=BE_MONTA_AFIR_EVSE_STATUS_SOURCE_UID,
        status_price_quality="source_be_monta_afir_status_price",
    ),
}

STATUS_MAP = {
    "available": "free",
    "occupied": "occupied",
    "reserved": "reserved",
    "outofservice": "out_of_order",
    "out_of_service": "out_of_order",
    "out of service": "out_of_order",
    "outoforder": "out_of_order",
    "out_of_order": "out_of_order",
    "faulted": "out_of_order",
    "unknown": "unknown",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _safe_id(value: Any) -> str:
    text = _text(value).lower()
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "-" for ch in text)
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "unknown"


def _json_text(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _list_value(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _dict_value(value: Any, *keys: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    for key in keys:
        child = value.get(key)
        if isinstance(child, dict):
            return child
    return {}


def _nested_dict(value: Any, *keys: str) -> dict[str, Any]:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return {}
        current = current.get(key)
    return current if isinstance(current, dict) else {}


def _nested_value(value: Any, *keys: str) -> Any:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value in (None, ""):
        return None
    text = _text(value).casefold()
    if text in {"1", "true", "yes", "y"}:
        return True
    if text in {"0", "false", "no", "n"}:
        return False
    return None


def _enum_value(value: Any) -> str:
    if isinstance(value, dict):
        return _text(value.get("value") or value.get("extendedValueG") or value.get("id"))
    return _text(value)


def _multilingual_text(value: Any) -> str:
    if isinstance(value, dict):
        values = value.get("values")
        if isinstance(values, list):
            return _join_unique(
                item.get("value") if isinstance(item, dict) else item for item in values
            )
        if "value" in value:
            return _text(value.get("value"))
    return _text(value)


def _join_unique(values: Iterable[Any]) -> str:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = _text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return "|".join(result)


def _power_kw(value: Any) -> float | None:
    power = _float_or_none(value)
    if power is None or power <= 0:
        return None
    return power / 1000.0 if power > 1000 else power


def _current_type(values: Iterable[Any]) -> str:
    text = " ".join(_text(value).casefold() for value in values)
    if "dc" in text or "direct" in text or "combo" in text or "chademo" in text:
        return "DC"
    if "ac" in text or "alternating" in text:
        return "AC"
    return ""


def monta_country_config(country_code: str = COUNTRY_CODE) -> MontaCountryConfig:
    country = _text(country_code).upper() or COUNTRY_CODE
    if country not in MONTA_COUNTRY_CONFIGS:
        raise ValueError(f"unsupported_monta_country:{country}")
    return MONTA_COUNTRY_CONFIGS[country]


def station_id_from_site_id(source_site_id: Any, *, country_code: str = COUNTRY_CODE) -> str:
    country = monta_country_config(country_code).country_code.lower()
    return f"{country}:monta:{_safe_id(source_site_id)}"


def charger_id_from_evse_id(source_evse_id: Any, *, country_code: str = COUNTRY_CODE) -> str:
    country = monta_country_config(country_code).country_code.lower()
    return f"{country}:monta:evse:{_safe_id(source_evse_id)}"


def _facility_location(site: dict[str, Any]) -> dict[str, Any]:
    return _nested_dict(
        site,
        "locationReference",
        "locPointLocation",
        "locLocationExtensionG",
        "FacilityLocation",
    )


def _coordinates(site: dict[str, Any]) -> tuple[float | None, float | None]:
    coordinates = _nested_dict(
        site,
        "locationReference",
        "locPointLocation",
        "pointByCoordinates",
        "pointCoordinates",
    )
    latitude = _float_or_none(coordinates.get("latitude"))
    longitude = _float_or_none(coordinates.get("longitude"))
    return latitude, longitude


def _address_parts(site: dict[str, Any]) -> dict[str, str]:
    address = _dict_value(_facility_location(site), "address")
    address_lines = []
    for item in sorted(
        (line for line in _list_value(address.get("addressLine")) if isinstance(line, dict)),
        key=lambda line: int(line.get("order") or 0),
    ):
        text = _multilingual_text(item.get("text"))
        if text:
            address_lines.append(text)
    return {
        "address": _join_unique(address_lines),
        "city": _multilingual_text(address.get("city")),
        "postal_code": _text(address.get("postcode")),
        "address_country": _text(address.get("countryCode")),
    }


def _organisation_name(site: dict[str, Any], key: str) -> str:
    organisation = _nested_dict(site, key, "afacAnOrganisation")
    return _multilingual_text(organisation.get("name"))


def _external_identifiers(refill_point: dict[str, Any]) -> list[str]:
    identifiers: list[str] = []
    for item in _list_value(refill_point.get("externalIdentifier")):
        if isinstance(item, dict):
            identifiers.append(_text(item.get("identifier") or item.get("id") or item.get("value")))
        else:
            identifiers.append(_text(item))
    return [identifier for identifier in identifiers if identifier]


def _primary_evse_id(refill_point: dict[str, Any]) -> str:
    identifiers = _external_identifiers(refill_point)
    for identifier in identifiers:
        if "*" in identifier:
            return identifier
    return identifiers[0] if identifiers else _text(refill_point.get("idG"))


def _source_evse_alias_ids(source_evse_id: str, refill_point: dict[str, Any]) -> list[str]:
    aliases = [_text(refill_point.get("idG")), *_external_identifiers(refill_point)]
    result: list[str] = []
    seen = {source_evse_id}
    for alias in aliases:
        if not alias or alias in seen:
            continue
        seen.add(alias)
        result.append(alias)
    return result


def _connector_rows(refill_point: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, connector in enumerate(_list_value(refill_point.get("connector")), start=1):
        if not isinstance(connector, dict):
            continue
        rows.append(
            {
                "connector_id": _text(connector.get("externalIdentifier")) or str(index),
                "connector_type": _enum_value(connector.get("connectorType")),
                "connector_format": _enum_value(connector.get("connectorFormat")),
                "charging_mode": _enum_value(connector.get("chargingMode")),
                "max_power_kw": _power_kw(connector.get("maxPowerAtSocket")),
                "maximum_current": _float_or_none(connector.get("maximumCurrent")),
                "voltage": _float_or_none(connector.get("voltage")),
            }
        )
    return rows


def _green_energy(refill_point: dict[str, Any]) -> bool | None:
    values = [
        _bool_or_none(mix.get("isGreenEnergy"))
        for mix in _list_value(refill_point.get("electricEnergyMix"))
        if isinstance(mix, dict)
    ]
    if any(value is True for value in values):
        return True
    if any(value is False for value in values):
        return False
    return None


def iter_static_rows(payload: Any, *, country_code: str = COUNTRY_CODE) -> Iterable[dict[str, Any]]:
    if not isinstance(payload, dict):
        return
    config = monta_country_config(country_code)
    publication_time = _text(payload.get("publicationTime"))
    for table in _list_value(payload.get("energyInfrastructureTable")):
        if not isinstance(table, dict):
            continue
        for site in _list_value(table.get("energyInfrastructureSite")):
            if isinstance(site, dict):
                yield from static_rows_from_site(
                    site,
                    publication_time=publication_time,
                    config=config,
                )


def static_rows_from_site(
    site: dict[str, Any],
    *,
    publication_time: str = "",
    config: MontaCountryConfig | None = None,
) -> Iterable[dict[str, Any]]:
    config = config or monta_country_config()
    source_site_id = _text(site.get("idG"))
    if not source_site_id:
        return
    address = _address_parts(site)
    latitude, longitude = _coordinates(site)
    operator_name = _organisation_name(site, "operator")
    owner_name = _organisation_name(site, "owner")
    site_name = _multilingual_text(site.get("name"))
    for station in _list_value(site.get("energyInfrastructureStation")):
        if not isinstance(station, dict):
            continue
        source_station_id = _text(station.get("idG")) or source_site_id
        service_types = _join_unique(
            _enum_value(item.get("serviceType"))
            for item in _list_value(station.get("serviceType"))
            if isinstance(item, dict)
        )
        for refill_point in _list_value(station.get("refillPoint")):
            if not isinstance(refill_point, dict):
                continue
            source_evse_id = _primary_evse_id(refill_point)
            if not source_evse_id:
                continue
            connectors = _connector_rows(refill_point)
            current_type = _current_type(
                [
                    _enum_value(refill_point.get("currentType")),
                    *(connector.get("connector_type") for connector in connectors),
                    *(connector.get("charging_mode") for connector in connectors),
                ]
            )
            powers = [
                connector["max_power_kw"]
                for connector in connectors
                if connector.get("max_power_kw") is not None
            ]
            yield {
                "country_code": config.country_code,
                "source_uid": config.table_source_uid,
                "provider_uid": config.provider_uid,
                "operator_name": operator_name or owner_name,
                "owner_name": owner_name,
                "station_id": station_id_from_site_id(source_site_id, country_code=config.country_code),
                "charger_id": charger_id_from_evse_id(source_evse_id, country_code=config.country_code),
                "source_station_id": source_site_id,
                "source_station_ref": source_station_id,
                "source_evse_id": source_evse_id,
                "source_evse_alias_ids": _source_evse_alias_ids(source_evse_id, refill_point),
                "public_evse_id": source_evse_id if "*" in source_evse_id else "",
                "source_refill_point_id": _text(refill_point.get("idG")),
                "station_name": site_name or source_site_id,
                "address": address["address"],
                "city": address["city"],
                "postal_code": address["postal_code"],
                "address_country": address["address_country"],
                "latitude": latitude,
                "longitude": longitude,
                "connector_count": len(connectors) or 1,
                "connector_id": _join_unique(connector.get("connector_id") for connector in connectors),
                "connector_types": _join_unique(connector.get("connector_type") for connector in connectors),
                "connector_formats": _join_unique(connector.get("connector_format") for connector in connectors),
                "charging_modes": _join_unique(connector.get("charging_mode") for connector in connectors),
                "current_type": current_type,
                "max_power_kw": max(powers) if powers else None,
                "maximum_current": max(
                    (
                        connector["maximum_current"]
                        for connector in connectors
                        if connector.get("maximum_current") is not None
                    ),
                    default=None,
                ),
                "service_types": service_types,
                "delivery_unit": _enum_value(refill_point.get("deliveryUnit")),
                "renewable_energy": _green_energy(refill_point),
                "date_updated": publication_time,
                "raw_static": _json_text(
                    {
                        "site_id": source_site_id,
                        "station_id": source_station_id,
                        "refill_point": refill_point,
                    }
                ),
            }


def parse_static_payload(payload: Any, *, country_code: str = COUNTRY_CODE) -> list[dict[str, Any]]:
    return list(iter_static_rows(payload, country_code=country_code))


def _status_from_source(source_status: str) -> str:
    normalized = _text(source_status).replace("-", "_").casefold()
    return STATUS_MAP.get(normalized, "unknown")


def _price_scalar(value: float) -> str:
    return f"{float(value):.6f}".rstrip("0").rstrip(".")


def _price_display(value: float, currency: str, unit: str, *, complex_tariff: bool) -> str:
    prefix = "from " if complex_tariff else ""
    return f"{prefix}{_price_scalar(value)} {currency}/{unit}".strip()


def _price_fields(rate_updates: Any, *, status_price_quality: str = "source_dk_monta_afir_status_price") -> dict[str, Any]:
    energy_values: list[float] = []
    minute_values: list[float] = []
    flat_values: list[float] = []
    currencies: list[str] = []
    for rate in _list_value(rate_updates):
        if not isinstance(rate, dict):
            continue
        currency = _text(rate.get("applicableCurrency")).upper()
        if currency:
            currencies.append(currency)
        for component in _list_value(rate.get("energyRate")):
            if not isinstance(component, dict):
                continue
            price = _float_or_none(component.get("price"))
            if price is None:
                continue
            unit_type = _text(component.get("unitType"))
            quantity = _text(component.get("applicableQuantity"))
            price_type = _text(component.get("priceType"))
            if unit_type == "perKilowattHour" or quantity == "energy":
                energy_values.append(price)
            elif unit_type == "perMinute" or quantity in {"time", "occupancy"}:
                minute_values.append(price)
            elif price_type == "flatRate" or unit_type == "perSession":
                flat_values.append(price)
    currency = currencies[0] if currencies else ""
    complex_tariff = bool(len(energy_values) > 1 or minute_values or flat_values)
    display = ""
    if currency and energy_values:
        display = _price_display(min(energy_values), currency, "kWh", complex_tariff=complex_tariff)
    elif currency and minute_values:
        display = _price_display(min(minute_values), currency, "min", complex_tariff=complex_tariff)
    elif currency and flat_values:
        prefix = "from " if complex_tariff else ""
        display = f"{prefix}{_price_scalar(min(flat_values))} {currency} flat"
    result: dict[str, Any] = {
        "price_display": display,
        "price_currency": currency,
        "price_quality": status_price_quality if display else "",
        "price_complex": complex_tariff,
        "price_source_text": _json_text(rate_updates),
    }
    if currency == "EUR":
        result.update(
            {
                "price_energy_eur_kwh_min": _price_scalar(min(energy_values)) if energy_values else "",
                "price_energy_eur_kwh_max": _price_scalar(max(energy_values)) if energy_values else "",
                "price_time_eur_min_min": min(minute_values) if minute_values else None,
                "price_time_eur_min_max": max(minute_values) if minute_values else None,
            }
        )
    return result


def iter_status_rows(payload: Any, *, country_code: str = COUNTRY_CODE) -> Iterable[dict[str, Any]]:
    if not isinstance(payload, dict):
        return
    config = monta_country_config(country_code)
    publication_time = _text(payload.get("publicationTime"))
    for status in _list_value(payload.get("electricChargingPointStatus")):
        if not isinstance(status, dict):
            continue
        source_evse_id = _text(status.get("evseId"))
        if not source_evse_id:
            continue
        source_status = _text(status.get("availabilityStatus")) or "unknown"
        row = {
            "country_code": config.country_code,
            "source_uid": config.status_source_uid,
            "provider_uid": config.provider_uid,
            "charger_id": charger_id_from_evse_id(source_evse_id, country_code=config.country_code),
            "source_evse_id": source_evse_id,
            "source_status": source_status,
            "availability_status": _status_from_source(source_status),
            "source_observed_at": _text(status.get("lastUpdated")) or publication_time,
            "publication_time": publication_time,
            "raw_dynamic": _json_text(status),
        }
        row.update(_price_fields(status.get("energyRateUpdate"), status_price_quality=config.status_price_quality))
        yield row


def parse_status_payload(payload: Any, *, country_code: str = COUNTRY_CODE) -> list[dict[str, Any]]:
    return list(iter_status_rows(payload, country_code=country_code))


def _json_payload_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str,
) -> Any:
    text_stream: TextIO = _text_stream_util(raw_stream, content_encoding=content_encoding)
    try:
        return json.load(text_stream)
    finally:
        text_stream.detach()


def iter_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
    country_code: str = COUNTRY_CODE,
) -> Iterable[dict[str, Any]]:
    yield from iter_static_rows(
        _json_payload_from_binary_stream(raw_stream, content_encoding=content_encoding),
        country_code=country_code,
    )


def iter_status_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
    country_code: str = COUNTRY_CODE,
) -> Iterable[dict[str, Any]]:
    yield from iter_status_rows(
        _json_payload_from_binary_stream(raw_stream, content_encoding=content_encoding),
        country_code=country_code,
    )


def iter_be_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    yield from iter_static_rows_from_binary_stream(
        raw_stream,
        content_encoding=content_encoding,
        country_code=BE_COUNTRY_CODE,
    )


def iter_be_status_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    yield from iter_status_rows_from_binary_stream(
        raw_stream,
        content_encoding=content_encoding,
        country_code=BE_COUNTRY_CODE,
    )


def extract_evse_ids_from_table_payload(payload: Any, *, country_code: str = COUNTRY_CODE) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for row in iter_static_rows(payload, country_code=country_code):
        evse_id = _text(row.get("source_evse_id"))
        if evse_id and evse_id not in seen:
            seen.add(evse_id)
            ids.append(evse_id)
    return ids


def count_table_payload(payload: Any) -> dict[str, Any]:
    table_count = 0
    site_count = 0
    station_count = 0
    refill_point_count = 0
    connector_count = 0
    if isinstance(payload, dict):
        for table in _list_value(payload.get("energyInfrastructureTable")):
            if not isinstance(table, dict):
                continue
            table_count += 1
            for site in _list_value(table.get("energyInfrastructureSite")):
                if not isinstance(site, dict):
                    continue
                site_count += 1
                for station in _list_value(site.get("energyInfrastructureStation")):
                    if not isinstance(station, dict):
                        continue
                    station_count += 1
                    for refill_point in _list_value(station.get("refillPoint")):
                        if not isinstance(refill_point, dict):
                            continue
                        refill_point_count += 1
                        connector_count += len(
                            [item for item in _list_value(refill_point.get("connector")) if isinstance(item, dict)]
                        )
    meta = payload.get("meta") if isinstance(payload, dict) and isinstance(payload.get("meta"), dict) else {}
    return {
        "publication_time": _text(payload.get("publicationTime")) if isinstance(payload, dict) else "",
        "table_count": table_count,
        "site_count": site_count,
        "station_count": station_count,
        "refill_point_count": refill_point_count,
        "connector_count": connector_count,
        "page": meta.get("page"),
        "per_page": meta.get("perPage"),
        "total": meta.get("total"),
    }


def count_table_payload_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> dict[str, Any]:
    return count_table_payload(
        _json_payload_from_binary_stream(raw_stream, content_encoding=content_encoding)
    )
