from __future__ import annotations

import io
import json
from typing import Any, Iterable

from .stream_utils import text_stream_from_binary_stream as _text_stream_util

COUNTRY_CODE = "FI"

DIGITRAFFIC_AFIR_DOCS_URL = "https://www.digitraffic.fi/en/road-traffic/afir/"
DIGITRAFFIC_DATEX_STATUS_URL = (
    "https://afir.digitraffic.fi/api/charging-network/v1/locations/statuses/datex2-3.6/all"
)
DIGITRAFFIC_DATEX_LOCATIONS_URL = (
    "https://afir.digitraffic.fi/api/charging-network/v1/locations/datex2-3.6/all"
)

DIGITRAFFIC_DATEX_STATUS_SOURCE_UID = "fi_digitraffic_afir_datex_statuses"
DIGITRAFFIC_DATEX_LOCATIONS_SOURCE_UID = "fi_digitraffic_afir_datex_locations"
DIGITRAFFIC_PROVIDER_UID = "fi_digitraffic_afir_datex"

STATUS_MAP = {
    "available": "free",
    "occupied": "occupied",
    "reserved": "reserved",
    "unavailable": "out_of_order",
    "outofservice": "out_of_order",
    "out_of_service": "out_of_order",
    "faulted": "out_of_order",
    "unknown": "unknown",
}


def _datex_wrappers(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if any(key.startswith("egiEnergyInfrastructure") for key in payload):
        return [payload]
    wrappers = payload.get("payload")
    if isinstance(wrappers, list):
        return [wrapper for wrapper in wrappers if isinstance(wrapper, dict)]
    return []


def count_datex_records(payload: dict[str, Any], *, publication_key: str, record_key: str) -> int:
    count = 0
    for wrapper in _datex_wrappers(payload):
        publication = wrapper.get(publication_key) or {}
        if not isinstance(publication, dict):
            continue
        records = publication.get(record_key) or []
        if isinstance(records, list):
            count += len(records)
    return count


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


def _dict_value(value: Any, *keys: str) -> dict[str, Any]:
    for key in keys:
        child = value.get(key) if isinstance(value, dict) else None
        if isinstance(child, dict):
            return child
    return {}


def _list_value(value: Any, *keys: str) -> list[Any]:
    for key in keys:
        child = value.get(key) if isinstance(value, dict) else None
        if isinstance(child, list):
            return child
    return []


def _localized_text(value: Any) -> str:
    values = _list_value(value, "values")
    for item in values:
        if not isinstance(item, dict):
            continue
        text = _text(item.get("value"))
        if text:
            return text
    return _text(value)


def _organisation_name(value: Any) -> str:
    org = _dict_value(value, "facOrganisationSpecification")
    return _localized_text(org.get("name")) or _localized_text(org.get("legalName")) or _text(org.get("idG"))


def _coordinates(site: dict[str, Any]) -> tuple[float | None, float | None]:
    coords = (
        site.get("locationReference", {})
        .get("locPointLocation", {})
        .get("pointByCoordinates", {})
        .get("pointCoordinates", {})
    )
    if not isinstance(coords, dict):
        return None, None
    return _float_or_none(coords.get("latitude")), _float_or_none(coords.get("longitude"))


def _connector_type(connector: dict[str, Any]) -> str:
    connector_type = _dict_value(connector, "connectorType")
    return _text(connector_type.get("value"))


def _max_power_kw(connectors: Iterable[dict[str, Any]]) -> float | None:
    values = []
    for connector in connectors:
        power_w = _float_or_none(connector.get("maxPowerAtSocket"))
        if power_w and power_w > 0:
            values.append(power_w / 1000.0)
    return max(values) if values else None


def station_id_from_site_id(source_site_id: Any) -> str:
    return f"fi:datex:{_safe_id(source_site_id)}"


def charger_id_from_evse_id(source_evse_id: Any) -> str:
    return f"fi:datex:evse:{_safe_id(source_evse_id)}"


def _text_stream_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str,
) -> io.TextIOWrapper:
    return _text_stream_util(raw_stream, content_encoding=content_encoding)


def iter_location_rows(payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for wrapper in _datex_wrappers(payload):
        publication = wrapper.get("egiEnergyInfrastructureTablePublication") or {}
        if not isinstance(publication, dict):
            continue
        for table in publication.get("energyInfrastructureTable") or []:
            if not isinstance(table, dict):
                continue
            for site in table.get("energyInfrastructureSite") or []:
                if not isinstance(site, dict):
                    continue
                source_site_id = _text(site.get("idG"))
                if not source_site_id:
                    continue
                station_id = station_id_from_site_id(source_site_id)
                lat, lon = _coordinates(site)
                operator_name = _organisation_name(site.get("operator")) or _organisation_name(site.get("owner"))
                site_name = _localized_text(site.get("name"))
                for station in site.get("energyInfrastructureStation") or []:
                    if not isinstance(station, dict):
                        continue
                    source_station_ref = _text(station.get("idG")) or source_site_id
                    for refill_point in station.get("refillPoint") or []:
                        if not isinstance(refill_point, dict):
                            continue
                        charging_point = _dict_value(refill_point, "egiElectricChargingPoint")
                        source_evse_id = _text(charging_point.get("idG"))
                        if not source_evse_id:
                            continue
                        connectors = [
                            connector
                            for connector in charging_point.get("connector") or []
                            if isinstance(connector, dict)
                        ]
                        yield {
                            "country_code": COUNTRY_CODE,
                            "source_uid": DIGITRAFFIC_DATEX_LOCATIONS_SOURCE_UID,
                            "provider_uid": DIGITRAFFIC_PROVIDER_UID,
                            "station_id": station_id,
                            "charger_id": charger_id_from_evse_id(source_evse_id),
                            "source_station_id": source_site_id,
                            "source_evse_id": source_evse_id,
                            "source_station_ref": source_station_ref,
                            "operator_name": operator_name,
                            "station_name": site_name,
                            "address": "",
                            "city": "",
                            "postal_code": "",
                            "latitude": lat,
                            "longitude": lon,
                            "connector_count": len(connectors) or 1,
                            "connector_types": "|".join(
                                dict.fromkeys(_connector_type(connector) for connector in connectors if _connector_type(connector))
                            ),
                            "current_type": "",
                            "max_power_kw": _max_power_kw(connectors),
                            "date_updated": _text(charging_point.get("lastUpdated")) or _text(station.get("lastUpdated")),
                        }


def iter_status_rows(payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for wrapper in _datex_wrappers(payload):
        publication = wrapper.get("egiEnergyInfrastructureStatusPublication") or {}
        if not isinstance(publication, dict):
            continue
        publication_time = _text(publication.get("publicationTime"))
        for site_status in publication.get("energyInfrastructureSiteStatus") or []:
            if not isinstance(site_status, dict):
                continue
            site_ref = _dict_value(site_status, "reference")
            source_site_id = _text(site_ref.get("idG"))
            if not source_site_id:
                continue
            station_id = station_id_from_site_id(source_site_id)
            for station_status in site_status.get("energyInfrastructureStationStatus") or []:
                if not isinstance(station_status, dict):
                    continue
                station_ref = _text(_dict_value(station_status, "reference").get("idG"))
                for refill_status in station_status.get("refillPointStatus") or []:
                    if not isinstance(refill_status, dict):
                        continue
                    point_status = _dict_value(refill_status, "egiElectricChargingPointStatus")
                    source_evse_id = _text(_dict_value(point_status, "reference").get("idG"))
                    if not source_evse_id:
                        continue
                    status = _text(_dict_value(point_status, "status").get("value")).casefold()
                    yield {
                        "country_code": COUNTRY_CODE,
                        "source_uid": DIGITRAFFIC_DATEX_STATUS_SOURCE_UID,
                        "provider_uid": DIGITRAFFIC_PROVIDER_UID,
                        "station_id": station_id,
                        "charger_id": charger_id_from_evse_id(source_evse_id),
                        "source_station_id": source_site_id,
                        "source_station_ref": station_ref,
                        "source_evse_id": source_evse_id,
                        "source_status": status or "unknown",
                        "availability_status": STATUS_MAP.get(status, "unknown"),
                        "source_observed_at": publication_time,
                    }


def iter_location_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        payload = json.load(text_stream)
        yield from iter_location_rows(payload)
    finally:
        text_stream.detach()


def iter_status_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        payload = json.load(text_stream)
        yield from iter_status_rows(payload)
    finally:
        text_stream.detach()
