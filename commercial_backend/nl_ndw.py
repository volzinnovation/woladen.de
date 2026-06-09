from __future__ import annotations

import io
import json
from collections.abc import Sequence
from typing import Any, Iterable, TextIO

from .stream_utils import text_stream_from_binary_stream as _text_stream_util

NL_OCPI_LOCATIONS_URL = "https://opendata.ndw.nu/charging_point_locations_ocpi.json.gz"
NL_GEOJSON_LOCATIONS_URL = "https://opendata.ndw.nu/charging_point_locations.geojson.gz"
NL_OCPI_TARIFFS_URL = "https://opendata.ndw.nu/charging_point_tariffs_ocpi.json.gz"
SOURCE_UID = "nl_ndw_dotnl_ocpi_locations"
GEOJSON_LOCATIONS_SOURCE_UID = "nl_ndw_dotnl_geojson_locations"
OCPI_TARIFFS_SOURCE_UID = "nl_ndw_dotnl_ocpi_tariffs"
COUNTRY_CODE = "NL"

STATUS_MAP = {
    "AVAILABLE": "free",
    "BLOCKED": "occupied",
    "CHARGING": "occupied",
    "INOPERATIVE": "out_of_order",
    "OUTOFORDER": "out_of_order",
    "PLANNED": "unknown",
    "REMOVED": "out_of_order",
    "RESERVED": "occupied",
    "UNKNOWN": "unknown",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _safe_id(value: Any) -> str:
    text = _text(value).lower()
    return "".join(ch if ch.isalnum() or ch in {"_", "-"} else "-" for ch in text).strip("-")


def _iter_unique_text(values: Iterable[Any]) -> Iterable[str]:
    seen: set[str] = set()
    for value in values:
        text = _text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        yield text


def _json_text(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _price_scalar(value: float) -> str:
    return f"{float(value):.6f}".rstrip("0").rstrip(".")


def _euro_amount(value: float) -> str:
    return f"{round(float(value) + 0.000000001, 2):.2f}".replace(".", ",")


def _price_fields_from_values(
    *,
    energy_values: list[float],
    minute_values: list[float],
    fixed_values: list[float],
    currency: str,
    complex_tariff: bool,
    quality: str,
    source_text: str,
) -> dict[str, Any]:
    complex_tariff = complex_tariff or bool(minute_values or fixed_values)
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
        display = f"ab {_euro_amount(minute_min)} €/min" if complex_tariff else f"{_euro_amount(minute_min)} €/min"
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


def station_id_from_location(*, party_id: str, location_id: str) -> str:
    party = _safe_id(party_id) or "dot-nl"
    location = _safe_id(location_id)
    if not location:
        raise ValueError("missing_location_id")
    return f"nl:ocpi:{party}:{location}"


def charger_id_from_evse_id(evse_id: str) -> str:
    cleaned = _safe_id(evse_id)
    if not cleaned:
        raise ValueError("missing_evse_id")
    return f"nl:ocpi:{cleaned}"


def iter_locations(payload: Any) -> Iterable[dict[str, Any]]:
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


def rows_from_location(location: dict[str, Any]) -> Iterable[dict[str, Any]]:
    source_station_id = _text(location.get("id"))
    if not source_station_id:
        return
    party_id = _text(location.get("party_id")) or _text((location.get("operator") or {}).get("name")) or "dot-nl"
    station_id = station_id_from_location(party_id=party_id, location_id=source_station_id)
    operator = location.get("operator") if isinstance(location.get("operator"), dict) else {}
    operator_name = _text(operator.get("name"))
    location_last_updated = _text(location.get("last_updated"))
    for evse in location.get("evses") or []:
        if not isinstance(evse, dict):
            continue
        source_evse_id = _text(evse.get("evse_id")) or _text(evse.get("uid"))
        if not source_evse_id:
            continue
        source_status = _text(evse.get("status")).upper() or "UNKNOWN"
        connectors = [connector for connector in (evse.get("connectors") or []) if isinstance(connector, dict)]
        tariff_ids = list(
            _iter_unique_text(
                value
                for connector in connectors
                for value in (connector.get("tariff_ids") or [])
            )
        )
        yield {
            "country_code": COUNTRY_CODE,
            "source_uid": SOURCE_UID,
            "operator_name": operator_name,
            "charger_id": charger_id_from_evse_id(source_evse_id),
            "station_id": station_id,
            "source_station_id": source_station_id,
            "source_evse_id": source_evse_id,
            "source_status": source_status,
            "availability_status": STATUS_MAP.get(source_status, "unknown"),
            "source_observed_at": _text(evse.get("last_updated")) or location_last_updated,
            "capabilities": _json_text(evse.get("capabilities")),
            "connector_count": len(connectors),
            "tariff_ids": "|".join(tariff_ids),
        }


def iter_location_payload_rows(payload: Any) -> Iterable[dict[str, Any]]:
    for location in iter_locations(payload):
        yield from rows_from_location(location)


def parse_location_payload(payload: Any) -> list[dict[str, Any]]:
    return list(iter_location_payload_rows(payload))


def _payload_items(payload: Any, *keys: str) -> Iterable[dict[str, Any]]:
    if isinstance(payload, list):
        yield from (item for item in payload if isinstance(item, dict))
        return
    if not isinstance(payload, dict):
        return
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            yield from (item for item in value if isinstance(item, dict))
            return
    data = payload.get("data")
    if isinstance(data, list):
        yield from (item for item in data if isinstance(item, dict))
    elif isinstance(data, dict):
        yield from _payload_items(data, *keys)


def _ocpi_tariff_price_fields(tariff: dict[str, Any]) -> dict[str, Any]:
    tariff_id = _text(tariff.get("id"))
    currency = _text(tariff.get("currency")).upper()
    energy_values: list[float] = []
    minute_values: list[float] = []
    fixed_values: list[float] = []
    complex_tariff = False
    for element in tariff.get("elements") or []:
        if not isinstance(element, dict):
            continue
        if element.get("restrictions"):
            complex_tariff = True
        for component in element.get("price_components") or []:
            if not isinstance(component, dict):
                continue
            price = _float_or_none(component.get("price"))
            if price is None:
                continue
            component_type = _text(component.get("type")).upper()
            if component_type == "ENERGY":
                energy_values.append(price)
            elif component_type in {"TIME", "PARKING_TIME"}:
                minute_values.append(price / 60.0)
            elif component_type == "FLAT":
                fixed_values.append(price)
            else:
                complex_tariff = True
    return _price_fields_from_values(
        energy_values=energy_values,
        minute_values=minute_values,
        fixed_values=fixed_values,
        currency=currency,
        complex_tariff=complex_tariff,
        quality="source_ocpi_tariff",
        source_text=tariff_id,
    )


def tariff_row(tariff: dict[str, Any]) -> dict[str, Any] | None:
    tariff_id = _text(tariff.get("id"))
    if not tariff_id:
        return None
    row = {
        "country_code": COUNTRY_CODE,
        "source_uid": OCPI_TARIFFS_SOURCE_UID,
        "tariff_id": tariff_id,
        "country_code_source": _text(tariff.get("country_code")),
        "party_id": _text(tariff.get("party_id")),
        "currency": _text(tariff.get("currency")).upper(),
        "type": _text(tariff.get("type")),
        "last_updated": _text(tariff.get("last_updated")),
        "elements": _json_text(tariff.get("elements")),
        "tariff_alt_text": _json_text(tariff.get("tariff_alt_text")),
        "tariff_alt_url": _text(tariff.get("tariff_alt_url")),
        "raw_static": _json_text(tariff),
    }
    row.update(_ocpi_tariff_price_fields(tariff))
    return row


def iter_tariff_rows(payload: Any) -> Iterable[dict[str, Any]]:
    for tariff in _payload_items(payload, "tariffs"):
        row = tariff_row(tariff)
        if row is not None:
            yield row


def parse_tariff_payload(payload: Any) -> list[dict[str, Any]]:
    return list(iter_tariff_rows(payload))


def iter_locations_from_text_stream(text_stream: TextIO, *, chunk_size: int = 1024 * 1024) -> Iterable[dict[str, Any]]:
    yield from _iter_objects_from_text_stream(
        text_stream,
        chunk_size=chunk_size,
        top_level_keys=("data", "locations"),
        label="nl_locations",
    )


def _iter_objects_from_text_stream(
    text_stream: TextIO,
    *,
    chunk_size: int = 1024 * 1024,
    top_level_keys: Sequence[str] = ("data",),
    label: str = "json",
) -> Iterable[dict[str, Any]]:
    decoder = json.JSONDecoder()
    buffer = ""
    index = 0
    eof = False

    def fill_buffer() -> bool:
        nonlocal buffer, index, eof
        if eof:
            return False
        chunk = text_stream.read(chunk_size)
        if chunk == "":
            eof = True
            return False
        if index:
            buffer = buffer[index:] + chunk
            index = 0
        else:
            buffer += chunk
        return True

    def ensure_buffer() -> bool:
        while index >= len(buffer) and not eof:
            fill_buffer()
        return index < len(buffer)

    def skip_whitespace() -> bool:
        nonlocal index
        while True:
            if not ensure_buffer():
                return False
            while index < len(buffer) and buffer[index].isspace():
                index += 1
            if index < len(buffer):
                return True

    def iter_array_items() -> Iterable[dict[str, Any]]:
        nonlocal index
        index += 1

        while True:
            if not skip_whitespace():
                raise ValueError(f"unexpected_end_of_{label}_array")
            if buffer[index] == "]":
                return

            while True:
                try:
                    value, end_index = decoder.raw_decode(buffer, index)
                    index = end_index
                    break
                except json.JSONDecodeError:
                    if eof:
                        raise
                    fill_buffer()

            if isinstance(value, dict):
                yield value

            if not skip_whitespace():
                raise ValueError(f"unexpected_end_after_{label}_item")
            if buffer[index] == ",":
                index += 1
                continue
            if buffer[index] == "]":
                return
            raise ValueError(f"expected_comma_or_end_after_{label}_item")

    if not skip_whitespace():
        return
    if buffer[index] == "[":
        yield from iter_array_items()
        return
    if buffer[index] != "{":
        raise ValueError(f"{label}_streaming_parser_expects_top_level_json_array_or_object")

    while True:
        try:
            payload, end_index = decoder.raw_decode(buffer, index)
            index = end_index
            break
        except json.JSONDecodeError:
            if eof:
                raise
            fill_buffer()
    yield from _payload_items(payload, *top_level_keys)


def iter_location_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_util(raw_stream, content_encoding=content_encoding)
    try:
        for location in iter_locations_from_text_stream(text_stream):
            yield from rows_from_location(location)
    finally:
        text_stream.detach()


def iter_tariff_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_util(raw_stream, content_encoding=content_encoding)
    try:
        for tariff in _iter_objects_from_text_stream(
            text_stream,
            top_level_keys=("data", "tariffs"),
            label="nl_tariffs",
        ):
            row = tariff_row(tariff)
            if row is not None:
                yield row
    finally:
        text_stream.detach()
