from __future__ import annotations

import io
import json
from typing import Any, Iterable, TextIO

from .stream_utils import text_stream_from_binary_stream as _text_stream_util

STATIC_DATA_URL = (
    "https://data.geo.admin.ch/ch.bfe.ladestellen-elektromobilitaet/"
    "data/oicp/ch.bfe.ladestellen-elektromobilitaet.json"
)
STATUS_DATA_URL = (
    "https://data.geo.admin.ch/ch.bfe.ladestellen-elektromobilitaet/"
    "status/oicp/ch.bfe.ladestellen-elektromobilitaet.json"
)
DATASET_URL = "https://data.opentransportdata.swiss/en/dataset/ladestationen"
STATIC_RESOURCE_URL = (
    "https://data.opentransportdata.swiss/en/dataset/ladestationen/"
    "resource/a0a6b847-fa71-46ca-bd17-32daba2afca1"
)
STATUS_RESOURCE_URL = (
    "https://data.opentransportdata.swiss/en/dataset/ladestationen/"
    "resource/9f8d8115-8f1d-4184-9f4d-bb7d1e31f7d4"
)

SOURCE_UID = "ch_bfe_ladestationen"
COUNTRY_CODE = "CH"

STATUS_MAP = {
    "Available": "free",
    "Occupied": "occupied",
    "Reserved": "reserved",
    "OutOfService": "out_of_order",
    "Unknown": "unknown",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _join_text(values: Any) -> str:
    if not isinstance(values, list):
        return ""
    return "|".join(_text(value) for value in values if _text(value))


def _json_text(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _charging_station_names(record: dict[str, Any]) -> str:
    values = []
    for item in record.get("ChargingStationNames", []) or []:
        if not isinstance(item, dict):
            continue
        text = _text(item.get("value"))
        if text:
            values.append(text)
    return "|".join(dict.fromkeys(values))


def charger_id_from_evse_id(evse_id: str) -> str:
    cleaned = _text(evse_id)
    if not cleaned:
        raise ValueError("missing_evse_id")
    return f"ch:oicp:{cleaned}"


def station_id_from_source_id(source_station_id: str) -> str:
    cleaned = _text(source_station_id)
    if not cleaned:
        raise ValueError("missing_station_id")
    return f"ch:station:{cleaned}"


def _iter_operator_records(payload: dict[str, Any], *, top_key: str, record_key: str) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:
    for group in payload.get(top_key, []) or []:
        if not isinstance(group, dict):
            continue
        for record in group.get(record_key, []) or []:
            if isinstance(record, dict):
                yield group, record


def _coordinates(record: dict[str, Any]) -> tuple[float | None, float | None]:
    coordinates = record.get("GeoCoordinates") or {}
    google_value = _text(coordinates.get("Google") if isinstance(coordinates, dict) else "")
    parts = google_value.split()
    if len(parts) != 2:
        return None, None
    try:
        latitude = float(parts[0])
        longitude = float(parts[1])
    except ValueError:
        return None, None
    return latitude, longitude


def _max_power_kw(record: dict[str, Any]) -> float | None:
    values = []
    for facility in record.get("ChargingFacilities", []) or []:
        if not isinstance(facility, dict):
            continue
        try:
            values.append(float(facility.get("power")))
        except (TypeError, ValueError):
            continue
    return max(values) if values else None


def _facility_values(record: dict[str, Any], key: str) -> str:
    values = []
    for facility in record.get("ChargingFacilities", []) or []:
        if not isinstance(facility, dict):
            continue
        text = _text(facility.get(key))
        if text:
            values.append(text)
    return "|".join(dict.fromkeys(values))


def static_row_from_record(group: dict[str, Any], record: dict[str, Any]) -> dict[str, Any] | None:
    evse_id = _text(record.get("EvseID"))
    if not evse_id:
        return None
    source_station_id = _text(record.get("ChargingStationId")) or evse_id
    address = record.get("Address") or {}
    latitude, longitude = _coordinates(record)
    opening_times = record.get("OpeningTimes")
    return {
        "country_code": COUNTRY_CODE,
        "source_uid": SOURCE_UID,
        "operator_id": _text(group.get("OperatorID")),
        "operator_name": _text(group.get("OperatorName")),
        "charger_id": charger_id_from_evse_id(evse_id),
        "station_id": station_id_from_source_id(source_station_id),
        "source_evse_id": evse_id,
        "source_station_id": source_station_id,
        "dynamic_info_available": _text(record.get("DynamicInfoAvailable")),
        "latitude": latitude,
        "longitude": longitude,
        "city": _text(address.get("City") if isinstance(address, dict) else ""),
        "postal_code": _text(address.get("PostalCode") if isinstance(address, dict) else ""),
        "street": _text(address.get("Street") if isinstance(address, dict) else ""),
        "address_country": _text(address.get("Country") if isinstance(address, dict) else ""),
        "address_floor": _text(address.get("Floor") if isinstance(address, dict) else ""),
        "address_region": _text(address.get("Region") if isinstance(address, dict) else ""),
        "parking_spot": _text(address.get("ParkingSpot") if isinstance(address, dict) else ""),
        "parking_facility": _text(address.get("ParkingFacility") if isinstance(address, dict) else ""),
        "accessibility": _text(record.get("Accessibility")),
        "accessibility_location": _text(record.get("AccessibilityLocation")),
        "is_open_24_hours": bool(record.get("IsOpen24Hours")),
        "opening_times": _json_text(opening_times),
        "plugs": "|".join(_text(value) for value in record.get("Plugs", []) or [] if _text(value)),
        "max_power_kw": _max_power_kw(record),
        "power_types": _facility_values(record, "powertype"),
        "authentication_modes": "|".join(
            _text(value) for value in record.get("AuthenticationModes", []) or [] if _text(value)
        ),
        "renewable_energy": record.get("RenewableEnergy"),
        "hotline_phone_number": _text(record.get("HotlinePhoneNumber")),
        "charging_station_names": _charging_station_names(record),
        "value_added_services": _join_text(record.get("ValueAddedServices")),
        "calibration_law_data_availability": _text(record.get("CalibrationLawDataAvailability")),
        "is_hubject_compatible": record.get("IsHubjectCompatible"),
        "geo_charging_point_entrance": _json_text(record.get("GeoChargingPointEntrance")),
        "charging_station_location_reference": _json_text(record.get("ChargingStationLocationReference")),
        "energy_source": _json_text(record.get("EnergySource")),
        "environmental_impact": _json_text(record.get("EnvironmentalImpact")),
        "location_image": _json_text(record.get("LocationImage")),
        "suboperator_name": _text(record.get("SuboperatorName")),
        "max_capacity": _text(record.get("MaxCapacity")),
        "additional_info": _text(record.get("AdditionalInfo")),
        "charging_pool_id": _text(record.get("ChargingPoolID")),
        "dynamic_power_level": _text(record.get("DynamicPowerLevel")),
        "hardware_manufacturer": _text(record.get("HardwareManufacturer")),
        "hub_operator_id": _text(record.get("HubOperatorID")),
    }


def parse_static_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group, record in _iter_operator_records(payload, top_key="EVSEData", record_key="EVSEDataRecord"):
        row = static_row_from_record(group, record)
        if row is not None:
            rows.append(row)
    return rows


def status_row_from_record(group: dict[str, Any], record: dict[str, Any]) -> dict[str, Any] | None:
    evse_id = _text(record.get("EvseID"))
    if not evse_id:
        return None
    source_status = _text(record.get("EVSEStatus"))
    return {
        "country_code": COUNTRY_CODE,
        "source_uid": SOURCE_UID,
        "operator_id": _text(group.get("OperatorID")),
        "operator_name": _text(group.get("OperatorName")),
        "charger_id": charger_id_from_evse_id(evse_id),
        "source_evse_id": evse_id,
        "source_status": source_status,
        "availability_status": STATUS_MAP.get(source_status, "unknown"),
    }


def parse_status_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for group, record in _iter_operator_records(payload, top_key="EVSEStatuses", record_key="EVSEStatusRecord"):
        row = status_row_from_record(group, record)
        if row is not None:
            rows.append(row)
    return rows


class _JsonObjectStream:
    def __init__(self, text_stream: TextIO, *, chunk_size: int = 1024 * 1024):
        self._stream = text_stream
        self._chunk_size = chunk_size
        self._decoder = json.JSONDecoder()
        self._buffer = ""
        self._index = 0
        self._eof = False

    def _fill(self) -> bool:
        if self._eof:
            return False
        chunk = self._stream.read(self._chunk_size)
        if chunk == "":
            self._eof = True
            return False
        if self._index:
            self._buffer = self._buffer[self._index :] + chunk
            self._index = 0
        else:
            self._buffer += chunk
        return True

    def _ensure(self) -> bool:
        while self._index >= len(self._buffer) and not self._eof:
            self._fill()
        return self._index < len(self._buffer)

    def skip_whitespace(self) -> bool:
        while True:
            if not self._ensure():
                return False
            while self._index < len(self._buffer) and self._buffer[self._index].isspace():
                self._index += 1
            if self._index < len(self._buffer):
                return True

    def peek(self) -> str:
        if not self.skip_whitespace():
            raise ValueError("unexpected_end_of_json_stream")
        return self._buffer[self._index]

    def expect(self, char: str) -> None:
        if self.peek() != char:
            raise ValueError(f"expected_json_char:{char}")
        self._index += 1

    def parse_value(self) -> Any:
        self.skip_whitespace()
        while True:
            try:
                value, end_index = self._decoder.raw_decode(self._buffer, self._index)
                self._index = end_index
                return value
            except json.JSONDecodeError:
                if self._eof:
                    raise
                self._fill()

    def skip_value(self) -> None:
        self.parse_value()


def _iter_operator_records_from_text_stream(
    text_stream: TextIO,
    *,
    top_key: str,
    record_key: str,
) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:
    parser = _JsonObjectStream(text_stream)
    parser.expect("{")
    while True:
        if parser.peek() == "}":
            parser.expect("}")
            return
        key = parser.parse_value()
        parser.expect(":")
        if key == top_key:
            yield from _iter_operator_groups(parser, record_key=record_key)
        else:
            parser.skip_value()
        if parser.peek() == ",":
            parser.expect(",")
            continue
        if parser.peek() == "}":
            parser.expect("}")
            return
        raise ValueError("expected_comma_or_end_of_ch_payload")


def _iter_operator_groups(
    parser: _JsonObjectStream,
    *,
    record_key: str,
) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:
    parser.expect("[")
    while True:
        if parser.peek() == "]":
            parser.expect("]")
            return
        yield from _iter_operator_group(parser, record_key=record_key)
        if parser.peek() == ",":
            parser.expect(",")
            continue
        if parser.peek() == "]":
            parser.expect("]")
            return
        raise ValueError("expected_comma_or_end_of_ch_group_array")


def _iter_operator_group(
    parser: _JsonObjectStream,
    *,
    record_key: str,
) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:
    group_context: dict[str, Any] = {}
    parser.expect("{")
    while True:
        if parser.peek() == "}":
            parser.expect("}")
            return
        key = parser.parse_value()
        parser.expect(":")
        if key == record_key:
            yield from _iter_group_records(parser, group_context=group_context)
        else:
            value = parser.parse_value()
            if key in {"OperatorID", "OperatorName"}:
                group_context[key] = value
        if parser.peek() == ",":
            parser.expect(",")
            continue
        if parser.peek() == "}":
            parser.expect("}")
            return
        raise ValueError("expected_comma_or_end_of_ch_group")


def _iter_group_records(
    parser: _JsonObjectStream,
    *,
    group_context: dict[str, Any],
) -> Iterable[tuple[dict[str, Any], dict[str, Any]]]:
    parser.expect("[")
    while True:
        if parser.peek() == "]":
            parser.expect("]")
            return
        record = parser.parse_value()
        if isinstance(record, dict):
            yield dict(group_context), record
        if parser.peek() == ",":
            parser.expect(",")
            continue
        if parser.peek() == "]":
            parser.expect("]")
            return
        raise ValueError("expected_comma_or_end_of_ch_record_array")


def _iter_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str,
    top_key: str,
    record_key: str,
    row_factory,
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_util(raw_stream, content_encoding=content_encoding)
    try:
        for group, record in _iter_operator_records_from_text_stream(
            text_stream,
            top_key=top_key,
            record_key=record_key,
        ):
            row = row_factory(group, record)
            if row is not None:
                yield row
    finally:
        text_stream.detach()


def iter_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    return _iter_rows_from_binary_stream(
        raw_stream,
        content_encoding=content_encoding,
        top_key="EVSEData",
        record_key="EVSEDataRecord",
        row_factory=static_row_from_record,
    )


def iter_status_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    return _iter_rows_from_binary_stream(
        raw_stream,
        content_encoding=content_encoding,
        top_key="EVSEStatuses",
        record_key="EVSEStatusRecord",
        row_factory=status_row_from_record,
    )
