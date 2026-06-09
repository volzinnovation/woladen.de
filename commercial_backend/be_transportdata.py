from __future__ import annotations

import gzip
import io
import json
import os
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, TextIO

from .config import REPO_ROOT
from .stream_utils import binary_stream_from_binary_stream as _binary_stream_util
from .stream_utils import text_stream_from_binary_stream as _text_stream_util

COUNTRY_CODE = "BE"

ENERGYVISION_LOCATIONS_SOURCE_UID = "be_energyvision_ocpi_locations"
ENERGYVISION_TARIFFS_SOURCE_UID = "be_energyvision_ocpi_tariffs"
ECO_MOVEMENT_STATIC_SOURCE_UID = "be_eco_movement_static_datex"
MONTA_AFIR_CHARGE_POINTS_SOURCE_UID = "be_monta_afir_charge_points"
ROAD_OCPI_LOCATIONS_SOURCE_UID = "be_road_ocpi_locations"
GROUP_INDIGO_STATIC_SOURCE_UID = "be_group_indigo_datex_static"

ENERGYVISION_PRODUCTION_BASE_URL = "https://ocpi.energyvision.be/cpo/"
ENERGYVISION_PRODUCTION_OCPI_VERSION = "2.1.1"
ENERGYVISION_STAGING_BASE_URL = "https://ocpi.myev-dev.be/cpo/"
ENERGYVISION_STAGING_OCPI_VERSION = "2.2.1"


def energyvision_module_url(*, base_url: str, ocpi_version: str, module: str) -> str:
    base = str(base_url or "").strip().rstrip("/")
    version = str(ocpi_version or "").strip().strip("/")
    module_name = str(module or "").strip().strip("/")
    if not base or not version or not module_name:
        raise ValueError("energyvision_module_url_requires_base_version_and_module")
    return f"{base}/{version}/{module_name}/"


ENERGYVISION_LOCATIONS_URL = energyvision_module_url(
    base_url=ENERGYVISION_PRODUCTION_BASE_URL,
    ocpi_version=ENERGYVISION_PRODUCTION_OCPI_VERSION,
    module="locations",
)
ENERGYVISION_TARIFFS_URL = energyvision_module_url(
    base_url=ENERGYVISION_PRODUCTION_BASE_URL,
    ocpi_version=ENERGYVISION_PRODUCTION_OCPI_VERSION,
    module="tariffs",
)
ECO_MOVEMENT_STATIC_URL = "https://api.eco-movement.com/api/nap/datexii/locations"
MONTA_AUTH_TOKEN_URL = "https://partner-api.monta.com/api/v1/auth/token"
MONTA_AFIR_CHARGE_POINTS_URL = "https://partner-api.monta.com/api/v1/afir/charge-points"
ROAD_OCPI_LOCATIONS_URL = (
    "https://roaming.road.io/files/9ef09c78-2666-418a-aa45-4f2261e2e305/"
    "locations.json?force=true"
)
GROUP_INDIGO_STATIC_URL = (
    "https://transportdata.be/dataset/27f1357d-71ee-48cb-84a1-96f3f4f034b8/"
    "resource/d4bc8ddd-c80f-4330-98e5-d86e5b2147c3/download/"
    "indigo-data-evcharging-static-datexii.xml"
)

SECRET_DIR = REPO_ROOT / "secret"

STATUS_MAP = {
    "AVAILABLE": "free",
    "FREE": "free",
    "BLOCKED": "occupied",
    "CHARGING": "occupied",
    "OCCUPIED": "occupied",
    "INOPERATIVE": "out_of_order",
    "UNAVAILABLE": "out_of_order",
    "OUTOFSERVICE": "out_of_order",
    "OUT_OF_SERVICE": "out_of_order",
    "OUTOFORDER": "out_of_order",
    "OUT_OF_ORDER": "out_of_order",
    "OUT OF ORDER": "out_of_order",
    "FAULTED": "out_of_order",
    "ERROR": "out_of_order",
    "RESERVED": "occupied",
    "IN_USE": "occupied",
    "UNKNOWN": "unknown",
    "REMOVED": "out_of_order",
}


@dataclass(frozen=True)
class TransportdataBESource:
    source_uid: str
    dataset: str
    organization: str
    resource_kind: str
    source_kind: str
    task_kind: str
    endpoint_url: str
    dataset_url: str
    resource_url: str
    access_status: str
    usage_status: str
    update_frequency: str
    credential_env: tuple[str, ...] = ()
    credential_files: tuple[str, ...] = ()
    open_static_bundle_status: str = "contract_review_required"
    private_dynamic_bundle_status: str = "allowed_after_access_review"

    def public_dict(self) -> dict[str, Any]:
        return asdict(self)


SOURCE_REGISTRY = (
    TransportdataBESource(
        source_uid=ENERGYVISION_LOCATIONS_SOURCE_UID,
        dataset="EnergyVision Public Charging Network (AFIR / OCPI 2.2.1)",
        organization="EnergyVision",
        resource_kind="ocpi_2_1_1_locations",
        source_kind="afir_ocpi_locations_with_status",
        task_kind="parse_dynamic_payload",
        endpoint_url=ENERGYVISION_LOCATIONS_URL,
        dataset_url="https://transportdata.be/en/dataset/energyvision-public-charging-network-locations-afir-ocpi-2-2-1",
        resource_url="https://transportdata.be/en/dataset/energyvision-public-charging-network-locations-afir-ocpi-2-2-1",
        access_status="access_expected_api_key_required",
        usage_status="terms_pending",
        update_frequency="every_minute",
        credential_env=("TRANSPORTDATA_BE_ENERGYVISION_API_KEY", "WOLADEN_TRANSPORTDATA_BE_API_KEY"),
        credential_files=("transportdata_be_energyvision_api_key.txt",),
    ),
    TransportdataBESource(
        source_uid=ENERGYVISION_TARIFFS_SOURCE_UID,
        dataset="EnergyVision Public Charging Network (AFIR / OCPI 2.2.1)",
        organization="EnergyVision",
        resource_kind="ocpi_2_1_1_tariffs",
        source_kind="afir_ocpi_tariffs",
        task_kind="parse_metadata_payload",
        endpoint_url=ENERGYVISION_TARIFFS_URL,
        dataset_url="https://transportdata.be/en/dataset/energyvision-public-charging-network-locations-afir-ocpi-2-2-1",
        resource_url="https://transportdata.be/en/dataset/energyvision-public-charging-network-locations-afir-ocpi-2-2-1",
        access_status="access_expected_api_key_required",
        usage_status="terms_pending",
        update_frequency="every_minute",
        credential_env=("TRANSPORTDATA_BE_ENERGYVISION_API_KEY", "WOLADEN_TRANSPORTDATA_BE_API_KEY"),
        credential_files=("transportdata_be_energyvision_api_key.txt",),
    ),
    TransportdataBESource(
        source_uid=ECO_MOVEMENT_STATIC_SOURCE_UID,
        dataset="Public charging infrastructure static dataset selected CPOs",
        organization="Eco-Movement",
        resource_kind="datexii_static_locations",
        source_kind="national_register_static",
        task_kind="parse_static_payload",
        endpoint_url=ECO_MOVEMENT_STATIC_URL,
        dataset_url="https://transportdata.be/en/dataset/afir-static-dataset-selected-cpos",
        resource_url="https://transportdata.be/en/dataset/afir-static-dataset-selected-cpos/resource/6b0535a5-1123-45aa-a0c8-ce4a61597859",
        access_status="token_not_available_public_token_not_tracked",
        usage_status="contract",
        update_frequency="daily",
        credential_env=("TRANSPORTDATA_BE_ECO_MOVEMENT_TOKEN",),
        credential_files=("transportdata_be_eco_movement_token.txt",),
    ),
    TransportdataBESource(
        source_uid=MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
        dataset="Public_charging_infrastructure_monta",
        organization="Monta",
        resource_kind="monta_afir_charge_points",
        source_kind="afir_static_dynamic_charge_points",
        task_kind="parse_dynamic_payload",
        endpoint_url=MONTA_AFIR_CHARGE_POINTS_URL,
        dataset_url="https://transportdata.be/en/dataset/public_charging_infrastructure_monta",
        resource_url="https://transportdata.be/dataset/public_charging_infrastructure_monta/resource/4e751ec3-0a40-4a08-9068-c0b2138cf2a8",
        access_status="access_expected_client_credentials_required",
        usage_status="contract",
        update_frequency="provider_api",
        credential_env=(
            "TRANSPORTDATA_BE_MONTA_CLIENT_ID",
            "TRANSPORTDATA_BE_MONTA_CLIENT_SECRET",
            "TRANSPORTDATA_BE_MONTA_BEARER_TOKEN",
        ),
        credential_files=(
            "transportdata_be_monta_client_id.txt",
            "transportdata_be_monta_client_secret.txt",
            "transportdata_be_monta_bearer_token.txt",
        ),
    ),
    TransportdataBESource(
        source_uid=ROAD_OCPI_LOCATIONS_SOURCE_UID,
        dataset="Road Public Charging Network",
        organization="Road",
        resource_kind="ocpi_locations_with_status",
        source_kind="afir_ocpi_locations_with_status",
        task_kind="parse_dynamic_payload",
        endpoint_url=ROAD_OCPI_LOCATIONS_URL,
        dataset_url="https://transportdata.be/en/dataset/road-public-charging-network",
        resource_url=ROAD_OCPI_LOCATIONS_URL,
        access_status="no_auth_json_verified_2026_06_07",
        usage_status="license_review_required",
        update_frequency="live_status_observed",
        open_static_bundle_status="license_review_required",
        private_dynamic_bundle_status="private_dynamic_after_archive_first_review",
    ),
    TransportdataBESource(
        source_uid=GROUP_INDIGO_STATIC_SOURCE_UID,
        dataset="indigo-open-data-evcharging",
        organization="Group INDIGO",
        resource_kind="datexii_static_locations",
        source_kind="national_register_static",
        task_kind="parse_static_payload",
        endpoint_url=GROUP_INDIGO_STATIC_URL,
        dataset_url="https://transportdata.be/en/dataset/indigo-open-data-evcharging",
        resource_url=GROUP_INDIGO_STATIC_URL,
        access_status="no_auth_static_xml_verified_2026_06_07",
        usage_status="license_review_required",
        update_frequency="static_dataset_metadata_modified_2026_06_05",
        open_static_bundle_status="license_review_required",
        private_dynamic_bundle_status="not_applicable_static_only",
    ),
)

SOURCE_REGISTRY_BY_UID = {source.source_uid: source for source in SOURCE_REGISTRY}


def load_secret(*, env_names: Iterable[str], filenames: Iterable[str]) -> str:
    for env_name in env_names:
        value = str(os.environ.get(env_name, "")).strip()
        if value:
            return value
    for filename in filenames:
        path = SECRET_DIR / filename
        if path.exists():
            value = path.read_text(encoding="utf-8").strip()
            if value:
                return value
    return ""


def secret_hint(source: TransportdataBESource) -> str:
    env_text = ", ".join(source.credential_env) or "none"
    file_text = ", ".join(f"secret/{name}" for name in source.credential_files) or "none"
    return f"missing credentials for {source.source_uid}; env: {env_text}; files: {file_text}"


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


def _opening_hours_text(opening_times: Any) -> str:
    if not isinstance(opening_times, dict):
        return ""
    if opening_times.get("twentyfourseven") is True or opening_times.get("twentyFourSeven") is True:
        return "24/7"
    regular_hours = opening_times.get("regular_hours") or opening_times.get("regularHours") or []
    parts: list[str] = []
    if isinstance(regular_hours, list):
        for item in regular_hours:
            if not isinstance(item, dict):
                continue
            weekday = _text(item.get("weekday"))
            begin = _text(item.get("period_begin") or item.get("periodBegin"))
            end = _text(item.get("period_end") or item.get("periodEnd"))
            if weekday and begin and end:
                parts.append(f"{weekday} {begin}-{end}")
    return "; ".join(parts)


def _max_connector_power_kw(connectors: Iterable[dict[str, Any]]) -> float | None:
    values: list[float] = []
    for connector in connectors:
        power_w = _float_or_none(connector.get("max_electric_power") or connector.get("maxElectricPower"))
        if power_w and power_w > 0:
            values.append(power_w / 1000.0)
            continue
        voltage = _float_or_none(connector.get("max_voltage") or connector.get("maxVoltage") or connector.get("voltage"))
        amperage = _float_or_none(connector.get("max_amperage") or connector.get("maxAmperage") or connector.get("amperage"))
        if voltage and amperage:
            power_type = _text(connector.get("power_type") or connector.get("powerType")).upper()
            if power_type == "AC_3_PHASE":
                multiplier = 3.0 if voltage <= 250 else 3 ** 0.5
            else:
                multiplier = 1.0
            values.append(voltage * amperage * multiplier / 1000.0)
    return max(values) if values else None


def station_id(source_uid: str, raw_id: Any) -> str:
    return f"be:{_safe_id(source_uid)}:{_safe_id(raw_id)}"


def charger_id(source_uid: str, raw_id: Any) -> str:
    return f"be:{_safe_id(source_uid)}:evse:{_safe_id(raw_id)}"


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


def _iter_unique_text(values: Iterable[Any]) -> Iterable[str]:
    seen: set[str] = set()
    for value in values:
        text = _text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        yield text


def _coordinates_from_value(value: dict[str, Any]) -> tuple[float | None, float | None]:
    coordinates = _dict_value(value, "coordinates", "geoCoordinates", "geo_coordinates")
    latitude = (
        _float_or_none(coordinates.get("latitude"))
        or _float_or_none(coordinates.get("lat"))
        or _float_or_none(value.get("latitude"))
        or _float_or_none(value.get("lat"))
    )
    longitude = (
        _float_or_none(coordinates.get("longitude"))
        or _float_or_none(coordinates.get("lon"))
        or _float_or_none(coordinates.get("lng"))
        or _float_or_none(value.get("longitude"))
        or _float_or_none(value.get("lon"))
        or _float_or_none(value.get("lng"))
    )
    return latitude, longitude


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


def _iter_array_objects(parser: _JsonObjectStream) -> Iterable[dict[str, Any]]:
    parser.expect("[")
    while True:
        if parser.peek() == "]":
            parser.expect("]")
            return
        value = parser.parse_value()
        if isinstance(value, dict):
            yield value
        if parser.peek() == ",":
            parser.expect(",")
            continue
        if parser.peek() == "]":
            parser.expect("]")
            return
        raise ValueError("expected_comma_or_end_of_json_array")


def _iter_payload_objects_from_text_stream(
    text_stream: TextIO,
    *,
    keys: tuple[str, ...],
) -> Iterable[dict[str, Any]]:
    parser = _JsonObjectStream(text_stream)
    first = parser.peek()
    if first == "[":
        yield from _iter_array_objects(parser)
        return
    if first != "{":
        raise ValueError("expected_json_array_or_object")

    parser.expect("{")
    while True:
        if parser.peek() == "}":
            parser.expect("}")
            return
        key = parser.parse_value()
        parser.expect(":")
        if key in keys:
            if parser.peek() == "[":
                yield from _iter_array_objects(parser)
            else:
                value = parser.parse_value()
                yield from _payload_items(value, *keys)
        else:
            parser.skip_value()
        if parser.peek() == ",":
            parser.expect(",")
            continue
        if parser.peek() == "}":
            parser.expect("}")
            return
        raise ValueError("expected_comma_or_end_of_json_object")


def _text_stream_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str,
) -> io.TextIOWrapper:
    return _text_stream_util(raw_stream, content_encoding=content_encoding)


def _binary_stream_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str,
) -> io.BufferedIOBase | gzip.GzipFile:
    return _binary_stream_util(raw_stream, content_encoding=content_encoding)


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


def _merge_tariff_price_fields(tariff_ids: Iterable[str], tariff_lookup: dict[str, dict[str, Any]]) -> dict[str, Any]:
    priced_rows = [
        tariff_lookup[tariff_id]
        for tariff_id in tariff_ids
        if tariff_id in tariff_lookup
        and (
            _text(tariff_lookup[tariff_id].get("price_display"))
            or _text(tariff_lookup[tariff_id].get("price_energy_eur_kwh_min"))
            or tariff_lookup[tariff_id].get("price_time_eur_min_min") is not None
        )
    ]
    if not priced_rows:
        return {}
    displays = list(dict.fromkeys(_text(row.get("price_display")) for row in priced_rows if _text(row.get("price_display"))))
    energy_values = [
        value
        for row in priced_rows
        for value in (_float_or_none(row.get("price_energy_eur_kwh_min")), _float_or_none(row.get("price_energy_eur_kwh_max")))
        if value is not None
    ]
    minute_values = [
        value
        for row in priced_rows
        for value in (_float_or_none(row.get("price_time_eur_min_min")), _float_or_none(row.get("price_time_eur_min_max")))
        if value is not None
    ]
    currency = next((_text(row.get("price_currency")) for row in priced_rows if _text(row.get("price_currency"))), "EUR")
    complex_tariff = len(displays) > 1 or any(bool(row.get("price_complex")) for row in priced_rows)
    source_text = "|".join(dict.fromkeys(_text(row.get("price_source_text")) for row in priced_rows if _text(row.get("price_source_text"))))
    return _price_fields_from_values(
        energy_values=energy_values,
        minute_values=minute_values,
        fixed_values=[],
        currency=currency,
        complex_tariff=complex_tariff,
        quality="source_ocpi_tariff",
        source_text=source_text,
    )


def energyvision_tariff_price_lookup(tariff_rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        _text(row.get("tariff_id")): row
        for row in tariff_rows
        if _text(row.get("tariff_id"))
    }


def attach_energyvision_tariff_prices(
    location_rows: Iterable[dict[str, Any]],
    tariff_rows: Iterable[dict[str, Any]],
) -> Iterable[dict[str, Any]]:
    tariff_lookup = energyvision_tariff_price_lookup(tariff_rows)
    for row in location_rows:
        enriched = dict(row)
        tariff_ids = [_text(value) for value in _text(row.get("tariff_ids")).split("|") if _text(value)]
        enriched.update(_merge_tariff_price_fields(tariff_ids, tariff_lookup))
        yield enriched


def ocpi_rows_from_location(
    location: dict[str, Any],
    *,
    source_uid: str,
    tariff_lookup: dict[str, dict[str, Any]] | None = None,
) -> Iterable[dict[str, Any]]:
    location_id = _text(location.get("id"))
    if not location_id:
        return
    operator = _dict_value(location, "operator")
    operator_name = _text(operator.get("name")) or _text(location.get("name"))
    party_id = _text(location.get("party_id"))
    sid = station_id(source_uid, f"{party_id}:{location_id}" if party_id else location_id)
    latitude, longitude = _coordinates_from_value(location)
    evses = _list_value(location, "evses")
    for evse in evses:
        if not isinstance(evse, dict):
            continue
        source_evse_id = _text(evse.get("evse_id")) or _text(evse.get("uid"))
        if not source_evse_id:
            continue
        connectors = [item for item in _list_value(evse, "connectors") if isinstance(item, dict)]
        tariff_ids = list(
            _iter_unique_text(
                value
                for item in connectors
                for value in (
                    item.get("tariff_ids")
                    if isinstance(item.get("tariff_ids"), list)
                    else [item.get("tariff_id")]
                )
            )
        )
        status = _text(evse.get("status")).upper() or "UNKNOWN"
        row = {
            "country_code": COUNTRY_CODE,
            "source_uid": source_uid,
            "operator_name": operator_name,
            "station_name": _text(location.get("name")) or _text(location.get("address")),
            "station_id": sid,
            "charger_id": charger_id(source_uid, source_evse_id),
            "source_station_id": location_id,
            "source_evse_id": source_evse_id,
            "connector_id": "|".join(_iter_unique_text(item.get("id") for item in connectors)),
            "source_status": status,
            "availability_status": STATUS_MAP.get(status, "unknown"),
            "source_observed_at": _text(evse.get("last_updated")) or _text(location.get("last_updated")),
            "date_updated": _text(evse.get("last_updated")) or _text(location.get("last_updated")),
            "address": _text(location.get("address")),
            "city": _text(location.get("city")),
            "postal_code": _text(location.get("postal_code")),
            "latitude": latitude,
            "longitude": longitude,
            "connector_count": len(connectors),
            "max_power_kw": _max_connector_power_kw(connectors),
            "connector_types": "|".join(_iter_unique_text(item.get("standard") for item in connectors)),
            "current_type": "|".join(_iter_unique_text(item.get("power_type") or item.get("powerType") for item in connectors)),
            "opening_hours": _opening_hours_text(location.get("opening_times") or location.get("openingTimes")),
            "tariff_ids": "|".join(tariff_ids),
            "raw_static": _json_text(
                {
                    "location": {key: value for key, value in location.items() if key != "evses"},
                    "evse": {key: value for key, value in evse.items() if key != "connectors"},
                    "connectors": connectors,
                }
            ),
        }
        if tariff_lookup:
            row.update(_merge_tariff_price_fields(tariff_ids, tariff_lookup))
        yield row


def energyvision_rows_from_location(
    location: dict[str, Any],
    *,
    tariff_lookup: dict[str, dict[str, Any]] | None = None,
) -> Iterable[dict[str, Any]]:
    yield from ocpi_rows_from_location(
        location,
        source_uid=ENERGYVISION_LOCATIONS_SOURCE_UID,
        tariff_lookup=tariff_lookup,
    )


def iter_energyvision_location_rows(payload: Any) -> Iterable[dict[str, Any]]:
    for location in _payload_items(payload, "locations"):
        yield from energyvision_rows_from_location(location)


def parse_energyvision_locations_payload(
    payload: Any,
    *,
    tariff_lookup: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    return [
        row
        for location in _payload_items(payload, "locations")
        for row in energyvision_rows_from_location(location, tariff_lookup=tariff_lookup)
    ]


def iter_energyvision_location_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        for location in _iter_payload_objects_from_text_stream(text_stream, keys=("data", "locations")):
            yield from energyvision_rows_from_location(location)
    finally:
        text_stream.detach()


def iter_road_location_rows(payload: Any) -> Iterable[dict[str, Any]]:
    for location in _payload_items(payload, "locations"):
        if _text(location.get("country_code")).upper() not in {"", COUNTRY_CODE}:
            continue
        yield from ocpi_rows_from_location(location, source_uid=ROAD_OCPI_LOCATIONS_SOURCE_UID)


def iter_road_location_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        for location in _iter_payload_objects_from_text_stream(text_stream, keys=("data", "locations")):
            if _text(location.get("country_code")).upper() not in {"", COUNTRY_CODE}:
                continue
            yield from ocpi_rows_from_location(location, source_uid=ROAD_OCPI_LOCATIONS_SOURCE_UID)
    finally:
        text_stream.detach()


def parse_road_locations_payload(payload: Any) -> list[dict[str, Any]]:
    return list(iter_road_location_rows(payload))


def tariff_row(tariff: dict[str, Any]) -> dict[str, Any] | None:
    tariff_id = _text(tariff.get("id"))
    if not tariff_id:
        return None
    row = {
        "country_code": COUNTRY_CODE,
        "source_uid": ENERGYVISION_TARIFFS_SOURCE_UID,
        "tariff_id": tariff_id,
        "currency": _text(tariff.get("currency")),
        "type": _text(tariff.get("type")),
        "last_updated": _text(tariff.get("last_updated")),
        "elements": _json_text(tariff.get("elements")),
        "raw_static": _json_text(tariff),
    }
    row.update(_ocpi_tariff_price_fields(tariff))
    return row


def iter_energyvision_tariff_rows(payload: Any) -> Iterable[dict[str, Any]]:
    for tariff in _payload_items(payload, "tariffs"):
        row = tariff_row(tariff)
        if row is not None:
            yield row


def parse_energyvision_tariffs_payload(payload: Any) -> list[dict[str, Any]]:
    return list(iter_energyvision_tariff_rows(payload))


def iter_energyvision_tariff_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        for tariff in _iter_payload_objects_from_text_stream(text_stream, keys=("data", "tariffs")):
            row = tariff_row(tariff)
            if row is not None:
                yield row
    finally:
        text_stream.detach()


def _reference_id(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("idG", "id", "value"):
            text = _text(value.get(key))
            if text:
                return text
    return _text(value)


def _iter_dicts_at_key(value: Any, key_name: str) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        for key, child in value.items():
            if key == key_name:
                if isinstance(child, list):
                    yield from (item for item in child if isinstance(item, dict))
                elif isinstance(child, dict):
                    yield child
            yield from _iter_dicts_at_key(child, key_name)
    elif isinstance(value, list):
        for child in value:
            yield from _iter_dicts_at_key(child, key_name)


def parse_eco_movement_static_payload(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for site in _iter_dicts_at_key(payload, "energyInfrastructureSite"):
        site_id = _reference_id(site.get("reference")) or _text(site.get("id")) or _text(site.get("idG"))
        operator_name = _text(site.get("operatorName") or site.get("name"))
        for station in _iter_dicts_at_key(site, "energyInfrastructureStation"):
            station_ref = _reference_id(station.get("reference")) or _text(station.get("id")) or site_id
            sid = station_id(ECO_MOVEMENT_STATIC_SOURCE_UID, station_ref or site_id)
            for refill_point in _iter_dicts_at_key(station, "refillPoint"):
                point = refill_point.get("aegiElectricChargingPoint")
                if not isinstance(point, dict):
                    point = refill_point
                evse_id = _reference_id(point.get("reference")) or _text(point.get("evseId") or point.get("id"))
                if not evse_id:
                    continue
                rows.append(
                    {
                        "country_code": COUNTRY_CODE,
                        "source_uid": ECO_MOVEMENT_STATIC_SOURCE_UID,
                        "operator_name": operator_name,
                        "station_id": sid,
                        "charger_id": charger_id(ECO_MOVEMENT_STATIC_SOURCE_UID, evse_id),
                        "source_site_id": site_id,
                        "source_station_id": station_ref,
                        "source_evse_id": evse_id,
                        "raw_static": _json_text(point),
                    }
                )
    return rows


def _xml_local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _xml_attr(element: ET.Element, name: str) -> str:
    return _text(element.attrib.get(name))


def _xml_first_descendant(element: ET.Element, name: str) -> ET.Element | None:
    for child in element.iter():
        if child is not element and _xml_local_name(child.tag) == name:
            return child
    return None


def _xml_text(element: ET.Element | None) -> str:
    if element is None:
        return ""
    return _text(element.text)


def _xml_first_text(element: ET.Element, name: str) -> str:
    return _xml_text(_xml_first_descendant(element, name))


def _xml_name_text(element: ET.Element | None) -> str:
    if element is None:
        return ""
    name = _xml_first_descendant(element, "name")
    if name is None:
        return ""
    return _xml_first_text(name, "value")


def _xml_float_text(element: ET.Element, name: str) -> float | None:
    return _float_or_none(_xml_first_text(element, name))


def _iter_xml_descendants(element: ET.Element, name: str) -> Iterable[ET.Element]:
    for child in element.iter():
        if child is not element and _xml_local_name(child.tag) == name:
            yield child


def _eco_xml_station_raw_id(site_id: str, station_ref: str) -> str:
    if not station_ref or station_ref == "1" or station_ref == site_id:
        return site_id
    return f"{site_id}:{station_ref}" if site_id else station_ref


def _datex_static_rows_from_xml_site(site: ET.Element, *, source_uid: str) -> Iterable[dict[str, Any]]:
    site_id = _xml_attr(site, "id") or _xml_first_text(site, "id")
    site_name = _xml_name_text(site)
    operator_name = _xml_name_text(_xml_first_descendant(site, "operator")) or site_name
    latitude = _xml_float_text(site, "latitude")
    longitude = _xml_float_text(site, "longitude")
    city = _xml_first_text(site, "city")
    postal_code = _xml_first_text(site, "postcode")
    address = _xml_first_text(site, "text")

    for station in _iter_xml_descendants(site, "energyInfrastructureStation"):
        station_ref = _xml_attr(station, "id") or site_id
        station_raw_id = _eco_xml_station_raw_id(site_id, station_ref)
        sid = station_id(source_uid, station_raw_id)
        auth_methods = list(
            _iter_unique_text(
                _xml_text(item)
                for item in _iter_xml_descendants(station, "authenticationAndIdentificationMethods")
            )
        )

        for refill_point in _iter_xml_descendants(station, "refillPoint"):
            evse_id = _xml_first_text(refill_point, "externalIdentifier") or _xml_attr(refill_point, "id")
            if not evse_id:
                continue
            connectors = list(_iter_xml_descendants(refill_point, "connector"))
            connector_types = list(
                _iter_unique_text(_xml_first_text(connector, "connectorType") for connector in connectors)
            )
            charging_modes = list(
                _iter_unique_text(_xml_first_text(connector, "chargingMode") for connector in connectors)
            )
            max_power_watts = [
                value
                for value in (_xml_float_text(connector, "maxPowerAtSocket") for connector in connectors)
                if value is not None
            ]
            max_power_kw = max(max_power_watts) / 1000.0 if max_power_watts else None
            raw_static = {
                "site_id": site_id,
                "site_name": site_name,
                "station_id": station_ref,
                "evse_id": evse_id,
                "connector_types": connector_types,
                "charging_modes": charging_modes,
                "authentication_methods": auth_methods,
            }
            yield {
                "country_code": COUNTRY_CODE,
                "source_uid": source_uid,
                "operator_name": operator_name,
                "station_id": sid,
                "charger_id": charger_id(source_uid, evse_id),
                "source_site_id": site_id,
                "source_station_id": station_raw_id,
                "source_evse_id": evse_id,
                "address": address,
                "city": city,
                "postal_code": postal_code,
                "latitude": latitude,
                "longitude": longitude,
                "connector_count": len(connectors),
                "max_power_kw": max_power_kw,
                "connector_types": "|".join(connector_types),
                "raw_static": _json_text(raw_static),
            }


def iter_eco_movement_static_rows_from_xml_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    binary_stream = _binary_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    for _event, element in ET.iterparse(binary_stream, events=("end",)):
        if _xml_local_name(element.tag) != "energyInfrastructureSite":
            continue
        yield from _datex_static_rows_from_xml_site(element, source_uid=ECO_MOVEMENT_STATIC_SOURCE_UID)
        element.clear()


def iter_eco_movement_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    binary_stream = _binary_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    buffered_stream = binary_stream if hasattr(binary_stream, "peek") else io.BufferedReader(binary_stream)
    first_non_ws = buffered_stream.peek(128).lstrip()[:1]
    if first_non_ws == b"<":
        yield from iter_eco_movement_static_rows_from_xml_binary_stream(buffered_stream)
        return

    text_stream = io.TextIOWrapper(buffered_stream, encoding="utf-8")
    try:
        payload = json.load(text_stream)
    finally:
        text_stream.detach()
    yield from parse_eco_movement_static_payload(payload)


def iter_group_indigo_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    binary_stream = _binary_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    for _event, element in ET.iterparse(binary_stream, events=("end",)):
        if _xml_local_name(element.tag) != "energyInfrastructureSite":
            continue
        yield from _datex_static_rows_from_xml_site(element, source_uid=GROUP_INDIGO_STATIC_SOURCE_UID)
        element.clear()


def monta_rows_from_charge_point(charge_point: dict[str, Any]) -> Iterable[dict[str, Any]]:
    charge_point_id = _text(
        charge_point.get("id")
        or charge_point.get("chargePointId")
        or charge_point.get("charge_point_id")
        or charge_point.get("uuid")
    )
    location = _dict_value(charge_point, "location", "address")
    operator = _dict_value(charge_point, "operator", "chargePointOperator", "charge_point_operator")
    station_raw_id = (
        _text(charge_point.get("siteId") or charge_point.get("site_id"))
        or _text(location.get("id"))
        or charge_point_id
    )
    sid = station_id(MONTA_AFIR_CHARGE_POINTS_SOURCE_UID, station_raw_id or charge_point_id)
    latitude, longitude = _coordinates_from_value(location or charge_point)
    connectors = [item for item in _list_value(charge_point, "connectors", "evses") if isinstance(item, dict)]
    observed_at = _text(
        charge_point.get("updatedAt")
        or charge_point.get("updated_at")
        or charge_point.get("lastUpdated")
        or charge_point.get("last_updated")
    )

    point_rows = connectors or [charge_point]
    for point in point_rows:
        evse_id = _text(
            point.get("evseId")
            or point.get("evse_id")
            or point.get("uid")
            or point.get("id")
            or charge_point.get("evseId")
            or charge_point.get("evse_id")
            or charge_point_id
        )
        if not evse_id:
            continue
        status = _text(
            point.get("status")
            or point.get("availability")
            or point.get("state")
            or charge_point.get("status")
            or charge_point.get("availability")
            or charge_point.get("state")
        ).upper() or "UNKNOWN"
        yield {
            "country_code": COUNTRY_CODE,
            "source_uid": MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
            "operator_name": _text(
                operator.get("name")
                or operator.get("operatorName")
                or charge_point.get("operatorName")
                or charge_point.get("operator_name")
            ),
            "station_id": sid,
            "charger_id": charger_id(MONTA_AFIR_CHARGE_POINTS_SOURCE_UID, evse_id),
            "source_station_id": station_raw_id or charge_point_id or evse_id,
            "source_evse_id": evse_id,
            "source_status": status,
            "availability_status": STATUS_MAP.get(status, "unknown"),
            "source_observed_at": (
                _text(point.get("updatedAt") or point.get("updated_at") or point.get("lastUpdated"))
                or observed_at
            ),
            "address": _text(location.get("address") or location.get("street") or charge_point.get("address")),
            "city": _text(location.get("city") or charge_point.get("city")),
            "postal_code": _text(
                location.get("zipCode")
                or location.get("postalCode")
                or location.get("postal_code")
                or charge_point.get("zipCode")
                or charge_point.get("postalCode")
            ),
            "latitude": latitude,
            "longitude": longitude,
            "connector_count": len(connectors),
            "max_power_kw": _max_connector_power_kw(connectors or [point]),
            "connector_types": "|".join(
                _iter_unique_text(
                    connector.get("standard") or connector.get("type") or connector.get("connectorType")
                    for connector in connectors
                )
            ),
            "raw_dynamic": _json_text(charge_point),
        }


def iter_monta_charge_point_rows(payload: Any) -> Iterable[dict[str, Any]]:
    for charge_point in _payload_items(payload, "chargePoints", "charge_points", "items"):
        yield from monta_rows_from_charge_point(charge_point)


def parse_monta_charge_points_payload(payload: Any) -> list[dict[str, Any]]:
    return list(iter_monta_charge_point_rows(payload))


def iter_monta_charge_point_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        for charge_point in _iter_payload_objects_from_text_stream(
            text_stream,
            keys=("data", "chargePoints", "charge_points", "items"),
        ):
            yield from monta_rows_from_charge_point(charge_point)
    finally:
        text_stream.detach()
