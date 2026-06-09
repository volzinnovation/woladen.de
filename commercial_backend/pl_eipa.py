from __future__ import annotations

import html
import io
import json
import re
from html.parser import HTMLParser
from typing import Any, Iterable
from urllib.parse import urlencode

from .stream_utils import text_stream_from_binary_stream as _text_stream_util

COUNTRY_CODE = "PL"

DANE_GOV_API_DOC_URL = "https://api.dane.gov.pl/doc"
DANE_GOV_API_SPEC_URL = "https://api.dane.gov.pl/1.4/spec"
DANE_GOV_AFIR_SEARCH_URL = "https://api.dane.gov.pl/1.4/datasets?q=AFIR&per_page=20"
DANE_GOV_CHARGING_SEARCH_URL = (
    "https://api.dane.gov.pl/1.4/datasets?q=%C5%82adowania&per_page=20"
)

EIPA_HOME_URL = "https://eipa.udt.gov.pl/"
EIPA_READER_DOCS_URL = "https://eipa.udt.gov.pl/reader/docs"
EIPA_BROWSER_URL = "https://eipa.udt.gov.pl/browser/"
EIPA_STATS_URL = "https://eipa.udt.gov.pl/stats"
EIPA_BROWSER_PAGE_BASE_URL = "https://eipa.udt.gov.pl/browser/page"
EIPA_READER_EXPORT_BASE_URL = "https://eipa.udt.gov.pl/reader/export-data"

DANE_GOV_API_DOC_SOURCE_UID = "pl_dane_gov_api_doc"
DANE_GOV_API_SPEC_SOURCE_UID = "pl_dane_gov_api_spec"
DANE_GOV_AFIR_SEARCH_SOURCE_UID = "pl_dane_gov_afir_search"
DANE_GOV_CHARGING_SEARCH_SOURCE_UID = "pl_dane_gov_charging_search"
EIPA_HOME_SOURCE_UID = "pl_eipa_home"
EIPA_READER_DOCS_SOURCE_UID = "pl_eipa_reader_docs"
EIPA_BROWSER_INDEX_SOURCE_UID = "pl_eipa_browser_index"
EIPA_BROWSER_PROVINCE_SOURCE_UID = "pl_eipa_browser_province_pages"
EIPA_STATS_SOURCE_UID = "pl_eipa_stats"
EIPA_READER_JSON_FILES_SOURCE_UID = "pl_eipa_reader_json_files"
EIPA_READER_OPERATOR_SOURCE_UID = "pl_eipa_reader_operator_json"
EIPA_READER_POOL_SOURCE_UID = "pl_eipa_reader_pool_json"
EIPA_READER_STATION_SOURCE_UID = "pl_eipa_reader_station_json"
EIPA_READER_POINT_SOURCE_UID = "pl_eipa_reader_point_json"
EIPA_READER_DICTIONARY_SOURCE_UID = "pl_eipa_reader_dictionary_json"
EIPA_READER_DYNAMIC_SOURCE_UID = "pl_eipa_reader_dynamic_json"

EIPA_READER_FILE_KEYS = (
    "operator",
    "pool",
    "station",
    "point",
    "dictionary",
    "dynamic",
)
EIPA_READER_STATIC_FILE_KEYS = EIPA_READER_FILE_KEYS[:-1]
EIPA_READER_FILE_SOURCE_UIDS = {
    "operator": EIPA_READER_OPERATOR_SOURCE_UID,
    "pool": EIPA_READER_POOL_SOURCE_UID,
    "station": EIPA_READER_STATION_SOURCE_UID,
    "point": EIPA_READER_POINT_SOURCE_UID,
    "dictionary": EIPA_READER_DICTIONARY_SOURCE_UID,
    "dynamic": EIPA_READER_DYNAMIC_SOURCE_UID,
}
EIPA_READER_JSON_FILENAMES = tuple(f"{file_key}.json" for file_key in EIPA_READER_FILE_KEYS)

STATUS_MAP = {
    "wolny": "free",
    "wolne": "free",
    "zajety": "occupied",
    "zajete": "occupied",
    "niedostepny": "out_of_order",
    "nieznany": "unknown",
}

EIPA_DYNAMIC_PROVIDER_UID = "pl_eipa_reader_json"


def _strip_markup(value: Any) -> str:
    text = re.sub(r"<[^>]+>", "", str(value or ""))
    return " ".join(html.unescape(text).split())


def summarize_dane_dataset_search_payload(
    payload: dict[str, Any],
    *,
    limit: int = 10,
) -> dict[str, Any]:
    meta = payload.get("meta") or {}
    candidates = []
    for item in payload.get("data") or []:
        if not isinstance(item, dict):
            continue
        attrs = item.get("attributes") or {}
        if not isinstance(attrs, dict):
            attrs = {}
        candidates.append(
            {
                "id": str(item.get("id") or "").strip(),
                "title": _strip_markup(attrs.get("title")),
                "slug_or_url": _strip_markup(attrs.get("url") or attrs.get("slug")),
            }
        )
    return {
        "total": int(meta.get("count") or 0),
        "server_time": str(meta.get("server_time") or "").strip(),
        "shown_dataset_candidates": candidates[:limit],
    }


class _EipaProvinceParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._select_id = ""
        self._option_value: str | None = None
        self._option_text: list[str] = []
        self.provinces: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key: value or "" for key, value in attrs}
        if tag == "select":
            self._select_id = attr_map.get("id", "")
        if tag == "option" and self._select_id == "browser_filter_province":
            self._option_value = attr_map.get("value", "")
            self._option_text = []

    def handle_data(self, data: str) -> None:
        if self._option_value is not None:
            self._option_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "option" and self._option_value is not None:
            value = " ".join((self._option_value or "".join(self._option_text)).split())
            if value:
                self.provinces.append(value)
            self._option_value = None
            self._option_text = []
        if tag == "select":
            self._select_id = ""


def extract_eipa_provinces(html_text: str) -> list[str]:
    parser = _EipaProvinceParser()
    parser.feed(html_text)
    return parser.provinces


def count_eipa_browser_rows(html_text: str) -> int:
    return html_text.count('class="browser_show_details"')


def max_eipa_browser_page(html_text: str) -> int:
    pages = [
        int(value)
        for value in re.findall(
            r"/browser/page/(\d+)\?filter_type=province(?:&amp;|&)filter_value=",
            html_text,
        )
    ]
    return max(pages) if pages else 1


def build_eipa_browser_province_url(*, province: str, page: int) -> str:
    return f"{EIPA_BROWSER_PAGE_BASE_URL}/{page}?{urlencode({'filter_type': 'province', 'filter_value': province})}"


def build_eipa_reader_export_url(*, file_key: str, token: str) -> str:
    if file_key not in EIPA_READER_FILE_SOURCE_UIDS:
        raise KeyError(f"unknown_eipa_reader_file:{file_key}")
    return f"{EIPA_READER_EXPORT_BASE_URL}/{file_key}/{token}"


def redacted_eipa_reader_export_url(*, file_key: str) -> str:
    if file_key not in EIPA_READER_FILE_SOURCE_UIDS:
        raise KeyError(f"unknown_eipa_reader_file:{file_key}")
    return f"{EIPA_READER_EXPORT_BASE_URL}/{file_key}/<token-redacted>"


def _text(value: Any) -> str:
    return " ".join(html.unescape(re.sub(r"<[^>]+>", " ", str(value or ""))).split())


def _text_stream_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str,
) -> io.TextIOWrapper:
    return _text_stream_util(raw_stream, content_encoding=content_encoding)


def _safe_id(value: Any) -> str:
    text = _text(value).lower()
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "-" for ch in text)
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


def station_id_from_source_id(source_station_id: Any) -> str:
    return f"pl:eipa:{_safe_id(source_station_id)}"


def charger_id_from_evse_id(source_evse_id: Any) -> str:
    return f"pl:eipa:evse:{_safe_id(source_evse_id)}"


def station_id_from_reader_station_id(source_station_id: Any) -> str:
    return f"pl:eipa:station:{_safe_id(source_station_id)}"


def charger_id_from_reader_point_code(source_point_code: Any) -> str:
    return f"pl:eipa:point:{_safe_id(source_point_code)}"


def _first_power_kw(text: str) -> float | None:
    matches = re.findall(r"z\s+mocą\s+([0-9]+(?:[,.][0-9]+)?)\s*kW", text, flags=re.IGNORECASE)
    values = [_float_or_none(match) for match in matches]
    values = [value for value in values if value is not None]
    return max(values) if values else None


def _connector_types(text: str) -> str:
    connectors = []
    for match in re.findall(r"([A-Z0-9-]{3,})\s+z\s+mocą", text):
        if match not in connectors:
            connectors.append(match)
    return "|".join(connectors)


def iter_browser_rows(html_text: str) -> list[dict[str, Any]]:
    row_pattern = re.compile(
        r"<tr>\s*<td>\s*\d+\.\s*</td>\s*"
        r"<td><strong>(?P<label>.*?)<br/>(?P<station_id>PL-[^<]+)</strong>.*?</td>\s*"
        r"<td>\s*<a href=\"javascript:mapsNavigateTo\('(?P<lat>[^']+)',\s*'(?P<lon>[^']+)'\);\">"
        r"(?P<address>.*?)</a></td>\s*"
        r"<td>(?P<operator>.*?)</td>\s*</tr>\s*"
        r"<tr class=\"d-none\">(?P<details>.*?)</tr>",
        flags=re.IGNORECASE | re.DOTALL,
    )
    rows: list[dict[str, Any]] = []
    for match in row_pattern.finditer(html_text):
        source_station_id = _text(match.group("station_id"))
        if not source_station_id:
            continue
        detail_html = match.group("details")
        station_id = station_id_from_source_id(source_station_id)
        operator_name = _text(match.group("operator").split("<br/>", 1)[0])
        source_evses = re.findall(r"Punkt\s+(PL-[A-Z0-9-]+)", detail_html, flags=re.IGNORECASE)
        if not source_evses:
            source_evses = [source_station_id]
        for index, source_evse_id in enumerate(source_evses, start=1):
            yield {
                "country_code": COUNTRY_CODE,
                "source_uid": EIPA_BROWSER_PROVINCE_SOURCE_UID,
                "provider_uid": EIPA_BROWSER_PROVINCE_SOURCE_UID,
                "station_id": station_id,
                "charger_id": charger_id_from_evse_id(source_evse_id),
                "source_station_id": source_station_id,
                "source_evse_id": source_evse_id,
                "connector_id": str(index),
                "operator_name": operator_name,
                "station_name": _text(match.group("label")),
                "address": _text(match.group("address")),
                "city": "",
                "postal_code": "",
                "latitude": _float_or_none(match.group("lat")),
                "longitude": _float_or_none(match.group("lon")),
                "connector_count": 1,
                "connector_types": _connector_types(detail_html),
                "current_type": "electricity",
                "max_power_kw": _first_power_kw(detail_html),
                "opening_hours": _text(detail_html),
            }


def _data_list(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


def _id_lookup(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {str(row.get("id")): row for row in rows if row.get("id") is not None}


def _dictionary_lookup(dictionary_payload: dict[str, Any], key: str) -> dict[str, str]:
    rows = dictionary_payload.get(key) if isinstance(dictionary_payload, dict) else None
    if not isinstance(rows, list):
        return {}
    lookup: dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict) or row.get("id") is None:
            continue
        label = _text(row.get("name") or row.get("description"))
        if label:
            lookup[str(row["id"])] = label
    return lookup


def _labels(values: Any, lookup: dict[str, str]) -> list[str]:
    if values is None:
        return []
    if not isinstance(values, list):
        values = [values]
    labels: list[str] = []
    for value in values:
        label = lookup.get(str(value), _text(value))
        if label and label not in labels:
            labels.append(label)
    return labels


def _max_power_kw(*groups: Any) -> float | None:
    powers: list[float] = []
    for group in groups:
        if not isinstance(group, list):
            continue
        for item in group:
            if not isinstance(item, dict):
                continue
            value = _float_or_none(item.get("power"))
            if value is not None and value > 0:
                powers.append(value)
    return max(powers) if powers else None


def _reader_current_type(
    *,
    charging_solutions: Any,
    charging_mode_lookup: dict[str, str],
    connector_types: list[str],
) -> str:
    mode_labels: list[str] = []
    if isinstance(charging_solutions, list):
        for solution in charging_solutions:
            if not isinstance(solution, dict):
                continue
            mode_labels.extend(_labels(solution.get("mode"), charging_mode_lookup))
    haystack = " ".join([*mode_labels, *connector_types]).upper()
    if "DC" in haystack or "COMBO" in haystack or "CHADEMO" in haystack:
        return "DC"
    if "AC" in haystack:
        return "AC"
    return ""


def _address_from_pool(pool: dict[str, Any]) -> str:
    street_line = " ".join(part for part in (_text(pool.get("street")), _text(pool.get("house_number"))) if part)
    city_line = " ".join(part for part in (_text(pool.get("postal_code")), _text(pool.get("city"))) if part)
    return ", ".join(part for part in (street_line, city_line) if part)


def _operating_hours_text(pool: dict[str, Any], weekday_lookup: dict[str, str]) -> str:
    rows = pool.get("operating_hours")
    if not isinstance(rows, list) or not rows:
        return ""
    spans: list[str] = []
    full_day_weekdays = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        weekday = str(row.get("weekday"))
        from_time = _text(row.get("from_time"))
        to_time = _text(row.get("to_time"))
        if from_time == "00:00" and to_time in {"23:59", "24:00"}:
            full_day_weekdays.add(weekday)
        label = weekday_lookup.get(weekday, weekday)
        if label and from_time and to_time:
            spans.append(f"{label} {from_time}-{to_time}")
    if full_day_weekdays == {str(index) for index in range(1, 8)}:
        return "24/7"
    return "; ".join(spans)


def _max_timestamp(*values: Any) -> str:
    timestamps = [_text(value) for value in values if _text(value)]
    return max(timestamps) if timestamps else ""


def _reader_dynamic_availability_status(status: dict[str, Any]) -> str:
    service_status = str(status.get("status") if status.get("status") is not None else "").strip()
    availability = str(status.get("availability") if status.get("availability") is not None else "").strip()
    if service_status == "0":
        return "out_of_order"
    if availability == "1":
        return "free"
    if availability == "0":
        return "occupied"
    return "unknown"


def _reader_dynamic_price_fields(item: dict[str, Any]) -> dict[str, Any]:
    energy_values: list[float] = []
    source_texts: list[str] = []
    for price_row in item.get("prices") or []:
        if not isinstance(price_row, dict):
            continue
        unit = _text(price_row.get("unit")).casefold()
        value = _float_or_none(price_row.get("price"))
        if value is None:
            continue
        if unit in {"kwh", "kw/h", "kwh."}:
            energy_values.append(value)
            source_texts.append(_text(price_row.get("literal")) or _text(price_row.get("price")))
    if not energy_values:
        return {}
    energy_min = min(energy_values)
    energy_max = max(energy_values)
    display = (
        f"{_euro_amount(energy_min)} €/kWh"
        if abs(energy_min - energy_max) < 0.000001
        else f"{_euro_amount(energy_min)}-{_euro_amount(energy_max)} €/kWh"
    )
    return {
        "price_display": display,
        "price_currency": "EUR",
        "price_energy_eur_kwh_min": _price_scalar(energy_min),
        "price_energy_eur_kwh_max": _price_scalar(energy_max),
        "price_time_eur_min_min": None,
        "price_time_eur_min_max": None,
        "price_quality": "private_pl_eipa_dynamic_price",
        "price_complex": False,
        "price_source_text": "|".join(dict.fromkeys(source_texts[:5])),
    }


def iter_reader_dynamic_rows(payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    for item in _data_list(payload):
        source_evse_id = _text(item.get("code") or item.get("point_id"))
        if not source_evse_id:
            continue
        status = item.get("status") if isinstance(item.get("status"), dict) else {}
        row = {
            "country_code": COUNTRY_CODE,
            "source_uid": EIPA_READER_DYNAMIC_SOURCE_UID,
            "provider_uid": EIPA_DYNAMIC_PROVIDER_UID,
            "station_id": "",
            "charger_id": charger_id_from_reader_point_code(source_evse_id),
            "source_station_id": "",
            "source_evse_id": source_evse_id,
            "source_status": f"availability={status.get('availability', '')};status={status.get('status', '')}",
            "availability_status": _reader_dynamic_availability_status(status),
            "source_observed_at": _text(status.get("ts")),
        }
        row.update(_reader_dynamic_price_fields(item))
        yield row


def iter_reader_dynamic_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Iterable[dict[str, Any]]:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        yield from iter_reader_dynamic_rows(json.load(text_stream))
    finally:
        text_stream.detach()


def iter_reader_static_rows(
    *,
    operator_payload: dict[str, Any],
    pool_payload: dict[str, Any],
    station_payload: dict[str, Any],
    point_payload: dict[str, Any],
    dictionary_payload: dict[str, Any],
) -> Iterable[dict[str, Any]]:
    operators_by_id = _id_lookup(_data_list(operator_payload))
    pools_by_id = _id_lookup(_data_list(pool_payload))
    stations_by_id = _id_lookup(_data_list(station_payload))
    connector_lookup = _dictionary_lookup(dictionary_payload, "connector_interface")
    charging_mode_lookup = _dictionary_lookup(dictionary_payload, "charging_mode")
    auth_lookup = _dictionary_lookup(dictionary_payload, "station_authentication_method")
    payment_lookup = _dictionary_lookup(dictionary_payload, "station_payment_method")
    weekday_lookup = _dictionary_lookup(dictionary_payload, "weekday")

    for point in _data_list(point_payload):
        station = stations_by_id.get(str(point.get("station_id")))
        if not station:
            continue
        pool = pools_by_id.get(str(station.get("pool_id"))) or {}
        if pool and pool.get("charging") is False:
            continue
        connectors = point.get("connectors") if isinstance(point.get("connectors"), list) else []
        charging_solutions = (
            point.get("charging_solutions") if isinstance(point.get("charging_solutions"), list) else []
        )
        if not connectors and not charging_solutions:
            continue

        operator = operators_by_id.get(str(pool.get("operator_id"))) or {}
        source_station_id = _text(station.get("id"))
        source_point_code = _text(point.get("code") or point.get("id"))
        if not source_station_id or not source_point_code:
            continue

        connector_types: list[str] = []
        for connector in connectors:
            if not isinstance(connector, dict):
                continue
            for label in _labels(connector.get("interfaces"), connector_lookup):
                if label not in connector_types:
                    connector_types.append(label)

        max_power_kw = _max_power_kw(connectors, charging_solutions)
        current_type = _reader_current_type(
            charging_solutions=charging_solutions,
            charging_mode_lookup=charging_mode_lookup,
            connector_types=connector_types,
        )
        city = _text((station.get("location") or {}).get("city") if isinstance(station.get("location"), dict) else "")
        yield {
            "country_code": COUNTRY_CODE,
            "source_uid": EIPA_READER_POINT_SOURCE_UID,
            "provider_uid": _text(operator.get("code")) or EIPA_READER_POINT_SOURCE_UID,
            "station_id": station_id_from_reader_station_id(source_station_id),
            "charger_id": charger_id_from_reader_point_code(source_point_code),
            "source_station_id": source_station_id,
            "source_evse_id": source_point_code,
            "connector_id": source_point_code,
            "operator_name": _text(operator.get("name") or operator.get("short_name") or operator.get("code")),
            "station_name": _text(pool.get("name") or pool.get("code") or source_station_id),
            "address": _address_from_pool(pool),
            "city": city or _text(pool.get("city")),
            "postal_code": _text(pool.get("postal_code")),
            "latitude": _float_or_none(station.get("latitude")) or _float_or_none(pool.get("latitude")),
            "longitude": _float_or_none(station.get("longitude")) or _float_or_none(pool.get("longitude")),
            "connector_count": max(len(connectors), len(charging_solutions), 1),
            "connector_types": "|".join(connector_types),
            "current_type": current_type,
            "max_power_kw": max_power_kw,
            "opening_hours": _operating_hours_text(pool, weekday_lookup),
            "payment_methods": "|".join(_labels(station.get("payment_methods"), payment_lookup)),
            "auth_methods": "|".join(_labels(station.get("authentication_methods"), auth_lookup)),
            "helpdesk_phone": _text(operator.get("phone")),
            "date_updated": _max_timestamp(point.get("ts"), station.get("ts"), pool.get("ts")),
        }
