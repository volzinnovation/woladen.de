from __future__ import annotations

import csv
import io
import json
import re
import unicodedata
from typing import Any

from .stream_utils import text_stream_from_binary_stream as _text_stream_util

COUNTRY_CODE = "FR"

AFIR_SEARCH_PAGE_URL = "https://www.data.gouv.fr/datasets/search?q=AFIR"
AFIR_SEARCH_API_URL = "https://www.data.gouv.fr/api/2/datasets/search/?q=AFIR&page_size=20"
ECO_MOVEMENT_DATASET_API_URL = (
    "https://www.data.gouv.fr/api/1/datasets/"
    "public-charging-stations-for-electric-cars-from-several-cpos/"
)
BASE_NATIONALE_DATASET_PAGE_URL = (
    "https://transport.data.gouv.fr/datasets/"
    "fichier-consolide-des-bornes-de-recharge-pour-vehicules-electriques/"
)
BASE_NATIONALE_DATASET_API_URL = (
    "https://www.data.gouv.fr/api/1/datasets/"
    "base-nationale-des-irve-infrastructures-de-recharge-pour-vehicules-electriques/"
)
BASE_NATIONALE_STATIC_RESOURCE_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/eb76d20a-8501-400e-b336-d85724de5435"
)
ECO_MOVEMENT_DYNAMIC_RESOURCE_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/b20d2793-db42-4d6d-a0b4-e94bf5ee4279"
)
ECO_MOVEMENT_STATIC_RESOURCE_URL = (
    "https://www.data.gouv.fr/api/1/datasets/r/b11113db-875d-41c7-8673-0cf8ad43e917"
)

AFIR_SEARCH_SOURCE_UID = "fr_data_gouv_afir_search"
BASE_NATIONALE_STATIC_SOURCE_UID = "fr_base_nationale_irve_static"
BASE_NATIONALE_PROVIDER_UID = "fr_base_nationale_irve"
ECO_MOVEMENT_DYNAMIC_SOURCE_UID = "fr_eco_movement_afir_irve_dynamic"
ECO_MOVEMENT_STATIC_SOURCE_UID = "fr_eco_movement_afir_irve_static"
ECO_MOVEMENT_PROVIDER_UID = "fr_eco_movement_afir_irve"

STATUS_MAP = {
    "libre": "free",
    "occupe": "occupied",
    "occupee": "occupied",
    "occupé": "occupied",
    "reserve": "reserved",
    "reservee": "reserved",
    "encharge": "occupied",
    "en_charge": "occupied",
    "horsservice": "out_of_order",
    "hors_service": "out_of_order",
    "indisponible": "out_of_order",
    "unknown": "unknown",
    "inconnu": "unknown",
}

_FREE_PRICE_DISPLAY = "gratuit"
_PRICE_NONE_CODES = {"", "inconnu", "na", "non", "false", "true"}
_PRICE_COMPLEX_MARKERS = (
    " abonnement",
    " abonnes",
    " abonné",
    " abonnés",
    " entre ",
    " frais",
    " hors charge",
    " minimum",
    " par defaut",
    " par défaut",
    " puis ",
    " selon ",
    " suppl",
    "+",
)
_KWH_DIRECT_RE = re.compile(
    r"(?<![\d:])(?P<value>\d+(?:[,.]\d+)?)\s*(?:\u20ac|eur)?\s*(?:/|par|pour|au)?\s*k\s*w\s*/?\s*h",
    flags=re.IGNORECASE,
)
_KWH_CONTEXT_RE = re.compile(
    r"(?<![\d:])(?P<value>\d+(?:[,.]\d+)?)(?!\s*:)[^,;\n]{0,48}(?:prix\s+au|par|/)\s*k\s*w\s*/?\s*h",
    flags=re.IGNORECASE,
)
_KWH_CENTS_RE = re.compile(
    r"(?<![\d:])(?P<value>\d+(?:[,.]\d+)?)\s*(?:cts?|centimes?)\s*/?\s*k\s*w\s*/?\s*h",
    flags=re.IGNORECASE,
)
_MINUTE_RE = re.compile(
    r"(?<![\d:])(?P<value>\d+(?:[,.]\d+)?)\s*(?:\u20ac|eur)?\s*(?:/|par)?\s*(?:min|minute)",
    flags=re.IGNORECASE,
)
_HOUR_RE = re.compile(
    r"(?<![\d:])(?P<value>\d+(?:[,.]\d+)?)\s*(?:\u20ac|eur)?\s*(?:/|par)?\s*(?:h|heure)",
    flags=re.IGNORECASE,
)
_BARE_EURO_RE = re.compile(r"\s*(?P<value>\d+(?:[,.]\d+)?)\s*(?:\u20ac|eur)?\s*", flags=re.IGNORECASE)


def summarize_afir_search_payload(payload: dict[str, Any], *, limit: int = 10) -> dict[str, Any]:
    items = []
    for item in payload.get("data") or []:
        if not isinstance(item, dict):
            continue
        organization = item.get("organization") or {}
        items.append(
            {
                "id": str(item.get("id") or "").strip(),
                "slug": str(item.get("slug") or "").strip(),
                "title": str(item.get("title") or "").strip(),
                "organization": str(organization.get("name") or "").strip()
                if isinstance(organization, dict)
                else "",
                "access_type": str(item.get("access_type") or "").strip(),
                "license": str(item.get("license") or "").strip(),
                "resource_count": int((item.get("resources") or {}).get("total") or 0)
                if isinstance(item.get("resources"), dict)
                else len(item.get("resources") or []),
                "page": str(item.get("page") or "").strip(),
            }
        )
    return {
        "total": int(payload.get("total") or 0),
        "page": int(payload.get("page") or 0),
        "page_size": int(payload.get("page_size") or 0),
        "shown_provider_candidates": items[:limit],
    }


def _text(value: Any) -> str:
    return str(value or "").strip()


def _safe_id(value: Any) -> str:
    text = _text(value).lower()
    cleaned = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "-" for ch in text)
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "unknown"


def _normalized_code(value: Any) -> str:
    text = unicodedata.normalize("NFKD", _text(value).casefold())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return "".join(ch for ch in text if ch.isalnum() or ch == "_")


def _float_or_none(value: Any) -> float | None:
    text = _text(value).replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _bool_text(value: Any) -> bool:
    return _text(value).casefold() in {"true", "1", "yes", "oui", "vrai"}


def _price_float_or_none(value: Any) -> float | None:
    text = str(value if value is not None else "").strip().replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _unique_prices(values: list[float]) -> list[float]:
    result: list[float] = []
    for value in values:
        if not any(abs(value - existing) < 0.000001 for existing in result):
            result.append(value)
    return result


def _price_scalar(value: float) -> str:
    return f"{float(value):.6f}".rstrip("0").rstrip(".")


def _euro_amount(value: float) -> str:
    return f"{round(float(value) + 1e-9, 2):.2f}".replace(".", ",")


def _normalize_price_text(value: Any) -> str:
    text = _text(value)
    text = " ".join(text.split())
    return text


def _is_non_price_text(text: str) -> bool:
    normalized = _normalized_code(text)
    if normalized in _PRICE_NONE_CODES:
        return True
    if text.casefold().startswith(("http://", "https://")):
        return True
    generic = text.casefold()
    return "les tarifs de recharge peuvent varier" in generic and "consulter directement" in generic


def _looks_like_price_text(text: str) -> bool:
    normalized = _normalized_code(text)
    return any(
        marker in normalized
        for marker in (
            "abonnement",
            "cts",
            "eur",
            "euro",
            "frais",
            "gratuit",
            "heure",
            "kwh",
            "minute",
            "prix",
            "tarif",
        )
    ) or "\u20ac" in text


def _price_text_has_complex_markers(text: str) -> bool:
    folded = text.casefold()
    normalized = _normalized_code(text)
    return any(marker in folded for marker in _PRICE_COMPLEX_MARKERS) or "par_defaut" in normalized


def _price_fields_from_json_tarification(payload: Any, source_text: str) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    energy_value = _price_float_or_none(
        payload.get("energyPrice") if payload.get("energyPrice") is not None else payload.get("energy_price")
    )
    if energy_value is None:
        return None
    complex_tariff = bool(
        payload.get("matrix")
        or payload.get("matrixOSF")
        or payload.get("hasDynamicTariff")
        or _price_float_or_none(payload.get("fixedPrice")) not in (None, 0.0)
        or _price_float_or_none(payload.get("minimumBilling")) not in (None, 0.0)
    )
    return {
        "price_display": ("ab " if complex_tariff else "") + f"{_euro_amount(energy_value)} \u20ac/kWh",
        "price_currency": "EUR",
        "price_energy_eur_kwh_min": _price_scalar(energy_value),
        "price_energy_eur_kwh_max": _price_scalar(energy_value),
        "price_time_eur_min_min": None,
        "price_time_eur_min_max": None,
        "price_quality": "source_tarification_json_complex" if complex_tariff else "source_tarification_json_exact",
        "price_complex": complex_tariff,
        "price_source_text": source_text,
    }


def _price_fields_from_static_record(record: dict[str, Any]) -> dict[str, Any]:
    source_text = _normalize_price_text(record.get("tarification"))
    gratuit = _bool_text(record.get("gratuit"))

    if source_text.startswith("{") or source_text.startswith("["):
        try:
            parsed = json.loads(source_text)
        except json.JSONDecodeError:
            parsed = None
        json_fields = _price_fields_from_json_tarification(parsed, source_text)
        if json_fields is not None:
            return json_fields

    if _is_non_price_text(source_text):
        source_text = ""

    energy_values: list[float] = []
    minute_values: list[float] = []

    for match in _KWH_CENTS_RE.finditer(source_text):
        value = _price_float_or_none(match.group("value"))
        if value is not None:
            energy_values.append(value / 100)
    for pattern in (_KWH_DIRECT_RE, _KWH_CONTEXT_RE):
        for match in pattern.finditer(source_text):
            value = _price_float_or_none(match.group("value"))
            if value is not None:
                energy_values.append(value)
    for match in _MINUTE_RE.finditer(source_text):
        value = _price_float_or_none(match.group("value"))
        if value is not None:
            minute_values.append(value)
    for match in _HOUR_RE.finditer(source_text):
        value = _price_float_or_none(match.group("value"))
        if value is not None:
            minute_values.append(value / 60)

    bare_match = _BARE_EURO_RE.fullmatch(source_text)
    if bare_match is not None:
        value = _price_float_or_none(bare_match.group("value"))
        if value is not None:
            energy_values.append(value)

    energy_values = _unique_prices(energy_values)
    minute_values = _unique_prices(minute_values)
    has_numeric_price = bool(energy_values or minute_values)
    text_says_free = "gratuit" in _normalized_code(source_text)
    free_without_numeric_price = (gratuit or text_says_free) and not has_numeric_price

    if not (source_text or gratuit):
        return {}
    if not has_numeric_price and not free_without_numeric_price and not _looks_like_price_text(source_text):
        return {"price_source_text": source_text} if source_text else {}

    complex_tariff = (
        len(energy_values) > 1
        or len(minute_values) > 1
        or bool(energy_values and minute_values)
        or _price_text_has_complex_markers(source_text)
    )
    if free_without_numeric_price:
        display = source_text if source_text else _FREE_PRICE_DISPLAY
        quality = "source_tarification_free"
        currency = "EUR"
        energy_min = energy_max = "0"
        minute_min = minute_max = None
        complex_tariff = False
    else:
        currency = "EUR" if has_numeric_price or "\u20ac" in source_text or "eur" in source_text.casefold() else ""
        energy_min_value = min(energy_values) if energy_values else None
        energy_max_value = max(energy_values) if energy_values else None
        minute_min = round(min(minute_values), 6) if minute_values else None
        minute_max = round(max(minute_values), 6) if minute_values else None
        energy_min = _price_scalar(energy_min_value) if energy_min_value is not None and currency == "EUR" else ""
        energy_max = _price_scalar(energy_max_value) if energy_max_value is not None and currency == "EUR" else ""
        if energy_min_value is not None and currency == "EUR":
            if complex_tariff:
                display = f"ab {_euro_amount(energy_min_value)} \u20ac/kWh"
            elif abs(energy_min_value - (energy_max_value or energy_min_value)) < 0.000001:
                display = f"{_euro_amount(energy_min_value)} \u20ac/kWh"
            else:
                display = f"{_euro_amount(energy_min_value)}-{_euro_amount(energy_max_value or energy_min_value)} \u20ac/kWh"
        elif minute_min is not None and currency == "EUR":
            display = f"ab {_euro_amount(minute_min)} \u20ac/min" if complex_tariff else f"{_euro_amount(minute_min)} \u20ac/min"
        else:
            display = source_text
        if has_numeric_price:
            quality = "source_tarification_complex" if complex_tariff else "source_tarification_exact"
        else:
            quality = "source_tarification_text"

    fields: dict[str, Any] = {
        "price_display": display,
        "price_currency": currency,
        "price_energy_eur_kwh_min": energy_min,
        "price_energy_eur_kwh_max": energy_max,
        "price_time_eur_min_min": minute_min,
        "price_time_eur_min_max": minute_max,
        "price_quality": quality,
        "price_complex": complex_tariff,
    }
    if source_text:
        fields["price_source_text"] = source_text
    return {key: value for key, value in fields.items() if value not in ("", None)}


def _coordinates(value: Any) -> tuple[float | None, float | None]:
    text = _text(value)
    if not text:
        return None, None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None, None
    if not isinstance(parsed, list) or len(parsed) < 2:
        return None, None
    lon = _float_or_none(parsed[0])
    lat = _float_or_none(parsed[1])
    return lat, lon


def station_id_from_source_id(source_station_id: Any) -> str:
    return f"fr:irve:{_safe_id(source_station_id)}"


def charger_id_from_evse_id(source_evse_id: Any) -> str:
    return f"fr:irve:evse:{_safe_id(source_evse_id)}"


def _text_stream_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str,
    encoding: str = "utf-8-sig",
) -> io.TextIOWrapper:
    return _text_stream_util(raw_stream, content_encoding=content_encoding, encoding=encoding, newline="")


def static_row_from_csv_record(
    record: dict[str, Any],
    *,
    source_uid: str = ECO_MOVEMENT_STATIC_SOURCE_UID,
    provider_uid: str = ECO_MOVEMENT_PROVIDER_UID,
) -> dict[str, Any] | None:
    source_evse_id = _text(record.get("id_pdc_itinerance"))
    if not source_evse_id:
        return None
    source_station_id = (
        _text(record.get("id_station_itinerance"))
        or _text(record.get("id_station_local"))
        or source_evse_id
    )
    lat, lon = _coordinates(record.get("coordonneesXY"))
    connector_types = []
    for field_name, connector_label in (
        ("prise_type_ef", "EF"),
        ("prise_type_2", "Type 2"),
        ("prise_type_combo_ccs", "CCS"),
        ("prise_type_chademo", "CHAdeMO"),
        ("prise_type_autre", "Other"),
    ):
        if _bool_text(record.get(field_name)):
            connector_types.append(connector_label)
    row = {
        "country_code": COUNTRY_CODE,
        "source_uid": source_uid,
        "provider_uid": provider_uid,
        "station_id": station_id_from_source_id(source_station_id),
        "charger_id": charger_id_from_evse_id(source_evse_id),
        "source_station_id": source_station_id,
        "source_evse_id": source_evse_id,
        "operator_name": _text(record.get("nom_operateur")) or _text(record.get("nom_amenageur")),
        "station_name": _text(record.get("nom_station")),
        "address": _text(record.get("adresse_station")),
        "city": "",
        "postal_code": "",
        "latitude": lat,
        "longitude": lon,
        "connector_count": len(connector_types) or 1,
        "connector_types": "|".join(connector_types),
        "current_type": "",
        "max_power_kw": _float_or_none(record.get("puissance_nominale")),
        "payment_methods": "|".join(
            label
            for field_name, label in (
                ("paiement_cb", "card"),
                ("paiement_acte", "ad_hoc"),
                ("paiement_autre", "other"),
            )
            if _bool_text(record.get(field_name))
        ),
        "opening_hours": _text(record.get("horaires")),
        "helpdesk_phone": _text(record.get("telephone_operateur")),
        "date_updated": _text(record.get("date_maj")),
    }
    row.update(_price_fields_from_static_record(record))
    return row


def dynamic_row_from_csv_record(record: dict[str, Any]) -> dict[str, Any] | None:
    source_evse_id = _text(record.get("id_pdc_itinerance"))
    if not source_evse_id:
        return None
    source_status = _text(record.get("etat_pdc"))
    occupancy = _text(record.get("occupation_pdc"))
    service_code = _normalized_code(source_status)
    occupancy_code = _normalized_code(occupancy)
    if service_code and service_code not in {"enservice", "en_service"}:
        availability_status = STATUS_MAP.get(service_code, "unknown")
    else:
        availability_status = STATUS_MAP.get(occupancy_code, "unknown")
    return {
        "country_code": COUNTRY_CODE,
        "source_uid": ECO_MOVEMENT_DYNAMIC_SOURCE_UID,
        "provider_uid": ECO_MOVEMENT_PROVIDER_UID,
        "station_id": "",
        "charger_id": charger_id_from_evse_id(source_evse_id),
        "source_station_id": "",
        "source_evse_id": source_evse_id,
        "source_status": source_status or occupancy,
        "availability_status": availability_status,
        "source_observed_at": _text(record.get("horodatage")),
    }


def iter_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
    source_uid: str = ECO_MOVEMENT_STATIC_SOURCE_UID,
    provider_uid: str = ECO_MOVEMENT_PROVIDER_UID,
) -> Any:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        for record in csv.DictReader(text_stream):
            row = static_row_from_csv_record(
                record,
                source_uid=source_uid,
                provider_uid=provider_uid,
            )
            if row is not None:
                yield row
    finally:
        text_stream.detach()


def iter_base_nationale_static_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Any:
    yield from iter_static_rows_from_binary_stream(
        raw_stream,
        content_encoding=content_encoding,
        source_uid=BASE_NATIONALE_STATIC_SOURCE_UID,
        provider_uid=BASE_NATIONALE_PROVIDER_UID,
    )


def iter_dynamic_rows_from_binary_stream(
    raw_stream: io.BufferedIOBase,
    *,
    content_encoding: str = "",
) -> Any:
    text_stream = _text_stream_from_binary_stream(raw_stream, content_encoding=content_encoding)
    try:
        for record in csv.DictReader(text_stream):
            row = dynamic_row_from_csv_record(record)
            if row is not None:
                yield row
    finally:
        text_stream.detach()
