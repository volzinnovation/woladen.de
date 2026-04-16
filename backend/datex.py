from __future__ import annotations

import gzip
import json
import re
from datetime import datetime, timezone
from typing import Any

from .models import DynamicFact, PriceSnapshot

PAYMENT_METHOD_LABELS = {
    "creditcard": "Kreditkarte",
    "debitcard": "Debitkarte",
    "nfc": "NFC",
    "website": "Web",
    "paymentcardreader": "Kartenterminal",
    "paymentcardcontactless": "Kontaktlos",
    "otheradhocpaymentoption": "Ad-hoc-Zahlung",
    "contractbasedpaymentoption": "Vertragsbasiert",
}


def _is_missing(value: Any) -> bool:
    return value is None or value == ""


def normalize_evse_id(value: Any) -> str:
    if _is_missing(value):
        return ""
    return "".join(ch for ch in str(value).upper() if ch.isalnum())


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = str(value).strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def choose_latest_timestamp(values: list[str]) -> str:
    parsed: list[tuple[datetime, str]] = []
    for value in values:
        dt = parse_iso_datetime(value)
        if dt is not None:
            parsed.append((dt, value))
    if parsed:
        parsed.sort(key=lambda item: item[0])
        return parsed[-1][1]
    return values[-1] if values else ""


def normalize_occupancy_status(value: Any) -> str:
    if _is_missing(value):
        return ""
    return re.sub(r"[^A-Z]", "", str(value).upper())


def normalize_code_value(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def map_display_value(value: Any, mapping: dict[str, str]) -> str:
    code = normalize_code_value(value)
    if not code:
        return ""
    return mapping.get(code, "")


def decode_json_payload(content: bytes) -> dict[str, Any]:
    raw = gzip.decompress(content) if content[:2] == b"\x1f\x8b" else content
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("expected_json_object_payload")
    return payload


def iter_status_publications(payload: dict[str, Any]):
    candidate_containers: list[dict[str, Any]] = [payload]

    message_container = payload.get("messageContainer")
    if isinstance(message_container, dict):
        message_payload = message_container.get("payload")
        if isinstance(message_payload, dict):
            candidate_containers.append(message_payload)
        elif isinstance(message_payload, list):
            candidate_containers.extend(item for item in message_payload if isinstance(item, dict))

    direct_payload = payload.get("payload")
    if isinstance(direct_payload, dict):
        candidate_containers.append(direct_payload)
    elif isinstance(direct_payload, list):
        candidate_containers.extend(item for item in direct_payload if isinstance(item, dict))

    seen_container_ids: set[int] = set()
    for container in candidate_containers:
        container_id = id(container)
        if container_id in seen_container_ids:
            continue
        seen_container_ids.add(container_id)

        publication = container.get("aegiEnergyInfrastructureStatusPublication")
        if isinstance(publication, dict):
            yield publication


def iter_walk_nodes(value: Any):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from iter_walk_nodes(item)
    elif isinstance(value, list):
        for item in value:
            yield from iter_walk_nodes(item)


def normalize_datex_occupancy_status(
    status_value: Any,
    *,
    opening_status: Any = None,
    operation_status: Any = None,
    status_description: Any = None,
) -> tuple[str, str]:
    available = {"AVAILABLE", "FREE", "OPEN"}
    occupied = {"OCCUPIED", "CHARGING", "BLOCKED", "RESERVED", "INUSE"}
    out_of_order = {"OUTOFORDER", "OUTOFSERVICE", "INOPERATIVE", "FAULTED", "CLOSED", "OFFLINE"}
    explicit_unknown = {"UNKNOWN", "UNKNOW"}

    def classify_availability(value: Any) -> str:
        candidate = normalize_occupancy_status(value)
        if not candidate:
            return ""
        if candidate in available or "AVAILABLE" in candidate:
            return "free"
        if candidate in occupied or "OCCUP" in candidate or "CHARG" in candidate:
            return "occupied"
        if candidate in out_of_order or "OUTOF" in candidate or "FAULT" in candidate or "CLOSED" in candidate:
            return "out_of_order"
        if candidate in explicit_unknown:
            return "unknown"
        return ""

    def classify_operational(value: Any) -> str:
        candidate = normalize_occupancy_status(value)
        if not candidate:
            return ""
        if candidate in available or "AVAILABLE" in candidate:
            return "AVAILABLE"
        if candidate in occupied or "OCCUP" in candidate or "CHARG" in candidate:
            return "CHARGING"
        if (
            candidate in out_of_order
            or candidate in explicit_unknown
            or "OUTOF" in candidate
            or "FAULT" in candidate
            or "CLOSED" in candidate
            or "OFFLINE" in candidate
        ):
            return "UNKNOWN"
        return candidate

    availability_status = ""
    for raw_value in (status_value, opening_status, operation_status, status_description):
        availability_status = classify_availability(raw_value)
        if availability_status:
            break

    operational_status = classify_operational(operation_status)
    if not operational_status:
        for raw_value in (status_value, opening_status, status_description):
            operational_status = classify_operational(raw_value)
            if operational_status:
                break

    if availability_status == "unknown" and operational_status == "UNKNOWN":
        availability_status = "out_of_order"

    if not availability_status:
        if operational_status == "AVAILABLE":
            availability_status = "free"
        elif operational_status == "CHARGING":
            availability_status = "occupied"
        elif operational_status == "UNKNOWN":
            availability_status = "out_of_order"
        else:
            availability_status = "unknown"

    return availability_status, operational_status


def _merge_unique_text(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        lowered = text.lower()
        if lowered in seen:
            continue
        seen.add(lowered)
        result.append(text)
    return result


def _clean_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, list):
        cleaned_items = [_clean_json_value(item) for item in value]
        cleaned_items = [item for item in cleaned_items if item is not None]
        return cleaned_items or None
    if isinstance(value, dict):
        cleaned_dict = {
            str(key): cleaned
            for key, raw in value.items()
            if (cleaned := _clean_json_value(raw)) is not None
        }
        return cleaned_dict or None
    return value


def normalize_json_list(value: Any) -> list[Any]:
    cleaned = _clean_json_value(value)
    if cleaned is None:
        return []
    if isinstance(cleaned, list):
        return cleaned
    return [cleaned]


def merge_unique_json_lists(*values: Any) -> list[Any]:
    result: list[Any] = []
    seen: set[str] = set()
    for value in values:
        for item in normalize_json_list(value):
            marker = json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            if marker in seen:
                continue
            seen.add(marker)
            result.append(item)
    return result


def collect_datex_price_components(value: Any) -> dict[str, Any]:
    kwh_values: list[float] = []
    minute_values: list[float] = []
    currencies: list[str] = []
    payment_methods: list[str] = []
    complex_tariff = False

    for node in iter_walk_nodes(value):
        if not isinstance(node, dict):
            continue
        rate_lists = [node.get("energyRate"), node.get("energyRateUpdate")]
        for energy_rates in rate_lists:
            if not isinstance(energy_rates, list):
                continue
            for rate in energy_rates:
                if not isinstance(rate, dict):
                    continue
                currencies.extend(
                    str(item).strip() for item in (rate.get("applicableCurrency") or []) if str(item).strip()
                )
                for method in ((rate.get("payment") or {}).get("paymentMeans") or []):
                    mapped = map_display_value((method or {}).get("value"), PAYMENT_METHOD_LABELS)
                    if mapped:
                        payment_methods.append(mapped)
                for price in rate.get("energyPrice") or []:
                    if not isinstance(price, dict):
                        continue
                    try:
                        numeric_value = float(price.get("value"))
                    except (TypeError, ValueError):
                        continue
                    price_type = normalize_code_value(((price.get("priceType") or {}).get("value")) or "")
                    if price_type == "priceperkwh":
                        kwh_values.append(numeric_value)
                    elif price_type == "priceperminute":
                        minute_values.append(numeric_value)
                    else:
                        complex_tariff = True
                    if price.get("timeBasedApplicability"):
                        complex_tariff = True

    return {
        "kwh_values": kwh_values,
        "minute_values": minute_values,
        "currencies": _merge_unique_text(currencies),
        "payment_methods": _merge_unique_text(payment_methods),
        "complex_tariff": complex_tariff,
    }


def format_euro_amount(value: float) -> str:
    rounded = round(float(value) + 1e-9, 2)
    return f"{rounded:.2f}".replace(".", ",")


def _to_float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_price_scalar(value: float) -> str:
    return f"{float(value):.6f}".rstrip("0").rstrip(".")


def summarize_price_snapshot(price_components: dict[str, Any]) -> PriceSnapshot:
    kwh_values = [float(item) for item in price_components.get("kwh_values", [])]
    minute_values = [float(item) for item in price_components.get("minute_values", [])]
    currencies = [item for item in price_components.get("currencies", []) if item]
    currency = currencies[0] if currencies else ("EUR" if kwh_values or minute_values else "")
    complex_tariff = bool(price_components.get("complex_tariff"))

    if not kwh_values and not minute_values:
        return PriceSnapshot("", currency, "", "", None, None, "", complex_tariff)

    display = ""
    quality = ""
    kwh_min = min(kwh_values) if kwh_values else None
    kwh_max = max(kwh_values) if kwh_values else None
    minute_min = min(minute_values) if minute_values else None
    minute_max = max(minute_values) if minute_values else None

    if kwh_values and currency == "EUR":
        if complex_tariff or minute_values:
            display = f"ab {format_euro_amount(kwh_min)} €/kWh"
            quality = "from"
        elif abs(kwh_min - kwh_max) < 0.0001:
            display = f"{format_euro_amount(kwh_min)} €/kWh"
            quality = "exact"
        else:
            display = f"{format_euro_amount(kwh_min)}–{format_euro_amount(kwh_max)} €/kWh"
            quality = "range"
    elif minute_values and currency == "EUR":
        if complex_tariff or abs(minute_min - minute_max) >= 0.0001:
            display = f"ab {format_euro_amount(minute_min)} €/min"
            quality = "from"
        else:
            display = f"{format_euro_amount(minute_min)} €/min"
            quality = "exact"

    return PriceSnapshot(
        display=display,
        currency=currency,
        energy_eur_kwh_min=format_price_scalar(kwh_min) if kwh_min is not None and currency == "EUR" else "",
        energy_eur_kwh_max=format_price_scalar(kwh_max) if kwh_max is not None and currency == "EUR" else "",
        time_eur_min_min=round(minute_min, 6) if minute_min is not None and currency == "EUR" else None,
        time_eur_min_max=round(minute_max, 6) if minute_max is not None and currency == "EUR" else None,
        quality=quality,
        complex_tariff=complex_tariff,
    )


def choose_price_snapshot(*snapshots: PriceSnapshot) -> PriceSnapshot:
    for snapshot in reversed(snapshots):
        if snapshot.display or snapshot.energy_eur_kwh_min or snapshot.time_eur_min_min is not None:
            return snapshot
    return snapshots[0]


def summarize_simple_price_snapshot(*, energy_eur_kwh: Any = None, time_eur_min: Any = None) -> PriceSnapshot:
    energy_value = _to_float_or_none(energy_eur_kwh)
    time_value = _to_float_or_none(time_eur_min)
    return summarize_price_snapshot(
        {
            "kwh_values": [energy_value] if energy_value is not None else [],
            "minute_values": [time_value] if time_value is not None else [],
            "currencies": ["EUR"] if energy_value is not None or time_value is not None else [],
            "complex_tariff": energy_value is not None and time_value is not None,
        }
    )


def normalize_eliso_occupancy_status(availability_value: Any, operational_value: Any) -> tuple[str, str]:
    availability_code = normalize_code_value(availability_value)
    operational_code = normalize_code_value(operational_value)

    if operational_code == "nonoperational":
        return "out_of_order", "UNKNOWN"
    if availability_code == "notinuse":
        return "free", "AVAILABLE"
    if availability_code == "inuse":
        return "occupied", "CHARGING"
    if operational_code == "operational":
        return "unknown", "AVAILABLE"
    return "unknown", "UNKNOWN" if operational_code else ""


def lookup_evse_match(
    evse_id: str,
    evse_station_map: dict[str, dict[str, str]] | None,
) -> dict[str, str]:
    if not evse_station_map:
        return {}

    candidates = [evse_id]
    match = re.search(r"(\d+)$", evse_id)
    if match is not None:
        numeric_suffix = match.group(1)
        if numeric_suffix not in candidates:
            candidates.append(numeric_suffix)

    for candidate in candidates:
        resolved = evse_station_map.get(candidate)
        if resolved is not None:
            return resolved
    return {}


def extract_dynamic_facts(
    payload: dict[str, Any],
    provider_uid: str,
    site_station_map: dict[str, str],
    evse_station_map: dict[str, dict[str, str]] | None = None,
) -> list[DynamicFact]:
    seen: dict[tuple[str, str], DynamicFact] = {}

    generic_evses = payload.get("evses")
    if isinstance(generic_evses, list):
        for item in generic_evses:
            if not isinstance(item, dict):
                continue

            evse_id = normalize_evse_id(item.get("evseId"))
            if not evse_id:
                continue

            evse_match = lookup_evse_match(evse_id, evse_station_map)
            availability_status, operational_status = normalize_eliso_occupancy_status(
                item.get("availability_status"),
                item.get("operational_status"),
            )
            source_observed_at = choose_latest_timestamp([str(item.get("mobilithek_last_updated_dts") or "").strip()])

            fact = DynamicFact(
                provider_uid=provider_uid,
                site_id=str(evse_match.get("site_id") or ""),
                station_ref=str(evse_match.get("station_ref") or ""),
                evse_id=evse_id,
                station_id=str(evse_match.get("station_id") or "") or None,
                availability_status=availability_status,
                operational_status=operational_status,
                price=summarize_simple_price_snapshot(
                    energy_eur_kwh=item.get("adhoc_price"),
                    time_eur_min=item.get("blocking_fee"),
                ),
                next_available_charging_slots=[],
                supplemental_facility_status=[],
                source_observed_at=source_observed_at,
            )

            key = (fact.site_id, evse_id)
            previous = seen.get(key)
            if previous is None:
                seen[key] = fact
                continue

            prev_dt = parse_iso_datetime(previous.source_observed_at)
            next_dt = parse_iso_datetime(fact.source_observed_at)
            if prev_dt is None or (next_dt is not None and next_dt >= prev_dt):
                seen[key] = fact

    for publication in iter_status_publications(payload):
        for site_status in publication.get("energyInfrastructureSiteStatus") or []:
            site_id = str(((site_status.get("reference") or {}).get("idG")) or "").strip()
            if not site_id:
                continue

            site_price = summarize_price_snapshot(collect_datex_price_components(site_status))
            site_last_updated = str(site_status.get("lastUpdated") or "").strip()
            site_supplemental_facility_status = normalize_json_list(site_status.get("supplementalFacilityStatus"))

            for station_status in site_status.get("energyInfrastructureStationStatus") or []:
                station_ref = str(((station_status.get("reference") or {}).get("idG")) or "").strip()
                station_price = choose_price_snapshot(
                    site_price,
                    summarize_price_snapshot(collect_datex_price_components(station_status)),
                )
                station_last_updated = str(station_status.get("lastUpdated") or "").strip()
                station_supplemental_facility_status = merge_unique_json_lists(
                    site_supplemental_facility_status,
                    station_status.get("supplementalFacilityStatus"),
                )

                for refill_point_status in station_status.get("refillPointStatus") or []:
                    refill_point_price = choose_price_snapshot(
                        station_price,
                        summarize_price_snapshot(collect_datex_price_components(refill_point_status)),
                    )
                    refill_point_supplemental_facility_status = merge_unique_json_lists(
                        station_supplemental_facility_status,
                        refill_point_status.get("supplementalFacilityStatus"),
                    )
                    charging_point_status = (
                        refill_point_status.get("aegiElectricChargingPointStatus")
                        or refill_point_status.get("aegiRefillPointStatus")
                        or {}
                    )
                    evse_id = normalize_evse_id(((charging_point_status.get("reference") or {}).get("idG")) or "")
                    if not evse_id:
                        continue

                    availability_status, operational_status = normalize_datex_occupancy_status(
                        ((charging_point_status.get("status") or {}).get("value")),
                        opening_status=((charging_point_status.get("openingStatus") or {}).get("value")),
                        operation_status=((charging_point_status.get("operationStatus") or {}).get("value")),
                        status_description=charging_point_status.get("statusDescription"),
                    )
                    point_last_updated = str(charging_point_status.get("lastUpdated") or "").strip()
                    source_observed_at = choose_latest_timestamp(
                        [
                            value
                            for value in (point_last_updated, station_last_updated, site_last_updated)
                            if value
                        ]
                    )
                    price = choose_price_snapshot(
                        refill_point_price,
                        summarize_price_snapshot(collect_datex_price_components(charging_point_status)),
                    )
                    next_available_charging_slots = merge_unique_json_lists(
                        refill_point_status.get("nextAvailableChargingSlots"),
                        charging_point_status.get("nextAvailableChargingSlots"),
                    )
                    supplemental_facility_status = merge_unique_json_lists(
                        refill_point_supplemental_facility_status,
                        charging_point_status.get("supplementalFacilityStatus"),
                    )
                    fact = DynamicFact(
                        provider_uid=provider_uid,
                        site_id=site_id,
                        station_ref=station_ref,
                        evse_id=evse_id,
                        station_id=site_station_map.get(site_id)
                        or (lookup_evse_match(evse_id, evse_station_map).get("station_id") or None),
                        availability_status=availability_status,
                        operational_status=operational_status,
                        price=price,
                        next_available_charging_slots=next_available_charging_slots,
                        supplemental_facility_status=supplemental_facility_status,
                        source_observed_at=source_observed_at,
                    )

                    key = (site_id, evse_id)
                    previous = seen.get(key)
                    if previous is None:
                        seen[key] = fact
                        continue

                    prev_dt = parse_iso_datetime(previous.source_observed_at)
                    next_dt = parse_iso_datetime(fact.source_observed_at)
                    if prev_dt is None or (next_dt is not None and next_dt >= prev_dt):
                        seen[key] = fact

    return sorted(seen.values(), key=lambda item: (item.site_id, item.evse_id))
