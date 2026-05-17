#!/usr/bin/env python3
"""Build an hourly occupied-EVSE bar chart for one station from local AFIR archives."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tarfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig, load_env_file
from backend.datex import decode_json_payload, extract_dynamic_facts, normalize_evse_id, parse_iso_datetime
from backend.loaders import load_evse_matches, load_site_matches, load_station_records
from backend.models import StationRecord

DEFAULT_DAYS = 7
DEFAULT_OUTPUT_DIR = REPO_ROOT / "analysis" / "output"
LOCAL_ARCHIVE_ENV_FILE_KEYS = frozenset(
    {
        "WOLADEN_LIVE_ARCHIVE_DIR",
        "WOLADEN_LIVE_ARCHIVE_TIMEZONE",
    }
)


@dataclass(frozen=True)
class StationIdentifiers:
    station: StationRecord
    provider_uids: set[str]
    site_ids: set[str]
    station_refs: set[str]
    evse_ids: set[str]


def _parse_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def default_end_date() -> date:
    return datetime.now(berlin_timezone_for_day(date.today())).date() - timedelta(days=1)


def date_window(end_date: date, days: int) -> list[date]:
    if days < 1:
        raise SystemExit("--days must be at least 1")
    return [end_date - timedelta(days=offset) for offset in range(days - 1, -1, -1)]


def _slugify(value: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9._-]+", "-", value.strip()).strip("-")
    return text or "station"


def _normalize_station_reference(value: str) -> str:
    text = value.strip()
    match = re.search(r"[?&]station=([A-Za-z0-9_-]+)", text)
    if match:
        return match.group(1)
    return text


def _station_search_text(station: StationRecord) -> str:
    return " ".join(
        [
            station.station_id,
            station.operator,
            station.address,
            station.postcode,
            station.city,
        ]
    ).lower()


def resolve_station(
    stations: list[StationRecord],
    *,
    station_reference: str | None,
    query: str | None,
    city: str | None,
) -> StationRecord:
    if station_reference:
        normalized = _normalize_station_reference(station_reference)
        matches = [station for station in stations if station.station_id == normalized]
        if len(matches) == 1:
            return matches[0]
        raise SystemExit(f"Station id not found: {normalized}")

    if not query:
        raise SystemExit("Pass either --station or --query.")

    query_parts = [part.lower() for part in query.split() if part.strip()]
    city_text = city.strip().lower() if city else ""
    matches = []
    for station in stations:
        haystack = _station_search_text(station)
        if city_text and station.city.lower() != city_text:
            continue
        if all(part in haystack for part in query_parts):
            matches.append(station)

    if len(matches) == 1:
        return matches[0]

    if not matches:
        raise SystemExit(f"No station matched query: {query}")

    preview = "\n".join(
        f"- {station.station_id}: {station.operator}, {station.address}, {station.city}"
        for station in matches[:20]
    )
    suffix = "" if len(matches) <= 20 else f"\n... {len(matches) - 20} more"
    raise SystemExit(f"Query matched {len(matches)} stations. Use --station with one id:\n{preview}{suffix}")


def build_station_identifiers(config: AppConfig, station: StationRecord) -> StationIdentifiers:
    station_catalog_path = config.full_chargers_csv_path or config.chargers_csv_path
    provider_uids: set[str] = set()
    site_ids: set[str] = set()
    station_refs: set[str] = set()
    evse_ids: set[str] = set()

    for match in load_site_matches(config.site_match_path, station_catalog_path):
        if match.station_id == station.station_id:
            provider_uids.add(match.provider_uid)
            site_ids.add(match.site_id)

    for match in load_evse_matches(station_catalog_path, config.site_match_path):
        if match.station_id == station.station_id:
            provider_uids.add(match.provider_uid)
            site_ids.add(match.site_id)
            station_refs.add(match.station_ref)
            evse_ids.add(match.evse_id)

    return StationIdentifiers(
        station=station,
        provider_uids=provider_uids,
        site_ids=site_ids,
        station_refs=station_refs,
        evse_ids=evse_ids,
    )


def berlin_timezone_for_day(target_date: date) -> tzinfo:
    """Return the Berlin timezone, with a fixed-offset fallback for systems without tzdata."""

    try:
        return ZoneInfo("Europe/Berlin")
    except ZoneInfoNotFoundError:
        pass

    def last_sunday(year: int, month: int) -> date:
        current = date(year, month + 1, 1) - timedelta(days=1)
        while current.weekday() != 6:
            current -= timedelta(days=1)
        return current

    # Good enough for day-level archive windows: DST is active between the last
    # Sunday in March and the last Sunday in October.
    dst_start = last_sunday(target_date.year, 3)
    dst_end = last_sunday(target_date.year, 10)
    if dst_start <= target_date < dst_end:
        return timezone(timedelta(hours=2), "CEST")
    return timezone(timedelta(hours=1), "CET")


def archive_path_for_date(config: AppConfig, target_date: date) -> Path:
    return config.archive_dir / f"live-provider-responses-{target_date.isoformat()}.tgz"


def _dict_items(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, dict):
        return [value]
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _reference_id(value: Any) -> str:
    if isinstance(value, dict):
        for key in ("idG", "id"):
            text = str(value.get(key) or "").strip()
            if text:
                return text
    return str(value or "").strip()


def _site_status_matches_identifiers(site_status: dict[str, Any], identifiers: StationIdentifiers) -> bool:
    site_id = _reference_id(site_status.get("reference"))
    if site_id and site_id in identifiers.site_ids:
        return True

    for station_status in _dict_items(site_status.get("energyInfrastructureStationStatus")):
        station_ref = _reference_id(station_status.get("reference"))
        if station_ref and station_ref in identifiers.station_refs:
            return True

        for refill_point_status in _dict_items(station_status.get("refillPointStatus")):
            charging_point_status = (
                refill_point_status.get("aegiElectricChargingPointStatus")
                or refill_point_status.get("aegiRefillPointStatus")
                or refill_point_status
            )
            if not isinstance(charging_point_status, dict):
                continue
            evse_id = normalize_evse_id(_reference_id(charging_point_status.get("reference")))
            if evse_id and evse_id in identifiers.evse_ids:
                return True

    return False


def prune_payload_to_station(payload: dict[str, Any], identifiers: StationIdentifiers) -> dict[str, Any]:
    """Trim decoded live payloads before running the generic DATEX extractor.

    Hugging Face daily archives store provider snapshots as large JSONL
    records. A single matching snapshot can contain tens of thousands of EVSEs,
    while this chart only needs one station. Mutating the decoded payload keeps
    backend status normalization unchanged but avoids materializing facts for
    unrelated stations.
    """

    def prune(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                prune(item)
            return
        if not isinstance(value, dict):
            return

        generic_evses = value.get("evses")
        if isinstance(generic_evses, list) and identifiers.evse_ids:
            value["evses"] = [
                item
                for item in generic_evses
                if isinstance(item, dict) and normalize_evse_id(item.get("evseId")) in identifiers.evse_ids
            ]

        site_statuses = value.get("energyInfrastructureSiteStatus")
        if site_statuses:
            filtered_statuses = [
                site_status
                for site_status in _dict_items(site_statuses)
                if _site_status_matches_identifiers(site_status, identifiers)
            ]
            value["energyInfrastructureSiteStatus"] = filtered_statuses

        for item in value.values():
            prune(item)

    prune(payload)
    return payload


def _record_timestamp(record: dict[str, Any]) -> str:
    return str(record.get("received_at") or record.get("fetched_at") or record.get("logged_at") or "")


def empty_archive_stats() -> dict[str, int]:
    return {
        "archive_members_seen": 0,
        "provider_members_seen": 0,
        "matching_messages": 0,
        "parse_errors": 0,
        "records_seen": 0,
        "records_skipped_by_token": 0,
    }


def combine_archive_stats(stats_objects: list[dict[str, int]]) -> dict[str, int]:
    combined = empty_archive_stats()
    for stats in stats_objects:
        for key in combined:
            combined[key] += int(stats.get(key, 0) or 0)
    return combined


def collect_status_events(
    archive_path: Path,
    *,
    target_date: date,
    identifiers: StationIdentifiers,
    config: AppConfig,
) -> tuple[list[tuple[datetime, str, str, str]], dict[str, int]]:
    if not archive_path.exists():
        raise FileNotFoundError(f"missing archive: {archive_path}")

    station_catalog_path = config.full_chargers_csv_path or config.chargers_csv_path
    site_station_maps: dict[str, dict[str, str]] = defaultdict(dict)
    for match in load_site_matches(config.site_match_path, station_catalog_path):
        site_station_maps[match.provider_uid][match.site_id] = match.station_id

    evse_station_maps: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)
    for match in load_evse_matches(station_catalog_path, config.site_match_path):
        evse_station_maps[match.provider_uid][match.evse_id] = {
            "station_id": match.station_id,
            "site_id": match.site_id,
            "station_ref": match.station_ref,
        }

    berlin_tz = berlin_timezone_for_day(target_date)
    window_start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=berlin_tz).astimezone(
        timezone.utc
    )
    window_end = (datetime(target_date.year, target_date.month, target_date.day, tzinfo=berlin_tz) + timedelta(days=1)).astimezone(
        timezone.utc
    )

    tokens = {
        *(value for value in identifiers.evse_ids if value),
        *(value for value in identifiers.site_ids if value),
        *(value for value in identifiers.station_refs if value),
    }
    token_bytes = [token.encode("utf-8") for token in sorted(tokens)]
    provider_filter = identifiers.provider_uids

    def iter_member_records(extracted: Any, member_name: str):
        if member_name.endswith(".jsonl"):
            for line in extracted:
                stripped = line.strip()
                if stripped:
                    yield stripped
            return
        raw_record = extracted.read()
        if raw_record.strip():
            yield raw_record

    def collect_record_events(raw_record: bytes, provider_uid: str) -> None:
        stats["records_seen"] += 1
        if token_bytes and not any(token in raw_record for token in token_bytes):
            stats["records_skipped_by_token"] += 1
            return
        stats["matching_messages"] += 1

        try:
            record = json.loads(raw_record.decode("utf-8"))
            payload = decode_json_payload(str(record.get("body_text") or "").encode("utf-8"))
            payload = prune_payload_to_station(payload, identifiers)
            facts = extract_dynamic_facts(
                payload,
                provider_uid,
                site_station_maps.get(provider_uid, {}),
                evse_station_maps.get(provider_uid, {}),
            )
        except Exception:
            stats["parse_errors"] += 1
            return

        message_timestamp = _record_timestamp(record)
        for fact in facts:
            if fact.station_id != identifiers.station.station_id and fact.evse_id not in identifiers.evse_ids:
                continue
            observed_at = parse_iso_datetime(fact.source_observed_at or message_timestamp)
            if observed_at is None or observed_at < window_start or observed_at > window_end:
                continue
            events.append(
                (
                    observed_at,
                    fact.evse_id,
                    str(fact.availability_status or "unknown"),
                    str(fact.operational_status or ""),
                )
            )

    events: list[tuple[datetime, str, str, str]] = []
    stats = empty_archive_stats()
    seen_provider_filter: set[str] = set()

    with tarfile.open(archive_path, mode="r|gz") as archive:
        for member in archive:
            if not member.isfile():
                continue
            stats["archive_members_seen"] += 1
            provider_uid = Path(member.name).parts[0] if Path(member.name).parts else ""
            if provider_filter and provider_uid not in provider_filter:
                if seen_provider_filter >= provider_filter:
                    break
                continue
            if provider_filter:
                seen_provider_filter.add(provider_uid)
            stats["provider_members_seen"] += 1

            extracted = archive.extractfile(member)
            if extracted is None:
                continue
            for raw_record in iter_member_records(extracted, member.name):
                collect_record_events(raw_record, provider_uid)

    events.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
    return events, stats


def reduce_to_status_changes(events: list[tuple[datetime, str, str, str]], evse_ids: set[str]) -> list[tuple[datetime, str, str, str]]:
    rows_by_evse: dict[str, list[tuple[datetime, str, str, str]]] = defaultdict(list)
    for event in events:
        rows_by_evse[event[1]].append(event)

    changes: list[tuple[datetime, str, str, str]] = []
    for evse_id in sorted(evse_ids or rows_by_evse.keys()):
        last_signature: tuple[str, str] | None = None
        seen: set[tuple[datetime, str, str, str]] = set()
        for event in sorted(rows_by_evse.get(evse_id, []), key=lambda item: (item[0], item[2], item[3])):
            if event in seen:
                continue
            seen.add(event)
            signature = (event[2], event[3])
            if signature == last_signature:
                continue
            changes.append(event)
            last_signature = signature
    changes.sort(key=lambda item: (item[0], item[1]))
    return changes


def hourly_average_occupied(
    changes: list[tuple[datetime, str, str, str]],
    *,
    evse_ids: set[str],
    target_date: date,
) -> list[float]:
    berlin_tz = berlin_timezone_for_day(target_date)
    local_midnight = datetime(target_date.year, target_date.month, target_date.day, tzinfo=berlin_tz)
    window_start = local_midnight.astimezone(timezone.utc)
    window_end = (local_midnight + timedelta(days=1)).astimezone(timezone.utc)

    hour_occupied_seconds = [0] * 24
    state = {evse_id: "unknown" for evse_id in sorted(evse_ids)}
    last_time = window_start

    def add_interval(start: datetime, end: datetime) -> None:
        if end <= start:
            return
        occupied_count = sum(1 for status in state.values() if status == "occupied")
        cursor = start
        while cursor < end:
            local_cursor = cursor.astimezone(berlin_tz)
            next_hour = (local_cursor.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)).astimezone(
                timezone.utc
            )
            segment_end = min(end, next_hour)
            hour_occupied_seconds[local_cursor.hour] += int((segment_end - cursor).total_seconds()) * occupied_count
            cursor = segment_end

    for observed_at, evse_id, status, _operational_status in changes:
        if observed_at < window_start or observed_at > window_end:
            continue
        add_interval(last_time, observed_at)
        state[evse_id] = status
        last_time = observed_at
    add_interval(last_time, window_end)

    return [seconds / 3600 for seconds in hour_occupied_seconds]


def average_hourly_values(daily_values: list[list[float]]) -> list[float]:
    if not daily_values:
        return [0.0] * 24
    return [sum(values[hour] for values in daily_values) / len(daily_values) for hour in range(24)]


def write_json_output(
    path: Path,
    *,
    station: StationRecord,
    start_date: date,
    end_date: date,
    requested_days: int,
    identifiers: StationIdentifiers,
    stats: dict[str, int],
    daily_results: list[dict[str, Any]],
    missing_archives: list[str],
    hourly_values: list[float],
) -> None:
    berlin_tz = berlin_timezone_for_day(end_date)
    payload = {
        "station": {
            "station_id": station.station_id,
            "operator": station.operator,
            "address": station.address,
            "city": station.city,
            "charging_points_count": station.charging_points_count,
            "max_power_kw": station.max_power_kw,
        },
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "requested_days": requested_days,
        "included_days": len(daily_results),
        "missing_archives": missing_archives,
        "timezone": "Europe/Berlin",
        "metric": "mean of daily average occupied EVSEs per hour; unknown status is not counted as occupied",
        "provider_uids": sorted(identifiers.provider_uids),
        "site_ids": sorted(identifiers.site_ids),
        "evse_ids": sorted(identifiers.evse_ids),
        "archive_stats": stats,
        "hourly_average_occupied": {f"{hour:02d}:00": round(value, 3) for hour, value in enumerate(hourly_values)},
        "daily_results": [
            {
                "date": result["date"],
                "archive": result["archive"],
                "archive_stats": result["archive_stats"],
                "status_change_count": len(result["changes"]),
                "hourly_average_occupied": {
                    f"{hour:02d}:00": round(value, 3) for hour, value in enumerate(result["hourly_values"])
                },
                "status_changes": [
                    {
                        "time_local": observed_at.astimezone(berlin_tz).isoformat(),
                        "evse_id": evse_id,
                        "availability_status": availability_status,
                        "operational_status": operational_status,
                    }
                    for observed_at, evse_id, availability_status, operational_status in result["changes"]
                ],
            }
            for result in daily_results
        ],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_svg_chart(
    path: Path,
    *,
    station: StationRecord,
    start_date: date,
    end_date: date,
    included_days: int,
    requested_days: int,
    hourly_values: list[float],
) -> None:
    width, height = 1200, 720
    left, right, top, bottom = 118, 72, 220, 112
    plot_w = width - left - right
    plot_h = height - top - bottom
    bar_gap = 8
    bar_w = (plot_w - bar_gap * 23) / 24
    max_y = max(1, station.charging_points_count, int(max(hourly_values + [0]) + 1))

    font_family = "'Space Grotesk', 'Avenir Next', Arial, sans-serif"
    color_bg = "#f8fafc"
    color_surface = "#ffffff"
    color_surface_dim = "#f1f5f9"
    color_primary = "#0f766e"
    color_primary_dark = "#0d5e56"
    color_text = "#1e293b"
    color_muted = "#64748b"
    color_border = "#e2e8f0"

    title = "Auslastung pro Stunde"
    if start_date == end_date:
        date_label = end_date.strftime("%d.%m.%Y")
    else:
        date_label = f"{start_date.strftime('%d.%m.%Y')} bis {end_date.strftime('%d.%m.%Y')}"
    subtitle = f"{station.operator}, {station.address}"
    note = (
        "Mittelwert der belegten Ladepunkte je Stunde; unbekannte Zustände zählen nicht als belegt."
    )
    coverage_label = f"{included_days}/{requested_days} Tage"

    svg: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<defs>",
        '<filter id="cardShadow" x="-5%" y="-5%" width="110%" height="120%">',
        '<feDropShadow dx="0" dy="10" stdDeviation="14" flood-color="#0f172a" flood-opacity="0.10"/>',
        "</filter>",
        '<linearGradient id="barGradient" x1="0" y1="0" x2="0" y2="1">',
        f'<stop offset="0%" stop-color="{color_primary}"/>',
        f'<stop offset="100%" stop-color="{color_primary_dark}"/>',
        "</linearGradient>",
        "</defs>",
        f'<rect width="100%" height="100%" fill="{color_bg}"/>',
        f'<rect x="48" y="42" width="{width - 96}" height="{height - 84}" rx="16" fill="{color_surface}" stroke="{color_border}" filter="url(#cardShadow)"/>',
        f'<text x="88" y="92" font-family="{font_family}" font-size="15" font-weight="700" fill="{color_primary_dark}">woladen.de</text>',
        f'<rect x="190" y="70" width="104" height="34" rx="17" fill="{color_surface_dim}" stroke="{color_border}"/>',
        f'<text x="242" y="92" text-anchor="middle" font-family="{font_family}" font-size="14" font-weight="700" fill="{color_text}">{coverage_label}</text>',
        f'<text x="88" y="145" font-family="{font_family}" font-size="38" font-weight="800" fill="{color_text}">{title}</text>',
        f'<text x="88" y="178" font-family="{font_family}" font-size="17" fill="{color_muted}">{html_escape(subtitle)}</text>',
        f'<text x="{width - 88}" y="146" text-anchor="end" font-family="{font_family}" font-size="17" font-weight="700" fill="{color_text}">{date_label}</text>',
        f'<text x="88" y="207" font-family="{font_family}" font-size="14" fill="{color_muted}">{note}</text>',
    ]
    for tick in range(0, max_y + 1):
        y = top + plot_h - (tick / max_y) * plot_h
        stroke_width = "1.4" if tick == 0 else "1"
        svg.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" stroke="{color_border}" stroke-width="{stroke_width}"/>')
        svg.append(
            f'<text x="{left - 18}" y="{y + 5:.1f}" text-anchor="end" font-family="{font_family}" font-size="13" fill="{color_muted}">{tick}</text>'
        )
    for hour, value in enumerate(hourly_values):
        x = left + hour * (bar_w + bar_gap)
        bar_h = (value / max_y) * plot_h
        y = top + plot_h - bar_h
        fill = "url(#barGradient)"
        if value <= 0:
            fill = color_surface_dim
            bar_h = 4
            y = top + plot_h - bar_h
        svg.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" rx="7" fill="{fill}"/>')
        if value > 0:
            svg.append(
                f'<text x="{x + bar_w / 2:.1f}" y="{y - 9:.1f}" text-anchor="middle" font-family="{font_family}" font-size="12" font-weight="700" fill="{color_text}">{value:.1f}</text>'
            )
        hour_color = color_text if hour % 3 == 0 else color_muted
        hour_weight = "700" if hour % 3 == 0 else "500"
        svg.append(
            f'<text x="{x + bar_w / 2:.1f}" y="{top + plot_h + 28}" text-anchor="middle" font-family="{font_family}" font-size="13" font-weight="{hour_weight}" fill="{hour_color}">{hour:02d}</text>'
        )
    svg.append(
        f'<text x="{left + plot_w / 2:.1f}" y="{height - 62}" text-anchor="middle" font-family="{font_family}" font-size="15" font-weight="700" fill="{color_text}">Uhrzeit</text>'
    )
    svg.append(
        f'<text transform="translate(66 {top + plot_h / 2:.1f}) rotate(-90)" text-anchor="middle" font-family="{font_family}" font-size="15" font-weight="700" fill="{color_text}">Durchschnittlich belegte Ladepunkte</text>'
    )
    svg.append(
        f'<text x="{width - 88}" y="{height - 62}" text-anchor="end" font-family="{font_family}" font-size="12" fill="{color_muted}">'
        "Quelle: lokales AFIR-Archiv aus Hugging Face loffenauer/AFIR"
        "</text>"
    )
    svg.append("</svg>")
    path.write_text("\n".join(svg), encoding="utf-8")


def html_escape(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create an hourly occupied-EVSE chart from local AFIR archives")
    parser.add_argument(
        "--date",
        type=_parse_date,
        default=None,
        help="End date in YYYY-MM-DD. Defaults to yesterday in Europe/Berlin.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of trailing days to average, ending at --date. Defaults to {DEFAULT_DAYS}. Use 1 for one day.",
    )
    parser.add_argument("--station", default="", help="Internal station id or woladen URL with ?station=<id>")
    parser.add_argument("--query", default="", help="Station search query if --station is not known")
    parser.add_argument("--city", default="", help="Optional exact city filter used with --query")
    parser.add_argument("--archive", type=Path, default=None, help="Path to live-provider-responses-YYYY-MM-DD.tgz. Only valid with --days 1.")
    parser.add_argument("--require-complete", action="store_true", help="Fail if any archive in the requested date window is missing")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory for JSON and SVG")
    parser.add_argument("--archive-dir", type=Path, default=None, help="Directory containing local live-provider-responses-YYYY-MM-DD.tgz archives")
    parser.add_argument("--env-file", type=Path, default=None, help="Optional env file with local archive settings")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-day progress output on stderr")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.env_file is not None:
        load_env_file(args.env_file, allowed_keys=LOCAL_ARCHIVE_ENV_FILE_KEYS)
    if args.archive_dir is not None:
        os.environ["WOLADEN_LIVE_ARCHIVE_DIR"] = str(args.archive_dir)
    end_date = args.date or default_end_date()
    target_dates = date_window(end_date, args.days)
    if args.archive is not None and args.days != 1:
        raise SystemExit("--archive can only be used with --days 1")

    config = AppConfig()
    station_catalog_path = config.full_chargers_csv_path or config.chargers_csv_path
    stations = load_station_records(station_catalog_path)
    station = resolve_station(
        stations,
        station_reference=args.station or None,
        query=args.query or None,
        city=args.city or None,
    )
    identifiers = build_station_identifiers(config, station)

    daily_results: list[dict[str, Any]] = []
    missing_archives: list[str] = []
    for day_index, target_date in enumerate(target_dates, start=1):
        archive_path = args.archive if args.archive is not None else archive_path_for_date(config, target_date)
        if not args.quiet:
            print(
                f"[{day_index}/{len(target_dates)}] {target_date.isoformat()}: reading {archive_path}",
                file=sys.stderr,
                flush=True,
            )
        if not archive_path.exists():
            missing_archives.append(target_date.isoformat())
            if not args.quiet:
                print(f"[{day_index}/{len(target_dates)}] {target_date.isoformat()}: missing archive", file=sys.stderr, flush=True)
            continue
        events, stats = collect_status_events(
            archive_path,
            target_date=target_date,
            identifiers=identifiers,
            config=config,
        )
        changes = reduce_to_status_changes(events, identifiers.evse_ids)
        hourly_values = hourly_average_occupied(changes, evse_ids=identifiers.evse_ids, target_date=target_date)
        daily_results.append(
            {
                "date": target_date.isoformat(),
                "archive": str(archive_path.resolve()),
                "archive_stats": stats,
                "changes": changes,
                "hourly_values": hourly_values,
            }
        )
        if not args.quiet:
            print(
                f"[{day_index}/{len(target_dates)}] {target_date.isoformat()}: "
                f"{len(changes)} status changes from {stats['matching_messages']} matching records",
                file=sys.stderr,
                flush=True,
            )

    if missing_archives and args.require_complete:
        raise SystemExit(f"Missing archives for requested window: {', '.join(missing_archives)}")
    if not daily_results:
        raise SystemExit("No local archives matched the requested window")

    combined_stats = combine_archive_stats([result["archive_stats"] for result in daily_results])
    hourly_values = average_hourly_values([result["hourly_values"] for result in daily_results])

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_stem = (
        f"{_slugify(station.station_id)}-hourly-occupancy-"
        f"{target_dates[0].isoformat()}-to-{end_date.isoformat()}"
    )
    json_path = args.output_dir / f"{output_stem}.json"
    svg_path = args.output_dir / f"{output_stem}.svg"
    write_json_output(
        json_path,
        station=station,
        start_date=target_dates[0],
        end_date=end_date,
        requested_days=args.days,
        identifiers=identifiers,
        stats=combined_stats,
        daily_results=daily_results,
        missing_archives=missing_archives,
        hourly_values=hourly_values,
    )
    write_svg_chart(
        svg_path,
        station=station,
        start_date=target_dates[0],
        end_date=end_date,
        included_days=len(daily_results),
        requested_days=args.days,
        hourly_values=hourly_values,
    )

    print(
        json.dumps(
            {
                "station_id": station.station_id,
                "operator": station.operator,
                "address": station.address,
                "start_date": target_dates[0].isoformat(),
                "end_date": end_date.isoformat(),
                "requested_days": args.days,
                "included_days": len(daily_results),
                "missing_archives": missing_archives,
                "json": str(json_path.resolve()),
                "svg": str(svg_path.resolve()),
                "matching_messages": combined_stats["matching_messages"],
                "hourly_average_occupied": {f"{hour:02d}:00": round(value, 3) for hour, value in enumerate(hourly_values)},
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
