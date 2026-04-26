#!/usr/bin/env python3
"""Build an hourly occupied-EVSE bar chart for one station from an AFIR archive."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tarfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone, tzinfo
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig
from backend.datex import decode_json_payload, extract_dynamic_facts, parse_iso_datetime
from backend.loaders import load_evse_matches, load_site_matches, load_station_records
from backend.models import StationRecord

DEFAULT_OUTPUT_DIR = REPO_ROOT / "analysis" / "output"


@dataclass(frozen=True)
class StationIdentifiers:
    station: StationRecord
    provider_uids: set[str]
    site_ids: set[str]
    station_refs: set[str]
    evse_ids: set[str]


def _parse_date(value: str) -> date:
    return date.fromisoformat(value.strip())


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


def _record_timestamp(record: dict[str, Any]) -> str:
    return str(record.get("received_at") or record.get("fetched_at") or record.get("logged_at") or "")


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

    events: list[tuple[datetime, str, str, str]] = []
    stats = {
        "archive_members_seen": 0,
        "provider_members_seen": 0,
        "matching_messages": 0,
        "parse_errors": 0,
    }

    with tarfile.open(archive_path, mode="r:gz") as archive:
        for member in archive:
            if not member.isfile():
                continue
            stats["archive_members_seen"] += 1
            provider_uid = Path(member.name).parts[0] if Path(member.name).parts else ""
            if provider_filter and provider_uid not in provider_filter:
                continue
            stats["provider_members_seen"] += 1

            extracted = archive.extractfile(member)
            if extracted is None:
                continue
            raw_record = extracted.read()
            if token_bytes and not any(token in raw_record for token in token_bytes):
                continue
            stats["matching_messages"] += 1

            try:
                record = json.loads(raw_record.decode("utf-8"))
                payload = decode_json_payload(str(record.get("body_text") or "").encode("utf-8"))
                facts = extract_dynamic_facts(
                    payload,
                    provider_uid,
                    site_station_maps.get(provider_uid, {}),
                    evse_station_maps.get(provider_uid, {}),
                )
            except Exception:
                stats["parse_errors"] += 1
                continue

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


def write_json_output(
    path: Path,
    *,
    station: StationRecord,
    target_date: date,
    identifiers: StationIdentifiers,
    stats: dict[str, int],
    changes: list[tuple[datetime, str, str, str]],
    hourly_values: list[float],
) -> None:
    berlin_tz = berlin_timezone_for_day(target_date)
    payload = {
        "station": {
            "station_id": station.station_id,
            "operator": station.operator,
            "address": station.address,
            "city": station.city,
            "charging_points_count": station.charging_points_count,
            "max_power_kw": station.max_power_kw,
        },
        "date": target_date.isoformat(),
        "timezone": "Europe/Berlin",
        "metric": "average occupied EVSEs per hour; unknown status is not counted as occupied",
        "provider_uids": sorted(identifiers.provider_uids),
        "site_ids": sorted(identifiers.site_ids),
        "evse_ids": sorted(identifiers.evse_ids),
        "archive_stats": stats,
        "hourly_average_occupied": {f"{hour:02d}:00": round(value, 3) for hour, value in enumerate(hourly_values)},
        "status_changes": [
            {
                "time_local": observed_at.astimezone(berlin_tz).isoformat(),
                "evse_id": evse_id,
                "availability_status": availability_status,
                "operational_status": operational_status,
            }
            for observed_at, evse_id, availability_status, operational_status in changes
        ],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_svg_chart(path: Path, *, station: StationRecord, target_date: date, hourly_values: list[float]) -> None:
    width, height = 1200, 720
    left, right, top, bottom = 92, 44, 120, 118
    plot_w = width - left - right
    plot_h = height - top - bottom
    bar_gap = 8
    bar_w = (plot_w - bar_gap * 23) / 24
    max_y = max(1, station.charging_points_count, int(max(hourly_values + [0]) + 1))

    title = "Belegte Ladepunkte pro Stunde"
    subtitle = f"{station.operator}, {station.address} - {target_date.strftime('%d.%m.%Y')}"
    note = "Wert = durchschnittlich belegte Ladepunkte je Stunde; unbekannte Zustaende zaehlen nicht als belegt."

    svg: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#fbfaf6"/>',
        f'<text x="92" y="50" font-family="Arial, sans-serif" font-size="30" font-weight="700" fill="#101820">{title}</text>',
        f'<text x="92" y="84" font-family="Arial, sans-serif" font-size="18" fill="#46515c">{html_escape(subtitle)}</text>',
        f'<text x="92" y="108" font-family="Arial, sans-serif" font-size="14" fill="#6b7280">{note}</text>',
    ]
    for tick in range(0, max_y + 1):
        y = top + plot_h - (tick / max_y) * plot_h
        svg.append(f'<line x1="{left}" y1="{y:.1f}" x2="{width - right}" y2="{y:.1f}" stroke="#d9ded7" stroke-width="1"/>')
        svg.append(
            f'<text x="{left - 16}" y="{y + 5:.1f}" text-anchor="end" font-family="Arial, sans-serif" font-size="14" fill="#4b5563">{tick}</text>'
        )
    svg.append(f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + plot_h}" stroke="#1f2937" stroke-width="1.4"/>')
    svg.append(
        f'<line x1="{left}" y1="{top + plot_h}" x2="{width - right}" y2="{top + plot_h}" stroke="#1f2937" stroke-width="1.4"/>'
    )
    for hour, value in enumerate(hourly_values):
        x = left + hour * (bar_w + bar_gap)
        bar_h = (value / max_y) * plot_h
        y = top + plot_h - bar_h
        fill = "#111111" if value > 0 else "#d8d5ca"
        svg.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{bar_h:.1f}" rx="5" fill="{fill}"/>')
        if value > 0:
            svg.append(
                f'<text x="{x + bar_w / 2:.1f}" y="{y - 7:.1f}" text-anchor="middle" font-family="Arial, sans-serif" font-size="12" fill="#111111">{value:.1f}</text>'
            )
        svg.append(
            f'<text x="{x + bar_w / 2:.1f}" y="{top + plot_h + 28}" text-anchor="middle" font-family="Arial, sans-serif" font-size="13" fill="#4b5563">{hour:02d}</text>'
        )
    svg.append(
        f'<text x="{left + plot_w / 2:.1f}" y="{height - 34}" text-anchor="middle" font-family="Arial, sans-serif" font-size="16" fill="#374151">Uhrzeit (Stunde des Tages)</text>'
    )
    svg.append(
        f'<text transform="translate(28 {top + plot_h / 2:.1f}) rotate(-90)" text-anchor="middle" font-family="Arial, sans-serif" font-size="16" fill="#374151">Durchschnittlich belegte Ladepunkte</text>'
    )
    svg.append(
        '<text x="1156" y="702" text-anchor="end" font-family="Arial, sans-serif" font-size="12" fill="#6b7280">'
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
    parser = argparse.ArgumentParser(description="Create an hourly occupied-EVSE chart from one daily AFIR archive")
    parser.add_argument("--date", required=True, type=_parse_date, help="Archive/local day in YYYY-MM-DD")
    parser.add_argument("--station", default="", help="Internal station id or woladen URL with ?station=<id>")
    parser.add_argument("--query", default="", help="Station search query if --station is not known")
    parser.add_argument("--city", default="", help="Optional exact city filter used with --query")
    parser.add_argument("--archive", type=Path, default=None, help="Path to live-provider-responses-YYYY-MM-DD.tgz")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory for JSON and SVG")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
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
    archive_path = args.archive or archive_path_for_date(config, args.date)

    events, stats = collect_status_events(
        archive_path,
        target_date=args.date,
        identifiers=identifiers,
        config=config,
    )
    changes = reduce_to_status_changes(events, identifiers.evse_ids)
    hourly_values = hourly_average_occupied(changes, evse_ids=identifiers.evse_ids, target_date=args.date)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_stem = f"{_slugify(station.station_id)}-hourly-occupancy-{args.date.isoformat()}"
    json_path = args.output_dir / f"{output_stem}.json"
    svg_path = args.output_dir / f"{output_stem}.svg"
    write_json_output(
        json_path,
        station=station,
        target_date=args.date,
        identifiers=identifiers,
        stats=stats,
        changes=changes,
        hourly_values=hourly_values,
    )
    write_svg_chart(svg_path, station=station, target_date=args.date, hourly_values=hourly_values)

    print(
        json.dumps(
            {
                "station_id": station.station_id,
                "operator": station.operator,
                "address": station.address,
                "date": args.date.isoformat(),
                "archive": str(archive_path.resolve()),
                "json": str(json_path.resolve()),
                "svg": str(svg_path.resolve()),
                "matching_messages": stats["matching_messages"],
                "hourly_average_occupied": {f"{hour:02d}:00": round(value, 3) for hour, value in enumerate(hourly_values)},
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
