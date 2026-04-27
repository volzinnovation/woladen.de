#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import tarfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analysis.output_io import publish_staged_directory, staged_output_directory, write_csv
from backend.config import AppConfig
from backend.datex import decode_json_payload, extract_dynamic_facts, parse_iso_datetime
from backend.loaders import load_evse_matches, load_provider_targets, load_site_matches, load_station_records

ARCHIVE_NAME_RE = re.compile(r"live-provider-responses-(\d{4}-\d{2}-\d{2})\.tgz$")

PROVIDER_CATALOG_FIELDS = [
    "provider_uid",
    "display_name",
    "publisher",
    "enabled_live_tracking",
    "has_static_feed",
    "static_publication_id",
    "static_access_mode",
    "static_matched_station_count",
    "static_matched_station_count_in_bundle",
    "has_dynamic_feed",
    "dynamic_publication_id",
    "dynamic_access_mode",
    "dynamic_delta_delivery",
    "dynamic_retention_period_minutes",
]

ARCHIVE_MESSAGE_FIELDS = [
    "archive_date",
    "archive_path",
    "archive_member",
    "provider_uid",
    "record_kind",
    "message_timestamp",
    "fetched_at",
    "received_at",
    "http_status",
    "payload_sha256",
    "payload_byte_length",
    "payload_is_gzip",
    "parse_result",
    "extracted_observation_count",
    "extracted_mapped_observation_count",
    "extracted_unmapped_observation_count",
    "source_observed_at_min",
    "source_observed_at_max",
]

EVSE_OBSERVATION_FIELDS = [
    "archive_date",
    "archive_path",
    "archive_member",
    "provider_uid",
    "message_kind",
    "message_timestamp",
    "source_observed_at",
    "event_timestamp",
    "http_status",
    "payload_sha256",
    "station_id",
    "mapped_station",
    "site_id",
    "station_ref",
    "provider_evse_id",
    "availability_status",
    "operational_status",
    "is_free",
    "is_occupied",
    "is_out_of_order",
    "is_unknown",
    "station_catalog_charging_points_count",
    "price_display",
    "price_currency",
    "price_energy_eur_kwh_min",
    "price_energy_eur_kwh_max",
    "price_time_eur_min_min",
    "price_time_eur_min_max",
    "price_quality",
    "price_complex",
    "next_available_charging_slots_json",
    "supplemental_facility_status_json",
]

EVSE_STATUS_CHANGE_FIELDS = [
    "provider_uid",
    "provider_evse_id",
    "station_id",
    "site_id",
    "station_ref",
    "archive_date",
    "change_rank",
    "message_kind",
    "message_timestamp",
    "source_observed_at",
    "status_started_at",
    "next_status_started_at",
    "duration_seconds",
    "is_open_interval",
    "availability_status",
    "operational_status",
    "payload_sha256",
]

STATION_DAILY_SUMMARY_FIELDS = [
    "archive_date",
    "provider_uid",
    "station_id",
    "station_operator",
    "station_city",
    "station_catalog_charging_points_count",
    "evses_observed",
    "free_evses",
    "occupied_evses",
    "out_of_order_evses",
    "unknown_evses",
    "station_availability_status",
    "station_any_out_of_order",
    "station_all_evses_out_of_order",
    "station_coverage_vs_catalog",
    "latest_event_timestamp",
]

PROVIDER_DAILY_SUMMARY_FIELDS = [
    "archive_date",
    "provider_uid",
    "display_name",
    "publisher",
    "enabled_live_tracking",
    "has_static_feed",
    "has_dynamic_feed",
    "dynamic_delta_delivery",
    "dynamic_retention_period_minutes",
    "static_matched_station_count",
    "static_matched_station_count_in_bundle",
    "messages_total",
    "parseable_messages_total",
    "extracted_observation_count_total",
    "extracted_mapped_observation_count_total",
    "extracted_unmapped_observation_count_total",
    "mapped_observation_ratio",
    "competitive_analysis_eligible",
    "competitive_analysis_tier",
    "competitive_analysis_reason",
    "evses_observed",
    "mapped_evses_observed",
    "unmapped_evses_observed",
    "mapped_stations_observed",
    "mapped_stations_observed_in_bundle",
    "free_evses_end_of_day",
    "occupied_evses_end_of_day",
    "out_of_order_evses_end_of_day",
    "unknown_evses_end_of_day",
    "stations_all_evses_out_of_order",
    "dynamic_station_coverage_ratio",
    "dynamic_station_coverage_ratio_in_bundle",
    "latest_event_timestamp",
]


@dataclass(frozen=True)
class AnalysisOutputs:
    provider_catalog_path: Path
    archive_messages_path: Path
    evse_observations_path: Path
    evse_status_changes_path: Path
    station_daily_summary_path: Path
    provider_daily_summary_path: Path


@dataclass(frozen=True)
class StreamedHistoryResult:
    message_rows: list[dict[str, Any]]
    latest_rows: list[dict[str, Any]]
    archive_dates: list[str]
    observation_row_count: int
    status_change_row_count: int


class StatusChangeTracker:
    def __init__(self):
        self._open_changes: dict[tuple[str, str], dict[str, Any]] = {}
        self._change_ranks: dict[tuple[str, str], int] = defaultdict(int)

    def observe(self, row: dict[str, Any]) -> dict[str, Any] | None:
        key = (str(row.get("provider_uid") or ""), str(row.get("provider_evse_id") or ""))
        signature = (
            str(row.get("station_id") or ""),
            str(row.get("availability_status") or ""),
            str(row.get("operational_status") or ""),
        )

        current = self._open_changes.get(key)
        if current is not None:
            current_signature = (
                str(current.get("station_id") or ""),
                str(current.get("availability_status") or ""),
                str(current.get("operational_status") or ""),
            )
            if current_signature == signature:
                return None

        self._change_ranks[key] += 1
        next_change = {
            "provider_uid": str(row.get("provider_uid") or ""),
            "provider_evse_id": str(row.get("provider_evse_id") or ""),
            "station_id": str(row.get("station_id") or ""),
            "site_id": str(row.get("site_id") or ""),
            "station_ref": str(row.get("station_ref") or ""),
            "archive_date": str(row.get("archive_date") or ""),
            "change_rank": self._change_ranks[key],
            "message_kind": str(row.get("message_kind") or ""),
            "message_timestamp": str(row.get("message_timestamp") or ""),
            "source_observed_at": str(row.get("source_observed_at") or ""),
            "status_started_at": _timestamp_text(row.get("event_timestamp"), row.get("message_timestamp")),
            "next_status_started_at": "",
            "duration_seconds": "",
            "is_open_interval": 0,
            "availability_status": str(row.get("availability_status") or ""),
            "operational_status": str(row.get("operational_status") or ""),
            "payload_sha256": str(row.get("payload_sha256") or ""),
        }

        closed_row: dict[str, Any] | None = None
        if current is not None:
            closed_row = dict(current)
            closed_row["next_status_started_at"] = next_change["status_started_at"]
            start_dt = parse_iso_datetime(str(closed_row["status_started_at"] or ""))
            end_dt = parse_iso_datetime(str(closed_row["next_status_started_at"] or ""))
            if start_dt is not None and end_dt is not None and end_dt >= start_dt:
                closed_row["duration_seconds"] = int((end_dt - start_dt).total_seconds())

        self._open_changes[key] = next_change
        return closed_row

    def finalize(self, analysis_window_end_at: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for key in sorted(self._open_changes):
            row = dict(self._open_changes[key])
            if analysis_window_end_at:
                row["next_status_started_at"] = analysis_window_end_at
                row["is_open_interval"] = 1
                start_dt = parse_iso_datetime(str(row["status_started_at"] or ""))
                end_dt = parse_iso_datetime(analysis_window_end_at)
                if start_dt is not None and end_dt is not None and end_dt >= start_dt:
                    row["duration_seconds"] = int((end_dt - start_dt).total_seconds())
            rows.append(row)
        return rows


def _parse_archive_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def _parse_archive_name(path: Path) -> date | None:
    match = ARCHIVE_NAME_RE.search(path.name)
    if match is None:
        return None
    return date.fromisoformat(match.group(1))


def _timestamp_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _json_text(value: Any) -> str:
    if value in (None, "", [], {}):
        return ""
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _bool_int(value: bool) -> int:
    return 1 if value else 0


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(float(numerator) / float(denominator), 6)


def _competitive_analysis_status(
    *,
    messages_total: int,
    parseable_messages_total: int,
    static_matched_station_count: int,
    mapped_observation_ratio: float,
) -> dict[str, Any]:
    if messages_total <= 0:
        return {
            "competitive_analysis_eligible": 0,
            "competitive_analysis_tier": "exclude",
            "competitive_analysis_reason": "no_messages",
        }
    if static_matched_station_count <= 0:
        return {
            "competitive_analysis_eligible": 0,
            "competitive_analysis_tier": "exclude",
            "competitive_analysis_reason": "no_static_matches",
        }
    if parseable_messages_total <= 0:
        return {
            "competitive_analysis_eligible": 0,
            "competitive_analysis_tier": "exclude",
            "competitive_analysis_reason": "no_parseable_messages",
        }
    if mapped_observation_ratio >= 0.5:
        return {
            "competitive_analysis_eligible": 1,
            "competitive_analysis_tier": "eligible",
            "competitive_analysis_reason": "ratio_ge_0_5",
        }
    if mapped_observation_ratio >= 0.2:
        return {
            "competitive_analysis_eligible": 0,
            "competitive_analysis_tier": "review",
            "competitive_analysis_reason": "ratio_ge_0_2",
        }
    return {
        "competitive_analysis_eligible": 0,
        "competitive_analysis_tier": "exclude",
        "competitive_analysis_reason": "ratio_lt_0_2",
    }


def _sort_timestamp(value: str) -> datetime:
    parsed = parse_iso_datetime(value)
    if parsed is not None:
        return parsed.astimezone(timezone.utc)
    return datetime.min.replace(tzinfo=timezone.utc)


def _observation_sort_key(row: dict[str, Any]) -> tuple[datetime, datetime, str, str]:
    event_at = _sort_timestamp(_timestamp_text(row.get("event_timestamp"), row.get("message_timestamp")))
    message_at = _sort_timestamp(row.get("message_timestamp"))
    return (
        event_at,
        message_at,
        str(row.get("archive_path") or ""),
        str(row.get("archive_member") or ""),
    )


def _message_sort_key(row: dict[str, Any]) -> tuple[datetime, str, str]:
    message_at = _sort_timestamp(row.get("message_timestamp"))
    return (
        message_at,
        str(row.get("archive_path") or ""),
        str(row.get("archive_member") or ""),
    )


def _resolve_archive_paths(
    *,
    archives: Sequence[Path] | None = None,
    archive_dir: Path | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[Path]:
    explicit_paths = [path.resolve() for path in (archives or [])]
    if explicit_paths:
        return sorted(explicit_paths)

    effective_dir = archive_dir or AppConfig().archive_dir
    if not effective_dir.exists():
        return []

    paths: list[Path] = []
    for path in sorted(effective_dir.glob("live-provider-responses-*.tgz")):
        archive_day = _parse_archive_name(path)
        if archive_day is None:
            continue
        if start_date is not None and archive_day < start_date:
            continue
        if end_date is not None and archive_day > end_date:
            continue
        paths.append(path.resolve())
    return paths


def _build_provider_catalog(config: AppConfig) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    payload = json.loads(config.provider_config_path.read_text(encoding="utf-8"))
    provider_targets = {
        item.provider_uid: item
        for item in load_provider_targets(
            config.provider_config_path,
            config.provider_override_path,
            config.subscription_registry_path,
        )
    }

    full_station_catalog_path = config.full_chargers_csv_path or config.chargers_csv_path
    static_station_counts: dict[str, set[str]] = defaultdict(set)
    for match in load_site_matches(config.site_match_path, full_station_catalog_path):
        static_station_counts[match.provider_uid].add(match.station_id)
    static_bundle_station_counts: dict[str, set[str]] = defaultdict(set)
    for match in load_site_matches(config.site_match_path, config.chargers_csv_path):
        static_bundle_station_counts[match.provider_uid].add(match.station_id)

    rows: list[dict[str, Any]] = []
    for provider in payload.get("providers", []):
        provider_uid = str(provider.get("uid") or "").strip()
        if not provider_uid:
            continue
        feeds = provider.get("feeds") or {}
        static_feed = feeds.get("static") or {}
        dynamic_feed = feeds.get("dynamic") or {}
        dynamic_content = dynamic_feed.get("content_data") or {}
        target = provider_targets.get(provider_uid)

        row = {
            "provider_uid": provider_uid,
            "display_name": str(provider.get("display_name") or provider_uid).strip(),
            "publisher": str(provider.get("publisher") or "").strip(),
            "enabled_live_tracking": _bool_int(bool(target.enabled) if target is not None else False),
            "has_static_feed": _bool_int(bool(static_feed.get("publication_id"))),
            "static_publication_id": str(static_feed.get("publication_id") or "").strip(),
            "static_access_mode": str(static_feed.get("access_mode") or "").strip(),
            "static_matched_station_count": len(static_station_counts.get(provider_uid, set())),
            "static_matched_station_count_in_bundle": len(static_bundle_station_counts.get(provider_uid, set())),
            "has_dynamic_feed": _bool_int(bool(dynamic_feed.get("publication_id"))),
            "dynamic_publication_id": str(dynamic_feed.get("publication_id") or "").strip(),
            "dynamic_access_mode": str(dynamic_feed.get("access_mode") or "").strip(),
            "dynamic_delta_delivery": _bool_int(
                bool(dynamic_feed.get("delta_delivery") or dynamic_content.get("deltaDelivery"))
            ),
            "dynamic_retention_period_minutes": (
                str(dynamic_content.get("retentionPeriod") or "").strip() or ""
            ),
        }
        rows.append(row)

    existing_provider_uids = {row["provider_uid"] for row in rows}
    for provider_uid in sorted(provider_targets):
        if provider_uid in existing_provider_uids:
            continue
        target = provider_targets[provider_uid]
        rows.append(
            {
                "provider_uid": provider_uid,
                "display_name": target.display_name or provider_uid,
                "publisher": target.publisher,
                "enabled_live_tracking": _bool_int(bool(target.enabled)),
                "has_static_feed": 0,
                "static_publication_id": "",
                "static_access_mode": "",
                "static_matched_station_count": len(static_station_counts.get(provider_uid, set())),
                "static_matched_station_count_in_bundle": len(static_bundle_station_counts.get(provider_uid, set())),
                "has_dynamic_feed": _bool_int(bool(target.publication_id)),
                "dynamic_publication_id": target.publication_id,
                "dynamic_access_mode": target.access_mode,
                "dynamic_delta_delivery": _bool_int(bool(target.delta_delivery)),
                "dynamic_retention_period_minutes": (
                    target.retention_period_minutes if target.retention_period_minutes is not None else ""
                ),
            }
        )

    rows.sort(key=lambda item: item["provider_uid"])
    return rows, {row["provider_uid"]: row for row in rows}


def _build_site_station_maps(config: AppConfig) -> dict[str, dict[str, str]]:
    rows: dict[str, dict[str, str]] = defaultdict(dict)
    station_catalog_path = config.full_chargers_csv_path or config.chargers_csv_path
    for match in load_site_matches(config.site_match_path, station_catalog_path):
        rows[match.provider_uid][match.site_id] = match.station_id
    return rows


def _build_evse_station_maps(config: AppConfig) -> dict[str, dict[str, dict[str, str]]]:
    rows: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)
    station_catalog_path = config.full_chargers_csv_path or config.chargers_csv_path
    for match in load_evse_matches(station_catalog_path, config.site_match_path):
        rows[match.provider_uid][match.evse_id] = {
            "station_id": match.station_id,
            "site_id": match.site_id,
            "station_ref": match.station_ref,
        }
    return rows


def _build_station_catalog(config: AppConfig) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    station_catalog_path = config.full_chargers_csv_path or config.chargers_csv_path
    for station in load_station_records(station_catalog_path):
        rows[station.station_id] = {
            "station_id": station.station_id,
            "operator": station.operator,
            "city": station.city,
            "charging_points_count": station.charging_points_count,
        }
    return rows


def _build_bundle_station_ids(config: AppConfig) -> set[str]:
    if not config.chargers_csv_path.exists():
        return set()
    return {station.station_id for station in load_station_records(config.chargers_csv_path)}


def _window_end_at(archive_dates: Iterable[str], config: AppConfig) -> str:
    parsed_days = [date.fromisoformat(value) for value in archive_dates if str(value).strip()]
    if not parsed_days:
        return ""
    day_after_last = max(parsed_days) + timedelta(days=1)
    local_midnight = datetime.combine(day_after_last, time.min, tzinfo=config.archive_timezone())
    return local_midnight.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _iter_archive_members(archive_path: Path) -> Iterable[tuple[str, dict[str, Any]]]:
    # Gzip streams cannot seek cheaply; read members in physical archive order.
    with tarfile.open(archive_path, mode="r|gz") as archive_handle:
        for member in archive_handle:
            if not member.isfile() or member.name == "manifest.json":
                continue
            extracted = archive_handle.extractfile(member)
            if extracted is None:
                continue
            if member.name.endswith(".json"):
                payload = json.loads(extracted.read().decode("utf-8"))
                if isinstance(payload, dict):
                    yield member.name, payload
                continue
            if member.name.endswith(".jsonl"):
                for line_number, line in enumerate(extracted, start=1):
                    stripped = line.strip()
                    if not stripped:
                        continue
                    payload = json.loads(stripped.decode("utf-8"))
                    if isinstance(payload, dict):
                        yield f"{member.name}#{line_number}", payload


def _extract_facts_from_record(
    record: dict[str, Any],
    *,
    provider_uid: str,
    site_station_maps: dict[str, dict[str, str]],
    evse_station_maps: dict[str, dict[str, dict[str, str]]],
) -> tuple[str, list[Any]]:
    record_kind = str(record.get("kind") or "").strip()
    if record_kind not in {"http_response", "push_request"}:
        return "skipped_non_payload_record", []
    http_status = int(record.get("http_status") or 0)
    if record_kind == "http_response" and http_status >= 400:
        return "skipped_http_error", []
    body_text = str(record.get("body_text") or "")
    if not body_text.strip():
        return "skipped_empty_body", []
    try:
        payload = decode_json_payload(body_text.encode("utf-8"))
        facts = extract_dynamic_facts(
            payload,
            provider_uid,
            site_station_maps.get(provider_uid, {}),
            evse_station_maps.get(provider_uid, {}),
        )
    except Exception as exc:
        return f"parse_error:{type(exc).__name__}", []
    return "ok", facts


def stream_archive_history(
    archive_paths: Sequence[Path],
    *,
    config: AppConfig,
    message_writer: csv.DictWriter,
    observation_writer: csv.DictWriter,
    status_change_writer: csv.DictWriter,
) -> StreamedHistoryResult:
    site_station_maps = _build_site_station_maps(config)
    evse_station_maps = _build_evse_station_maps(config)
    station_catalog = _build_station_catalog(config)

    message_rows: list[dict[str, Any]] = []
    latest_rows_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    archive_dates_seen: set[str] = set()
    observation_row_count = 0
    status_change_row_count = 0
    status_change_tracker = StatusChangeTracker()

    for archive_path in archive_paths:
        fallback_archive_date = _parse_archive_name(archive_path)
        for member_name, record in _iter_archive_members(archive_path):
            provider_uid = str(record.get("provider_uid") or "").strip() or Path(member_name).parts[0]
            archive_date_text = str(record.get("archive_date") or "").strip()
            if not archive_date_text and fallback_archive_date is not None:
                archive_date_text = fallback_archive_date.isoformat()
            if archive_date_text:
                archive_dates_seen.add(archive_date_text)

            parse_result, facts = _extract_facts_from_record(
                record,
                provider_uid=provider_uid,
                site_station_maps=site_station_maps,
                evse_station_maps=evse_station_maps,
            )

            source_observed_values = [str(fact.source_observed_at or "").strip() for fact in facts if fact.source_observed_at]
            message_row = {
                "archive_date": archive_date_text,
                "archive_path": str(archive_path),
                "archive_member": member_name,
                "provider_uid": provider_uid,
                "record_kind": str(record.get("kind") or "").strip(),
                "message_timestamp": _timestamp_text(record.get("fetched_at"), record.get("received_at"), record.get("logged_at")),
                "fetched_at": str(record.get("fetched_at") or "").strip(),
                "received_at": str(record.get("received_at") or "").strip(),
                "http_status": int(record.get("http_status") or 0),
                "payload_sha256": str(record.get("payload_sha256") or "").strip(),
                "payload_byte_length": int(record.get("payload_byte_length") or 0),
                "payload_is_gzip": _bool_int(bool(record.get("payload_is_gzip"))),
                "parse_result": parse_result,
                "extracted_observation_count": len(facts),
                "extracted_mapped_observation_count": sum(1 for fact in facts if fact.station_id),
                "extracted_unmapped_observation_count": sum(1 for fact in facts if not fact.station_id),
                "source_observed_at_min": min(source_observed_values) if source_observed_values else "",
                "source_observed_at_max": max(source_observed_values) if source_observed_values else "",
            }
            message_rows.append(message_row)
            message_writer.writerow(message_row)

            for fact in facts:
                station_id = str(fact.station_id or "").strip()
                station_meta = station_catalog.get(station_id, {})
                observation_row = {
                    "archive_date": archive_date_text,
                    "archive_path": str(archive_path),
                    "archive_member": member_name,
                    "provider_uid": provider_uid,
                    "message_kind": message_row["record_kind"],
                    "message_timestamp": message_row["message_timestamp"],
                    "source_observed_at": str(fact.source_observed_at or "").strip(),
                    "event_timestamp": _timestamp_text(fact.source_observed_at, message_row["message_timestamp"]),
                    "http_status": message_row["http_status"],
                    "payload_sha256": message_row["payload_sha256"],
                    "station_id": station_id,
                    "mapped_station": _bool_int(bool(station_id)),
                    "site_id": str(fact.site_id or "").strip(),
                    "station_ref": str(fact.station_ref or "").strip(),
                    "provider_evse_id": str(fact.evse_id or "").strip(),
                    "availability_status": str(fact.availability_status or "").strip(),
                    "operational_status": str(fact.operational_status or "").strip(),
                    "is_free": _bool_int(fact.availability_status == "free"),
                    "is_occupied": _bool_int(fact.availability_status == "occupied"),
                    "is_out_of_order": _bool_int(fact.availability_status == "out_of_order"),
                    "is_unknown": _bool_int(fact.availability_status == "unknown"),
                    "station_catalog_charging_points_count": int(station_meta.get("charging_points_count") or 0),
                    "price_display": str(fact.price.display or "").strip(),
                    "price_currency": str(fact.price.currency or "").strip(),
                    "price_energy_eur_kwh_min": str(fact.price.energy_eur_kwh_min or "").strip(),
                    "price_energy_eur_kwh_max": str(fact.price.energy_eur_kwh_max or "").strip(),
                    "price_time_eur_min_min": fact.price.time_eur_min_min if fact.price.time_eur_min_min is not None else "",
                    "price_time_eur_min_max": fact.price.time_eur_min_max if fact.price.time_eur_min_max is not None else "",
                    "price_quality": str(fact.price.quality or "").strip(),
                    "price_complex": _bool_int(bool(fact.price.complex_tariff)),
                    "next_available_charging_slots_json": _json_text(fact.next_available_charging_slots),
                    "supplemental_facility_status_json": _json_text(fact.supplemental_facility_status),
                }
                observation_writer.writerow(observation_row)
                observation_row_count += 1

                latest_key = (
                    str(observation_row.get("archive_date") or ""),
                    str(observation_row.get("provider_uid") or ""),
                    str(observation_row.get("provider_evse_id") or ""),
                )
                existing_latest = latest_rows_by_key.get(latest_key)
                if existing_latest is None or _observation_sort_key(observation_row) >= _observation_sort_key(existing_latest):
                    latest_rows_by_key[latest_key] = dict(observation_row)

                closed_change = status_change_tracker.observe(observation_row)
                if closed_change is not None:
                    status_change_writer.writerow(closed_change)
                    status_change_row_count += 1

    final_window_end_at = _window_end_at(archive_dates_seen, config)
    for row in status_change_tracker.finalize(final_window_end_at):
        status_change_writer.writerow(row)
        status_change_row_count += 1

    return StreamedHistoryResult(
        message_rows=message_rows,
        latest_rows=sorted(latest_rows_by_key.values(), key=_observation_sort_key),
        archive_dates=sorted(archive_dates_seen),
        observation_row_count=observation_row_count,
        status_change_row_count=status_change_row_count,
    )


def build_station_daily_summary(
    latest_rows: Sequence[dict[str, Any]],
    *,
    station_catalog: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in latest_rows:
        station_id = str(row.get("station_id") or "").strip()
        if not station_id:
            continue
        key = (
            str(row.get("archive_date") or ""),
            str(row.get("provider_uid") or ""),
            station_id,
        )
        grouped[key].append(row)

    summary_rows: list[dict[str, Any]] = []
    for key in sorted(grouped):
        archive_date_text, provider_uid, station_id = key
        rows = grouped[key]
        counts = {
            "free": sum(1 for row in rows if row.get("availability_status") == "free"),
            "occupied": sum(1 for row in rows if row.get("availability_status") == "occupied"),
            "out_of_order": sum(1 for row in rows if row.get("availability_status") == "out_of_order"),
            "unknown": sum(1 for row in rows if row.get("availability_status") == "unknown"),
        }
        station_availability_status = "unknown"
        if counts["free"] > 0:
            station_availability_status = "free"
        elif counts["occupied"] > 0:
            station_availability_status = "occupied"
        elif counts["out_of_order"] > 0:
            station_availability_status = "out_of_order"

        station_meta = station_catalog.get(station_id, {})
        catalog_total = int(station_meta.get("charging_points_count") or 0)
        evses_observed = len(rows)
        summary_rows.append(
            {
                "archive_date": archive_date_text,
                "provider_uid": provider_uid,
                "station_id": station_id,
                "station_operator": str(station_meta.get("operator") or ""),
                "station_city": str(station_meta.get("city") or ""),
                "station_catalog_charging_points_count": catalog_total,
                "evses_observed": evses_observed,
                "free_evses": counts["free"],
                "occupied_evses": counts["occupied"],
                "out_of_order_evses": counts["out_of_order"],
                "unknown_evses": counts["unknown"],
                "station_availability_status": station_availability_status,
                "station_any_out_of_order": _bool_int(counts["out_of_order"] > 0),
                "station_all_evses_out_of_order": _bool_int(
                    evses_observed > 0 and counts["out_of_order"] == evses_observed
                ),
                "station_coverage_vs_catalog": _ratio(evses_observed, catalog_total),
                "latest_event_timestamp": max(
                    (_timestamp_text(row.get("event_timestamp"), row.get("message_timestamp")) for row in rows),
                    default="",
                ),
            }
        )
    return summary_rows


def build_provider_daily_summary(
    archive_dates: Sequence[str],
    provider_catalog_rows: Sequence[dict[str, Any]],
    message_rows: Sequence[dict[str, Any]],
    latest_rows: Sequence[dict[str, Any]],
    station_daily_rows: Sequence[dict[str, Any]],
    *,
    bundle_station_ids: set[str],
) -> list[dict[str, Any]]:
    message_counts: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {
            "messages_total": 0,
            "parseable_messages_total": 0,
            "extracted_observation_count_total": 0,
            "extracted_mapped_observation_count_total": 0,
            "extracted_unmapped_observation_count_total": 0,
        }
    )
    for row in message_rows:
        key = (str(row.get("archive_date") or ""), str(row.get("provider_uid") or ""))
        message_counts[key]["messages_total"] += 1
        if str(row.get("parse_result") or "") == "ok":
            message_counts[key]["parseable_messages_total"] += 1
        message_counts[key]["extracted_observation_count_total"] += int(row.get("extracted_observation_count") or 0)
        message_counts[key]["extracted_mapped_observation_count_total"] += int(
            row.get("extracted_mapped_observation_count") or 0
        )
        message_counts[key]["extracted_unmapped_observation_count_total"] += int(
            row.get("extracted_unmapped_observation_count") or 0
        )

    latest_by_provider_day: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in latest_rows:
        key = (str(row.get("archive_date") or ""), str(row.get("provider_uid") or ""))
        latest_by_provider_day[key].append(row)

    station_out_of_order_counts: dict[tuple[str, str], int] = defaultdict(int)
    for row in station_daily_rows:
        if int(row.get("station_all_evses_out_of_order") or 0):
            key = (str(row.get("archive_date") or ""), str(row.get("provider_uid") or ""))
            station_out_of_order_counts[key] += 1

    catalog_by_uid = {row["provider_uid"]: row for row in provider_catalog_rows}
    summary_rows: list[dict[str, Any]] = []
    for archive_date_text in sorted(archive_dates):
        for provider_uid in sorted(catalog_by_uid):
            catalog_row = catalog_by_uid[provider_uid]
            latest_provider_rows = latest_by_provider_day.get((archive_date_text, provider_uid), [])
            mapped_station_ids = {
                str(row.get("station_id") or "")
                for row in latest_provider_rows
                if str(row.get("station_id") or "").strip()
            }
            mapped_station_ids_in_bundle = mapped_station_ids & bundle_station_ids
            counts = {
                "free": sum(1 for row in latest_provider_rows if row.get("availability_status") == "free"),
                "occupied": sum(1 for row in latest_provider_rows if row.get("availability_status") == "occupied"),
                "out_of_order": sum(1 for row in latest_provider_rows if row.get("availability_status") == "out_of_order"),
                "unknown": sum(1 for row in latest_provider_rows if row.get("availability_status") == "unknown"),
            }
            message_totals = message_counts[(archive_date_text, provider_uid)]
            mapped_observation_ratio = _ratio(
                message_totals["extracted_mapped_observation_count_total"],
                message_totals["extracted_observation_count_total"],
            )
            competitive_status = _competitive_analysis_status(
                messages_total=message_totals["messages_total"],
                parseable_messages_total=message_totals["parseable_messages_total"],
                static_matched_station_count=int(catalog_row["static_matched_station_count"] or 0),
                mapped_observation_ratio=mapped_observation_ratio,
            )
            summary_rows.append(
                {
                    "archive_date": archive_date_text,
                    "provider_uid": provider_uid,
                    "display_name": catalog_row["display_name"],
                    "publisher": catalog_row["publisher"],
                    "enabled_live_tracking": catalog_row["enabled_live_tracking"],
                    "has_static_feed": catalog_row["has_static_feed"],
                    "has_dynamic_feed": catalog_row["has_dynamic_feed"],
                    "dynamic_delta_delivery": catalog_row["dynamic_delta_delivery"],
                    "dynamic_retention_period_minutes": catalog_row["dynamic_retention_period_minutes"],
                    "static_matched_station_count": catalog_row["static_matched_station_count"],
                    "static_matched_station_count_in_bundle": catalog_row["static_matched_station_count_in_bundle"],
                    "messages_total": message_totals["messages_total"],
                    "parseable_messages_total": message_totals["parseable_messages_total"],
                    "extracted_observation_count_total": message_totals["extracted_observation_count_total"],
                    "extracted_mapped_observation_count_total": message_totals[
                        "extracted_mapped_observation_count_total"
                    ],
                    "extracted_unmapped_observation_count_total": message_totals[
                        "extracted_unmapped_observation_count_total"
                    ],
                    "mapped_observation_ratio": mapped_observation_ratio,
                    "competitive_analysis_eligible": competitive_status["competitive_analysis_eligible"],
                    "competitive_analysis_tier": competitive_status["competitive_analysis_tier"],
                    "competitive_analysis_reason": competitive_status["competitive_analysis_reason"],
                    "evses_observed": len(latest_provider_rows),
                    "mapped_evses_observed": sum(1 for row in latest_provider_rows if int(row.get("mapped_station") or 0)),
                    "unmapped_evses_observed": sum(1 for row in latest_provider_rows if not int(row.get("mapped_station") or 0)),
                    "mapped_stations_observed": len(mapped_station_ids),
                    "mapped_stations_observed_in_bundle": len(mapped_station_ids_in_bundle),
                    "free_evses_end_of_day": counts["free"],
                    "occupied_evses_end_of_day": counts["occupied"],
                    "out_of_order_evses_end_of_day": counts["out_of_order"],
                    "unknown_evses_end_of_day": counts["unknown"],
                    "stations_all_evses_out_of_order": station_out_of_order_counts[(archive_date_text, provider_uid)],
                    "dynamic_station_coverage_ratio": _ratio(
                        len(mapped_station_ids),
                        int(catalog_row["static_matched_station_count"] or 0),
                    ),
                    "dynamic_station_coverage_ratio_in_bundle": _ratio(
                        len(mapped_station_ids_in_bundle),
                        int(catalog_row["static_matched_station_count_in_bundle"] or 0),
                    ),
                    "latest_event_timestamp": max(
                        (_timestamp_text(row.get("event_timestamp"), row.get("message_timestamp")) for row in latest_provider_rows),
                        default="",
                    ),
                }
            )
    return summary_rows


def run_analysis(
    *,
    archive_paths: Sequence[Path],
    output_dir: Path,
    config: AppConfig | None = None,
) -> dict[str, Any]:
    effective_config = config or AppConfig()
    provider_catalog_rows, _ = _build_provider_catalog(effective_config)
    station_catalog = _build_station_catalog(effective_config)
    bundle_station_ids = _build_bundle_station_ids(effective_config)
    outputs = AnalysisOutputs(
        provider_catalog_path=output_dir / "provider_catalog.csv",
        archive_messages_path=output_dir / "archive_messages.csv",
        evse_observations_path=output_dir / "evse_observations.csv",
        evse_status_changes_path=output_dir / "evse_status_changes.csv",
        station_daily_summary_path=output_dir / "station_daily_summary.csv",
        provider_daily_summary_path=output_dir / "provider_daily_summary.csv",
    )
    with staged_output_directory(output_dir) as staged_dir:
        staged_outputs = AnalysisOutputs(
            provider_catalog_path=staged_dir / "provider_catalog.csv",
            archive_messages_path=staged_dir / "archive_messages.csv",
            evse_observations_path=staged_dir / "evse_observations.csv",
            evse_status_changes_path=staged_dir / "evse_status_changes.csv",
            station_daily_summary_path=staged_dir / "station_daily_summary.csv",
            provider_daily_summary_path=staged_dir / "provider_daily_summary.csv",
        )
        write_csv(staged_outputs.provider_catalog_path, PROVIDER_CATALOG_FIELDS, provider_catalog_rows)

        with staged_outputs.archive_messages_path.open("w", encoding="utf-8", newline="") as archive_messages_handle, staged_outputs.evse_observations_path.open(
            "w", encoding="utf-8", newline=""
        ) as evse_observations_handle, staged_outputs.evse_status_changes_path.open("w", encoding="utf-8", newline="") as evse_status_changes_handle:
            archive_messages_writer = csv.DictWriter(archive_messages_handle, fieldnames=ARCHIVE_MESSAGE_FIELDS)
            evse_observations_writer = csv.DictWriter(evse_observations_handle, fieldnames=EVSE_OBSERVATION_FIELDS)
            evse_status_changes_writer = csv.DictWriter(evse_status_changes_handle, fieldnames=EVSE_STATUS_CHANGE_FIELDS)
            archive_messages_writer.writeheader()
            evse_observations_writer.writeheader()
            evse_status_changes_writer.writeheader()
            streamed = stream_archive_history(
                archive_paths,
                config=effective_config,
                message_writer=archive_messages_writer,
                observation_writer=evse_observations_writer,
                status_change_writer=evse_status_changes_writer,
            )

        station_daily_rows = build_station_daily_summary(streamed.latest_rows, station_catalog=station_catalog)
        provider_daily_rows = build_provider_daily_summary(
            streamed.archive_dates,
            provider_catalog_rows,
            streamed.message_rows,
            streamed.latest_rows,
            station_daily_rows,
            bundle_station_ids=bundle_station_ids,
        )
        write_csv(staged_outputs.station_daily_summary_path, STATION_DAILY_SUMMARY_FIELDS, station_daily_rows)
        write_csv(staged_outputs.provider_daily_summary_path, PROVIDER_DAILY_SUMMARY_FIELDS, provider_daily_rows)
        publish_staged_directory(staged_dir, output_dir)

    return {
        "archive_count": len(archive_paths),
        "archive_dates": streamed.archive_dates,
        "message_row_count": len(streamed.message_rows),
        "observation_row_count": streamed.observation_row_count,
        "status_change_row_count": streamed.status_change_row_count,
        "station_daily_row_count": len(station_daily_rows),
        "provider_daily_row_count": len(provider_daily_rows),
        "output_dir": str(output_dir.resolve()),
        "outputs": {
            "provider_catalog": str(outputs.provider_catalog_path.resolve()),
            "archive_messages": str(outputs.archive_messages_path.resolve()),
            "evse_observations": str(outputs.evse_observations_path.resolve()),
            "evse_status_changes": str(outputs.evse_status_changes_path.resolve()),
            "station_daily_summary": str(outputs.station_daily_summary_path.resolve()),
            "provider_daily_summary": str(outputs.provider_daily_summary_path.resolve()),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize archived AFIR live payloads into history CSVs")
    parser.add_argument(
        "--archive",
        action="append",
        default=[],
        type=Path,
        help="Path to a local live-provider-responses-YYYY-MM-DD.tgz archive. Can be passed multiple times.",
    )
    parser.add_argument(
        "--archive-dir",
        type=Path,
        default=REPO_ROOT / "data" / "live_archives",
        help="Directory containing local archive tgz files.",
    )
    parser.add_argument("--start-date", type=_parse_archive_date, default=None, help="Inclusive YYYY-MM-DD filter")
    parser.add_argument("--end-date", type=_parse_archive_date, default=None, help="Inclusive YYYY-MM-DD filter")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPO_ROOT / "analysis" / "output",
        help="Directory for generated CSV files.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    archive_paths = _resolve_archive_paths(
        archives=args.archive,
        archive_dir=args.archive_dir,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    if not archive_paths:
        raise SystemExit("No archive tgz files matched the requested range")

    result = run_analysis(
        archive_paths=archive_paths,
        output_dir=args.output_dir,
        config=AppConfig(),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
