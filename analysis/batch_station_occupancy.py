#!/usr/bin/env python3
"""Build compact hourly occupancy data for all mapped stations from AFIR archives."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tarfile
from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from analysis.output_io import write_json_atomic
from backend.config import AppConfig, load_env_file
from backend.datex import (
    decode_json_payload,
    iter_status_publications,
    normalize_datex_occupancy_status,
    normalize_eliso_occupancy_status,
    normalize_evse_id,
    parse_iso_datetime,
)
from backend.loaders import load_evse_matches, load_site_matches, load_station_records
from backend.models import StationRecord

DEFAULT_DAYS = 7
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data" / "station_occupancy"
ARCHIVE_NAME_RE = re.compile(r"live-provider-responses-(\d{4}-\d{2}-\d{2})\.tgz$")
LOCAL_ARCHIVE_ENV_FILE_KEYS = frozenset(
    {
        "WOLADEN_LIVE_ARCHIVE_DIR",
        "WOLADEN_LIVE_ARCHIVE_TIMEZONE",
    }
)


@dataclass
class EvseState:
    provider_uid: str
    station_id: str
    last_time: datetime
    availability_status: str = "unknown"
    operational_status: str = ""


@dataclass(frozen=True)
class ProviderScope:
    site_ids: frozenset[str]
    station_refs: frozenset[str]
    evse_ids: frozenset[str]


@dataclass(frozen=True)
class OccupancyEvent:
    station_id: str
    evse_id: str
    availability_status: str
    operational_status: str
    source_observed_at: str


@dataclass
class ProviderStationDayAggregate:
    hourly_occupied_seconds: list[int] = field(default_factory=lambda: [0] * 24)
    observed_evses: set[str] = field(default_factory=set)
    matching_observations: int = 0
    occupied_observations: int = 0
    status_changes: int = 0
    latest_event_timestamp: str = ""


@dataclass
class ProviderStationAggregate:
    hourly_occupied_sum: list[float] = field(default_factory=lambda: [0.0] * 24)
    observed_days: set[str] = field(default_factory=set)
    observed_evses: set[str] = field(default_factory=set)
    matching_observations: int = 0
    occupied_observations: int = 0
    status_changes: int = 0
    latest_event_timestamp: str = ""


@dataclass
class ArchiveStats:
    archive_members_seen: int = 0
    provider_members_seen: int = 0
    records_seen: int = 0
    records_skipped_non_payload: int = 0
    records_skipped_http_error: int = 0
    records_skipped_empty_body: int = 0
    parse_errors: int = 0
    facts_seen: int = 0
    mapped_facts_seen: int = 0
    facts_skipped_missing_evse: int = 0
    facts_skipped_station_scope: int = 0
    facts_skipped_timestamp: int = 0
    facts_skipped_out_of_window: int = 0
    out_of_order_events: int = 0
    records_skipped_provider_scope: int = 0
    records_skipped_raw_prefilter: int = 0

    def add(self, other: "ArchiveStats") -> None:
        for key in self.to_dict():
            setattr(self, key, getattr(self, key) + getattr(other, key))

    def to_dict(self) -> dict[str, int]:
        return {
            "archive_members_seen": self.archive_members_seen,
            "provider_members_seen": self.provider_members_seen,
            "records_seen": self.records_seen,
            "records_skipped_non_payload": self.records_skipped_non_payload,
            "records_skipped_http_error": self.records_skipped_http_error,
            "records_skipped_empty_body": self.records_skipped_empty_body,
            "parse_errors": self.parse_errors,
            "facts_seen": self.facts_seen,
            "mapped_facts_seen": self.mapped_facts_seen,
            "facts_skipped_missing_evse": self.facts_skipped_missing_evse,
            "facts_skipped_station_scope": self.facts_skipped_station_scope,
            "facts_skipped_timestamp": self.facts_skipped_timestamp,
            "facts_skipped_out_of_window": self.facts_skipped_out_of_window,
            "out_of_order_events": self.out_of_order_events,
            "records_skipped_provider_scope": self.records_skipped_provider_scope,
            "records_skipped_raw_prefilter": self.records_skipped_raw_prefilter,
        }


class DayAccumulator:
    def __init__(self, target_date: date, *, archive_tz: Any):
        self.target_date = target_date
        self.archive_tz = archive_tz
        local_midnight = datetime.combine(target_date, time.min, tzinfo=archive_tz)
        self.window_start = local_midnight.astimezone(timezone.utc)
        self.window_end = (local_midnight + timedelta(days=1)).astimezone(timezone.utc)
        self.evse_states: dict[tuple[str, str], EvseState] = {}
        self.provider_station: dict[tuple[str, str], ProviderStationDayAggregate] = defaultdict(
            ProviderStationDayAggregate
        )
        self.stats = ArchiveStats()

    def observe(
        self,
        *,
        provider_uid: str,
        station_id: str,
        evse_id: str,
        observed_at: datetime,
        availability_status: str,
        operational_status: str,
    ) -> None:
        observed_at = observed_at.astimezone(timezone.utc)
        if observed_at < self.window_start or observed_at > self.window_end:
            self.stats.facts_skipped_out_of_window += 1
            return

        provider_station_key = (provider_uid, station_id)
        station_day = self.provider_station[provider_station_key]
        station_day.matching_observations += 1
        station_day.observed_evses.add(evse_id)
        if availability_status == "occupied":
            station_day.occupied_observations += 1
        observed_text = observed_at.replace(microsecond=0).isoformat()
        if observed_text > station_day.latest_event_timestamp:
            station_day.latest_event_timestamp = observed_text

        evse_key = (provider_uid, evse_id)
        state = self.evse_states.get(evse_key)
        if state is None:
            state = EvseState(provider_uid=provider_uid, station_id=station_id, last_time=self.window_start)
            self.evse_states[evse_key] = state
        elif observed_at < state.last_time:
            self.stats.out_of_order_events += 1
            return

        current_signature = (state.station_id, state.availability_status, state.operational_status)
        next_signature = (station_id, availability_status, operational_status)
        if current_signature == next_signature:
            return

        self._add_interval(state, observed_at)
        state.station_id = station_id
        state.last_time = observed_at
        state.availability_status = availability_status
        state.operational_status = operational_status
        station_day.status_changes += 1

    def finalize(self) -> None:
        for state in self.evse_states.values():
            self._add_interval(state, self.window_end)

    def _add_interval(self, state: EvseState, end_time: datetime) -> None:
        if state.availability_status != "occupied" or end_time <= state.last_time:
            return

        cursor = max(state.last_time, self.window_start)
        end_time = min(end_time, self.window_end)
        station_day = self.provider_station[(state.provider_uid, state.station_id)]
        while cursor < end_time:
            local_cursor = cursor.astimezone(self.archive_tz)
            next_hour = (
                local_cursor.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
            ).astimezone(timezone.utc)
            segment_end = min(end_time, next_hour)
            station_day.hourly_occupied_seconds[local_cursor.hour] += int(
                (segment_end - cursor).total_seconds()
            )
            cursor = segment_end


def _parse_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def default_end_date(config: AppConfig) -> date:
    return datetime.now(config.archive_timezone()).date() - timedelta(days=1)


def date_window(end_date: date, days: int) -> list[date]:
    if days < 1:
        raise SystemExit("--days must be at least 1")
    return [end_date - timedelta(days=offset) for offset in range(days - 1, -1, -1)]


def archive_path_for_date(config: AppConfig, target_date: date) -> Path:
    return config.archive_dir / f"live-provider-responses-{target_date.isoformat()}.tgz"


def parse_archive_date(path: Path) -> date | None:
    match = ARCHIVE_NAME_RE.search(path.name)
    if match is None:
        return None
    return date.fromisoformat(match.group(1))


def record_timestamp(record: dict[str, Any]) -> str:
    return str(record.get("received_at") or record.get("fetched_at") or record.get("logged_at") or "")


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


def _value_field(value: Any) -> Any:
    if isinstance(value, dict):
        return value.get("value")
    return value


def _choose_latest_timestamp(values: Iterable[str]) -> str:
    latest_text = ""
    latest_dt: datetime | None = None
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        parsed = parse_iso_datetime(text)
        if parsed is None:
            continue
        if latest_dt is None or parsed >= latest_dt:
            latest_dt = parsed
            latest_text = text
    return latest_text


def build_site_station_maps(config: AppConfig) -> dict[str, dict[str, str]]:
    station_catalog_path = config.full_chargers_csv_path or config.chargers_csv_path
    rows: dict[str, dict[str, str]] = defaultdict(dict)
    for match in load_site_matches(config.site_match_path, station_catalog_path):
        rows[match.provider_uid][match.site_id] = match.station_id
    return rows


def build_evse_station_maps(config: AppConfig) -> dict[str, dict[str, dict[str, str]]]:
    station_catalog_path = config.full_chargers_csv_path or config.chargers_csv_path
    rows: dict[str, dict[str, dict[str, str]]] = defaultdict(dict)
    for match in load_evse_matches(station_catalog_path, config.site_match_path):
        rows[match.provider_uid][match.evse_id] = {
            "station_id": match.station_id,
            "site_id": match.site_id,
            "station_ref": match.station_ref,
        }
    return rows


def build_provider_scopes(config: AppConfig, station_ids: set[str]) -> dict[str, ProviderScope]:
    station_catalog_path = config.full_chargers_csv_path or config.chargers_csv_path
    site_ids_by_provider: dict[str, set[str]] = defaultdict(set)
    station_refs_by_provider: dict[str, set[str]] = defaultdict(set)
    evse_ids_by_provider: dict[str, set[str]] = defaultdict(set)

    for match in load_site_matches(config.site_match_path, station_catalog_path):
        if match.station_id in station_ids and match.site_id:
            site_ids_by_provider[match.provider_uid].add(match.site_id)

    for match in load_evse_matches(station_catalog_path, config.site_match_path):
        if match.station_id not in station_ids:
            continue
        if match.site_id:
            site_ids_by_provider[match.provider_uid].add(match.site_id)
        if match.station_ref:
            station_refs_by_provider[match.provider_uid].add(match.station_ref)
        if match.evse_id:
            evse_ids_by_provider[match.provider_uid].add(match.evse_id)

    provider_uids = set(site_ids_by_provider) | set(station_refs_by_provider) | set(evse_ids_by_provider)
    return {
        provider_uid: ProviderScope(
            site_ids=frozenset(site_ids_by_provider.get(provider_uid, set())),
            station_refs=frozenset(station_refs_by_provider.get(provider_uid, set())),
            evse_ids=frozenset(evse_ids_by_provider.get(provider_uid, set())),
        )
        for provider_uid in provider_uids
    }


def build_provider_prefilters(provider_scopes: dict[str, ProviderScope]) -> dict[str, re.Pattern[bytes]]:
    patterns: dict[str, re.Pattern[bytes]] = {}
    for provider_uid, provider_scope in provider_scopes.items():
        tokens = {
            token
            for token in (set(provider_scope.site_ids) | set(provider_scope.station_refs) | set(provider_scope.evse_ids))
            if len(token) >= 3
        }
        if not tokens:
            continue
        pattern = b"|".join(
            re.escape(token.encode("utf-8"))
            for token in sorted(tokens, key=lambda item: (-len(item), item))
        )
        patterns[provider_uid] = re.compile(pattern)
    return patterns


def site_status_matches_scope(site_status: dict[str, Any], provider_scope: ProviderScope) -> bool:
    site_id = _reference_id(site_status.get("reference"))
    if site_id and site_id in provider_scope.site_ids:
        return True

    for station_status in _dict_items(site_status.get("energyInfrastructureStationStatus")):
        station_ref = _reference_id(station_status.get("reference"))
        if station_ref and station_ref in provider_scope.station_refs:
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
            if evse_id and evse_id in provider_scope.evse_ids:
                return True

    return False


def prune_payload_to_scope(payload: dict[str, Any], provider_scope: ProviderScope) -> dict[str, Any]:
    def prune(value: Any) -> None:
        if isinstance(value, list):
            for item in value:
                prune(item)
            return
        if not isinstance(value, dict):
            return

        generic_evses = value.get("evses")
        if isinstance(generic_evses, list) and provider_scope.evse_ids:
            value["evses"] = [
                item
                for item in generic_evses
                if isinstance(item, dict) and normalize_evse_id(item.get("evseId")) in provider_scope.evse_ids
            ]

        site_statuses = value.get("energyInfrastructureSiteStatus")
        if site_statuses:
            value["energyInfrastructureSiteStatus"] = [
                site_status
                for site_status in _dict_items(site_statuses)
                if site_status_matches_scope(site_status, provider_scope)
            ]

        for item in value.values():
            prune(item)

    prune(payload)
    return payload


def extract_occupancy_events(
    payload: dict[str, Any],
    *,
    site_station_map: dict[str, str],
    evse_station_map: dict[str, dict[str, str]],
    provider_scope: ProviderScope,
) -> list[OccupancyEvent]:
    events_by_key: dict[tuple[str, str], OccupancyEvent] = {}

    def keep_latest(site_id: str, event: OccupancyEvent) -> None:
        key = (site_id, event.evse_id)
        previous = events_by_key.get(key)
        if previous is None:
            events_by_key[key] = event
            return
        prev_dt = parse_iso_datetime(previous.source_observed_at)
        next_dt = parse_iso_datetime(event.source_observed_at)
        if prev_dt is None or (next_dt is not None and next_dt >= prev_dt):
            events_by_key[key] = event

    generic_evses = payload.get("evses")
    if isinstance(generic_evses, list):
        for item in generic_evses:
            if not isinstance(item, dict):
                continue
            evse_id = normalize_evse_id(item.get("evseId"))
            if not evse_id or evse_id not in provider_scope.evse_ids:
                continue
            evse_match = evse_station_map.get(evse_id) or {}
            station_id = str(evse_match.get("station_id") or "")
            if not station_id:
                continue
            availability_status, operational_status = normalize_eliso_occupancy_status(
                item.get("availability_status"),
                item.get("operational_status"),
            )
            keep_latest(
                str(evse_match.get("site_id") or ""),
                OccupancyEvent(
                    station_id=station_id,
                    evse_id=evse_id,
                    availability_status=availability_status,
                    operational_status=operational_status,
                    source_observed_at=str(item.get("mobilithek_last_updated_dts") or "").strip(),
                ),
            )

    for publication in iter_status_publications(payload):
        for site_status in _dict_items(publication.get("energyInfrastructureSiteStatus")):
            site_id = _reference_id(site_status.get("reference"))
            if not site_id:
                continue
            site_in_scope = site_id in provider_scope.site_ids
            site_last_updated = str(site_status.get("lastUpdated") or "").strip()
            site_station_id = site_station_map.get(site_id, "") if site_in_scope else ""

            for station_status in _dict_items(site_status.get("energyInfrastructureStationStatus")):
                station_ref = _reference_id(station_status.get("reference"))
                station_in_scope = bool(station_ref and station_ref in provider_scope.station_refs)
                station_last_updated = str(station_status.get("lastUpdated") or "").strip()

                for refill_point_status in _dict_items(station_status.get("refillPointStatus")):
                    charging_point_status = (
                        refill_point_status.get("aegiElectricChargingPointStatus")
                        or refill_point_status.get("aegiRefillPointStatus")
                        or refill_point_status
                    )
                    if not isinstance(charging_point_status, dict):
                        continue
                    evse_id = normalize_evse_id(_reference_id(charging_point_status.get("reference")))
                    if not evse_id:
                        continue
                    evse_in_scope = evse_id in provider_scope.evse_ids
                    if not (site_in_scope or station_in_scope or evse_in_scope):
                        continue
                    evse_match = evse_station_map.get(evse_id) or {}
                    station_id = site_station_id or str(evse_match.get("station_id") or "")
                    if not station_id:
                        continue
                    availability_status, operational_status = normalize_datex_occupancy_status(
                        _value_field(charging_point_status.get("status")),
                        opening_status=_value_field(charging_point_status.get("openingStatus")),
                        operation_status=_value_field(charging_point_status.get("operationStatus")),
                        status_description=charging_point_status.get("statusDescription"),
                    )
                    source_observed_at = _choose_latest_timestamp(
                        [
                            str(charging_point_status.get("lastUpdated") or "").strip(),
                            station_last_updated,
                            site_last_updated,
                        ]
                    )
                    keep_latest(
                        site_id,
                        OccupancyEvent(
                            station_id=station_id,
                            evse_id=evse_id,
                            availability_status=availability_status,
                            operational_status=operational_status,
                            source_observed_at=source_observed_at,
                        ),
                    )

    return sorted(events_by_key.values(), key=lambda item: (item.station_id, item.evse_id))


def load_station_scope(config: AppConfig, scope: str) -> dict[str, StationRecord]:
    if scope == "all":
        catalog_path = config.full_chargers_csv_path or config.chargers_csv_path
    else:
        catalog_path = config.chargers_csv_path
    return {station.station_id: station for station in load_station_records(catalog_path)}


def iter_member_records(archive_path: Path) -> Iterable[tuple[str, str, bytes]]:
    with tarfile.open(archive_path, mode="r|gz") as archive:
        for member in archive:
            if not member.isfile() or member.name == "manifest.json":
                continue
            if not (member.name.endswith(".json") or member.name.endswith(".jsonl")):
                continue
            provider_uid = Path(member.name).parts[0] if Path(member.name).parts else ""
            extracted = archive.extractfile(member)
            if extracted is None:
                continue
            if member.name.endswith(".jsonl"):
                for line in extracted:
                    stripped = line.strip()
                    if stripped:
                        yield member.name, provider_uid, stripped
            else:
                raw_record = extracted.read()
                if raw_record.strip():
                    yield member.name, provider_uid, raw_record


def process_archive(
    archive_path: Path,
    *,
    target_date: date,
    config: AppConfig,
    station_ids: set[str],
    site_station_maps: dict[str, dict[str, str]],
    evse_station_maps: dict[str, dict[str, dict[str, str]]],
    provider_scopes: dict[str, ProviderScope],
    provider_prefilters: dict[str, re.Pattern[bytes]],
    quiet: bool,
) -> DayAccumulator:
    accumulator = DayAccumulator(target_date, archive_tz=config.archive_timezone())
    fallback_archive_date = parse_archive_date(archive_path)

    for member_name, member_provider_uid, raw_record in iter_member_records(archive_path):
        accumulator.stats.archive_members_seen += 1
        if member_provider_uid:
            accumulator.stats.provider_members_seen += 1
        accumulator.stats.records_seen += 1

        try:
            record = json.loads(raw_record.decode("utf-8"))
            if not isinstance(record, dict):
                accumulator.stats.records_skipped_non_payload += 1
                continue
            record_kind = str(record.get("kind") or "").strip()
            if record_kind not in {"http_response", "push_request"}:
                accumulator.stats.records_skipped_non_payload += 1
                continue
            http_status = int(record.get("http_status") or 0)
            if record_kind == "http_response" and http_status >= 400:
                accumulator.stats.records_skipped_http_error += 1
                continue
            body_text = str(record.get("body_text") or "")
            if not body_text.strip():
                accumulator.stats.records_skipped_empty_body += 1
                continue

            archive_date_text = str(record.get("archive_date") or "").strip()
            if not archive_date_text and fallback_archive_date is not None:
                archive_date_text = fallback_archive_date.isoformat()
            if archive_date_text and date.fromisoformat(archive_date_text) != target_date:
                continue

            provider_uid = str(record.get("provider_uid") or "").strip() or member_provider_uid
            provider_scope = provider_scopes.get(provider_uid)
            if provider_scope is None:
                accumulator.stats.records_skipped_provider_scope += 1
                continue
            provider_prefilter = provider_prefilters.get(provider_uid)
            if provider_prefilter is not None and provider_prefilter.search(raw_record) is None:
                accumulator.stats.records_skipped_raw_prefilter += 1
                continue
            payload = decode_json_payload(body_text.encode("utf-8"))
            events = extract_occupancy_events(
                payload,
                site_station_map=site_station_maps.get(provider_uid, {}),
                evse_station_map=evse_station_maps.get(provider_uid, {}),
                provider_scope=provider_scope,
            )
        except Exception:
            accumulator.stats.parse_errors += 1
            continue

        message_timestamp = record_timestamp(record)
        for event in events:
            accumulator.stats.facts_seen += 1
            station_id = event.station_id.strip()
            if station_id:
                accumulator.stats.mapped_facts_seen += 1
            if station_id not in station_ids:
                accumulator.stats.facts_skipped_station_scope += 1
                continue
            evse_id = event.evse_id.strip()
            if not evse_id:
                accumulator.stats.facts_skipped_missing_evse += 1
                continue
            observed_at = parse_iso_datetime(str(event.source_observed_at or message_timestamp))
            if observed_at is None:
                accumulator.stats.facts_skipped_timestamp += 1
                continue
            accumulator.observe(
                provider_uid=provider_uid,
                station_id=station_id,
                evse_id=evse_id,
                observed_at=observed_at,
                availability_status=str(event.availability_status or "unknown"),
                operational_status=str(event.operational_status or ""),
            )

    accumulator.finalize()
    if not quiet:
        print(
            f"{target_date.isoformat()}: {len(accumulator.provider_station)} provider-stations, "
            f"{accumulator.stats.mapped_facts_seen} mapped facts",
            file=sys.stderr,
            flush=True,
        )
    return accumulator


def merge_day(
    aggregates: dict[tuple[str, str], ProviderStationAggregate],
    *,
    target_date: date,
    day: DayAccumulator,
) -> None:
    target_date_text = target_date.isoformat()
    for key, daily in day.provider_station.items():
        if daily.matching_observations <= 0:
            continue
        aggregate = aggregates[key]
        for hour, seconds in enumerate(daily.hourly_occupied_seconds):
            aggregate.hourly_occupied_sum[hour] += seconds / 3600
        aggregate.observed_days.add(target_date_text)
        aggregate.observed_evses.update(daily.observed_evses)
        aggregate.matching_observations += daily.matching_observations
        aggregate.occupied_observations += daily.occupied_observations
        aggregate.status_changes += daily.status_changes
        if daily.latest_event_timestamp > aggregate.latest_event_timestamp:
            aggregate.latest_event_timestamp = daily.latest_event_timestamp


def select_primary_provider_rows(
    aggregates: dict[tuple[str, str], ProviderStationAggregate],
) -> dict[str, tuple[str, ProviderStationAggregate]]:
    rows_by_station: dict[str, list[tuple[str, ProviderStationAggregate]]] = defaultdict(list)
    for (provider_uid, station_id), aggregate in aggregates.items():
        rows_by_station[station_id].append((provider_uid, aggregate))

    selected: dict[str, tuple[str, ProviderStationAggregate]] = {}
    for station_id, rows in rows_by_station.items():
        selected[station_id] = max(
            rows,
            key=lambda item: (
                len(item[1].observed_days),
                len(item[1].observed_evses),
                item[1].matching_observations,
                item[1].occupied_observations,
                item[1].status_changes,
                item[1].latest_event_timestamp,
                item[0],
            ),
        )
    return selected


def station_payload(
    *,
    station: StationRecord,
    provider_uid: str,
    aggregate: ProviderStationAggregate,
    denominator_days: int,
) -> dict[str, Any]:
    hourly_values = [
        round((value / denominator_days) if denominator_days > 0 else 0.0, 3)
        for value in aggregate.hourly_occupied_sum
    ]
    peak_value = max(hourly_values) if hourly_values else 0.0
    peak_hour = hourly_values.index(peak_value) if hourly_values else 0
    return {
        "station_id": station.station_id,
        "operator": station.operator,
        "address": station.address,
        "postcode": station.postcode,
        "city": station.city,
        "lat": round(station.lat, 7),
        "lon": round(station.lon, 7),
        "charging_points_count": station.charging_points_count,
        "max_power_kw": station.max_power_kw,
        "provider_uid": provider_uid,
        "observed_days": len(aggregate.observed_days),
        "observed_evses": len(aggregate.observed_evses),
        "matching_observations": aggregate.matching_observations,
        "occupied_observations": aggregate.occupied_observations,
        "status_changes": aggregate.status_changes,
        "latest_event_timestamp": aggregate.latest_event_timestamp,
        "peak_hour": f"{peak_hour:02d}:00",
        "peak_average_occupied": peak_value,
        "hourly_average_occupied": hourly_values,
    }


def build_payload(
    *,
    start_date: date,
    end_date: date,
    requested_days: int,
    included_dates: list[date],
    missing_archives: list[str],
    station_scope: str,
    denominator_mode: str,
    raw_prefilter: bool,
    station_catalog: dict[str, StationRecord],
    aggregates: dict[tuple[str, str], ProviderStationAggregate],
    stats: ArchiveStats,
) -> dict[str, Any]:
    selected = select_primary_provider_rows(aggregates)
    denominator_for_summary = len(included_dates) if denominator_mode == "included-days" else None
    stations: list[dict[str, Any]] = []
    for station_id in sorted(selected):
        station = station_catalog.get(station_id)
        if station is None:
            continue
        provider_uid, aggregate = selected[station_id]
        denominator_days = denominator_for_summary or len(aggregate.observed_days)
        stations.append(
            station_payload(
                station=station,
                provider_uid=provider_uid,
                aggregate=aggregate,
                denominator_days=denominator_days,
            )
        )

    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "requested_days": requested_days,
        "included_days": len(included_dates),
        "included_dates": [value.isoformat() for value in included_dates],
        "missing_archives": missing_archives,
        "timezone": "Europe/Berlin",
        "station_scope": station_scope,
        "provider_selection": "primary",
        "denominator": denominator_mode,
        "raw_prefilter": raw_prefilter,
        "metric": (
            "Mean occupied EVSEs per local hour. Unknown or missing status is not counted as occupied; "
            "one primary provider is selected per station to avoid duplicate provider mappings."
        ),
        "hour_labels": [f"{hour:02d}:00" for hour in range(24)],
        "source": {
            "archive_pattern": "live-provider-responses-YYYY-MM-DD.tgz",
            "mapping": "data/mobilithek_afir_static_matches.csv",
        },
        "summary": {
            "station_count": len(stations),
            "provider_station_count": len(aggregates),
            "station_catalog_count": len(station_catalog),
            "stations_with_occupied_observations": sum(
                1 for _key, aggregate in aggregates.items() if aggregate.occupied_observations > 0
            ),
            "matching_observations": sum(aggregate.matching_observations for aggregate in aggregates.values()),
            "occupied_observations": sum(aggregate.occupied_observations for aggregate in aggregates.values()),
            "status_changes": sum(aggregate.status_changes for aggregate in aggregates.values()),
            "archive_stats": stats.to_dict(),
        },
        "stations": stations,
    }


def generate_batch_station_occupancy(
    *,
    end_date: date | None = None,
    days: int = DEFAULT_DAYS,
    archive_dir: Path | None = None,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    scope: str = "fast",
    denominator: str = "included-days",
    require_complete: bool = False,
    write_latest: bool = True,
    raw_prefilter: bool = False,
    pretty: bool = False,
    quiet: bool = False,
    config: AppConfig | None = None,
) -> dict[str, Any]:
    effective_config = config or AppConfig()
    if archive_dir is not None:
        effective_config = replace(effective_config, archive_dir=archive_dir)
    effective_end_date = end_date or default_end_date(effective_config)
    target_dates = date_window(effective_end_date, days)
    start_date = target_dates[0]

    archive_paths: list[tuple[date, Path]] = []
    missing_archives: list[str] = []
    for target_date in target_dates:
        archive_path = archive_path_for_date(effective_config, target_date)
        if archive_path.exists():
            archive_paths.append((target_date, archive_path))
        else:
            missing_archives.append(target_date.isoformat())

    if missing_archives and require_complete:
        raise SystemExit(f"Missing archives for requested window: {', '.join(missing_archives)}")
    if not archive_paths:
        raise SystemExit("No local archives matched the requested window")

    station_catalog = load_station_scope(effective_config, scope)
    site_station_maps = build_site_station_maps(effective_config)
    evse_station_maps = build_evse_station_maps(effective_config)
    provider_scopes = build_provider_scopes(effective_config, set(station_catalog))
    provider_prefilters = build_provider_prefilters(provider_scopes) if raw_prefilter else {}

    aggregates: dict[tuple[str, str], ProviderStationAggregate] = defaultdict(ProviderStationAggregate)
    combined_stats = ArchiveStats()
    for index, (target_date, archive_path) in enumerate(archive_paths, start=1):
        if not quiet:
            print(
                f"[{index}/{len(archive_paths)}] {target_date.isoformat()}: reading {archive_path}",
                file=sys.stderr,
                flush=True,
            )
        day = process_archive(
            archive_path,
            target_date=target_date,
            config=effective_config,
            station_ids=set(station_catalog),
            site_station_maps=site_station_maps,
            evse_station_maps=evse_station_maps,
            provider_scopes=provider_scopes,
            provider_prefilters=provider_prefilters,
            quiet=quiet,
        )
        combined_stats.add(day.stats)
        merge_day(aggregates, target_date=target_date, day=day)

    payload = build_payload(
        start_date=start_date,
        end_date=effective_end_date,
        requested_days=days,
        included_dates=[target_date for target_date, _path in archive_paths],
        missing_archives=missing_archives,
        station_scope=scope,
        denominator_mode=denominator,
        raw_prefilter=raw_prefilter,
        station_catalog=station_catalog,
        aggregates=aggregates,
        stats=combined_stats,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    output_stem = f"station-occupancy-{start_date.isoformat()}-to-{effective_end_date.isoformat()}"
    json_path = output_dir / f"{output_stem}.json"
    write_json_atomic(json_path, payload, pretty=pretty)
    latest_path = None
    if write_latest:
        latest_path = output_dir / "station-occupancy-latest.json"
        write_json_atomic(latest_path, payload, pretty=pretty)

    return {
        "start_date": start_date.isoformat(),
        "end_date": effective_end_date.isoformat(),
        "requested_days": days,
        "included_days": len(archive_paths),
        "missing_archives": missing_archives,
        "station_scope": scope,
        "station_count": payload["summary"]["station_count"],
        "provider_station_count": payload["summary"]["provider_station_count"],
        "matching_observations": payload["summary"]["matching_observations"],
        "status_changes": payload["summary"]["status_changes"],
        "json": str(json_path.resolve()),
        "latest_json": str(latest_path.resolve()) if latest_path is not None else "",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create all-station hourly occupancy JSON from local AFIR archives")
    parser.add_argument(
        "--date",
        type=_parse_date,
        default=None,
        help="End date in YYYY-MM-DD. Defaults to yesterday in the configured archive timezone.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of trailing days to average, ending at --date. Defaults to {DEFAULT_DAYS}.",
    )
    parser.add_argument(
        "--scope",
        choices=("fast", "all"),
        default="fast",
        help="Station catalog scope. Defaults to fast chargers.",
    )
    parser.add_argument(
        "--denominator",
        choices=("included-days", "observed-days"),
        default="included-days",
        help="Average over all included archive days or only days where the selected provider observed the station.",
    )
    parser.add_argument("--archive-dir", type=Path, default=None, help="Directory containing local archive tgz files")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Output directory for JSON files")
    parser.add_argument("--env-file", type=Path, default=None, help="Optional env file with local archive settings")
    parser.add_argument("--require-complete", action="store_true", help="Fail if any archive in the requested window is missing")
    parser.add_argument("--no-latest", action="store_true", help="Do not update station-occupancy-latest.json")
    parser.add_argument(
        "--raw-prefilter",
        action="store_true",
        help="Try raw identifier prefiltering before JSON decoding. This can be slower for large providers.",
    )
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-archive progress output on stderr")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.env_file is not None:
        load_env_file(args.env_file, allowed_keys=LOCAL_ARCHIVE_ENV_FILE_KEYS)
    result = generate_batch_station_occupancy(
        end_date=args.date,
        days=args.days,
        archive_dir=args.archive_dir,
        output_dir=args.output_dir,
        scope=args.scope,
        denominator=args.denominator,
        require_complete=args.require_complete,
        write_latest=not args.no_latest,
        raw_prefilter=args.raw_prefilter,
        pretty=args.pretty,
        quiet=args.quiet,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
