from __future__ import annotations

import csv
import json
import tempfile
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from itertools import groupby
from pathlib import Path
from typing import Any

from analysis.afir_history import run_analysis
from analysis.output_io import publish_staged_directory, staged_output_directory, write_json
from backend.config import AppConfig
from backend.loaders import load_station_records

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MANAGEMENT_OUTPUT_ROOT = REPO_ROOT / "data" / "management"
DEFAULT_PUBLIC_SITE_ORIGIN = "https://woladen.de"
HF_DATASET_URL = "https://huggingface.co/datasets/loffenauer/AFIR"
SNAPSHOT_TOP_LIMIT = 10
SNAPSHOT_PROVIDER_LIMIT = 30


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _snapshot_relpath(target_date: date) -> Path:
    return Path("days") / f"{target_date:%Y}" / f"{target_date:%m}" / f"{target_date:%d}" / "snapshot.json"


def _snapshot_path(root: Path, target_date: date) -> Path:
    return root / _snapshot_relpath(target_date)


def _archive_path(archive_dir: Path, target_date: date) -> Path:
    return archive_dir / f"live-provider-responses-{target_date.isoformat()}.tgz"


def _public_snapshot_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "snapshot_date": str(payload.get("snapshot_date") or ""),
        "generated_at": str(payload.get("generated_at") or ""),
        "source": dict(payload.get("source") or {}),
        "summary": dict(payload.get("summary") or {}),
        "busiest_stations": list(payload.get("busiest_stations") or [])[:SNAPSHOT_TOP_LIMIT],
        "broken_stations": list(payload.get("broken_stations") or [])[:SNAPSHOT_TOP_LIMIT],
        "provider_reports": list(payload.get("provider_reports") or [])[:SNAPSHOT_PROVIDER_LIMIT],
    }


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _int_value(value: Any) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _float_value(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _timestamp_sort_value(value: Any) -> str:
    return str(value or "").strip()


def _absolute_station_url(station_id: str) -> str:
    return f"{DEFAULT_PUBLIC_SITE_ORIGIN}/?station={station_id}"


def _station_metadata(csv_path: Path, *, include_url: bool) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for station in load_station_records(csv_path):
        rows[station.station_id] = {
            "station_id": station.station_id,
            "operator": station.operator,
            "address": station.address,
            "postcode": station.postcode,
            "city": station.city,
            "max_power_kw": station.max_power_kw,
            "charging_points_count": station.charging_points_count,
            "lat": station.lat,
            "lon": station.lon,
            "station_url": _absolute_station_url(station.station_id) if include_url else "",
        }
    return rows


def _select_primary_station_rows(
    *,
    station_daily_rows: list[dict[str, str]],
    station_ids: set[str] | None = None,
) -> dict[str, dict[str, str]]:
    candidate_rows: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in station_daily_rows:
        station_id = str(row.get("station_id") or "").strip()
        if station_id and (station_ids is None or station_id in station_ids):
            candidate_rows[station_id].append(row)

    selected_rows: dict[str, dict[str, str]] = {}
    for station_id, rows in candidate_rows.items():
        selected_rows[station_id] = max(
            rows,
            key=lambda row: (
                _float_value(row.get("station_coverage_vs_catalog")),
                _int_value(row.get("evses_observed")),
                _timestamp_sort_value(row.get("latest_event_timestamp")),
                str(row.get("provider_uid") or ""),
            ),
        )
    return selected_rows


def _build_status_rollups(
    *,
    target_date: date,
    analysis_output_dir: Path,
    station_metadata: dict[str, dict[str, Any]],
    bundle_station_metadata: dict[str, dict[str, Any]],
    primary_station_rows: dict[str, dict[str, str]],
) -> dict[str, Any]:
    target_date_text = target_date.isoformat()
    rows = [
        row
        for row in _read_csv_rows(analysis_output_dir / "evse_status_changes.csv")
        if str(row.get("archive_date") or "") == target_date_text
    ]

    station_busy_counts: Counter[str] = Counter()
    station_busy_evses: dict[str, set[str]] = defaultdict(set)
    station_broken_evses: dict[str, set[str]] = defaultdict(set)
    station_broken_duration_seconds: Counter[str] = Counter()
    status_change_count = 0

    primary_provider_by_station_id = {
        station_id: str(row.get("provider_uid") or "")
        for station_id, row in primary_station_rows.items()
    }

    def _is_primary_row(row: dict[str, str]) -> bool:
        station_id = str(row.get("station_id") or "")
        provider_uid = str(row.get("provider_uid") or "")
        return (
            station_id in primary_provider_by_station_id
            and primary_provider_by_station_id[station_id] == provider_uid
        )

    primary_rows = [row for row in rows if _is_primary_row(row)]

    for row in primary_rows:
        status_change_count += 1
        station_id = str(row.get("station_id") or "")
        provider_evse_id = str(row.get("provider_evse_id") or "")
        availability_status = str(row.get("availability_status") or "")
        if availability_status == "out_of_order":
            if provider_evse_id:
                station_broken_evses[station_id].add(provider_evse_id)
            station_broken_duration_seconds[station_id] += _int_value(row.get("duration_seconds"))

    primary_rows_sorted = sorted(
        primary_rows,
        key=lambda row: (
            str(row.get("station_id") or ""),
            str(row.get("provider_evse_id") or ""),
            _int_value(row.get("change_rank")),
        ),
    )
    for (_station_id, _provider_evse_id), group in groupby(
        primary_rows_sorted,
        key=lambda row: (str(row.get("station_id") or ""), str(row.get("provider_evse_id") or "")),
    ):
        ordered_rows = list(group)
        for previous_row, current_row in zip(ordered_rows, ordered_rows[1:]):
            previous_status = str(previous_row.get("availability_status") or "")
            current_status = str(current_row.get("availability_status") or "")
            if {previous_status, current_status} != {"free", "occupied"}:
                continue
            station_id = str(current_row.get("station_id") or previous_row.get("station_id") or "")
            provider_evse_id = str(current_row.get("provider_evse_id") or previous_row.get("provider_evse_id") or "")
            if station_id:
                station_busy_counts[station_id] += 1
                if provider_evse_id:
                    station_busy_evses[station_id].add(provider_evse_id)

    busiest_stations = []
    for station_id, transition_count in station_busy_counts.most_common(SNAPSHOT_TOP_LIMIT):
        meta = station_metadata.get(station_id, {})
        bundle_meta = bundle_station_metadata.get(station_id, {})
        busiest_stations.append(
            {
                "station_id": station_id,
                "station_url": str(bundle_meta.get("station_url") or ""),
                "operator": str(meta.get("operator") or ""),
                "address": str(meta.get("address") or ""),
                "city": str(meta.get("city") or ""),
                "max_power_kw": meta.get("max_power_kw") or 0,
                "charging_points_count": meta.get("charging_points_count") or 0,
                "busy_transition_count": int(transition_count),
                "busy_evse_count": len(station_busy_evses.get(station_id, set())),
            }
        )

    broken_stations = []
    for station_id, selected_row in primary_station_rows.items():
        meta = station_metadata.get(station_id, {})
        bundle_meta = bundle_station_metadata.get(station_id, {})
        affected_charger_count = len(station_broken_evses.get(station_id, set()))
        current_broken_charger_count = _int_value(selected_row.get("out_of_order_evses"))
        if affected_charger_count <= 0 and current_broken_charger_count <= 0:
            continue

        fully_broken_now = _int_value(selected_row.get("station_all_evses_out_of_order")) > 0
        if fully_broken_now:
            status_label = "Komplett gestört"
        elif current_broken_charger_count > 0:
            status_label = "Derzeit eingeschränkt"
        else:
            status_label = "Im Tagesverlauf gestört"

        broken_stations.append(
            {
                "station_id": station_id,
                "station_url": str(bundle_meta.get("station_url") or ""),
                "operator": str(meta.get("operator") or selected_row.get("station_operator") or ""),
                "address": str(meta.get("address") or ""),
                "city": str(meta.get("city") or selected_row.get("station_city") or ""),
                "max_power_kw": meta.get("max_power_kw") or 0,
                "charging_points_count": meta.get("charging_points_count") or 0,
                "affected_charger_count": affected_charger_count,
                "current_broken_charger_count": current_broken_charger_count,
                "out_of_order_duration_seconds_total": int(station_broken_duration_seconds.get(station_id, 0)),
                "fully_broken_now": fully_broken_now,
                "status_label": status_label,
            }
        )

    broken_stations.sort(
        key=lambda row: (
            int(bool(row.get("fully_broken_now"))),
            _int_value(row.get("current_broken_charger_count")),
            _int_value(row.get("out_of_order_duration_seconds_total")),
            _int_value(row.get("affected_charger_count")),
            str(row.get("station_id") or ""),
        ),
        reverse=True,
    )
    broken_stations = broken_stations[:SNAPSHOT_TOP_LIMIT]

    return {
        "status_change_count_total": status_change_count,
        "busiest_stations": busiest_stations,
        "broken_stations": broken_stations,
        "busy_transition_count_total": int(sum(station_busy_counts.values())),
        "busy_evse_count_total": len(
            {
                (station_id, provider_evse_id)
                for station_id, evse_ids in station_busy_evses.items()
                for provider_evse_id in evse_ids
            }
        ),
        "busy_station_count_total": len(station_busy_counts),
        "any_out_of_order_station_total": len(station_broken_evses),
        "out_of_order_duration_seconds_total": int(sum(station_broken_duration_seconds.values())),
        "current_out_of_order_station_total": sum(
            1
            for row in primary_station_rows.values()
            if _int_value(row.get("out_of_order_evses")) > 0
        ),
        "current_out_of_order_evse_total": int(
            sum(_int_value(row.get("out_of_order_evses")) for row in primary_station_rows.values())
        ),
        "fully_out_of_service_station_total": sum(
            1
            for row in primary_station_rows.values()
            if _int_value(row.get("station_all_evses_out_of_order")) > 0
        ),
    }


def _build_provider_reports(*, target_date: date, analysis_output_dir: Path) -> list[dict[str, Any]]:
    target_date_text = target_date.isoformat()
    provider_summary_path = analysis_output_dir / "provider_daily_summary.csv"
    provider_meta: dict[str, dict[str, str]] = {}
    if provider_summary_path.exists():
        provider_meta = {
            str(row.get("provider_uid") or ""): row
            for row in _read_csv_rows(provider_summary_path)
            if str(row.get("archive_date") or "") == target_date_text
        }

    archive_messages_path = analysis_output_dir / "archive_messages.csv"
    if not archive_messages_path.exists():
        return []

    grouped: dict[str, dict[str, Any]] = {}
    for row in _read_csv_rows(archive_messages_path):
        if str(row.get("archive_date") or "") != target_date_text:
            continue
        provider_uid = str(row.get("provider_uid") or "").strip()
        if not provider_uid:
            continue
        meta = provider_meta.get(provider_uid, {})
        report = grouped.setdefault(
            provider_uid,
            {
                "provider_uid": provider_uid,
                "display_name": str(meta.get("display_name") or provider_uid),
                "publisher": str(meta.get("publisher") or ""),
                "messages_total": 0,
                "push_messages_total": 0,
                "http_response_messages_total": 0,
                "fetch_failure_messages_total": 0,
                "http_error_messages_total": 0,
                "payload_byte_length_total": 0,
                "parseable_messages_total": _int_value(meta.get("parseable_messages_total")),
                "observations_total": _int_value(meta.get("extracted_observation_count_total")),
                "mapped_observations_total": _int_value(meta.get("extracted_mapped_observation_count_total")),
                "mapped_observation_ratio": _float_value(meta.get("mapped_observation_ratio")),
                "mapped_stations_observed": _int_value(meta.get("mapped_stations_observed")),
                "mapped_stations_observed_in_bundle": _int_value(meta.get("mapped_stations_observed_in_bundle")),
                "first_message_timestamp": "",
                "latest_message_timestamp": "",
            },
        )
        record_kind = str(row.get("record_kind") or "")
        message_timestamp = _timestamp_sort_value(row.get("message_timestamp"))
        http_status = _int_value(row.get("http_status"))
        report["messages_total"] += 1
        report["payload_byte_length_total"] += _int_value(row.get("payload_byte_length"))
        if record_kind == "push_request":
            report["push_messages_total"] += 1
        elif record_kind == "http_response":
            report["http_response_messages_total"] += 1
        elif record_kind == "fetch_failure":
            report["fetch_failure_messages_total"] += 1
        if http_status >= 400:
            report["http_error_messages_total"] += 1
        if message_timestamp:
            if not report["first_message_timestamp"] or message_timestamp < report["first_message_timestamp"]:
                report["first_message_timestamp"] = message_timestamp
            if not report["latest_message_timestamp"] or message_timestamp > report["latest_message_timestamp"]:
                report["latest_message_timestamp"] = message_timestamp

    provider_reports = list(grouped.values())
    provider_reports.sort(
        key=lambda row: (
            -_int_value(row.get("messages_total")),
            -_int_value(row.get("push_messages_total")),
            -_int_value(row.get("payload_byte_length_total")),
            str(row.get("display_name") or row.get("provider_uid") or ""),
        ),
    )
    return provider_reports[:SNAPSHOT_PROVIDER_LIMIT]


def build_management_snapshot_from_analysis_outputs(
    *,
    target_date: date,
    analysis_output_dir: Path,
    output_root: Path = DEFAULT_MANAGEMENT_OUTPUT_ROOT,
    config: AppConfig | None = None,
    analysis_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    effective_config = config or AppConfig()
    bundle_station_metadata = _station_metadata(effective_config.chargers_csv_path, include_url=True)
    station_metadata = _station_metadata(effective_config.full_chargers_csv_path, include_url=False)
    station_daily_rows = [
        row
        for row in _read_csv_rows(analysis_output_dir / "station_daily_summary.csv")
        if str(row.get("archive_date") or "") == target_date.isoformat()
    ]
    primary_station_rows = _select_primary_station_rows(
        station_daily_rows=station_daily_rows,
        station_ids=set(station_metadata),
    )
    primary_bundle_station_rows = _select_primary_station_rows(
        station_daily_rows=station_daily_rows,
        station_ids=set(bundle_station_metadata),
    )
    status_rollups = _build_status_rollups(
        target_date=target_date,
        analysis_output_dir=analysis_output_dir,
        station_metadata=station_metadata,
        bundle_station_metadata=bundle_station_metadata,
        primary_station_rows=primary_station_rows,
    )
    provider_reports = _build_provider_reports(
        target_date=target_date,
        analysis_output_dir=analysis_output_dir,
    )

    summary = {
        "afir_stations_observed": len(primary_station_rows),
        "bundle_stations_observed": len(primary_bundle_station_rows),
        "bundle_stations_observed_unique": len(primary_bundle_station_rows),
        "stations_with_disruptions": status_rollups["any_out_of_order_station_total"],
        "disruptions_at_end_of_day": status_rollups["current_out_of_order_station_total"],
        "current_out_of_order_stations": status_rollups["current_out_of_order_station_total"],
        "current_out_of_order_evses": status_rollups["current_out_of_order_evse_total"],
        "fully_out_of_service_stations": status_rollups["fully_out_of_service_station_total"],
        "out_of_order_duration_seconds_total": status_rollups["out_of_order_duration_seconds_total"],
        "out_of_order_evses_end_of_day": status_rollups["current_out_of_order_evse_total"],
        "stations_all_evses_out_of_order_end_of_day": status_rollups["fully_out_of_service_station_total"],
        "high_utilization_stations": status_rollups["busy_station_count_total"],
        "charger_changes_total": status_rollups["status_change_count_total"],
        "status_changes_total": status_rollups["status_change_count_total"],
        "busy_transition_count": status_rollups["busy_transition_count_total"],
        "busy_evse_count": status_rollups["busy_evse_count_total"],
        "busy_station_count": status_rollups["busy_station_count_total"],
        "archive_messages_total": _int_value((analysis_result or {}).get("message_row_count")),
        "observations_total": _int_value((analysis_result or {}).get("observation_row_count")),
    }
    snapshot = _public_snapshot_payload(
        {
        "snapshot_date": target_date.isoformat(),
        "generated_at": _utc_now_iso(),
        "source": {
            "hf_dataset_url": HF_DATASET_URL,
            "archive_name": _archive_path(effective_config.archive_dir, target_date).name,
            "archive_date": target_date.isoformat(),
        },
        "summary": summary,
        "busiest_stations": status_rollups["busiest_stations"],
        "broken_stations": status_rollups["broken_stations"],
        "provider_reports": provider_reports,
        }
    )

    output_path = _snapshot_path(output_root, target_date)
    write_json(output_path, snapshot, pretty=True)
    return snapshot


def rebuild_management_indexes(output_root: Path = DEFAULT_MANAGEMENT_OUTPUT_ROOT) -> dict[str, Any]:
    snapshot_paths = sorted(output_root.glob("days/*/*/*/snapshot.json"))
    snapshots = []
    for path in snapshot_paths:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        snapshot_date_text = str(payload.get("snapshot_date") or "").strip()
        if not snapshot_date_text:
            continue
        snapshots.append(_public_snapshot_payload(payload))
    snapshots.sort(key=lambda payload: str(payload.get("snapshot_date") or ""))

    available_dates = [str(snapshot.get("snapshot_date") or "") for snapshot in snapshots]
    summary_series = [
        {"snapshot_date": str(snapshot.get("snapshot_date") or "")} | dict(snapshot.get("summary") or {})
        for snapshot in snapshots
    ]

    trends = {
        "generated_at": _utc_now_iso(),
        "available_dates": available_dates,
        "summary_series": summary_series,
    }
    index = {
        "generated_at": _utc_now_iso(),
        "latest_date": available_dates[-1] if available_dates else "",
        "available_dates": available_dates,
        "day_count": len(available_dates),
        "snapshot_paths": {
            snapshot_date_text: str(_snapshot_relpath(date.fromisoformat(snapshot_date_text)).as_posix())
            for snapshot_date_text in available_dates
        },
        "trends_path": "trends.json",
    }

    write_json(output_root / "index.json", index, pretty=True)
    write_json(output_root / "trends.json", trends, pretty=True)
    return {"index": index, "trends": trends}


def generate_management_snapshot(
    *,
    target_date: date,
    archive_dir: Path | None = None,
    output_root: Path = DEFAULT_MANAGEMENT_OUTPUT_ROOT,
    config: AppConfig | None = None,
) -> dict[str, Any]:
    effective_config = config or AppConfig()
    effective_archive_dir = archive_dir or effective_config.archive_dir
    archive_path = _archive_path(effective_archive_dir, target_date)
    if not archive_path.exists():
        raise FileNotFoundError(f"missing_archive:{archive_path}")

    with tempfile.TemporaryDirectory(prefix=f"management-{target_date.isoformat()}-") as temp_dir_str:
        temp_dir = Path(temp_dir_str)
        analysis_output_dir = temp_dir / "analysis-output"
        analysis_result = run_analysis(
            archive_paths=[archive_path],
            output_dir=analysis_output_dir,
            config=effective_config,
        )

        with staged_output_directory(output_root) as staged_output_root:
            existing_snapshot_paths = sorted(output_root.glob("days/*/*/*/snapshot.json"))
            for existing_snapshot_path in existing_snapshot_paths:
                relative_path = existing_snapshot_path.relative_to(output_root)
                staged_path = staged_output_root / relative_path
                staged_path.parent.mkdir(parents=True, exist_ok=True)
                try:
                    existing_payload = json.loads(existing_snapshot_path.read_text(encoding="utf-8"))
                except Exception:
                    continue
                if isinstance(existing_payload, dict):
                    write_json(staged_path, _public_snapshot_payload(existing_payload), pretty=True)

            snapshot = build_management_snapshot_from_analysis_outputs(
                target_date=target_date,
                analysis_output_dir=analysis_output_dir,
                output_root=staged_output_root,
                config=effective_config,
                analysis_result=analysis_result,
            )
            index_payloads = rebuild_management_indexes(staged_output_root)
            publish_staged_directory(staged_output_root, output_root)

    return {
        "snapshot_date": target_date.isoformat(),
        "output_root": str(output_root.resolve()),
        "snapshot_path": str(_snapshot_path(output_root, target_date).resolve()),
        "archive_path": str(archive_path.resolve()),
        "summary": snapshot["summary"],
        "broken_station_count": len(snapshot["broken_stations"]),
        "busy_station_count": len(snapshot["busiest_stations"]),
        "available_dates": index_payloads["index"]["available_dates"],
    }
