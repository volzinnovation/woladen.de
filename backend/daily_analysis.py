from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable

HOUR_LABELS = tuple(f"{hour:02d}:00" for hour in range(24))
OUT_OF_ORDER_PROBABILITY_LABELS = {
    "mostly_broken": "mostly broken",
    "often_broken": "often broken",
    "sometimes_broken": "sometimes broken",
}
OUT_OF_ORDER_PROBABILITY_THRESHOLDS = (
    (0.50, "mostly_broken"),
    (0.25, "often_broken"),
    (0.01, "sometimes_broken"),
)
OCCUPANCY_STATUS_LABELS = {
    "constrained_hotspot": "often occupied",
    "high_demand": "often occupied",
}
OCCUPANCY_STATUS_THRESHOLDS = (
    (0.80, "constrained_hotspot"),
    (0.55, "high_demand"),
)
CONFIDENT_ANALYSIS_LABELS = {"high", "medium"}
FREQUENT_OUT_OF_ORDER_STATUSES = {"mostly_broken", "often_broken"}
FREQUENT_OCCUPIED_STATUSES = {"constrained_hotspot", "high_demand"}
DAILY_ANALYSIS_OUT_OF_ORDER_LABEL = "h\u00e4ufig gest\u00f6rt laut Tagesanalyse"
DAILY_ANALYSIS_OCCUPIED_LABEL = "h\u00e4ufig belegt laut Tagesanalyse"


def classify_out_of_order_probability(value: Any) -> str:
    probability = _float_or_none(value)
    if probability is None:
        return ""
    for threshold, status in OUT_OF_ORDER_PROBABILITY_THRESHOLDS:
        if probability > threshold:
            return status
    return ""


def out_of_order_probability_label(status: str) -> str:
    return OUT_OF_ORDER_PROBABILITY_LABELS.get(str(status or "").strip(), "")


def classify_occupancy_share(value: Any) -> str:
    share = _float_or_none(value)
    if share is None:
        return ""
    for threshold, status in OCCUPANCY_STATUS_THRESHOLDS:
        if share >= threshold:
            return status
    return ""


def occupancy_status_label(status: str) -> str:
    return OCCUPANCY_STATUS_LABELS.get(str(status or "").strip(), "")


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _float_or_zero(value: Any) -> float:
    parsed = _float_or_none(value)
    return parsed if parsed is not None else 0.0


def _int_or_zero(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _json_array(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _row_keys(row: sqlite3.Row) -> set[str]:
    try:
        return set(row.keys())
    except AttributeError:
        return set()


def _row_value(row: sqlite3.Row, key: str, default: Any = None, *, keys: set[str] | None = None) -> Any:
    available = keys if keys is not None else _row_keys(row)
    if key not in available:
        return default
    return row[key]


def _profile_out_of_order_probability(profile: sqlite3.Row, keys: set[str]) -> float | None:
    value = _row_value(profile, "out_of_order_probability", None, keys=keys)
    if value is None:
        value = _row_value(profile, "out_of_order_share", None, keys=keys)
    return _float_or_none(value)


def _profile_out_of_order_status(profile: sqlite3.Row, keys: set[str], probability: float | None) -> str:
    status = str(_row_value(profile, "out_of_order_probability_status", "", keys=keys) or "").strip()
    if status in OUT_OF_ORDER_PROBABILITY_LABELS:
        return status
    return classify_out_of_order_probability(probability)


def _profile_confidence_label(profile: sqlite3.Row, keys: set[str]) -> str:
    return str(_row_value(profile, "confidence_label", "low", keys=keys) or "low").strip().lower() or "low"


def _profile_occupancy_status(profile: sqlite3.Row, keys: set[str]) -> str:
    status = str(_row_value(profile, "occupancy_probability_status", "", keys=keys) or "").strip()
    if status in OCCUPANCY_STATUS_LABELS:
        return status
    return classify_occupancy_share(_row_value(profile, "occupancy_share", None, keys=keys))


def _daily_analysis_fields(
    *,
    confidence_label: str,
    out_of_order_status: str,
    occupancy_status: str,
) -> dict[str, Any]:
    is_confident = confidence_label in CONFIDENT_ANALYSIS_LABELS
    frequently_out_of_order = is_confident and out_of_order_status in FREQUENT_OUT_OF_ORDER_STATUSES
    frequently_occupied = is_confident and occupancy_status in FREQUENT_OCCUPIED_STATUSES
    labels: list[str] = []
    if frequently_out_of_order:
        labels.append(DAILY_ANALYSIS_OUT_OF_ORDER_LABEL)
    if frequently_occupied:
        labels.append(DAILY_ANALYSIS_OCCUPIED_LABEL)
    return {
        "daily_analysis_data_available": is_confident,
        "frequently_out_of_order_daily_analysis": frequently_out_of_order,
        "frequently_occupied_daily_analysis": frequently_occupied,
        "daily_analysis_out_of_order_color": "sehr_hellrot" if frequently_out_of_order else "",
        "daily_analysis_occupied_color": "hellgrau" if frequently_occupied else "",
        "occupancy_probability_status": occupancy_status,
        "occupancy_probability_label": occupancy_status_label(occupancy_status),
        "station_qualification_labels": "|".join(labels),
        "daily_analysis": {
            "data_available": is_confident,
            "frequently_out_of_order": frequently_out_of_order,
            "frequently_occupied": frequently_occupied,
            "out_of_order_color": "sehr_hellrot" if frequently_out_of_order else "",
            "occupied_color": "hellgrau" if frequently_occupied else "",
            "labels": labels,
        },
    }


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def _connect_readonly(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _candidate_station_ids(station_id: str) -> list[str]:
    text = str(station_id or "").strip()
    if not text:
        return []
    candidates = [text]
    if text.lower().startswith("de:"):
        suffix = text[3:]
        candidates.extend((f"DE:{suffix}", suffix))
    elif ":" not in text:
        candidates.append(f"DE:{text}")
    seen: set[str] = set()
    unique: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            unique.append(candidate)
            seen.add(candidate)
    return unique


def _normalize_requested_station_ids(station_ids: Iterable[str]) -> list[str]:
    requested: list[str] = []
    seen: set[str] = set()
    for raw_station_id in station_ids:
        station_id = str(raw_station_id or "").strip()
        if not station_id or station_id in seen:
            continue
        requested.append(station_id)
        seen.add(station_id)
    return requested


def _safe_station_id(station_id: str) -> str:
    safe_station_id = re.sub(r"[^A-Za-z0-9._-]+", "_", station_id.strip())
    return safe_station_id.strip("._-") or "station"


def _station_relative_path(station_id: str) -> Path:
    safe_station_id = _safe_station_id(station_id)
    shard_key = re.sub(r"[^A-Za-z0-9]+", "", safe_station_id).lower()
    shards = [shard_key[index : index + 2] for index in range(0, min(len(shard_key), 6), 2)]
    shards = [shard for shard in shards if shard]
    return Path(*shards, f"{safe_station_id}.json") if shards else Path(f"{safe_station_id}.json")


def _station_occupancy_path(root: Path, station_id: str) -> Path | None:
    for candidate in _candidate_station_ids(station_id):
        bare_candidate = candidate[3:] if candidate.lower().startswith("de:") else candidate
        path = root / _station_relative_path(bare_candidate)
        if path.exists() and path.is_file():
            return path
    return None


def _hourly_occupied_values(payload: dict[str, Any]) -> list[float]:
    hourly = payload.get("hourly_average_occupied")
    if isinstance(hourly, list):
        return [_float_or_zero(value) for value in hourly[:24]]
    if isinstance(hourly, dict):
        return [_float_or_zero(hourly.get(label, 0.0)) for label in HOUR_LABELS]
    return []


def _json_occupancy_share(payload: dict[str, Any]) -> float | None:
    station = payload.get("station") if isinstance(payload.get("station"), dict) else {}
    charging_points = _float_or_none(station.get("charging_points_count")) or _float_or_none(
        payload.get("observed_evses")
    )
    if charging_points is None or charging_points <= 0:
        return None
    hourly_values = _hourly_occupied_values(payload)
    if not hourly_values:
        return None
    return min(max(sum(hourly_values) / len(hourly_values) / charging_points, 0.0), 1.0)


def _station_profile_payload(profile: sqlite3.Row) -> dict[str, Any]:
    station_id = str(profile["station_id"] or "")
    profile_keys = _row_keys(profile)
    out_of_order_probability = _profile_out_of_order_probability(profile, profile_keys)
    out_of_order_status = _profile_out_of_order_status(profile, profile_keys, out_of_order_probability)
    occupancy_status = _profile_occupancy_status(profile, profile_keys)
    confidence_label = _profile_confidence_label(profile, profile_keys)
    return {
        "station_id": station_id,
        "data_available": True,
        "start_date": str(profile["start_date"] or ""),
        "end_date": str(profile["end_date"] or ""),
        "included_days": _int_or_zero(profile["included_days"]),
        "observed_days": _int_or_zero(profile["observed_days"]),
        "latest_observed_at": str(profile["latest_observed_at"] or ""),
        "generated_at": str(profile["generated_at"] or ""),
        "confidence_label": confidence_label,
        "out_of_order_probability_status": out_of_order_status,
        "out_of_order_probability_label": out_of_order_probability_label(out_of_order_status),
        "provider_uids": [str(value) for value in _json_array(profile["provider_uids_json"])],
        **_daily_analysis_fields(
            confidence_label=confidence_label,
            out_of_order_status=out_of_order_status,
            occupancy_status=occupancy_status,
        ),
    }


def _station_json_payload(payload: dict[str, Any], requested_station_id: str) -> dict[str, Any]:
    occupancy_share = _json_occupancy_share(payload)
    out_of_order_probability = _float_or_none(
        payload.get("out_of_order_probability", payload.get("out_of_order_share"))
    )
    out_of_order_status = str(payload.get("out_of_order_probability_status") or "").strip()
    if out_of_order_status not in OUT_OF_ORDER_PROBABILITY_LABELS:
        out_of_order_status = classify_out_of_order_probability(out_of_order_probability)
    occupancy_status = str(payload.get("occupancy_probability_status") or "").strip()
    if occupancy_status not in OCCUPANCY_STATUS_LABELS:
        occupancy_status = classify_occupancy_share(occupancy_share)
    included_days = _int_or_zero(payload.get("included_days"))
    confidence_label = "medium" if included_days >= 3 and occupancy_share is not None else "low"
    station_id = str(payload.get("station_id") or requested_station_id).strip()
    return {
        "station_id": station_id,
        "data_available": True,
        "start_date": str(payload.get("start_date") or ""),
        "end_date": str(payload.get("end_date") or ""),
        "included_days": included_days,
        "observed_days": included_days,
        "latest_observed_at": str(payload.get("latest_observed_at") or ""),
        "generated_at": str(payload.get("generated_at") or ""),
        "confidence_label": confidence_label,
        "out_of_order_probability_status": out_of_order_status,
        "out_of_order_probability_label": out_of_order_probability_label(out_of_order_status),
        "provider_uids": [str(value) for value in payload.get("provider_uids") or []],
        **_daily_analysis_fields(
            confidence_label=confidence_label,
            out_of_order_status=out_of_order_status,
            occupancy_status=occupancy_status,
        ),
    }


class DailyAnalysisReader:
    def __init__(self, sqlite_path: Path | None, station_occupancy_dir: Path | None = None):
        self.sqlite_path = sqlite_path
        self.station_occupancy_dir = station_occupancy_dir

    def station_insights_by_requested_id(self, station_ids: Iterable[str]) -> dict[str, dict[str, Any]]:
        requested = _normalize_requested_station_ids(station_ids)
        if not requested:
            return {}
        insights = self._sqlite_insights_by_requested_id(requested)
        missing = [station_id for station_id in requested if station_id not in insights]
        if missing:
            insights.update(self._json_insights_by_requested_id(missing))
        return insights

    def _sqlite_insights_by_requested_id(self, requested: list[str]) -> dict[str, dict[str, Any]]:
        if self.sqlite_path is None or not self.sqlite_path.exists() or not self.sqlite_path.is_file():
            return {}
        lookup_ids: list[str] = []
        seen_lookup_ids: set[str] = set()
        for station_id in requested:
            for candidate in _candidate_station_ids(station_id):
                if candidate not in seen_lookup_ids:
                    lookup_ids.append(candidate)
                    seen_lookup_ids.add(candidate)
        if not lookup_ids:
            return {}
        placeholders = ",".join("?" for _ in lookup_ids)
        connection: sqlite3.Connection | None = None
        try:
            connection = _connect_readonly(self.sqlite_path)
            if not _table_exists(connection, "station_occupancy_profiles"):
                return {}
            rows = connection.execute(
                f"""
                SELECT *
                FROM station_occupancy_profiles
                WHERE station_id IN ({placeholders})
                """,
                lookup_ids,
            ).fetchall()
        except sqlite3.Error:
            return {}
        finally:
            if connection is not None:
                connection.close()
        profiles_by_station_id = {str(row["station_id"] or ""): row for row in rows}
        insights: dict[str, dict[str, Any]] = {}
        for station_id in requested:
            profile = None
            for candidate in _candidate_station_ids(station_id):
                profile = profiles_by_station_id.get(candidate)
                if profile is not None:
                    break
            if profile is not None:
                insights[station_id] = _station_profile_payload(profile)
        return insights

    def _json_insights_by_requested_id(self, requested: list[str]) -> dict[str, dict[str, Any]]:
        if (
            self.station_occupancy_dir is None
            or not self.station_occupancy_dir.exists()
            or not self.station_occupancy_dir.is_dir()
        ):
            return {}
        insights: dict[str, dict[str, Any]] = {}
        for station_id in requested:
            path = _station_occupancy_path(self.station_occupancy_dir, station_id)
            if path is None:
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(payload, dict):
                insights[station_id] = _station_json_payload(payload, station_id)
        return insights
