from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .config import AppConfig
from .models import DynamicFact, PriceSnapshot, ProviderTarget, SiteMatch, StationRecord

LIVE_JSON_FIELDS = ("next_available_charging_slots", "supplemental_facility_status")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _iso_or_empty(value: str | None) -> str:
    return str(value or "").strip()


def _parse_iso_utc(value: str | None) -> datetime | None:
    text = _iso_or_empty(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _shift_iso(value: str | None, seconds: float) -> str:
    base = _parse_iso_utc(value) or datetime.now(timezone.utc)
    shifted = base + timedelta(seconds=max(0.0, seconds))
    return shifted.replace(microsecond=0).isoformat()


def _seconds_until_iso(value: str | None) -> float | None:
    target = _parse_iso_utc(value)
    if target is None:
        return None
    return (target - datetime.now(timezone.utc)).total_seconds()


class LiveStore:
    def __init__(self, config: AppConfig):
        self.config = config

    @contextmanager
    def connection(self):
        conn = sqlite3.connect(self.config.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={self.config.sqlite_busy_timeout_ms}")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def initialize(self) -> None:
        self.config.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.raw_payload_dir.mkdir(parents=True, exist_ok=True)
        self.config.archive_dir.mkdir(parents=True, exist_ok=True)
        dropped_legacy_history = False
        with self.connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS providers (
                    provider_uid TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    publisher TEXT NOT NULL,
                    publication_id TEXT NOT NULL,
                    access_mode TEXT NOT NULL,
                    fetch_kind TEXT NOT NULL,
                    fetch_url TEXT NOT NULL,
                    subscription_id TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    delta_delivery INTEGER NOT NULL DEFAULT 0,
                    retention_period_minutes INTEGER,
                    last_polled_at TEXT NOT NULL DEFAULT '',
                    next_poll_at TEXT NOT NULL DEFAULT '',
                    last_result TEXT NOT NULL DEFAULT '',
                    last_error_text TEXT NOT NULL DEFAULT '',
                    consecutive_no_data_count INTEGER NOT NULL DEFAULT 0,
                    consecutive_error_count INTEGER NOT NULL DEFAULT 0,
                    consecutive_unchanged_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS provider_site_matches (
                    provider_uid TEXT NOT NULL,
                    site_id TEXT NOT NULL,
                    station_id TEXT NOT NULL,
                    score REAL NOT NULL DEFAULT 0,
                    PRIMARY KEY (provider_uid, site_id)
                );

                CREATE TABLE IF NOT EXISTS stations (
                    station_id TEXT PRIMARY KEY,
                    operator TEXT NOT NULL,
                    address TEXT NOT NULL,
                    postcode TEXT NOT NULL,
                    city TEXT NOT NULL,
                    lat REAL NOT NULL,
                    lon REAL NOT NULL,
                    charging_points_count INTEGER NOT NULL,
                    max_power_kw REAL NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS raw_payloads (
                    payload_sha256 TEXT PRIMARY KEY,
                    stored_at TEXT NOT NULL,
                    path TEXT NOT NULL,
                    content_type TEXT NOT NULL,
                    byte_length INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS provider_poll_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider_uid TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    ended_at TEXT NOT NULL DEFAULT '',
                    fetched_at TEXT NOT NULL DEFAULT '',
                    result TEXT NOT NULL DEFAULT 'started',
                    http_status INTEGER NOT NULL DEFAULT 0,
                    error_text TEXT NOT NULL DEFAULT '',
                    payload_sha256 TEXT NOT NULL DEFAULT '',
                    observation_count INTEGER NOT NULL DEFAULT 0,
                    changed_observation_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS provider_push_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    provider_uid TEXT NOT NULL,
                    subscription_id TEXT NOT NULL DEFAULT '',
                    publication_id TEXT NOT NULL DEFAULT '',
                    started_at TEXT NOT NULL,
                    ended_at TEXT NOT NULL DEFAULT '',
                    received_at TEXT NOT NULL DEFAULT '',
                    result TEXT NOT NULL DEFAULT 'started',
                    content_type TEXT NOT NULL DEFAULT '',
                    content_encoding TEXT NOT NULL DEFAULT '',
                    request_path TEXT NOT NULL DEFAULT '',
                    request_query TEXT NOT NULL DEFAULT '',
                    error_text TEXT NOT NULL DEFAULT '',
                    payload_sha256 TEXT NOT NULL DEFAULT '',
                    observation_count INTEGER NOT NULL DEFAULT 0,
                    changed_observation_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS evse_current_state (
                    provider_uid TEXT NOT NULL,
                    provider_site_id TEXT NOT NULL,
                    provider_station_ref TEXT NOT NULL,
                    provider_evse_id TEXT NOT NULL,
                    station_id TEXT NOT NULL DEFAULT '',
                    availability_status TEXT NOT NULL,
                    operational_status TEXT NOT NULL,
                    price_display TEXT NOT NULL,
                    price_currency TEXT NOT NULL,
                    price_energy_eur_kwh_min REAL,
                    price_energy_eur_kwh_max REAL,
                    price_time_eur_min_min REAL,
                    price_time_eur_min_max REAL,
                    price_quality TEXT NOT NULL,
                    price_complex INTEGER NOT NULL DEFAULT 0,
                    next_available_charging_slots TEXT NOT NULL DEFAULT '',
                    supplemental_facility_status TEXT NOT NULL DEFAULT '',
                    source_observed_at TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    PRIMARY KEY (provider_uid, provider_evse_id)
                );

                CREATE TABLE IF NOT EXISTS station_current_state (
                    station_id TEXT PRIMARY KEY,
                    provider_uid TEXT NOT NULL,
                    availability_status TEXT NOT NULL,
                    available_evses INTEGER NOT NULL DEFAULT 0,
                    occupied_evses INTEGER NOT NULL DEFAULT 0,
                    out_of_order_evses INTEGER NOT NULL DEFAULT 0,
                    unknown_evses INTEGER NOT NULL DEFAULT 0,
                    total_evses INTEGER NOT NULL DEFAULT 0,
                    price_display TEXT NOT NULL,
                    price_currency TEXT NOT NULL,
                    price_energy_eur_kwh_min REAL,
                    price_energy_eur_kwh_max REAL,
                    price_time_eur_min_min REAL,
                    price_time_eur_min_max REAL,
                    price_complex INTEGER NOT NULL DEFAULT 0,
                    source_observed_at TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    ingested_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_provider_poll_runs_provider
                    ON provider_poll_runs (provider_uid, started_at DESC);
                CREATE INDEX IF NOT EXISTS idx_provider_push_runs_provider
                    ON provider_push_runs (provider_uid, started_at DESC);
                """
            )
            self._ensure_provider_columns(conn)
            self._ensure_live_state_columns(conn)
            dropped_legacy_history = self._drop_legacy_observation_storage(conn)
        if dropped_legacy_history:
            self._vacuum_database()

    def _ensure_provider_columns(self, conn: sqlite3.Connection) -> None:
        existing = {str(row["name"]) for row in conn.execute("PRAGMA table_info(providers)").fetchall()}
        additions = [
            ("delta_delivery", "INTEGER NOT NULL DEFAULT 0"),
            ("retention_period_minutes", "INTEGER"),
            ("next_poll_at", "TEXT NOT NULL DEFAULT ''"),
            ("consecutive_no_data_count", "INTEGER NOT NULL DEFAULT 0"),
            ("consecutive_error_count", "INTEGER NOT NULL DEFAULT 0"),
            ("consecutive_unchanged_count", "INTEGER NOT NULL DEFAULT 0"),
            ("last_push_received_at", "TEXT NOT NULL DEFAULT ''"),
            ("last_push_result", "TEXT NOT NULL DEFAULT ''"),
            ("last_push_error_text", "TEXT NOT NULL DEFAULT ''"),
        ]
        for column_name, definition in additions:
            if column_name in existing:
                continue
            conn.execute(f"ALTER TABLE providers ADD COLUMN {column_name} {definition}")

    def _ensure_table_columns(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        additions: list[tuple[str, str]],
    ) -> None:
        existing = {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
        for column_name, definition in additions:
            if column_name in existing:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")

    def _ensure_live_state_columns(self, conn: sqlite3.Connection) -> None:
        additions = [
            ("next_available_charging_slots", "TEXT NOT NULL DEFAULT ''"),
            ("supplemental_facility_status", "TEXT NOT NULL DEFAULT ''"),
        ]
        self._ensure_table_columns(conn, "evse_current_state", additions)

    def _drop_legacy_observation_storage(self, conn: sqlite3.Connection) -> bool:
        table_exists = (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'evse_observations'"
            ).fetchone()
            is not None
        )
        if not table_exists:
            return False
        conn.execute("DROP TABLE IF EXISTS evse_observations")
        conn.execute("DROP INDEX IF EXISTS idx_evse_observations_station_id")
        conn.execute("DROP INDEX IF EXISTS idx_evse_observations_evse")
        return True

    def _vacuum_database(self) -> None:
        conn = sqlite3.connect(self.config.db_path)
        try:
            conn.execute(f"PRAGMA busy_timeout={self.config.sqlite_busy_timeout_ms}")
            conn.execute("VACUUM")
        finally:
            conn.close()

    def upsert_provider_targets(self, providers: Iterable[ProviderTarget]) -> None:
        now = utc_now_iso()
        with self.connection() as conn:
            for provider in providers:
                conn.execute(
                    """
                    INSERT INTO providers (
                        provider_uid, display_name, publisher, publication_id, access_mode,
                        fetch_kind, fetch_url, subscription_id, enabled, delta_delivery,
                        retention_period_minutes, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(provider_uid) DO UPDATE SET
                        display_name=excluded.display_name,
                        publisher=excluded.publisher,
                        publication_id=excluded.publication_id,
                        access_mode=excluded.access_mode,
                        fetch_kind=excluded.fetch_kind,
                        fetch_url=excluded.fetch_url,
                        subscription_id=excluded.subscription_id,
                        enabled=excluded.enabled,
                        delta_delivery=excluded.delta_delivery,
                        retention_period_minutes=excluded.retention_period_minutes,
                        updated_at=excluded.updated_at
                    """,
                    (
                        provider.provider_uid,
                        provider.display_name,
                        provider.publisher,
                        provider.publication_id,
                        provider.access_mode,
                        provider.fetch_kind,
                        provider.fetch_url,
                        provider.subscription_id,
                        1 if provider.enabled else 0,
                        1 if provider.delta_delivery else 0,
                        provider.retention_period_minutes,
                        now,
                        now,
                    ),
                )

    def upsert_site_matches(self, matches: Iterable[SiteMatch]) -> None:
        with self.connection() as conn:
            for match in matches:
                conn.execute(
                    """
                    INSERT INTO provider_site_matches (provider_uid, site_id, station_id, score)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(provider_uid, site_id) DO UPDATE SET
                        station_id=excluded.station_id,
                        score=excluded.score
                    """,
                    (match.provider_uid, match.site_id, match.station_id, match.score),
                )

    def reconcile_station_ids_from_site_matches(self) -> int:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT
                    c.provider_uid,
                    c.provider_site_id,
                    c.station_id AS old_station_id,
                    m.station_id AS new_station_id
                FROM evse_current_state c
                JOIN provider_site_matches m
                  ON m.provider_uid = c.provider_uid
                 AND m.site_id = c.provider_site_id
                WHERE COALESCE(c.station_id, '') <> m.station_id
                """
            ).fetchall()
            if not rows:
                return 0

            affected_station_ids: set[str] = set()
            for row in rows:
                old_station_id = str(row["old_station_id"] or "").strip()
                new_station_id = str(row["new_station_id"] or "").strip()
                if old_station_id:
                    affected_station_ids.add(old_station_id)
                if new_station_id:
                    affected_station_ids.add(new_station_id)

            conn.execute(
                """
                UPDATE evse_current_state
                SET station_id = (
                    SELECT m.station_id
                    FROM provider_site_matches m
                    WHERE m.provider_uid = evse_current_state.provider_uid
                      AND m.site_id = evse_current_state.provider_site_id
                )
                WHERE EXISTS (
                    SELECT 1
                    FROM provider_site_matches m
                    WHERE m.provider_uid = evse_current_state.provider_uid
                      AND m.site_id = evse_current_state.provider_site_id
                      AND COALESCE(evse_current_state.station_id, '') <> m.station_id
                )
                """
            )

            for station_id in affected_station_ids:
                self._refresh_station_current_state(conn, station_id)

        return len(rows)

    def upsert_stations(self, stations: Iterable[StationRecord]) -> None:
        now = utc_now_iso()
        with self.connection() as conn:
            for station in stations:
                conn.execute(
                    """
                    INSERT INTO stations (
                        station_id, operator, address, postcode, city, lat, lon,
                        charging_points_count, max_power_kw, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(station_id) DO UPDATE SET
                        operator=excluded.operator,
                        address=excluded.address,
                        postcode=excluded.postcode,
                        city=excluded.city,
                        lat=excluded.lat,
                        lon=excluded.lon,
                        charging_points_count=excluded.charging_points_count,
                        max_power_kw=excluded.max_power_kw,
                        updated_at=excluded.updated_at
                    """,
                    (
                        station.station_id,
                        station.operator,
                        station.address,
                        station.postcode,
                        station.city,
                        station.lat,
                        station.lon,
                        station.charging_points_count,
                        station.max_power_kw,
                        now,
                    ),
                )

    def list_providers(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        sql = "SELECT * FROM providers"
        params: tuple[Any, ...] = ()
        if enabled_only:
            sql += " WHERE enabled = 1"
        sql += " ORDER BY COALESCE(next_poll_at, ''), COALESCE(last_polled_at, ''), provider_uid"
        with self.connection() as conn:
            return [dict(row) for row in conn.execute(sql, params).fetchall()]

    def get_provider(self, provider_uid: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM providers WHERE provider_uid = ?", (provider_uid,)).fetchone()
        return dict(row) if row else None

    def get_provider_by_subscription_id(self, subscription_id: str) -> dict[str, Any] | None:
        if not str(subscription_id or "").strip():
            return None
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM providers
                WHERE subscription_id = ?
                ORDER BY enabled DESC, provider_uid
                LIMIT 1
                """,
                (str(subscription_id).strip(),),
            ).fetchone()
        return dict(row) if row else None

    def get_provider_by_publication_id(self, publication_id: str) -> dict[str, Any] | None:
        if not str(publication_id or "").strip():
            return None
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM providers
                WHERE publication_id = ?
                ORDER BY enabled DESC, provider_uid
                LIMIT 1
                """,
                (str(publication_id).strip(),),
            ).fetchone()
        return dict(row) if row else None

    def get_next_provider_for_round_robin(self) -> dict[str, Any] | None:
        now = utc_now_iso()
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM providers
                WHERE enabled = 1
                  AND (next_poll_at = '' OR next_poll_at <= ?)
                ORDER BY
                    CASE WHEN last_polled_at = '' THEN 0 ELSE 1 END,
                    CASE WHEN next_poll_at = '' THEN 0 ELSE 1 END,
                    next_poll_at,
                    last_polled_at,
                    provider_uid
                LIMIT 1
                """
                ,
                (now,),
            ).fetchone()
        return dict(row) if row else None

    def seconds_until_next_provider_due(self) -> float | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT next_poll_at
                FROM providers
                WHERE enabled = 1
                ORDER BY
                    CASE WHEN next_poll_at = '' THEN 0 ELSE 1 END,
                    next_poll_at,
                    provider_uid
                LIMIT 1
                """
            ).fetchone()
        if row is None:
            return None
        next_poll_at = _iso_or_empty(row["next_poll_at"])
        if not next_poll_at:
            return 0.0
        delay = _seconds_until_iso(next_poll_at)
        return max(0.0, delay or 0.0)

    def get_site_station_map(self, provider_uid: str) -> dict[str, str]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT site_id, station_id FROM provider_site_matches WHERE provider_uid = ?",
                (provider_uid,),
            ).fetchall()
        return {str(row["site_id"]): str(row["station_id"]) for row in rows}

    def start_poll_run(self, provider_uid: str) -> int:
        started_at = utc_now_iso()
        with self.connection() as conn:
            cursor = conn.execute(
                "INSERT INTO provider_poll_runs (provider_uid, started_at) VALUES (?, ?)",
                (provider_uid, started_at),
            )
            return int(cursor.lastrowid)

    def start_push_run(
        self,
        provider_uid: str,
        *,
        subscription_id: str = "",
        publication_id: str = "",
        received_at: str = "",
        content_type: str = "",
        content_encoding: str = "",
        request_path: str = "",
        request_query: str = "",
    ) -> int:
        started_at = utc_now_iso()
        with self.connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO provider_push_runs (
                    provider_uid, subscription_id, publication_id, started_at, received_at,
                    content_type, content_encoding, request_path, request_query
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    provider_uid,
                    subscription_id,
                    publication_id,
                    started_at,
                    received_at,
                    content_type,
                    content_encoding,
                    request_path,
                    request_query,
                ),
            )
            return int(cursor.lastrowid)

    def finish_poll_run(
        self,
        poll_run_id: int,
        *,
        provider_uid: str,
        result: str,
        fetched_at: str = "",
        ended_at: str | None = None,
        http_status: int = 0,
        error_text: str = "",
        payload_sha256: str = "",
        observation_count: int = 0,
        changed_observation_count: int = 0,
    ) -> None:
        ended_text = ended_at or utc_now_iso()
        with self.connection() as conn:
            provider_row = conn.execute("SELECT * FROM providers WHERE provider_uid = ?", (provider_uid,)).fetchone()
            provider = dict(provider_row) if provider_row else {}
            next_poll_at, no_data_count, error_count, unchanged_count = self._next_poll_state(
                provider,
                result=result,
                changed_observation_count=changed_observation_count,
                reference_time=fetched_at or ended_text,
            )
            conn.execute(
                """
                UPDATE provider_poll_runs
                SET ended_at = ?, fetched_at = ?, result = ?, http_status = ?, error_text = ?,
                    payload_sha256 = ?, observation_count = ?, changed_observation_count = ?
                WHERE id = ?
                """,
                (
                    ended_text,
                    fetched_at,
                    result,
                    http_status,
                    error_text,
                    payload_sha256,
                    observation_count,
                    changed_observation_count,
                    poll_run_id,
                ),
            )
            conn.execute(
                """
                UPDATE providers
                SET last_polled_at = ?, next_poll_at = ?, last_result = ?, last_error_text = ?,
                    consecutive_no_data_count = ?, consecutive_error_count = ?, consecutive_unchanged_count = ?,
                    updated_at = ?
                WHERE provider_uid = ?
                """,
                (
                    fetched_at or ended_text,
                    next_poll_at,
                    result,
                    error_text,
                    no_data_count,
                    error_count,
                    unchanged_count,
                    ended_text,
                    provider_uid,
                ),
            )

    def finish_push_run(
        self,
        push_run_id: int,
        *,
        provider_uid: str,
        result: str,
        received_at: str = "",
        ended_at: str | None = None,
        error_text: str = "",
        payload_sha256: str = "",
        observation_count: int = 0,
        changed_observation_count: int = 0,
    ) -> None:
        ended_text = ended_at or utc_now_iso()
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE provider_push_runs
                SET ended_at = ?, received_at = ?, result = ?, error_text = ?, payload_sha256 = ?,
                    observation_count = ?, changed_observation_count = ?
                WHERE id = ?
                """,
                (
                    ended_text,
                    received_at,
                    result,
                    error_text,
                    payload_sha256,
                    observation_count,
                    changed_observation_count,
                    push_run_id,
                ),
            )
            conn.execute(
                """
                UPDATE providers
                SET last_push_received_at = ?, last_push_result = ?, last_push_error_text = ?, updated_at = ?
                WHERE provider_uid = ?
                """,
                (
                    received_at or ended_text,
                    result,
                    error_text,
                    ended_text,
                    provider_uid,
                ),
            )

    def _base_poll_interval_seconds(self, provider: dict[str, Any]) -> int:
        if bool(provider.get("delta_delivery")):
            return max(1, int(self.config.poll_interval_delta_seconds))
        return max(1, int(self.config.poll_interval_snapshot_seconds))

    def _cap_interval_seconds(self, provider: dict[str, Any], max_seconds: int) -> int:
        retention_minutes = provider.get("retention_period_minutes")
        if retention_minutes in (None, ""):
            return max_seconds
        try:
            retention_seconds = max(1, int(retention_minutes) * 60)
        except (TypeError, ValueError):
            return max_seconds
        return min(max_seconds, retention_seconds)

    def _next_poll_state(
        self,
        provider: dict[str, Any],
        *,
        result: str,
        changed_observation_count: int,
        reference_time: str,
    ) -> tuple[str, int, int, int]:
        base_interval = self._base_poll_interval_seconds(provider)
        no_data_count = 0
        error_count = 0
        unchanged_count = 0

        if result in {"error", "timeout"}:
            error_count = int(provider.get("consecutive_error_count") or 0) + 1
            interval_seconds = min(
                base_interval * (2 ** min(error_count, 6)),
                self._cap_interval_seconds(provider, self.config.poll_interval_error_max_seconds),
            )
        elif result in {"no_data", "not_modified"}:
            no_data_count = int(provider.get("consecutive_no_data_count") or 0) + 1
            interval_seconds = min(
                base_interval * (2 ** min(no_data_count, 6)),
                self._cap_interval_seconds(provider, self.config.poll_interval_no_data_max_seconds),
            )
        elif result == "ok" and changed_observation_count <= 0:
            unchanged_count = int(provider.get("consecutive_unchanged_count") or 0) + 1
            if bool(provider.get("delta_delivery")):
                interval_seconds = base_interval
            else:
                interval_seconds = min(
                    base_interval * min(unchanged_count + 1, 10),
                    self._cap_interval_seconds(provider, self.config.poll_interval_unchanged_max_seconds),
                )
        else:
            interval_seconds = base_interval

        return _shift_iso(reference_time, interval_seconds), no_data_count, error_count, unchanged_count

    def _price_values(self, price: PriceSnapshot) -> tuple[Any, ...]:
        return (
            price.display,
            price.currency,
            price.energy_eur_kwh_min,
            price.energy_eur_kwh_max,
            price.time_eur_min_min,
            price.time_eur_min_max,
            price.quality,
            1 if price.complex_tariff else 0,
        )

    def _json_field_text(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return ""
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                return text
            return self._json_field_text(parsed)
        if isinstance(value, (list, dict)):
            if not value:
                return ""
            return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def _json_field_value(self, value: Any) -> list[Any]:
        if isinstance(value, list):
            return value
        text = str(value or "").strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        if parsed in (None, "", [], {}):
            return []
        if isinstance(parsed, list):
            return parsed
        return [parsed]

    def _dynamic_extra_values(self, fact: DynamicFact) -> tuple[str, str]:
        return (
            self._json_field_text(fact.next_available_charging_slots),
            self._json_field_text(fact.supplemental_facility_status),
        )

    def _deserialize_live_row(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        payload = dict(row)
        for field_name in LIVE_JSON_FIELDS:
            if field_name not in payload:
                continue
            payload[field_name] = self._json_field_value(payload[field_name])
        return payload

    def _state_signature(self, row: sqlite3.Row | dict[str, Any] | None) -> tuple[Any, ...]:
        if not row:
            return ()
        return (
            row["availability_status"],
            row["operational_status"],
            row["price_display"],
            row["price_currency"],
            row["price_energy_eur_kwh_min"],
            row["price_energy_eur_kwh_max"],
            row["price_time_eur_min_min"],
            row["price_time_eur_min_max"],
            row["price_quality"],
            row["price_complex"],
            self._json_field_text(row["next_available_charging_slots"]),
            self._json_field_text(row["supplemental_facility_status"]),
        )

    def _fact_signature(self, fact: DynamicFact) -> tuple[Any, ...]:
        return (
            fact.availability_status,
            fact.operational_status,
            fact.price.display,
            fact.price.currency,
            fact.price.energy_eur_kwh_min,
            fact.price.energy_eur_kwh_max,
            fact.price.time_eur_min_min,
            fact.price.time_eur_min_max,
            fact.price.quality,
            1 if fact.price.complex_tariff else 0,
            self._json_field_text(fact.next_available_charging_slots),
            self._json_field_text(fact.supplemental_facility_status),
        )

    def persist_provider_observations(
        self,
        *,
        provider_uid: str,
        facts: list[DynamicFact],
        fetched_at: str,
        payload_bytes: bytes,
        content_type: str,
    ) -> tuple[str, int, int]:
        ingested_at = utc_now_iso()
        changed_count = 0
        affected_station_ids: set[str] = set()

        with self.connection() as conn:
            payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()

            for fact in facts:
                current_row = conn.execute(
                    """
                    SELECT *
                    FROM evse_current_state
                    WHERE provider_uid = ? AND provider_evse_id = ?
                    """,
                    (provider_uid, fact.evse_id),
                ).fetchone()
                changed_since_previous = 1 if self._state_signature(current_row) != self._fact_signature(fact) else 0
                changed_count += changed_since_previous
                station_id = fact.station_id or ""
                if station_id:
                    affected_station_ids.add(station_id)

                conn.execute(
                    """
                    INSERT INTO evse_current_state (
                        provider_uid, provider_site_id, provider_station_ref, provider_evse_id,
                        station_id, availability_status, operational_status, price_display,
                        price_currency, price_energy_eur_kwh_min, price_energy_eur_kwh_max,
                        price_time_eur_min_min, price_time_eur_min_max, price_quality,
                        price_complex, next_available_charging_slots, supplemental_facility_status,
                        source_observed_at, fetched_at, ingested_at, payload_sha256
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(provider_uid, provider_evse_id) DO UPDATE SET
                        provider_site_id=excluded.provider_site_id,
                        provider_station_ref=excluded.provider_station_ref,
                        station_id=excluded.station_id,
                        availability_status=excluded.availability_status,
                        operational_status=excluded.operational_status,
                        price_display=excluded.price_display,
                        price_currency=excluded.price_currency,
                        price_energy_eur_kwh_min=excluded.price_energy_eur_kwh_min,
                        price_energy_eur_kwh_max=excluded.price_energy_eur_kwh_max,
                        price_time_eur_min_min=excluded.price_time_eur_min_min,
                        price_time_eur_min_max=excluded.price_time_eur_min_max,
                        price_quality=excluded.price_quality,
                        price_complex=excluded.price_complex,
                        next_available_charging_slots=excluded.next_available_charging_slots,
                        supplemental_facility_status=excluded.supplemental_facility_status,
                        source_observed_at=excluded.source_observed_at,
                        fetched_at=excluded.fetched_at,
                        ingested_at=excluded.ingested_at,
                        payload_sha256=excluded.payload_sha256
                    """,
                    (
                        provider_uid,
                        fact.site_id,
                        fact.station_ref,
                        fact.evse_id,
                        station_id,
                        fact.availability_status,
                        fact.operational_status,
                        *self._price_values(fact.price),
                        *self._dynamic_extra_values(fact),
                        _iso_or_empty(fact.source_observed_at),
                        fetched_at,
                        ingested_at,
                        payload_sha256,
                    ),
                )

            for station_id in affected_station_ids:
                self._refresh_station_current_state(conn, station_id)

        return payload_sha256, len(facts), changed_count

    def _refresh_station_current_state(self, conn: sqlite3.Connection, station_id: str) -> None:
        rows = conn.execute(
            """
            SELECT *
            FROM evse_current_state
            WHERE station_id = ?
            ORDER BY provider_uid, provider_evse_id
            """,
            (station_id,),
        ).fetchall()
        if not rows:
            conn.execute("DELETE FROM station_current_state WHERE station_id = ?", (station_id,))
            return

        counts = {"free": 0, "occupied": 0, "out_of_order": 0, "unknown": 0}
        provider_uid = str(rows[0]["provider_uid"])
        fetched_at = max(str(row["fetched_at"]) for row in rows if str(row["fetched_at"]))
        ingested_at = max(str(row["ingested_at"]) for row in rows if str(row["ingested_at"]))
        observed_candidates = [str(row["source_observed_at"]) for row in rows if str(row["source_observed_at"])]
        source_observed_at = max(observed_candidates) if observed_candidates else ""

        price_display = ""
        price_currency = ""
        energy_min: float | None = None
        energy_max: float | None = None
        time_min: float | None = None
        time_max: float | None = None
        price_complex = 0

        for row in rows:
            status = str(row["availability_status"])
            counts[status if status in counts else "unknown"] += 1
            if row["price_display"] and not price_display:
                price_display = str(row["price_display"])
            if row["price_currency"] and not price_currency:
                price_currency = str(row["price_currency"])
            if row["price_energy_eur_kwh_min"] is not None:
                energy_min = row["price_energy_eur_kwh_min"] if energy_min is None else min(
                    energy_min, row["price_energy_eur_kwh_min"]
                )
                energy_max = row["price_energy_eur_kwh_max"] if energy_max is None else max(
                    energy_max or row["price_energy_eur_kwh_max"], row["price_energy_eur_kwh_max"]
                )
            if row["price_time_eur_min_min"] is not None:
                time_min = row["price_time_eur_min_min"] if time_min is None else min(
                    time_min, row["price_time_eur_min_min"]
                )
                time_max = row["price_time_eur_min_max"] if time_max is None else max(
                    time_max or row["price_time_eur_min_max"], row["price_time_eur_min_max"]
                )
            price_complex = max(price_complex, int(row["price_complex"] or 0))

        availability_status = "unknown"
        if counts["free"] > 0:
            availability_status = "free"
        elif counts["occupied"] > 0:
            availability_status = "occupied"
        elif counts["out_of_order"] > 0:
            availability_status = "out_of_order"

        conn.execute(
            """
            INSERT INTO station_current_state (
                station_id, provider_uid, availability_status, available_evses, occupied_evses,
                out_of_order_evses, unknown_evses, total_evses, price_display, price_currency,
                price_energy_eur_kwh_min, price_energy_eur_kwh_max, price_time_eur_min_min,
                price_time_eur_min_max, price_complex, source_observed_at, fetched_at, ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(station_id) DO UPDATE SET
                provider_uid=excluded.provider_uid,
                availability_status=excluded.availability_status,
                available_evses=excluded.available_evses,
                occupied_evses=excluded.occupied_evses,
                out_of_order_evses=excluded.out_of_order_evses,
                unknown_evses=excluded.unknown_evses,
                total_evses=excluded.total_evses,
                price_display=excluded.price_display,
                price_currency=excluded.price_currency,
                price_energy_eur_kwh_min=excluded.price_energy_eur_kwh_min,
                price_energy_eur_kwh_max=excluded.price_energy_eur_kwh_max,
                price_time_eur_min_min=excluded.price_time_eur_min_min,
                price_time_eur_min_max=excluded.price_time_eur_min_max,
                price_complex=excluded.price_complex,
                source_observed_at=excluded.source_observed_at,
                fetched_at=excluded.fetched_at,
                ingested_at=excluded.ingested_at
            """,
            (
                station_id,
                provider_uid,
                availability_status,
                counts["free"],
                counts["occupied"],
                counts["out_of_order"],
                counts["unknown"],
                len(rows),
                price_display,
                price_currency,
                energy_min,
                energy_max,
                time_min,
                time_max,
                price_complex,
                source_observed_at,
                fetched_at,
                ingested_at,
            ),
        )

    def list_station_summaries(
        self,
        *,
        provider_uid: str = "",
        status: str = "",
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT
                s.station_id,
                s.operator,
                s.address,
                s.postcode,
                s.city,
                s.lat,
                s.lon,
                s.charging_points_count,
                s.max_power_kw,
                c.provider_uid,
                c.availability_status,
                c.available_evses,
                c.occupied_evses,
                c.out_of_order_evses,
                c.unknown_evses,
                c.total_evses,
                c.price_display,
                c.price_currency,
                c.price_energy_eur_kwh_min,
                c.price_energy_eur_kwh_max,
                c.price_time_eur_min_min,
                c.price_time_eur_min_max,
                c.price_complex,
                c.source_observed_at,
                c.fetched_at,
                c.ingested_at
            FROM station_current_state c
            JOIN stations s ON s.station_id = c.station_id
            WHERE 1 = 1
        """
        params: list[Any] = []
        if provider_uid:
            sql += " AND c.provider_uid = ?"
            params.append(provider_uid)
        if status:
            sql += " AND c.availability_status = ?"
            params.append(status)
        sql += " ORDER BY c.fetched_at DESC, s.station_id LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        with self.connection() as conn:
            return [dict(row) for row in conn.execute(sql, tuple(params)).fetchall()]

    def list_station_summaries_by_ids(self, station_ids: Iterable[str]) -> list[dict[str, Any]]:
        ordered_station_ids: list[str] = []
        seen_station_ids: set[str] = set()
        for station_id in station_ids:
            normalized = str(station_id or "").strip()
            if not normalized or normalized in seen_station_ids:
                continue
            seen_station_ids.add(normalized)
            ordered_station_ids.append(normalized)

        if not ordered_station_ids:
            return []

        placeholders = ", ".join(["?"] * len(ordered_station_ids))
        sql = f"""
            SELECT
                s.station_id,
                s.operator,
                s.address,
                s.postcode,
                s.city,
                s.lat,
                s.lon,
                s.charging_points_count,
                s.max_power_kw,
                c.provider_uid,
                c.availability_status,
                c.available_evses,
                c.occupied_evses,
                c.out_of_order_evses,
                c.unknown_evses,
                c.total_evses,
                c.price_display,
                c.price_currency,
                c.price_energy_eur_kwh_min,
                c.price_energy_eur_kwh_max,
                c.price_time_eur_min_min,
                c.price_time_eur_min_max,
                c.price_complex,
                c.source_observed_at,
                c.fetched_at,
                c.ingested_at
            FROM station_current_state c
            JOIN stations s ON s.station_id = c.station_id
            WHERE s.station_id IN ({placeholders})
        """

        with self.connection() as conn:
            rows = conn.execute(sql, tuple(ordered_station_ids)).fetchall()

        row_by_station_id = {str(row["station_id"]): dict(row) for row in rows}
        return [row_by_station_id[station_id] for station_id in ordered_station_ids if station_id in row_by_station_id]

    def get_station_detail(self, station_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            current = conn.execute(
                """
                SELECT
                    s.station_id,
                    s.operator,
                    s.address,
                    s.postcode,
                    s.city,
                    s.lat,
                    s.lon,
                    s.charging_points_count,
                    s.max_power_kw,
                    c.provider_uid,
                    c.availability_status,
                    c.available_evses,
                    c.occupied_evses,
                    c.out_of_order_evses,
                    c.unknown_evses,
                    c.total_evses,
                    c.price_display,
                    c.price_currency,
                    c.price_energy_eur_kwh_min,
                    c.price_energy_eur_kwh_max,
                    c.price_time_eur_min_min,
                    c.price_time_eur_min_max,
                    c.price_complex,
                    c.source_observed_at,
                    c.fetched_at,
                    c.ingested_at
                FROM station_current_state c
                JOIN stations s ON s.station_id = c.station_id
                WHERE s.station_id = ?
                """,
                (station_id,),
            ).fetchone()
            if current is None:
                return None

            current_evses = conn.execute(
                """
                SELECT *
                FROM evse_current_state
                WHERE station_id = ?
                ORDER BY provider_uid, provider_evse_id
                """,
                (station_id,),
            ).fetchall()

        return {
            "station": dict(current),
            "evses": [self._deserialize_live_row(row) for row in current_evses],
            "recent_observations": [],
        }

    def get_evse_detail(self, provider_uid: str, provider_evse_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            current = conn.execute(
                """
                SELECT *
                FROM evse_current_state
                WHERE provider_uid = ? AND provider_evse_id = ?
                """,
                (provider_uid, provider_evse_id),
            ).fetchone()
            if current is None:
                return None
        return {
            "current": self._deserialize_live_row(current),
            "recent_observations": [],
        }
