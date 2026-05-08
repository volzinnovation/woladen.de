from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from .config import AppConfig
from .models import DynamicFact, EvseMatch, PriceSnapshot, ProviderTarget, SiteMatch, StationRecord

LIVE_JSON_FIELDS = ("next_available_charging_slots", "supplemental_facility_status")
DESCRIPTIVE_PRICE_FIELDS = ("price_energy_eur_kwh_min", "price_energy_eur_kwh_max")
SQLITE_LOCK_ERROR_MARKERS = (
    "database is locked",
    "database table is locked",
    "database schema is locked",
    "database is busy",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


IMMEDIATE_DUE_AT = datetime(1970, 1, 1, tzinfo=timezone.utc)


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


def _record_timing_metric(timings: dict[str, float] | None, metric_name: str, started_at: float) -> None:
    if timings is None:
        return
    elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    timings[metric_name] = timings.get(metric_name, 0.0) + elapsed_ms


class LiveStore:
    def __init__(self, config: AppConfig):
        self.config = config

    @contextmanager
    def connection(self):
        timeout_seconds = max(float(self.config.sqlite_busy_timeout_ms) / 1000.0, 1.0)
        conn = sqlite3.connect(self.config.db_path, timeout=timeout_seconds)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={self.config.sqlite_busy_timeout_ms}")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _is_retryable_sqlite_error(self, exc: sqlite3.OperationalError) -> bool:
        text = str(exc).strip().lower()
        return any(marker in text for marker in SQLITE_LOCK_ERROR_MARKERS)

    def _run_write_with_retry(self, operation):
        retry_window_seconds = max(float(self.config.sqlite_lock_retry_seconds), 0.0)
        deadline = time.monotonic() + retry_window_seconds
        attempt = 0

        while True:
            try:
                return operation()
            except sqlite3.OperationalError as exc:
                if not self._is_retryable_sqlite_error(exc):
                    raise
                if retry_window_seconds <= 0.0:
                    raise
                remaining = deadline - time.monotonic()
                if remaining <= 0.0:
                    raise
                sleep_seconds = min(0.25 * (2**attempt), 2.0, remaining)
                if sleep_seconds > 0.0:
                    time.sleep(sleep_seconds)
                attempt += 1

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
                    delivery_mode TEXT NOT NULL DEFAULT 'poll_only',
                    push_fallback_after_seconds INTEGER,
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

                CREATE TABLE IF NOT EXISTS provider_evse_matches (
                    provider_uid TEXT NOT NULL,
                    provider_evse_id TEXT NOT NULL,
                    station_id TEXT NOT NULL,
                    site_id TEXT NOT NULL DEFAULT '',
                    station_ref TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (provider_uid, provider_evse_id)
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
                    mapped_observation_count INTEGER NOT NULL DEFAULT 0,
                    dropped_observation_count INTEGER NOT NULL DEFAULT 0,
                    changed_observation_count INTEGER NOT NULL DEFAULT 0,
                    changed_mapped_observation_count INTEGER NOT NULL DEFAULT 0,
                    changed_dropped_observation_count INTEGER NOT NULL DEFAULT 0
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
                    mapped_observation_count INTEGER NOT NULL DEFAULT 0,
                    dropped_observation_count INTEGER NOT NULL DEFAULT 0,
                    changed_observation_count INTEGER NOT NULL DEFAULT 0,
                    changed_mapped_observation_count INTEGER NOT NULL DEFAULT 0,
                    changed_dropped_observation_count INTEGER NOT NULL DEFAULT 0
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
                    price_energy_eur_kwh_min TEXT NOT NULL DEFAULT '',
                    price_energy_eur_kwh_max TEXT NOT NULL DEFAULT '',
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
                    price_energy_eur_kwh_min TEXT NOT NULL DEFAULT '',
                    price_energy_eur_kwh_max TEXT NOT NULL DEFAULT '',
                    price_time_eur_min_min REAL,
                    price_time_eur_min_max REAL,
                    price_complex INTEGER NOT NULL DEFAULT 0,
                    source_observed_at TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    evses_json TEXT NOT NULL DEFAULT '[]'
                );

                CREATE TABLE IF NOT EXISTS station_ratings (
                    station_id TEXT NOT NULL,
                    client_id_hash TEXT NOT NULL,
                    rating INTEGER NOT NULL CHECK (rating BETWEEN 1 AND 5),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (station_id, client_id_hash)
                );
                CREATE INDEX IF NOT EXISTS idx_provider_poll_runs_provider
                    ON provider_poll_runs (provider_uid, started_at DESC);
                CREATE INDEX IF NOT EXISTS idx_provider_push_runs_provider
                    ON provider_push_runs (provider_uid, started_at DESC);
                """
            )
            self._ensure_provider_columns(conn)
            self._ensure_run_columns(conn)
            self._ensure_live_state_columns(conn)
            self._ensure_indexes(conn)
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
            ("delivery_mode", "TEXT NOT NULL DEFAULT 'poll_only'"),
            ("push_fallback_after_seconds", "INTEGER"),
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
    ) -> set[str]:
        existing = {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
        added_columns: set[str] = set()
        for column_name, definition in additions:
            if column_name in existing:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")
            added_columns.add(column_name)
        return added_columns

    def _ensure_live_state_columns(self, conn: sqlite3.Connection) -> None:
        additions = [
            ("next_available_charging_slots", "TEXT NOT NULL DEFAULT ''"),
            ("supplemental_facility_status", "TEXT NOT NULL DEFAULT ''"),
        ]
        self._ensure_table_columns(conn, "evse_current_state", additions)
        self._migrate_live_state_price_columns(conn)
        self._ensure_table_columns(
            conn,
            "station_current_state",
            [("evses_json", "TEXT NOT NULL DEFAULT '[]'")],
        )
        if self._station_detail_backfill_needed(conn):
            self._backfill_station_detail_json(conn)

    def _ensure_run_columns(self, conn: sqlite3.Connection) -> None:
        additions = [
            ("mapped_observation_count", "INTEGER NOT NULL DEFAULT 0"),
            ("dropped_observation_count", "INTEGER NOT NULL DEFAULT 0"),
            ("changed_mapped_observation_count", "INTEGER NOT NULL DEFAULT 0"),
            ("changed_dropped_observation_count", "INTEGER NOT NULL DEFAULT 0"),
        ]
        self._ensure_table_columns(conn, "provider_poll_runs", additions)
        self._ensure_table_columns(conn, "provider_push_runs", additions)

    def _ensure_indexes(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_evse_current_state_station_lookup
            ON evse_current_state (station_id, provider_uid, provider_evse_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_station_ratings_station_lookup
            ON station_ratings (station_id)
            """
        )

    def _live_state_table_needs_price_migration(self, conn: sqlite3.Connection, table_name: str) -> bool:
        column_types = {
            str(row["name"]): str(row["type"] or "").upper()
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        return any(column_types.get(field_name) != "TEXT" for field_name in DESCRIPTIVE_PRICE_FIELDS)

    def _migrate_live_state_price_columns(self, conn: sqlite3.Connection) -> None:
        if self._live_state_table_needs_price_migration(conn, "evse_current_state"):
            conn.execute("ALTER TABLE evse_current_state RENAME TO evse_current_state__legacy_price_columns")
            conn.executescript(
                """
                CREATE TABLE evse_current_state (
                    provider_uid TEXT NOT NULL,
                    provider_site_id TEXT NOT NULL,
                    provider_station_ref TEXT NOT NULL,
                    provider_evse_id TEXT NOT NULL,
                    station_id TEXT NOT NULL DEFAULT '',
                    availability_status TEXT NOT NULL,
                    operational_status TEXT NOT NULL,
                    price_display TEXT NOT NULL,
                    price_currency TEXT NOT NULL,
                    price_energy_eur_kwh_min TEXT NOT NULL DEFAULT '',
                    price_energy_eur_kwh_max TEXT NOT NULL DEFAULT '',
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
                """
            )
            conn.execute(
                """
                INSERT INTO evse_current_state (
                    provider_uid,
                    provider_site_id,
                    provider_station_ref,
                    provider_evse_id,
                    station_id,
                    availability_status,
                    operational_status,
                    price_display,
                    price_currency,
                    price_energy_eur_kwh_min,
                    price_energy_eur_kwh_max,
                    price_time_eur_min_min,
                    price_time_eur_min_max,
                    price_quality,
                    price_complex,
                    next_available_charging_slots,
                    supplemental_facility_status,
                    source_observed_at,
                    fetched_at,
                    ingested_at,
                    payload_sha256
                )
                SELECT
                    provider_uid,
                    provider_site_id,
                    provider_station_ref,
                    provider_evse_id,
                    station_id,
                    availability_status,
                    operational_status,
                    price_display,
                    price_currency,
                    COALESCE(CAST(price_energy_eur_kwh_min AS TEXT), ''),
                    COALESCE(CAST(price_energy_eur_kwh_max AS TEXT), ''),
                    price_time_eur_min_min,
                    price_time_eur_min_max,
                    price_quality,
                    price_complex,
                    next_available_charging_slots,
                    supplemental_facility_status,
                    source_observed_at,
                    fetched_at,
                    ingested_at,
                    payload_sha256
                FROM evse_current_state__legacy_price_columns
                """
            )
            conn.execute("DROP TABLE evse_current_state__legacy_price_columns")

        if self._live_state_table_needs_price_migration(conn, "station_current_state"):
            conn.execute("ALTER TABLE station_current_state RENAME TO station_current_state__legacy_price_columns")
            conn.executescript(
                """
                CREATE TABLE station_current_state (
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
                    price_energy_eur_kwh_min TEXT NOT NULL DEFAULT '',
                    price_energy_eur_kwh_max TEXT NOT NULL DEFAULT '',
                    price_time_eur_min_min REAL,
                    price_time_eur_min_max REAL,
                    price_complex INTEGER NOT NULL DEFAULT 0,
                    source_observed_at TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    ingested_at TEXT NOT NULL,
                    evses_json TEXT NOT NULL DEFAULT '[]'
                );
                """
            )
            conn.execute(
                """
                INSERT INTO station_current_state (
                    station_id,
                    provider_uid,
                    availability_status,
                    available_evses,
                    occupied_evses,
                    out_of_order_evses,
                    unknown_evses,
                    total_evses,
                    price_display,
                    price_currency,
                    price_energy_eur_kwh_min,
                    price_energy_eur_kwh_max,
                    price_time_eur_min_min,
                    price_time_eur_min_max,
                    price_complex,
                    source_observed_at,
                    fetched_at,
                    ingested_at,
                    evses_json
                )
                SELECT
                    station_id,
                    provider_uid,
                    availability_status,
                    available_evses,
                    occupied_evses,
                    out_of_order_evses,
                    unknown_evses,
                    total_evses,
                    price_display,
                    price_currency,
                    COALESCE(CAST(price_energy_eur_kwh_min AS TEXT), ''),
                    COALESCE(CAST(price_energy_eur_kwh_max AS TEXT), ''),
                    price_time_eur_min_min,
                    price_time_eur_min_max,
                    price_complex,
                    source_observed_at,
                    fetched_at,
                    ingested_at,
                    '[]'
                FROM station_current_state__legacy_price_columns
                """
            )
            conn.execute("DROP TABLE station_current_state__legacy_price_columns")

    def _station_detail_backfill_needed(self, conn: sqlite3.Connection) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM station_current_state
            WHERE total_evses > 0
              AND COALESCE(evses_json, '[]') IN ('', '[]')
            LIMIT 1
            """
        ).fetchone()
        return row is not None

    def _station_detail_evses_json(self, station_rows: list[sqlite3.Row | dict[str, Any]]) -> str:
        evses = [self._deserialize_live_row(row) for row in station_rows]
        return json.dumps(evses, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

    def _backfill_station_detail_json(self, conn: sqlite3.Connection) -> None:
        station_rows = conn.execute(
            """
            SELECT station_id
            FROM station_current_state
            WHERE total_evses > 0
              AND COALESCE(evses_json, '[]') IN ('', '[]')
            ORDER BY station_id
            """
        ).fetchall()
        station_ids = [str(row["station_id"] or "").strip() for row in station_rows if str(row["station_id"] or "").strip()]
        if not station_ids:
            return

        placeholders = ", ".join(["?"] * len(station_ids))
        evse_rows = conn.execute(
            f"""
            SELECT *
            FROM evse_current_state
            WHERE station_id IN ({placeholders})
            ORDER BY station_id, provider_uid, provider_evse_id
            """,
            tuple(station_ids),
        ).fetchall()

        rows_by_station_id: dict[str, list[sqlite3.Row]] = {station_id: [] for station_id in station_ids}
        for row in evse_rows:
            rows_by_station_id.setdefault(str(row["station_id"]), []).append(row)

        update_rows = [
            (self._station_detail_evses_json(rows_by_station_id[station_id]), station_id)
            for station_id in station_ids
            if rows_by_station_id.get(station_id)
        ]
        if update_rows:
            conn.executemany(
                "UPDATE station_current_state SET evses_json = ? WHERE station_id = ?",
                update_rows,
            )

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
                        retention_period_minutes, delivery_mode, push_fallback_after_seconds,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        delivery_mode=excluded.delivery_mode,
                        push_fallback_after_seconds=excluded.push_fallback_after_seconds,
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
                        provider.delivery_mode,
                        provider.push_fallback_after_seconds,
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

    def upsert_evse_matches(self, matches: Iterable[EvseMatch]) -> None:
        with self.connection() as conn:
            for match in matches:
                conn.execute(
                    """
                    INSERT INTO provider_evse_matches (
                        provider_uid, provider_evse_id, station_id, site_id, station_ref
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(provider_uid, provider_evse_id) DO UPDATE SET
                        station_id=excluded.station_id,
                        site_id=excluded.site_id,
                        station_ref=excluded.station_ref
                    """,
                    (
                        match.provider_uid,
                        match.evse_id,
                        match.station_id,
                        match.site_id,
                        match.station_ref,
                    ),
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

            self._refresh_station_current_states(conn, affected_station_ids)

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

    def _push_fallback_after_seconds(self, provider: dict[str, Any]) -> int:
        configured = provider.get("push_fallback_after_seconds")
        try:
            configured_seconds = int(configured)
        except (TypeError, ValueError):
            configured_seconds = 0
        if configured_seconds > 0:
            return configured_seconds
        return max(self._base_poll_interval_seconds(provider) * 4, 60)

    def _push_fallback_grace_seconds(self, provider: dict[str, Any]) -> int:
        return max(1, self._base_poll_interval_seconds(provider))

    def push_duplicate_window_seconds(self, provider: dict[str, Any]) -> int:
        return max(1, self._base_poll_interval_seconds(provider))

    def _provider_due_at(self, provider: dict[str, Any]) -> datetime | None:
        if not bool(provider.get("enabled")):
            return None

        delivery_mode = str(provider.get("delivery_mode") or "poll_only").strip().lower() or "poll_only"
        if delivery_mode == "push_only":
            return None

        next_poll_at = _parse_iso_utc(provider.get("next_poll_at"))
        if delivery_mode != "push_with_poll_fallback":
            return next_poll_at or IMMEDIATE_DUE_AT

        last_push_received_at = _parse_iso_utc(provider.get("last_push_received_at"))
        if last_push_received_at is None:
            return next_poll_at or IMMEDIATE_DUE_AT

        push_due_at = last_push_received_at + timedelta(
            seconds=self._push_fallback_after_seconds(provider) + self._push_fallback_grace_seconds(provider)
        )
        if next_poll_at is None:
            return push_due_at
        return max(next_poll_at, push_due_at)

    def get_provider(self, provider_uid: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM providers WHERE provider_uid = ?", (provider_uid,)).fetchone()
        return dict(row) if row else None

    def find_recent_push_run(
        self,
        provider_uid: str,
        *,
        payload_sha256: str,
        received_at: str = "",
        within_seconds: int = 0,
    ) -> dict[str, Any] | None:
        provider_uid_text = str(provider_uid or "").strip()
        payload_sha256_text = str(payload_sha256 or "").strip()
        if not provider_uid_text or not payload_sha256_text:
            return None
        window_seconds = max(1, int(within_seconds or 0))
        reference_dt = _parse_iso_utc(received_at) or datetime.now(timezone.utc)
        cutoff_text = (reference_dt - timedelta(seconds=window_seconds)).replace(microsecond=0).isoformat()
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM provider_push_runs
                WHERE provider_uid = ?
                  AND payload_sha256 = ?
                  AND started_at >= ?
                  AND result IN ('started', 'queued', 'ok', 'duplicate')
                ORDER BY id DESC
                LIMIT 1
                """,
                (provider_uid_text, payload_sha256_text, cutoff_text),
            ).fetchone()
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

    def list_recent_provider_updates(self, *, limit_per_provider: int = 10) -> dict[str, list[dict[str, Any]]]:
        updates_by_provider: dict[str, list[dict[str, Any]]] = {}
        if limit_per_provider <= 0:
            return updates_by_provider

        with self.connection() as conn:
            poll_rows = conn.execute(
                """
                SELECT
                    provider_uid,
                    started_at,
                    ended_at,
                    fetched_at,
                    result,
                    http_status,
                    error_text,
                    payload_sha256,
                    observation_count,
                    mapped_observation_count,
                    dropped_observation_count,
                    changed_observation_count,
                    changed_mapped_observation_count,
                    changed_dropped_observation_count
                FROM provider_poll_runs
                WHERE result <> 'started'
                """
            ).fetchall()
            push_rows = conn.execute(
                """
                SELECT
                    provider_uid,
                    subscription_id,
                    publication_id,
                    started_at,
                    ended_at,
                    received_at,
                    result,
                    error_text,
                    payload_sha256,
                    observation_count,
                    mapped_observation_count,
                    dropped_observation_count,
                    changed_observation_count,
                    changed_mapped_observation_count,
                    changed_dropped_observation_count
                FROM provider_push_runs
                WHERE result <> 'started'
                """
            ).fetchall()

        normalized_by_provider: dict[str, list[dict[str, Any]]] = {}
        for row in poll_rows:
            provider_uid = str(row["provider_uid"])
            normalized_by_provider.setdefault(provider_uid, []).append(
                {
                    "update_kind": "poll",
                    "update_at": str(row["fetched_at"] or row["started_at"] or "") or None,
                    "started_at": str(row["started_at"] or "") or None,
                    "ended_at": str(row["ended_at"] or "") or None,
                    "fetched_at": str(row["fetched_at"] or "") or None,
                    "received_at": None,
                    "subscription_id": None,
                    "publication_id": None,
                    "result": str(row["result"] or ""),
                    "http_status": int(row["http_status"]) if row["http_status"] is not None else None,
                    "error_text": str(row["error_text"] or "") or None,
                    "payload_sha256": str(row["payload_sha256"] or "") or None,
                    "observation_count": int(row["observation_count"] or 0),
                    "mapped_observation_count": int(row["mapped_observation_count"] or 0),
                    "dropped_observation_count": int(row["dropped_observation_count"] or 0),
                    "changed_observation_count": int(row["changed_observation_count"] or 0),
                    "changed_mapped_observation_count": int(row["changed_mapped_observation_count"] or 0),
                    "changed_dropped_observation_count": int(row["changed_dropped_observation_count"] or 0),
                }
            )

        for row in push_rows:
            provider_uid = str(row["provider_uid"])
            normalized_by_provider.setdefault(provider_uid, []).append(
                {
                    "update_kind": "push",
                    "update_at": str(row["received_at"] or row["started_at"] or "") or None,
                    "started_at": str(row["started_at"] or "") or None,
                    "ended_at": str(row["ended_at"] or "") or None,
                    "fetched_at": None,
                    "received_at": str(row["received_at"] or "") or None,
                    "subscription_id": str(row["subscription_id"] or "") or None,
                    "publication_id": str(row["publication_id"] or "") or None,
                    "result": str(row["result"] or ""),
                    "http_status": None,
                    "error_text": str(row["error_text"] or "") or None,
                    "payload_sha256": str(row["payload_sha256"] or "") or None,
                    "observation_count": int(row["observation_count"] or 0),
                    "mapped_observation_count": int(row["mapped_observation_count"] or 0),
                    "dropped_observation_count": int(row["dropped_observation_count"] or 0),
                    "changed_observation_count": int(row["changed_observation_count"] or 0),
                    "changed_mapped_observation_count": int(row["changed_mapped_observation_count"] or 0),
                    "changed_dropped_observation_count": int(row["changed_dropped_observation_count"] or 0),
                }
            )

        for provider_uid, items in normalized_by_provider.items():
            items.sort(
                key=lambda item: (
                    str(item.get("update_at") or ""),
                    str(item.get("started_at") or ""),
                    str(item.get("update_kind") or ""),
                ),
                reverse=True,
            )
            updates_by_provider[provider_uid] = items[:limit_per_provider]

        return updates_by_provider

    def get_next_provider_for_round_robin(self) -> dict[str, Any] | None:
        now = datetime.now(timezone.utc)
        due_candidates: list[tuple[int, datetime, str, str, dict[str, Any]]] = []
        for provider in self.list_providers(enabled_only=True):
            due_at = self._provider_due_at(provider)
            if due_at is None or due_at > now:
                continue
            last_polled_at = _iso_or_empty(provider.get("last_polled_at"))
            due_candidates.append(
                (
                    0 if not last_polled_at else 1,
                    due_at,
                    last_polled_at,
                    str(provider["provider_uid"]),
                    provider,
                )
            )
        if not due_candidates:
            return None
        due_candidates.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
        return due_candidates[0][4]

    def seconds_until_next_provider_due(self) -> float | None:
        now = datetime.now(timezone.utc)
        due_at_values = [self._provider_due_at(provider) for provider in self.list_providers(enabled_only=True)]
        due_at_values = [value for value in due_at_values if value is not None]
        if not due_at_values:
            return None
        delay = min((value - now).total_seconds() for value in due_at_values)
        return max(0.0, delay)

    def get_site_station_map(self, provider_uid: str) -> dict[str, str]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT site_id, station_id FROM provider_site_matches WHERE provider_uid = ?",
                (provider_uid,),
            ).fetchall()
        return {str(row["site_id"]): str(row["station_id"]) for row in rows}

    def get_evse_station_map(self, provider_uid: str) -> dict[str, dict[str, str]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT provider_evse_id, station_id, site_id, station_ref
                FROM provider_evse_matches
                WHERE provider_uid = ?
                """,
                (provider_uid,),
            ).fetchall()
        return {
            str(row["provider_evse_id"]): {
                "station_id": str(row["station_id"]),
                "site_id": str(row["site_id"]),
                "station_ref": str(row["station_ref"]),
            }
            for row in rows
        }

    def start_poll_run(self, provider_uid: str) -> int:
        started_at = utc_now_iso()
        return int(
            self._run_write_with_retry(
                lambda: self._start_poll_run_once(provider_uid, started_at=started_at)
            )
        )

    def _start_poll_run_once(self, provider_uid: str, *, started_at: str) -> int:
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
        return int(
            self._run_write_with_retry(
                lambda: self._start_push_run_once(
                    provider_uid,
                    subscription_id=subscription_id,
                    publication_id=publication_id,
                    started_at=started_at,
                    received_at=received_at,
                    content_type=content_type,
                    content_encoding=content_encoding,
                    request_path=request_path,
                    request_query=request_query,
                )
            )
        )

    def _start_push_run_once(
        self,
        provider_uid: str,
        *,
        subscription_id: str,
        publication_id: str,
        started_at: str,
        received_at: str,
        content_type: str,
        content_encoding: str,
        request_path: str,
        request_query: str,
    ) -> int:
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
        mapped_observation_count: int = 0,
        dropped_observation_count: int = 0,
        changed_observation_count: int = 0,
        changed_mapped_observation_count: int = 0,
        changed_dropped_observation_count: int = 0,
    ) -> None:
        ended_text = ended_at or utc_now_iso()
        self._run_write_with_retry(
            lambda: self._finish_poll_run_once(
                poll_run_id,
                provider_uid=provider_uid,
                result=result,
                fetched_at=fetched_at,
                ended_text=ended_text,
                http_status=http_status,
                error_text=error_text,
                payload_sha256=payload_sha256,
                observation_count=observation_count,
                mapped_observation_count=mapped_observation_count,
                dropped_observation_count=dropped_observation_count,
                changed_observation_count=changed_observation_count,
                changed_mapped_observation_count=changed_mapped_observation_count,
                changed_dropped_observation_count=changed_dropped_observation_count,
            )
        )

    def _finish_poll_run_once(
        self,
        poll_run_id: int,
        *,
        provider_uid: str,
        result: str,
        fetched_at: str,
        ended_text: str,
        http_status: int,
        error_text: str,
        payload_sha256: str,
        observation_count: int,
        mapped_observation_count: int,
        dropped_observation_count: int,
        changed_observation_count: int,
        changed_mapped_observation_count: int,
        changed_dropped_observation_count: int,
    ) -> None:
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
                    payload_sha256 = ?, observation_count = ?, mapped_observation_count = ?,
                    dropped_observation_count = ?, changed_observation_count = ?,
                    changed_mapped_observation_count = ?, changed_dropped_observation_count = ?
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
                    mapped_observation_count,
                    dropped_observation_count,
                    changed_observation_count,
                    changed_mapped_observation_count,
                    changed_dropped_observation_count,
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

    def queue_poll_run(
        self,
        poll_run_id: int,
        *,
        provider_uid: str,
        fetched_at: str,
        http_status: int = 0,
        payload_sha256: str = "",
    ) -> None:
        ended_text = utc_now_iso()
        provider = self.get_provider(provider_uid) or {}
        base_next_poll_at = _shift_iso(fetched_at or ended_text, self._base_poll_interval_seconds(provider))
        self._run_write_with_retry(
            lambda: self._queue_poll_run_once(
                poll_run_id,
                provider_uid=provider_uid,
                fetched_at=fetched_at,
                ended_text=ended_text,
                http_status=http_status,
                payload_sha256=payload_sha256,
                base_next_poll_at=base_next_poll_at,
            )
        )

    def _queue_poll_run_once(
        self,
        poll_run_id: int,
        *,
        provider_uid: str,
        fetched_at: str,
        ended_text: str,
        http_status: int,
        payload_sha256: str,
        base_next_poll_at: str,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE provider_poll_runs
                SET ended_at = ?, fetched_at = ?, result = ?, http_status = ?, payload_sha256 = ?
                WHERE id = ?
                """,
                (ended_text, fetched_at, "queued", http_status, payload_sha256, poll_run_id),
            )
            conn.execute(
                """
                UPDATE providers
                SET last_polled_at = ?, next_poll_at = ?, last_result = ?, last_error_text = ?, updated_at = ?
                WHERE provider_uid = ?
                """,
                (fetched_at or ended_text, base_next_poll_at, "queued", "", ended_text, provider_uid),
            )

    def complete_poll_run(
        self,
        poll_run_id: int,
        *,
        provider_uid: str,
        result: str,
        fetched_at: str,
        http_status: int = 0,
        error_text: str = "",
        payload_sha256: str = "",
        observation_count: int = 0,
        mapped_observation_count: int = 0,
        dropped_observation_count: int = 0,
        changed_observation_count: int = 0,
        changed_mapped_observation_count: int = 0,
        changed_dropped_observation_count: int = 0,
    ) -> None:
        ended_text = utc_now_iso()
        self._run_write_with_retry(
            lambda: self._complete_poll_run_once(
                poll_run_id,
                provider_uid=provider_uid,
                result=result,
                fetched_at=fetched_at,
                ended_text=ended_text,
                http_status=http_status,
                error_text=error_text,
                payload_sha256=payload_sha256,
                observation_count=observation_count,
                mapped_observation_count=mapped_observation_count,
                dropped_observation_count=dropped_observation_count,
                changed_observation_count=changed_observation_count,
                changed_mapped_observation_count=changed_mapped_observation_count,
                changed_dropped_observation_count=changed_dropped_observation_count,
            )
        )

    def _complete_poll_run_once(
        self,
        poll_run_id: int,
        *,
        provider_uid: str,
        result: str,
        fetched_at: str,
        ended_text: str,
        http_status: int,
        error_text: str,
        payload_sha256: str,
        observation_count: int,
        mapped_observation_count: int,
        dropped_observation_count: int,
        changed_observation_count: int,
        changed_mapped_observation_count: int,
        changed_dropped_observation_count: int,
    ) -> None:
        with self.connection() as conn:
            provider_row = conn.execute("SELECT * FROM providers WHERE provider_uid = ?", (provider_uid,)).fetchone()
            provider = dict(provider_row) if provider_row else {}
            desired_next_poll_at, no_data_count, error_count, unchanged_count = self._next_poll_state(
                provider,
                result=result,
                changed_observation_count=changed_observation_count,
                reference_time=fetched_at or ended_text,
            )
            current_next_poll_at = _parse_iso_utc(provider.get("next_poll_at"))
            desired_next_poll_dt = _parse_iso_utc(desired_next_poll_at)
            if current_next_poll_at is not None and desired_next_poll_dt is not None:
                effective_next_poll_at = max(current_next_poll_at, desired_next_poll_dt).replace(microsecond=0).isoformat()
            else:
                effective_next_poll_at = desired_next_poll_at
            conn.execute(
                """
                UPDATE provider_poll_runs
                SET ended_at = ?, fetched_at = ?, result = ?, http_status = ?, error_text = ?,
                    payload_sha256 = ?, observation_count = ?, mapped_observation_count = ?,
                    dropped_observation_count = ?, changed_observation_count = ?,
                    changed_mapped_observation_count = ?, changed_dropped_observation_count = ?
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
                    mapped_observation_count,
                    dropped_observation_count,
                    changed_observation_count,
                    changed_mapped_observation_count,
                    changed_dropped_observation_count,
                    poll_run_id,
                ),
            )
            conn.execute(
                """
                UPDATE providers
                SET next_poll_at = ?, last_result = ?, last_error_text = ?,
                    consecutive_no_data_count = ?, consecutive_error_count = ?, consecutive_unchanged_count = ?,
                    updated_at = ?
                WHERE provider_uid = ?
                """,
                (
                    effective_next_poll_at,
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
        mapped_observation_count: int = 0,
        dropped_observation_count: int = 0,
        changed_observation_count: int = 0,
        changed_mapped_observation_count: int = 0,
        changed_dropped_observation_count: int = 0,
    ) -> None:
        ended_text = ended_at or utc_now_iso()
        self._run_write_with_retry(
            lambda: self._finish_push_run_once(
                push_run_id,
                provider_uid=provider_uid,
                result=result,
                received_at=received_at,
                ended_text=ended_text,
                error_text=error_text,
                payload_sha256=payload_sha256,
                observation_count=observation_count,
                mapped_observation_count=mapped_observation_count,
                dropped_observation_count=dropped_observation_count,
                changed_observation_count=changed_observation_count,
                changed_mapped_observation_count=changed_mapped_observation_count,
                changed_dropped_observation_count=changed_dropped_observation_count,
            )
        )

    def _finish_push_run_once(
        self,
        push_run_id: int,
        *,
        provider_uid: str,
        result: str,
        received_at: str,
        ended_text: str,
        error_text: str,
        payload_sha256: str,
        observation_count: int,
        mapped_observation_count: int,
        dropped_observation_count: int,
        changed_observation_count: int,
        changed_mapped_observation_count: int,
        changed_dropped_observation_count: int,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE provider_push_runs
                SET ended_at = ?, received_at = ?, result = ?, error_text = ?, payload_sha256 = ?,
                    observation_count = ?, mapped_observation_count = ?, dropped_observation_count = ?,
                    changed_observation_count = ?, changed_mapped_observation_count = ?,
                    changed_dropped_observation_count = ?
                WHERE id = ?
                """,
                (
                    ended_text,
                    received_at,
                    result,
                    error_text,
                    payload_sha256,
                    observation_count,
                    mapped_observation_count,
                    dropped_observation_count,
                    changed_observation_count,
                    changed_mapped_observation_count,
                    changed_dropped_observation_count,
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

    def queue_push_run(
        self,
        push_run_id: int,
        *,
        provider_uid: str,
        received_at: str,
        payload_sha256: str = "",
    ) -> None:
        ended_text = utc_now_iso()
        self._run_write_with_retry(
            lambda: self._queue_push_run_once(
                push_run_id,
                provider_uid=provider_uid,
                received_at=received_at,
                ended_text=ended_text,
                payload_sha256=payload_sha256,
            )
        )

    def _queue_push_run_once(
        self,
        push_run_id: int,
        *,
        provider_uid: str,
        received_at: str,
        ended_text: str,
        payload_sha256: str,
    ) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE provider_push_runs
                SET ended_at = ?, received_at = ?, result = ?, payload_sha256 = ?
                WHERE id = ?
                """,
                (ended_text, received_at, "queued", payload_sha256, push_run_id),
            )
            conn.execute(
                """
                UPDATE providers
                SET last_push_received_at = ?, last_push_result = ?, last_push_error_text = ?, updated_at = ?
                WHERE provider_uid = ?
                """,
                (received_at or ended_text, "queued", "", ended_text, provider_uid),
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
            self._descriptive_price_text(price.energy_eur_kwh_min),
            self._descriptive_price_text(price.energy_eur_kwh_max),
            price.time_eur_min_min,
            price.time_eur_min_max,
            price.quality,
            1 if price.complex_tariff else 0,
        )

    @staticmethod
    def _descriptive_price_text(value: Any) -> str:
        if value is None:
            return ""
        text = str(value).strip()
        if not text:
            return ""
        try:
            number = float(text)
        except (TypeError, ValueError):
            return text
        return f"{number:.6f}".rstrip("0").rstrip(".")

    def _normalize_descriptive_price_fields(self, payload: dict[str, Any]) -> dict[str, Any]:
        for field_name in DESCRIPTIVE_PRICE_FIELDS:
            if field_name not in payload:
                continue
            payload[field_name] = self._descriptive_price_text(payload[field_name])
        return payload

    def _price_snapshot_from_row(
        self,
        row: sqlite3.Row | dict[str, Any],
    ) -> tuple[str, str, str, str, float | None, float | None, int] | None:
        price_display = str(row["price_display"] or "")
        price_currency = str(row["price_currency"] or "")
        energy_min = self._descriptive_price_text(row["price_energy_eur_kwh_min"])
        energy_max = self._descriptive_price_text(row["price_energy_eur_kwh_max"])
        time_min = row["price_time_eur_min_min"]
        time_max = row["price_time_eur_min_max"]
        price_complex = int(row["price_complex"] or 0)
        if not (
            price_display or energy_min or energy_max or time_min is not None or time_max is not None or price_complex
        ):
            return None
        return (price_display, price_currency, energy_min, energy_max, time_min, time_max, price_complex)

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
        return self._normalize_descriptive_price_fields(payload)

    def _state_signature(self, row: sqlite3.Row | dict[str, Any] | None) -> tuple[Any, ...]:
        if not row:
            return ()
        normalized_row = self._normalize_descriptive_price_fields(dict(row))
        return (
            normalized_row["availability_status"],
            normalized_row["operational_status"],
            normalized_row["price_display"],
            normalized_row["price_currency"],
            normalized_row["price_energy_eur_kwh_min"],
            normalized_row["price_energy_eur_kwh_max"],
            normalized_row["price_time_eur_min_min"],
            normalized_row["price_time_eur_min_max"],
            normalized_row["price_quality"],
            normalized_row["price_complex"],
            self._json_field_text(normalized_row["next_available_charging_slots"]),
            self._json_field_text(normalized_row["supplemental_facility_status"]),
        )

    def _fact_signature(self, fact: DynamicFact) -> tuple[Any, ...]:
        return (
            fact.availability_status,
            fact.operational_status,
            fact.price.display,
            fact.price.currency,
            self._descriptive_price_text(fact.price.energy_eur_kwh_min),
            self._descriptive_price_text(fact.price.energy_eur_kwh_max),
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
    ) -> dict[str, Any]:
        ingested_at = utc_now_iso()
        mapped_count = 0
        dropped_count = 0
        changed_count = 0
        changed_mapped_count = 0
        changed_dropped_count = 0
        affected_station_ids: set[str] = set()

        with self.connection() as conn:
            payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
            current_by_evse_id: dict[str, sqlite3.Row] = {}
            fact_evse_ids = sorted({str(fact.evse_id) for fact in facts if str(fact.evse_id).strip()})
            if fact_evse_ids:
                placeholders = ", ".join(["?"] * len(fact_evse_ids))
                current_rows = conn.execute(
                    f"""
                    SELECT *
                    FROM evse_current_state
                    WHERE provider_uid = ? AND provider_evse_id IN ({placeholders})
                    """,
                    (provider_uid, *fact_evse_ids),
                ).fetchall()
                current_by_evse_id = {str(row["provider_evse_id"]): row for row in current_rows}

            upsert_rows: list[tuple[Any, ...]] = []
            for fact in facts:
                current_row = current_by_evse_id.get(str(fact.evse_id))
                changed_since_previous = 1 if self._state_signature(current_row) != self._fact_signature(fact) else 0
                previous_station_id = str(current_row["station_id"] or "").strip() if current_row else ""
                changed_count += changed_since_previous
                station_id = fact.station_id or ""
                if station_id:
                    mapped_count += 1
                    affected_station_ids.add(station_id)
                    if changed_since_previous:
                        changed_mapped_count += 1
                else:
                    dropped_count += 1
                    if changed_since_previous:
                        changed_dropped_count += 1
                if previous_station_id and previous_station_id != station_id:
                    affected_station_ids.add(previous_station_id)
                upsert_rows.append(
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
                    )
                )
            if upsert_rows:
                conn.executemany(
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
                    upsert_rows,
                )

            self._refresh_station_current_states(conn, affected_station_ids)

        return {
            "payload_sha256": payload_sha256,
            "observation_count": len(facts),
            "mapped_observation_count": mapped_count,
            "dropped_observation_count": dropped_count,
            "changed_observation_count": changed_count,
            "changed_mapped_observation_count": changed_mapped_count,
            "changed_dropped_observation_count": changed_dropped_count,
        }

    def _refresh_station_current_states(self, conn: sqlite3.Connection, station_ids: Iterable[str]) -> None:
        normalized_station_ids = []
        seen_station_ids: set[str] = set()
        for station_id in station_ids:
            normalized = str(station_id or "").strip()
            if not normalized or normalized in seen_station_ids:
                continue
            seen_station_ids.add(normalized)
            normalized_station_ids.append(normalized)
        if not normalized_station_ids:
            return

        placeholders = ", ".join(["?"] * len(normalized_station_ids))
        rows = conn.execute(
            f"""
            SELECT *
            FROM evse_current_state
            WHERE station_id IN ({placeholders})
            ORDER BY station_id, provider_uid, provider_evse_id
            """,
            tuple(normalized_station_ids),
        ).fetchall()

        rows_by_station_id: dict[str, list[sqlite3.Row]] = {station_id: [] for station_id in normalized_station_ids}
        for row in rows:
            rows_by_station_id.setdefault(str(row["station_id"]), []).append(row)

        upsert_rows: list[tuple[Any, ...]] = []
        empty_station_ids = [station_id for station_id, station_rows in rows_by_station_id.items() if not station_rows]
        for station_id in empty_station_ids:
            conn.execute("DELETE FROM station_current_state WHERE station_id = ?", (station_id,))

        for station_id, station_rows in rows_by_station_id.items():
            if not station_rows:
                continue
            counts = {"free": 0, "occupied": 0, "out_of_order": 0, "unknown": 0}
            provider_uid = str(station_rows[0]["provider_uid"])
            fetched_at = max(str(row["fetched_at"]) for row in station_rows if str(row["fetched_at"]))
            ingested_at = max(str(row["ingested_at"]) for row in station_rows if str(row["ingested_at"]))
            observed_candidates = [str(row["source_observed_at"]) for row in station_rows if str(row["source_observed_at"])]
            source_observed_at = max(observed_candidates) if observed_candidates else ""

            price_display = ""
            price_currency = ""
            energy_min = ""
            energy_max = ""
            time_min: float | None = None
            time_max: float | None = None
            price_complex = 0
            fallback_price_snapshot: tuple[str, str, str, str, float | None, float | None, int] | None = None
            chosen_price_snapshot: tuple[str, str, str, str, float | None, float | None, int] | None = None

            for row in station_rows:
                status = str(row["availability_status"])
                counts[status if status in counts else "unknown"] += 1
                price_snapshot = self._price_snapshot_from_row(row)
                if price_snapshot is None:
                    continue
                if fallback_price_snapshot is None:
                    fallback_price_snapshot = price_snapshot
                if price_snapshot[0] and chosen_price_snapshot is None:
                    chosen_price_snapshot = price_snapshot

            selected_price_snapshot = chosen_price_snapshot or fallback_price_snapshot
            if selected_price_snapshot is not None:
                price_display, price_currency, energy_min, energy_max, time_min, time_max, price_complex = (
                    selected_price_snapshot
                )

            availability_status = "unknown"
            if counts["free"] > 0:
                availability_status = "free"
            elif counts["occupied"] > 0:
                availability_status = "occupied"
            elif counts["out_of_order"] > 0:
                availability_status = "out_of_order"

            upsert_rows.append(
                (
                    station_id,
                    provider_uid,
                    availability_status,
                    counts["free"],
                    counts["occupied"],
                    counts["out_of_order"],
                    counts["unknown"],
                    len(station_rows),
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
                    self._station_detail_evses_json(station_rows),
                )
            )

        if upsert_rows:
            conn.executemany(
                """
                INSERT INTO station_current_state (
                    station_id, provider_uid, availability_status, available_evses, occupied_evses,
                    out_of_order_evses, unknown_evses, total_evses, price_display, price_currency,
                    price_energy_eur_kwh_min, price_energy_eur_kwh_max, price_time_eur_min_min,
                    price_time_eur_min_max, price_complex, source_observed_at, fetched_at, ingested_at,
                    evses_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    ingested_at=excluded.ingested_at,
                    evses_json=excluded.evses_json
                """,
                upsert_rows,
            )

    def _rating_client_hash(self, client_id: str) -> str:
        normalized = str(client_id or "").strip()
        return hashlib.sha256(f"woladen-rating-client:{normalized}".encode("utf-8")).hexdigest()

    def _deserialize_rating_summary(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        return {
            "station_id": str(row["station_id"] or ""),
            "average_rating": round(float(row["average_rating"] or 0.0), 2),
            "rating_count": int(row["rating_count"] or 0),
        }

    def _get_station_rating_summary_with_connection(
        self,
        conn: sqlite3.Connection,
        station_id: str,
    ) -> dict[str, Any] | None:
        row = conn.execute(
            """
            SELECT
                station_id,
                AVG(rating) AS average_rating,
                COUNT(*) AS rating_count
            FROM station_ratings
            WHERE station_id = ?
            GROUP BY station_id
            """,
            (station_id,),
        ).fetchone()
        if row is None:
            return None
        return self._deserialize_rating_summary(row)

    def upsert_station_rating(self, station_id: str, rating: int, client_id: str) -> dict[str, Any]:
        normalized_station_id = str(station_id or "").strip()
        normalized_client_id = str(client_id or "").strip()
        normalized_rating = int(rating)
        if not normalized_station_id:
            raise ValueError("missing_station_id")
        if len(normalized_client_id) < 16:
            raise ValueError("missing_client_id")
        if normalized_rating < 1 or normalized_rating > 5:
            raise ValueError("invalid_rating")

        client_id_hash = self._rating_client_hash(normalized_client_id)
        updated_at = utc_now_iso()

        def operation() -> dict[str, Any]:
            with self.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO station_ratings (
                        station_id,
                        client_id_hash,
                        rating,
                        created_at,
                        updated_at
                    )
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(station_id, client_id_hash) DO UPDATE SET
                        rating = excluded.rating,
                        updated_at = excluded.updated_at
                    """,
                    (
                        normalized_station_id,
                        client_id_hash,
                        normalized_rating,
                        updated_at,
                        updated_at,
                    ),
                )
                summary = self._get_station_rating_summary_with_connection(conn, normalized_station_id)
                if summary is None:
                    raise RuntimeError("rating_summary_missing")
                return summary

        return self._run_write_with_retry(operation)

    def get_station_rating_summary(self, station_id: str) -> dict[str, Any] | None:
        normalized_station_id = str(station_id or "").strip()
        if not normalized_station_id:
            return None
        with self.connection() as conn:
            return self._get_station_rating_summary_with_connection(conn, normalized_station_id)

    def list_station_rating_summaries_by_ids(
        self,
        station_ids: Iterable[str],
        *,
        timings: dict[str, float] | None = None,
    ) -> list[dict[str, Any]]:
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
                station_id,
                AVG(rating) AS average_rating,
                COUNT(*) AS rating_count
            FROM station_ratings
            WHERE station_id IN ({placeholders})
            GROUP BY station_id
        """
        query_started_at = time.perf_counter()
        with self.connection() as conn:
            rows = conn.execute(sql, tuple(ordered_station_ids)).fetchall()
        _record_timing_metric(timings, "db_query_ms", query_started_at)

        decode_started_at = time.perf_counter()
        row_by_station_id = {
            str(row["station_id"]): self._deserialize_rating_summary(row)
            for row in rows
        }
        ordered_rows = [
            row_by_station_id[station_id]
            for station_id in ordered_station_ids
            if station_id in row_by_station_id
        ]
        _record_timing_metric(timings, "db_decode_ms", decode_started_at)
        return ordered_rows

    def list_station_summaries(
        self,
        *,
        provider_uid: str = "",
        status: str = "",
        limit: int = 100,
        offset: int = 0,
        timings: dict[str, float] | None = None,
    ) -> list[dict[str, Any]]:
        sql = """
            SELECT
                c.station_id,
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
            WHERE 1 = 1
        """
        params: list[Any] = []
        if provider_uid:
            sql += " AND c.provider_uid = ?"
            params.append(provider_uid)
        if status:
            sql += " AND c.availability_status = ?"
            params.append(status)
        sql += " ORDER BY c.fetched_at DESC, c.station_id LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        query_started_at = time.perf_counter()
        with self.connection() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        _record_timing_metric(timings, "db_query_ms", query_started_at)

        decode_started_at = time.perf_counter()
        decoded_rows = [self._deserialize_live_row(row) for row in rows]
        _record_timing_metric(timings, "db_decode_ms", decode_started_at)
        return decoded_rows

    def list_station_summaries_by_ids(
        self,
        station_ids: Iterable[str],
        *,
        timings: dict[str, float] | None = None,
    ) -> list[dict[str, Any]]:
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
                c.station_id,
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
            WHERE c.station_id IN ({placeholders})
        """

        query_started_at = time.perf_counter()
        with self.connection() as conn:
            rows = conn.execute(sql, tuple(ordered_station_ids)).fetchall()
        _record_timing_metric(timings, "db_query_ms", query_started_at)

        decode_started_at = time.perf_counter()
        row_by_station_id = {str(row["station_id"]): self._deserialize_live_row(row) for row in rows}
        ordered_rows = [row_by_station_id[station_id] for station_id in ordered_station_ids if station_id in row_by_station_id]
        _record_timing_metric(timings, "db_decode_ms", decode_started_at)
        return ordered_rows

    def get_station_detail(self, station_id: str, *, timings: dict[str, float] | None = None) -> dict[str, Any] | None:
        with self.connection() as conn:
            query_started_at = time.perf_counter()
            current = conn.execute(
                """
                SELECT
                    station_id,
                    provider_uid,
                    availability_status,
                    available_evses,
                    occupied_evses,
                    out_of_order_evses,
                    unknown_evses,
                    total_evses,
                    price_display,
                    price_currency,
                    price_energy_eur_kwh_min,
                    price_energy_eur_kwh_max,
                    price_time_eur_min_min,
                    price_time_eur_min_max,
                    price_complex,
                    source_observed_at,
                    fetched_at,
                    ingested_at,
                    evses_json
                FROM station_current_state
                WHERE station_id = ?
                """,
                (station_id,),
            ).fetchone()
            _record_timing_metric(timings, "db_query_ms", query_started_at)
            if current is None:
                return None

        decode_started_at = time.perf_counter()
        station_payload = self._deserialize_live_row(current)
        evses = self._json_field_value(station_payload.pop("evses_json", "[]"))
        payload = {
            "station": station_payload,
            "evses": [
                self._normalize_descriptive_price_fields(dict(item))
                for item in evses
                if isinstance(item, dict)
            ],
            "recent_observations": [],
        }
        _record_timing_metric(timings, "db_decode_ms", decode_started_at)
        return payload

    def get_evse_detail(
        self,
        provider_uid: str,
        provider_evse_id: str,
        *,
        timings: dict[str, float] | None = None,
    ) -> dict[str, Any] | None:
        with self.connection() as conn:
            query_started_at = time.perf_counter()
            current = conn.execute(
                """
                SELECT *
                FROM evse_current_state
                WHERE provider_uid = ? AND provider_evse_id = ?
                """,
                (provider_uid, provider_evse_id),
            ).fetchone()
            _record_timing_metric(timings, "db_query_ms", query_started_at)
            if current is None:
                return None
        decode_started_at = time.perf_counter()
        payload = {
            "current": self._deserialize_live_row(current),
            "recent_observations": [],
        }
        _record_timing_metric(timings, "db_decode_ms", decode_started_at)
        return payload
