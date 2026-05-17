from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OCCUPANCY_DB_PATH = REPO_ROOT / "data" / "occupancy.sqlite3"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _as_json_list(values: Iterable[str]) -> str:
    return json.dumps(sorted({str(value) for value in values if str(value).strip()}), separators=(",", ":"))


class OccupancyStore:
    def __init__(self, db_path: Path = DEFAULT_OCCUPANCY_DB_PATH):
        self.db_path = db_path

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            conn.close()

    def initialize(self) -> None:
        with self.connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS occupancy_archive_days (
                    archive_date TEXT PRIMARY KEY,
                    archive_path TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    archive_sha256 TEXT NOT NULL,
                    record_count INTEGER NOT NULL,
                    mapped_event_count INTEGER NOT NULL,
                    stored_event_count INTEGER NOT NULL DEFAULT 0,
                    provider_station_count INTEGER NOT NULL,
                    status_change_count INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS evse_status_events (
                    archive_date TEXT NOT NULL,
                    archive_member TEXT NOT NULL,
                    record_index INTEGER NOT NULL,
                    event_index INTEGER NOT NULL,
                    provider_uid TEXT NOT NULL,
                    station_id TEXT NOT NULL,
                    provider_evse_id TEXT NOT NULL,
                    source_observed_at TEXT NOT NULL,
                    availability_status TEXT NOT NULL,
                    operational_status TEXT NOT NULL,
                    message_timestamp TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL,
                    PRIMARY KEY (
                        archive_date,
                        archive_member,
                        record_index,
                        event_index,
                        provider_uid,
                        provider_evse_id
                    )
                );

                CREATE TABLE IF NOT EXISTS station_daily_occupancy (
                    station_id TEXT NOT NULL,
                    provider_uid TEXT NOT NULL,
                    archive_date TEXT NOT NULL,
                    observed_evse_ids_json TEXT NOT NULL,
                    observed_evses INTEGER NOT NULL,
                    matching_observations INTEGER NOT NULL,
                    occupied_observations INTEGER NOT NULL,
                    status_changes INTEGER NOT NULL,
                    latest_event_timestamp TEXT NOT NULL,
                    PRIMARY KEY (station_id, provider_uid, archive_date)
                );

                CREATE TABLE IF NOT EXISTS station_hourly_occupancy (
                    station_id TEXT NOT NULL,
                    provider_uid TEXT NOT NULL,
                    archive_date TEXT NOT NULL,
                    hour INTEGER NOT NULL CHECK (hour BETWEEN 0 AND 23),
                    occupied_seconds INTEGER NOT NULL,
                    PRIMARY KEY (station_id, provider_uid, archive_date, hour)
                );

                CREATE INDEX IF NOT EXISTS idx_evse_status_events_station_date
                    ON evse_status_events (station_id, archive_date);

                CREATE INDEX IF NOT EXISTS idx_station_daily_occupancy_station_date
                    ON station_daily_occupancy (station_id, archive_date);

                CREATE INDEX IF NOT EXISTS idx_station_hourly_occupancy_station_date
                    ON station_hourly_occupancy (station_id, archive_date);

                CREATE INDEX IF NOT EXISTS idx_station_daily_occupancy_date_provider_station
                    ON station_daily_occupancy (archive_date, provider_uid, station_id);

                CREATE INDEX IF NOT EXISTS idx_station_hourly_occupancy_date_provider_station_hour
                    ON station_hourly_occupancy (archive_date, provider_uid, station_id, hour);

                CREATE INDEX IF NOT EXISTS idx_station_hourly_occupancy_provider_station_hour_date
                    ON station_hourly_occupancy (provider_uid, station_id, hour, archive_date);
                """
            )
            self._ensure_archive_day_columns(conn)
            self._ensure_event_index_column(conn)

    def _ensure_archive_day_columns(self, conn: sqlite3.Connection) -> None:
        self._ensure_table_columns(
            conn,
            "occupancy_archive_days",
            [("stored_event_count", "INTEGER NOT NULL DEFAULT 0")],
        )

    def _ensure_table_columns(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        additions: Sequence[tuple[str, str]],
    ) -> set[str]:
        existing = {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}
        added_columns: set[str] = set()
        for column_name, definition in additions:
            if column_name in existing:
                continue
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")
            added_columns.add(column_name)
        return added_columns

    def _ensure_event_index_column(self, conn: sqlite3.Connection) -> None:
        columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(evse_status_events)")}
        if not columns or "event_index" in columns:
            return

        conn.execute("DROP INDEX IF EXISTS idx_evse_status_events_station_date")
        conn.execute("ALTER TABLE evse_status_events RENAME TO evse_status_events_legacy")
        conn.executescript(
            """
            CREATE TABLE evse_status_events (
                archive_date TEXT NOT NULL,
                archive_member TEXT NOT NULL,
                record_index INTEGER NOT NULL,
                event_index INTEGER NOT NULL,
                provider_uid TEXT NOT NULL,
                station_id TEXT NOT NULL,
                provider_evse_id TEXT NOT NULL,
                source_observed_at TEXT NOT NULL,
                availability_status TEXT NOT NULL,
                operational_status TEXT NOT NULL,
                message_timestamp TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                PRIMARY KEY (
                    archive_date,
                    archive_member,
                    record_index,
                    event_index,
                    provider_uid,
                    provider_evse_id
                )
            );

            INSERT OR IGNORE INTO evse_status_events (
                archive_date, archive_member, record_index, event_index, provider_uid,
                station_id, provider_evse_id, source_observed_at, availability_status,
                operational_status, message_timestamp, payload_sha256
            )
            SELECT
                archive_date, archive_member, record_index, 0, provider_uid,
                station_id, provider_evse_id, source_observed_at, availability_status,
                operational_status, message_timestamp, payload_sha256
            FROM evse_status_events_legacy;

            DROP TABLE evse_status_events_legacy;

            CREATE INDEX IF NOT EXISTS idx_evse_status_events_station_date
                ON evse_status_events (station_id, archive_date);
            """
        )
        conn.commit()
        conn.execute("VACUUM")

    def archive_day(self, archive_date: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM occupancy_archive_days
                WHERE archive_date = ?
                """,
                (archive_date,),
            ).fetchone()
        return dict(row) if row is not None else None

    def available_dates(self, *, start_date: str, end_date: str) -> list[str]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT archive_date
                FROM occupancy_archive_days
                WHERE archive_date BETWEEN ? AND ?
                ORDER BY archive_date
                """,
                (start_date, end_date),
            ).fetchall()
        return [str(row["archive_date"]) for row in rows]

    def replace_archive_day(
        self,
        *,
        archive_date: str,
        archive_path: Path,
        archive_sha256: str,
        record_count: int,
        mapped_event_count: int,
        stored_event_count: int,
        provider_station_count: int,
        status_change_count: int,
        events: Sequence[Mapping[str, Any]],
        daily_rows: Sequence[Mapping[str, Any]],
        hourly_rows: Sequence[Mapping[str, Any]],
    ) -> None:
        with self.connection() as conn:
            conn.execute("DELETE FROM evse_status_events WHERE archive_date = ?", (archive_date,))
            conn.execute("DELETE FROM station_hourly_occupancy WHERE archive_date = ?", (archive_date,))
            conn.execute("DELETE FROM station_daily_occupancy WHERE archive_date = ?", (archive_date,))
            conn.execute("DELETE FROM occupancy_archive_days WHERE archive_date = ?", (archive_date,))

            if events:
                conn.executemany(
                    """
                    INSERT INTO evse_status_events (
                        archive_date, archive_member, record_index, event_index, provider_uid,
                        station_id, provider_evse_id, source_observed_at, availability_status,
                        operational_status, message_timestamp, payload_sha256
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            archive_date,
                            str(row.get("archive_member") or ""),
                            int(row.get("record_index") or 0),
                            int(row.get("event_index") or 0),
                            str(row.get("provider_uid") or ""),
                            str(row.get("station_id") or ""),
                            str(row.get("provider_evse_id") or ""),
                            str(row.get("source_observed_at") or ""),
                            str(row.get("availability_status") or "unknown"),
                            str(row.get("operational_status") or ""),
                            str(row.get("message_timestamp") or ""),
                            str(row.get("payload_sha256") or ""),
                        )
                        for row in events
                    ],
                )

            conn.executemany(
                """
                INSERT INTO station_daily_occupancy (
                    station_id, provider_uid, archive_date, observed_evse_ids_json,
                    observed_evses, matching_observations, occupied_observations,
                    status_changes, latest_event_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        str(row.get("station_id") or ""),
                        str(row.get("provider_uid") or ""),
                        archive_date,
                        str(row.get("observed_evse_ids_json") or "[]"),
                        int(row.get("observed_evses") or 0),
                        int(row.get("matching_observations") or 0),
                        int(row.get("occupied_observations") or 0),
                        int(row.get("status_changes") or 0),
                        str(row.get("latest_event_timestamp") or ""),
                    )
                    for row in daily_rows
                ],
            )

            conn.executemany(
                """
                INSERT INTO station_hourly_occupancy (
                    station_id, provider_uid, archive_date, hour, occupied_seconds
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        str(row.get("station_id") or ""),
                        str(row.get("provider_uid") or ""),
                        archive_date,
                        int(row.get("hour") or 0),
                        int(row.get("occupied_seconds") or 0),
                    )
                    for row in hourly_rows
                ],
            )

            conn.execute(
                """
                INSERT INTO occupancy_archive_days (
                    archive_date, archive_path, processed_at, archive_sha256,
                    record_count, mapped_event_count, stored_event_count, provider_station_count,
                    status_change_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    archive_date,
                    str(archive_path),
                    utc_now_iso(),
                    archive_sha256,
                    int(record_count),
                    int(mapped_event_count),
                    int(stored_event_count),
                    int(provider_station_count),
                    int(status_change_count),
                ),
            )

    def clear_status_events(self, *, vacuum: bool = False) -> int:
        with self.connection() as conn:
            row = conn.execute("SELECT COUNT(*) FROM evse_status_events").fetchone()
            deleted_count = int(row[0] if row is not None else 0)
            conn.execute("DELETE FROM evse_status_events")
            conn.execute("UPDATE occupancy_archive_days SET stored_event_count = 0")
        if vacuum and deleted_count:
            self.vacuum()
        return deleted_count

    def prune_archive_window(self, *, start_date: str, end_date: str, vacuum: bool = False) -> int:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT archive_date
                FROM occupancy_archive_days
                WHERE archive_date < ? OR archive_date > ?
                ORDER BY archive_date
                """,
                (start_date, end_date),
            ).fetchall()
            pruned_dates = [str(row["archive_date"]) for row in rows]
            if not pruned_dates:
                return 0

            for table_name in (
                "evse_status_events",
                "station_hourly_occupancy",
                "station_daily_occupancy",
                "occupancy_archive_days",
            ):
                conn.execute(
                    f"DELETE FROM {table_name} WHERE archive_date < ? OR archive_date > ?",
                    (start_date, end_date),
                )

        if vacuum:
            self.vacuum()
        return len(pruned_dates)

    def vacuum(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("VACUUM")
        finally:
            conn.close()


def daily_rows_from_provider_station(
    archive_date: str,
    provider_station: Mapping[tuple[str, str], Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (provider_uid, station_id), daily in sorted(provider_station.items()):
        if int(getattr(daily, "matching_observations", 0) or 0) <= 0:
            continue
        observed_evse_ids = sorted(getattr(daily, "observed_evses", set()) or set())
        rows.append(
            {
                "archive_date": archive_date,
                "provider_uid": provider_uid,
                "station_id": station_id,
                "observed_evse_ids_json": _as_json_list(observed_evse_ids),
                "observed_evses": len(observed_evse_ids),
                "matching_observations": int(getattr(daily, "matching_observations", 0) or 0),
                "occupied_observations": int(getattr(daily, "occupied_observations", 0) or 0),
                "status_changes": int(getattr(daily, "status_changes", 0) or 0),
                "latest_event_timestamp": str(getattr(daily, "latest_event_timestamp", "") or ""),
            }
        )
    return rows


def hourly_rows_from_provider_station(
    archive_date: str,
    provider_station: Mapping[tuple[str, str], Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for (provider_uid, station_id), daily in sorted(provider_station.items()):
        if int(getattr(daily, "matching_observations", 0) or 0) <= 0:
            continue
        for hour, occupied_seconds in enumerate(getattr(daily, "hourly_occupied_seconds", []) or []):
            rows.append(
                {
                    "archive_date": archive_date,
                    "provider_uid": provider_uid,
                    "station_id": station_id,
                    "hour": hour,
                    "occupied_seconds": int(occupied_seconds or 0),
                }
            )
    return rows
