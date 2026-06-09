from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import shutil
import sqlite3
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import unquote

from .config import AppConfig

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


@dataclass(frozen=True)
class ReceiptRecord:
    receipt_id: str
    country_code: str
    source_uid: str
    payload_sha256: str
    byte_length: int
    duplicate_payload: bool
    storage_uri: str
    queued: bool


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PayloadArchiveMixin:
    config: AppConfig

    def record_push_payload(
        self,
        *,
        country_code: str,
        source_uid: str,
        payload_bytes: bytes,
        content_type: str,
        content_encoding: str,
        request_path: str,
        request_query: str,
        request_headers: dict[str, str],
        remote_addr: str = "",
    ) -> ReceiptRecord:
        temp_dir = self.config.raw_payload_dir / "_incoming"
        temp_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=temp_dir, prefix="push-", suffix=".bin", delete=False) as handle:
            handle.write(payload_bytes)
            temp_path = Path(handle.name)
        return self.record_push_payload_file(
            country_code=country_code,
            source_uid=source_uid,
            payload_path=temp_path,
            payload_sha256=hashlib.sha256(payload_bytes).hexdigest(),
            byte_length=len(payload_bytes),
            content_type=content_type,
            content_encoding=content_encoding,
            request_path=request_path,
            request_query=request_query,
            request_headers=request_headers,
            remote_addr=remote_addr,
        )

    def _append_raw_payload(
        self,
        *,
        country_code: str,
        source_uid: str,
        received_at: datetime,
        payload_sha256: str,
        payload_path: Path,
        byte_length: int,
    ) -> str:
        safe_source = "".join(ch if ch.isalnum() or ch in {"_", "-", "."} else "_" for ch in source_uid)
        target_dir = (
            self.config.raw_payload_dir
            / country_code
            / safe_source
            / f"{received_at:%Y}"
            / f"{received_at:%m}"
            / f"{received_at:%d}"
        )
        target_dir.mkdir(parents=True, exist_ok=True)
        blob_path = target_dir / "payloads.bin"
        manifest_path = target_dir / "payloads.ndjson"
        with blob_path.open("a+b") as blob_handle:
            fcntl.flock(blob_handle.fileno(), fcntl.LOCK_EX)
            try:
                blob_handle.seek(0, os.SEEK_END)
                offset = blob_handle.tell()
                with payload_path.open("rb") as payload_handle:
                    shutil.copyfileobj(payload_handle, blob_handle, length=1024 * 1024)
                blob_handle.flush()
                os.fsync(blob_handle.fileno())
                storage_uri = (
                    f"{blob_path.resolve().as_uri()}#offset={offset}"
                    f"&length={byte_length}&sha256={payload_sha256}"
                )
                manifest_row = {
                    "received_at": received_at.isoformat(),
                    "country_code": country_code,
                    "source_uid": source_uid,
                    "payload_sha256": payload_sha256,
                    "byte_length": byte_length,
                    "blob_uri": blob_path.resolve().as_uri(),
                    "offset": offset,
                    "length": byte_length,
                    "storage_uri": storage_uri,
                }
                with manifest_path.open("a", encoding="utf-8") as manifest_handle:
                    manifest_handle.write(json.dumps(manifest_row, sort_keys=True) + "\n")
                    manifest_handle.flush()
                    os.fsync(manifest_handle.fileno())
            finally:
                fcntl.flock(blob_handle.fileno(), fcntl.LOCK_UN)
        payload_path.unlink(missing_ok=True)
        return storage_uri

    def _storage_uri_parts(self, storage_uri: str) -> tuple[Path, int, int]:
        if "#offset=" not in storage_uri:
            raise ValueError("storage_uri_missing_offset")
        uri, fragment = storage_uri.split("#", 1)
        params = {}
        for part in fragment.split("&"):
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            params[key] = value
        offset = int(params.get("offset") or 0)
        length = int(params.get("length") or 0)
        if not uri.startswith("file://") or length <= 0:
            raise ValueError("unsupported_storage_uri")
        path = Path(unquote(uri.removeprefix("file://")))
        return path, offset, length

    @contextmanager
    def open_storage_uri_reader(self, storage_uri: str) -> Iterator[io.BufferedReader]:
        path, offset, length = self._storage_uri_parts(storage_uri)
        with path.open("rb") as handle:
            handle.seek(offset)
            yield io.BufferedReader(_LimitedBinaryReader(handle, length))

    def _read_storage_uri_bytes(self, storage_uri: str) -> bytes:
        try:
            path, offset, length = self._storage_uri_parts(storage_uri)
        except ValueError:
            return b""
        with path.open("rb") as handle:
            handle.seek(offset)
            return handle.read(length)


class _LimitedBinaryReader(io.RawIOBase):
    def __init__(self, handle, length: int):
        self._handle = handle
        self._remaining = max(int(length), 0)

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        if size is None or size < 0 or size > self._remaining:
            size = self._remaining
        chunk = self._handle.read(size)
        self._remaining -= len(chunk)
        return chunk

    def readinto(self, buffer) -> int:
        chunk = self.read(len(buffer))
        size = len(chunk)
        buffer[:size] = chunk
        return size


class SQLiteIngestStore(PayloadArchiveMixin):
    def __init__(self, config: AppConfig):
        self.config = config

    def _connect(self) -> sqlite3.Connection:
        self.config.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.config.sqlite_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def initialize(self) -> None:
        self.config.raw_payload_dir.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS ingest_sources (
                    source_uid TEXT PRIMARY KEY,
                    country_code TEXT NOT NULL,
                    source_kind TEXT NOT NULL DEFAULT 'afir_dynamic',
                    display_name TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    accepts_push INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_ingest_sources_country
                    ON ingest_sources (country_code);

                CREATE TABLE IF NOT EXISTS raw_payloads (
                    payload_sha256 TEXT PRIMARY KEY,
                    first_received_at TEXT NOT NULL,
                    country_code TEXT NOT NULL,
                    source_uid TEXT NOT NULL,
                    storage_uri TEXT NOT NULL,
                    byte_length INTEGER NOT NULL,
                    content_type TEXT NOT NULL DEFAULT '',
                    content_encoding TEXT NOT NULL DEFAULT '',
                    inline_payload BLOB
                );

                CREATE INDEX IF NOT EXISTS idx_raw_payloads_source_received
                    ON raw_payloads (country_code, source_uid, first_received_at DESC);

                CREATE TABLE IF NOT EXISTS push_receipts (
                    receipt_id TEXT PRIMARY KEY,
                    received_at TEXT NOT NULL,
                    country_code TEXT NOT NULL,
                    source_uid TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL REFERENCES raw_payloads(payload_sha256),
                    byte_length INTEGER NOT NULL,
                    duplicate_payload INTEGER NOT NULL DEFAULT 0,
                    result TEXT NOT NULL DEFAULT 'queued',
                    content_type TEXT NOT NULL DEFAULT '',
                    content_encoding TEXT NOT NULL DEFAULT '',
                    request_path TEXT NOT NULL DEFAULT '',
                    request_query TEXT NOT NULL DEFAULT '',
                    request_headers TEXT NOT NULL DEFAULT '{}',
                    remote_addr TEXT NOT NULL DEFAULT '',
                    error_text TEXT NOT NULL DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_push_receipts_received
                    ON push_receipts (received_at DESC);

                CREATE INDEX IF NOT EXISTS idx_push_receipts_source_received
                    ON push_receipts (country_code, source_uid, received_at DESC);

                CREATE TABLE IF NOT EXISTS ingest_tasks (
                    task_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    country_code TEXT NOT NULL,
                    source_uid TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL REFERENCES raw_payloads(payload_sha256),
                    receipt_id TEXT NOT NULL REFERENCES push_receipts(receipt_id),
                    task_kind TEXT NOT NULL DEFAULT 'parse_dynamic_payload',
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    locked_at TEXT,
                    finished_at TEXT,
                    error_text TEXT NOT NULL DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS idx_ingest_tasks_status_created
                    ON ingest_tasks (status, created_at);
                """
            )
            for country_code in self.config.normalized_country_codes():
                conn.execute(
                    """
                    INSERT OR IGNORE INTO ingest_sources (
                        source_uid, country_code, source_kind, display_name
                    )
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        f"{country_code.lower()}_unassigned",
                        country_code,
                        "country_placeholder",
                        f"{country_code} unassigned source placeholder",
                    ),
                )

    def healthcheck(self) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 AS ok").fetchone()
        return {"sqlite": bool(row and row["ok"] == 1), "sqlite_path": str(self.config.sqlite_path)}

    def record_push_payload_file(
        self,
        *,
        country_code: str,
        source_uid: str,
        payload_path: Path,
        payload_sha256: str,
        byte_length: int,
        content_type: str,
        content_encoding: str,
        request_path: str,
        request_query: str,
        request_headers: dict[str, str],
        remote_addr: str = "",
    ) -> ReceiptRecord:
        return self._record_payload_file(
            country_code=country_code,
            source_uid=source_uid,
            payload_path=payload_path,
            payload_sha256=payload_sha256,
            byte_length=byte_length,
            content_type=content_type,
            content_encoding=content_encoding,
            request_path=request_path,
            request_query=request_query,
            request_headers=request_headers,
            remote_addr=remote_addr,
            enforce_push=True,
            source_kind="afir_dynamic",
            display_name=source_uid,
            task_kind="parse_dynamic_payload",
        )

    def record_pull_payload_file(
        self,
        *,
        country_code: str,
        source_uid: str,
        payload_path: Path,
        payload_sha256: str,
        byte_length: int,
        content_type: str,
        content_encoding: str,
        request_path: str,
        request_query: str,
        request_headers: dict[str, str],
        source_kind: str,
        display_name: str,
        task_kind: str,
        remote_addr: str = "",
    ) -> ReceiptRecord:
        return self._record_payload_file(
            country_code=country_code,
            source_uid=source_uid,
            payload_path=payload_path,
            payload_sha256=payload_sha256,
            byte_length=byte_length,
            content_type=content_type,
            content_encoding=content_encoding,
            request_path=request_path,
            request_query=request_query,
            request_headers=request_headers,
            remote_addr=remote_addr,
            enforce_push=False,
            source_kind=source_kind,
            display_name=display_name,
            task_kind=task_kind,
        )

    def _record_payload_file(
        self,
        *,
        country_code: str,
        source_uid: str,
        payload_path: Path,
        payload_sha256: str,
        byte_length: int,
        content_type: str,
        content_encoding: str,
        request_path: str,
        request_query: str,
        request_headers: dict[str, str],
        remote_addr: str,
        enforce_push: bool,
        source_kind: str,
        display_name: str,
        task_kind: str,
    ) -> ReceiptRecord:
        country_code = country_code.upper()
        source_uid = source_uid.strip()
        received_at = utc_now()
        received_at_text = received_at.isoformat()
        receipt_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())

        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            source = conn.execute(
                "SELECT source_uid, enabled, accepts_push FROM ingest_sources WHERE source_uid = ?",
                (source_uid,),
            ).fetchone()
            if source is None:
                if enforce_push and not self.config.accept_unknown_sources:
                    raise KeyError(f"unknown_source:{source_uid}")
                conn.execute(
                    """
                    INSERT OR IGNORE INTO ingest_sources (
                        source_uid,
                        country_code,
                        source_kind,
                        display_name,
                        accepts_push
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (source_uid, country_code, source_kind, display_name or source_uid, int(enforce_push)),
                )
            elif not bool(source["enabled"]):
                raise ValueError(f"source_disabled:{source_uid}")
            elif enforce_push and not bool(source["accepts_push"]):
                raise ValueError(f"source_push_disabled:{source_uid}")

            existing_payload = conn.execute(
                "SELECT storage_uri FROM raw_payloads WHERE payload_sha256 = ?",
                (payload_sha256,),
            ).fetchone()
            if existing_payload is None:
                storage_uri = self._append_raw_payload(
                    country_code=country_code,
                    source_uid=source_uid,
                    received_at=received_at,
                    payload_sha256=payload_sha256,
                    payload_path=payload_path,
                    byte_length=byte_length,
                )
                inline_payload = None
                if 0 < self.config.inline_payload_max_bytes >= byte_length:
                    inline_payload = self._read_storage_uri_bytes(storage_uri)
                conn.execute(
                    """
                    INSERT INTO raw_payloads (
                        payload_sha256,
                        first_received_at,
                        country_code,
                        source_uid,
                        storage_uri,
                        byte_length,
                        content_type,
                        content_encoding,
                        inline_payload
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload_sha256,
                        received_at_text,
                        country_code,
                        source_uid,
                        storage_uri,
                        byte_length,
                        content_type,
                        content_encoding,
                        inline_payload,
                    ),
                )
                duplicate_payload = False
            else:
                payload_path.unlink(missing_ok=True)
                storage_uri = str(existing_payload["storage_uri"])
                duplicate_payload = True

            conn.execute(
                """
                INSERT INTO push_receipts (
                    receipt_id,
                    received_at,
                    country_code,
                    source_uid,
                    payload_sha256,
                    byte_length,
                    duplicate_payload,
                    result,
                    content_type,
                    content_encoding,
                    request_path,
                    request_query,
                    request_headers,
                    remote_addr
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt_id,
                    received_at_text,
                    country_code,
                    source_uid,
                    payload_sha256,
                    byte_length,
                    int(duplicate_payload),
                    "duplicate" if duplicate_payload else "queued",
                    content_type,
                    content_encoding,
                    request_path,
                    request_query,
                    json.dumps(request_headers, sort_keys=True),
                    remote_addr,
                ),
            )

            if not duplicate_payload:
                conn.execute(
                    """
                    INSERT INTO ingest_tasks (
                        task_id,
                        created_at,
                        country_code,
                        source_uid,
                        payload_sha256,
                        receipt_id,
                        task_kind
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (task_id, received_at_text, country_code, source_uid, payload_sha256, receipt_id, task_kind),
                )

        return ReceiptRecord(
            receipt_id=receipt_id,
            country_code=country_code,
            source_uid=source_uid,
            payload_sha256=payload_sha256,
            byte_length=byte_length,
            duplicate_payload=duplicate_payload,
            storage_uri=storage_uri,
            queued=not duplicate_payload,
        )


class PostgresIngestStore(PayloadArchiveMixin):
    def __init__(self, config: AppConfig):
        self.config = config

    def _connect(self):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - exercised in runtime environments.
            raise RuntimeError(
                "psycopg is required for the open-static ingest backend. "
                "Install requirements-open-static.txt."
            ) from exc
        return psycopg.connect(self.config.database_url, row_factory=dict_row)

    def initialize(self) -> None:
        with self._connect() as conn:
            for migration_path in sorted(MIGRATIONS_DIR.glob("*.sql")):
                conn.execute(migration_path.read_text(encoding="utf-8"))
            for country_code in self.config.normalized_country_codes():
                conn.execute(
                    """
                    INSERT INTO ingest_sources (source_uid, country_code, source_kind, display_name)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (source_uid) DO NOTHING
                    """,
                    (
                        f"{country_code.lower()}_unassigned",
                        country_code,
                        "country_placeholder",
                        f"{country_code} unassigned source placeholder",
                    ),
                )

    def healthcheck(self) -> dict[str, Any]:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 AS ok").fetchone()
        return {"postgres": bool(row and row["ok"] == 1)}

    def record_push_payload_file(
        self,
        *,
        country_code: str,
        source_uid: str,
        payload_path: Path,
        payload_sha256: str,
        byte_length: int,
        content_type: str,
        content_encoding: str,
        request_path: str,
        request_query: str,
        request_headers: dict[str, str],
        remote_addr: str = "",
    ) -> ReceiptRecord:
        return self._record_payload_file(
            country_code=country_code,
            source_uid=source_uid,
            payload_path=payload_path,
            payload_sha256=payload_sha256,
            byte_length=byte_length,
            content_type=content_type,
            content_encoding=content_encoding,
            request_path=request_path,
            request_query=request_query,
            request_headers=request_headers,
            remote_addr=remote_addr,
            enforce_push=True,
            source_kind="afir_dynamic",
            display_name=source_uid,
            task_kind="parse_dynamic_payload",
        )

    def record_pull_payload_file(
        self,
        *,
        country_code: str,
        source_uid: str,
        payload_path: Path,
        payload_sha256: str,
        byte_length: int,
        content_type: str,
        content_encoding: str,
        request_path: str,
        request_query: str,
        request_headers: dict[str, str],
        source_kind: str,
        display_name: str,
        task_kind: str,
        remote_addr: str = "",
    ) -> ReceiptRecord:
        return self._record_payload_file(
            country_code=country_code,
            source_uid=source_uid,
            payload_path=payload_path,
            payload_sha256=payload_sha256,
            byte_length=byte_length,
            content_type=content_type,
            content_encoding=content_encoding,
            request_path=request_path,
            request_query=request_query,
            request_headers=request_headers,
            remote_addr=remote_addr,
            enforce_push=False,
            source_kind=source_kind,
            display_name=display_name,
            task_kind=task_kind,
        )

    def _record_payload_file(
        self,
        *,
        country_code: str,
        source_uid: str,
        payload_path: Path,
        payload_sha256: str,
        byte_length: int,
        content_type: str,
        content_encoding: str,
        request_path: str,
        request_query: str,
        request_headers: dict[str, str],
        remote_addr: str,
        enforce_push: bool,
        source_kind: str,
        display_name: str,
        task_kind: str,
    ) -> ReceiptRecord:
        country_code = country_code.upper()
        source_uid = source_uid.strip()
        received_at = utc_now()
        receipt_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())

        with self._connect() as conn:
            source = conn.execute(
                "SELECT source_uid, enabled, accepts_push FROM ingest_sources WHERE source_uid = %s",
                (source_uid,),
            ).fetchone()
            if source is None:
                if enforce_push and not self.config.accept_unknown_sources:
                    raise KeyError(f"unknown_source:{source_uid}")
                conn.execute(
                    """
                    INSERT INTO ingest_sources (
                        source_uid,
                        country_code,
                        source_kind,
                        display_name,
                        accepts_push
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (source_uid) DO NOTHING
                    """,
                    (source_uid, country_code, source_kind, display_name or source_uid, enforce_push),
                )
            elif not source["enabled"]:
                raise ValueError(f"source_disabled:{source_uid}")
            elif enforce_push and not source["accepts_push"]:
                raise ValueError(f"source_push_disabled:{source_uid}")

            existing_payload = conn.execute(
                "SELECT storage_uri FROM raw_payloads WHERE payload_sha256 = %s",
                (payload_sha256,),
            ).fetchone()
            if existing_payload is None:
                storage_uri = self._append_raw_payload(
                    country_code=country_code,
                    source_uid=source_uid,
                    received_at=received_at,
                    payload_sha256=payload_sha256,
                    payload_path=payload_path,
                    byte_length=byte_length,
                )
                inline_payload = None
                if 0 < self.config.inline_payload_max_bytes >= byte_length:
                    inline_payload = self._read_storage_uri_bytes(storage_uri)
            else:
                payload_path.unlink(missing_ok=True)
                storage_uri = str(existing_payload["storage_uri"])
                inline_payload = None

            inserted_payload = conn.execute(
                """
                INSERT INTO raw_payloads (
                    payload_sha256,
                    first_received_at,
                    country_code,
                    source_uid,
                    storage_uri,
                    byte_length,
                    content_type,
                    content_encoding,
                    inline_payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (payload_sha256) DO NOTHING
                RETURNING payload_sha256
                """,
                (
                    payload_sha256,
                    received_at,
                    country_code,
                    source_uid,
                    storage_uri,
                    byte_length,
                    content_type,
                    content_encoding,
                    inline_payload,
                ),
            ).fetchone()
            duplicate_payload = inserted_payload is None

            conn.execute(
                """
                INSERT INTO push_receipts (
                    receipt_id,
                    received_at,
                    country_code,
                    source_uid,
                    payload_sha256,
                    byte_length,
                    duplicate_payload,
                    result,
                    content_type,
                    content_encoding,
                    request_path,
                    request_query,
                    request_headers,
                    remote_addr
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                """,
                (
                    receipt_id,
                    received_at,
                    country_code,
                    source_uid,
                    payload_sha256,
                    byte_length,
                    duplicate_payload,
                    "duplicate" if duplicate_payload else "queued",
                    content_type,
                    content_encoding,
                    request_path,
                    request_query,
                    json.dumps(request_headers, sort_keys=True),
                    remote_addr,
                ),
            )

            if not duplicate_payload:
                conn.execute(
                    """
                    INSERT INTO ingest_tasks (
                        task_id,
                        created_at,
                        country_code,
                        source_uid,
                        payload_sha256,
                        receipt_id,
                        task_kind
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (task_id, received_at, country_code, source_uid, payload_sha256, receipt_id, task_kind),
                )

        return ReceiptRecord(
            receipt_id=receipt_id,
            country_code=country_code,
            source_uid=source_uid,
            payload_sha256=payload_sha256,
            byte_length=byte_length,
            duplicate_payload=duplicate_payload,
            storage_uri=storage_uri,
            queued=not duplicate_payload,
        )


def create_ingest_store(config: AppConfig):
    backend = str(config.store_backend or "sqlite").strip().lower()
    if backend == "postgres":
        return PostgresIngestStore(config)
    if backend == "sqlite":
        return SQLiteIngestStore(config)
    raise ValueError(f"unsupported_commercial_store_backend:{config.store_backend}")
