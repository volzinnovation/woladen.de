from __future__ import annotations

import json
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import AppConfig


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_datetime(value: str | None) -> datetime | None:
    text = str(value or "").strip()
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


def utc_now_iso() -> str:
    return _utc_now().replace(microsecond=0).isoformat()


@dataclass(frozen=True)
class ReceiptTask:
    task_id: str
    task_kind: str
    provider_uid: str
    run_id: int
    receipt_log_path: str
    receipt_at: str
    content_type: str = ""
    http_status: int = 0
    subscription_id: str = ""
    publication_id: str = ""
    enqueued_at: str = ""
    claim_path: str = ""

    @classmethod
    def from_dict(cls, payload: dict[str, Any], *, claim_path: Path | None = None) -> "ReceiptTask":
        return cls(
            task_id=str(payload.get("task_id") or "").strip(),
            task_kind=str(payload.get("task_kind") or "").strip(),
            provider_uid=str(payload.get("provider_uid") or "").strip(),
            run_id=int(payload.get("run_id") or 0),
            receipt_log_path=str(payload.get("receipt_log_path") or "").strip(),
            receipt_at=str(payload.get("receipt_at") or "").strip(),
            content_type=str(payload.get("content_type") or "").strip(),
            http_status=int(payload.get("http_status") or 0),
            subscription_id=str(payload.get("subscription_id") or "").strip(),
            publication_id=str(payload.get("publication_id") or "").strip(),
            enqueued_at=str(payload.get("enqueued_at") or "").strip(),
            claim_path=str(claim_path) if claim_path is not None else str(payload.get("claim_path") or "").strip(),
        )

    def with_claim_path(self, claim_path: Path) -> "ReceiptTask":
        payload = asdict(self)
        payload["claim_path"] = str(claim_path)
        return ReceiptTask.from_dict(payload, claim_path=claim_path)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if not payload["claim_path"]:
            payload.pop("claim_path", None)
        return payload


class ReceiptQueue:
    def __init__(self, config: AppConfig):
        self.config = config
        self.root_dir = config.queue_dir
        self.pending_dir = self.root_dir / "pending"
        self.processing_dir = self.root_dir / "processing"
        self.done_dir = self.root_dir / "done"
        self.failed_dir = self.root_dir / "failed"
        self.db_path = self.root_dir / "receipt_queue.sqlite3"
        self._next_cleanup_at_monotonic = 0.0

    def initialize(self) -> None:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        for path in (self.pending_dir, self.processing_dir, self.done_dir, self.failed_dir):
            path.mkdir(parents=True, exist_ok=True)
        with self._connection() as conn:
            self._ensure_schema(conn)

    def enqueue(self, task: ReceiptTask) -> Path:
        self.initialize()
        enqueued_at = task.enqueued_at or utc_now_iso()
        archive_date = self.task_archive_date(task)
        payload = task.to_dict()
        payload["enqueued_at"] = enqueued_at
        with self._connection() as conn:
            self._ensure_schema(conn)
            conn.execute(
                """
                INSERT INTO receipt_tasks (
                    task_id, state, task_kind, provider_uid, run_id, receipt_log_path,
                    receipt_at, archive_date, content_type, http_status, subscription_id,
                    publication_id, enqueued_at, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task.task_id,
                    "pending",
                    task.task_kind,
                    task.provider_uid,
                    int(task.run_id),
                    task.receipt_log_path,
                    task.receipt_at,
                    archive_date,
                    task.content_type,
                    int(task.http_status),
                    task.subscription_id,
                    task.publication_id,
                    enqueued_at,
                    json.dumps(payload, ensure_ascii=False, sort_keys=True),
                    utc_now_iso(),
                ),
            )
        return self.pending_dir / f"{task.task_id}.json"

    def build_task(
        self,
        *,
        task_kind: str,
        provider_uid: str,
        run_id: int,
        receipt_log_path: Path | str,
        receipt_at: str,
        content_type: str = "",
        http_status: int = 0,
        subscription_id: str = "",
        publication_id: str = "",
    ) -> ReceiptTask:
        stamp = _utc_now().strftime("%Y%m%dT%H%M%S%fZ")
        return ReceiptTask(
            task_id=f"{stamp}-{uuid.uuid4().hex[:12]}",
            task_kind=task_kind,
            provider_uid=provider_uid,
            run_id=run_id,
            receipt_log_path=str(receipt_log_path),
            receipt_at=receipt_at,
            content_type=content_type,
            http_status=http_status,
            subscription_id=subscription_id,
            publication_id=publication_id,
            enqueued_at=utc_now_iso(),
        )

    def claim_next(self) -> ReceiptTask | None:
        self.initialize()
        claimed_at = utc_now_iso()
        with self._connection() as conn:
            self._ensure_schema(conn)
            conn.commit()
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT *
                FROM receipt_tasks
                WHERE state = 'pending'
                ORDER BY enqueued_at, task_id
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                conn.commit()
                return None
            conn.execute(
                """
                UPDATE receipt_tasks
                SET state = 'processing', claimed_at = ?, attempts = attempts + 1, updated_at = ?
                WHERE task_id = ? AND state = 'pending'
                """,
                (claimed_at, claimed_at, row["task_id"]),
            )
            conn.commit()
        return self._task_from_row(row, claim_state="processing")

    def mark_done(self, task: ReceiptTask) -> None:
        self.initialize()
        completed_at = utc_now_iso()
        with self._connection() as conn:
            self._ensure_schema(conn)
            conn.execute(
                """
                UPDATE receipt_tasks
                SET state = 'done', completed_at = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (completed_at, completed_at, task.task_id),
            )
        self.cleanup_completed()

    def mark_failed(self, task: ReceiptTask, *, error_text: str = "") -> None:
        self.initialize()
        completed_at = utc_now_iso()
        with self._connection() as conn:
            self._ensure_schema(conn)
            conn.execute(
                """
                UPDATE receipt_tasks
                SET state = 'failed', error_text = ?, completed_at = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (error_text, completed_at, completed_at, task.task_id),
            )
        self.cleanup_completed()

    def cleanup_completed(self, *, force: bool = False) -> dict[str, int]:
        self.initialize()
        now_monotonic = time.monotonic()
        if not force and now_monotonic < self._next_cleanup_at_monotonic:
            return {"done_deleted": 0, "failed_deleted": 0}
        self._next_cleanup_at_monotonic = now_monotonic + max(self.config.queue_cleanup_interval_seconds, 0.0)

        return {
            "done_deleted": self._prune_state(
                "done",
                retention_seconds=float(self.config.queue_done_retention_seconds),
            ),
            "failed_deleted": self._prune_state(
                "failed",
                retention_seconds=float(self.config.queue_failed_retention_seconds),
            ),
        }

    def stats(self) -> dict[str, Any]:
        self.initialize()
        oldest_pending_age_seconds = None
        oldest_enqueued_at = None
        with self._connection() as conn:
            self._ensure_schema(conn)
            counts = {
                str(row["state"]): int(row["count"])
                for row in conn.execute(
                    """
                    SELECT state, COUNT(*) AS count
                    FROM receipt_tasks
                    GROUP BY state
                    """
                )
            }
            oldest_row = conn.execute(
                """
                SELECT enqueued_at, receipt_at
                FROM receipt_tasks
                WHERE state = 'pending'
                ORDER BY enqueued_at, task_id
                LIMIT 1
                """
            ).fetchone()
        if oldest_row is not None:
            oldest_enqueued_at = str(oldest_row["enqueued_at"] or oldest_row["receipt_at"] or "") or None
            oldest_dt = _parse_iso_datetime(oldest_enqueued_at)
            if oldest_dt is not None:
                oldest_pending_age_seconds = max(0.0, (_utc_now() - oldest_dt).total_seconds())
        return {
            "pending_count": counts.get("pending", 0),
            "processing_count": counts.get("processing", 0),
            "failed_count": counts.get("failed", 0),
            "oldest_pending_enqueued_at": oldest_enqueued_at,
            "oldest_pending_age_seconds": oldest_pending_age_seconds,
        }

    def task_archive_date(self, task: ReceiptTask) -> str:
        receipt_log_path_text = str(task.receipt_log_path or "").strip()
        if receipt_log_path_text:
            receipt_log_path = Path(receipt_log_path_text)
            try:
                relative_path = receipt_log_path.relative_to(self.config.raw_payload_dir)
            except ValueError:
                relative_path = None
            if relative_path is not None and len(relative_path.parts) >= 2:
                try:
                    datetime.strptime(relative_path.parts[1], "%Y-%m-%d")
                    return relative_path.parts[1]
                except ValueError:
                    pass

        receipt_at = _parse_iso_datetime(task.receipt_at)
        if receipt_at is None:
            return ""
        return receipt_at.astimezone(self.config.archive_timezone()).date().isoformat()

    def active_reference_for_archive_date(self, archive_date: str) -> dict[str, str] | None:
        self.initialize()
        with self._connection() as conn:
            self._ensure_schema(conn)
            row = conn.execute(
                """
                SELECT *
                FROM receipt_tasks
                WHERE state IN ('pending', 'processing') AND archive_date = ?
                ORDER BY enqueued_at, task_id
                LIMIT 1
                """,
                (archive_date,),
            ).fetchone()
        if row is not None:
            return {
                "queue_store": "sqlite",
                "queue_state": str(row["state"]),
                "task_id": str(row["task_id"]),
                "receipt_at": str(row["receipt_at"] or ""),
                "receipt_log_path": str(row["receipt_log_path"] or ""),
            }
        return self._legacy_active_reference()

    def _read_task(self, path: Path) -> ReceiptTask:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"invalid_receipt_task:{path}")
        return ReceiptTask.from_dict(payload, claim_path=path)

    def _claim_path(self, task: ReceiptTask) -> Path:
        claim_path = Path(task.claim_path)
        if not str(claim_path).strip():
            raise ValueError("missing_claim_path")
        return claim_path

    def _write_json(self, target_path: Path, payload: dict[str, Any]) -> None:
        temp_path = target_path.with_suffix(f"{target_path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temp_path.replace(target_path)

    def _connection(self) -> sqlite3.Connection:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        timeout_seconds = max(float(self.config.sqlite_busy_timeout_ms) / 1000.0, 1.0)
        conn = sqlite3.connect(self.db_path, timeout=timeout_seconds)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS receipt_tasks (
                task_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                task_kind TEXT NOT NULL,
                provider_uid TEXT NOT NULL,
                run_id INTEGER NOT NULL,
                receipt_log_path TEXT NOT NULL,
                receipt_at TEXT NOT NULL,
                archive_date TEXT NOT NULL,
                content_type TEXT NOT NULL DEFAULT '',
                http_status INTEGER NOT NULL DEFAULT 0,
                subscription_id TEXT NOT NULL DEFAULT '',
                publication_id TEXT NOT NULL DEFAULT '',
                enqueued_at TEXT NOT NULL,
                claimed_at TEXT NOT NULL DEFAULT '',
                completed_at TEXT NOT NULL DEFAULT '',
                error_text TEXT NOT NULL DEFAULT '',
                attempts INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL DEFAULT '{}',
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_receipt_tasks_state_enqueued
            ON receipt_tasks (state, enqueued_at, task_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_receipt_tasks_archive_state
            ON receipt_tasks (archive_date, state, enqueued_at, task_id)
            """
        )

    def _task_from_row(self, row: sqlite3.Row, *, claim_state: str = "") -> ReceiptTask:
        payload = json.loads(str(row["payload_json"] or "{}"))
        if not isinstance(payload, dict):
            payload = {}
        payload.update(
            {
                "task_id": str(row["task_id"]),
                "task_kind": str(row["task_kind"]),
                "provider_uid": str(row["provider_uid"]),
                "run_id": int(row["run_id"]),
                "receipt_log_path": str(row["receipt_log_path"]),
                "receipt_at": str(row["receipt_at"]),
                "content_type": str(row["content_type"] or ""),
                "http_status": int(row["http_status"] or 0),
                "subscription_id": str(row["subscription_id"] or ""),
                "publication_id": str(row["publication_id"] or ""),
                "enqueued_at": str(row["enqueued_at"] or ""),
            }
        )
        claim_path = self.root_dir / claim_state / f"{row['task_id']}.json" if claim_state else None
        return ReceiptTask.from_dict(payload, claim_path=claim_path)

    def _prune_state(self, state: str, *, retention_seconds: float) -> int:
        if retention_seconds <= 0:
            return 0
        cutoff_dt = datetime.fromtimestamp(time.time() - retention_seconds, tz=timezone.utc)
        cutoff = cutoff_dt.replace(microsecond=0).isoformat()
        with self._connection() as conn:
            self._ensure_schema(conn)
            cursor = conn.execute(
                """
                DELETE FROM receipt_tasks
                WHERE state = ? AND completed_at != '' AND completed_at < ?
                """,
                (state, cutoff),
            )
            return int(cursor.rowcount if cursor.rowcount is not None else 0)

    def _legacy_active_reference(self) -> dict[str, str] | None:
        for queue_name, queue_dir in (("pending", self.pending_dir), ("processing", self.processing_dir)):
            if not queue_dir.exists():
                continue
            try:
                for task_path in queue_dir.iterdir():
                    if task_path.is_file() and task_path.suffix == ".json":
                        return {
                            "queue_store": "legacy_files",
                            "queue_state": queue_name,
                            "task_id": task_path.stem,
                            "receipt_at": "",
                            "receipt_log_path": "",
                        }
            except OSError:
                return {
                    "queue_store": "legacy_files",
                    "queue_state": queue_name,
                    "task_id": "",
                    "receipt_at": "",
                    "receipt_log_path": "",
                }
        return None
