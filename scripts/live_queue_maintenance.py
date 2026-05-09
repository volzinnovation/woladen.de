#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import tarfile
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.archive import DailyResponseArchiver
from backend.config import AppConfig, load_env_file
from backend.receipt_queue import ReceiptQueue, ReceiptTask

QUEUE_ENV_FILE_KEYS = frozenset(
    {
        "WOLADEN_LIVE_DB_PATH",
        "WOLADEN_LIVE_RAW_PAYLOAD_DIR",
        "WOLADEN_LIVE_ARCHIVE_DIR",
        "WOLADEN_LIVE_QUEUE_DIR",
        "WOLADEN_LIVE_ARCHIVE_TIMEZONE",
        "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID",
        "WOLADEN_LIVE_HF_ARCHIVE_REPO_TYPE",
        "WOLADEN_LIVE_HF_ARCHIVE_PATH_PREFIX",
        "WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE",
        "WOLADEN_LIVE_HF_ARCHIVE_TOKEN",
        "HF_TOKEN",
        "HUGGINGFACE_HUB_TOKEN",
        "HUGGINGFACE_TOKEN",
    }
)

LEGACY_QUEUE_STATES = ("pending", "processing", "failed", "done")
ACTIVE_STATES = {"pending", "processing"}


@dataclass(frozen=True)
class LegacyQueueItem:
    state: str
    path: Path
    task: ReceiptTask | None
    archive_date: str
    receipt_log_path: str
    raw_exists: bool
    hf_uploaded: bool
    classification: str
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "path": str(self.path),
            "task_id": self.task.task_id if self.task else self.path.stem,
            "archive_date": self.archive_date,
            "receipt_log_path": self.receipt_log_path,
            "raw_exists": self.raw_exists,
            "hf_uploaded": self.hf_uploaded,
            "classification": self.classification,
            "error": self.error,
        }


def _load_task(path: Path) -> ReceiptTask:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("invalid_task_payload")
    return ReceiptTask.from_dict(payload)


def _uploaded_dates(config: AppConfig) -> set[str]:
    if not config.has_hf_archive_upload_config():
        return set()
    return {
        target_date.isoformat()
        for target_date in DailyResponseArchiver(config)._uploaded_archive_dates(best_effort=True)
    }


def iter_legacy_queue_items(config: AppConfig, *, uploaded_dates: set[str] | None = None) -> Iterable[LegacyQueueItem]:
    queue = ReceiptQueue(config)
    effective_uploaded_dates = uploaded_dates if uploaded_dates is not None else _uploaded_dates(config)
    for state in LEGACY_QUEUE_STATES:
        state_dir = config.queue_dir / state
        if not state_dir.exists():
            continue
        for path in sorted(state_dir.glob("*.json")):
            task: ReceiptTask | None = None
            archive_date = ""
            receipt_log_path = ""
            raw_exists = False
            hf_uploaded = False
            classification = "invalid"
            error = ""
            try:
                task = _load_task(path)
                archive_date = queue.task_archive_date(task)
                receipt_log_path = task.receipt_log_path
                raw_exists = bool(receipt_log_path and Path(receipt_log_path).exists())
                hf_uploaded = bool(archive_date and archive_date in effective_uploaded_dates)
                if state in ACTIVE_STATES and raw_exists:
                    classification = "active_raw_present"
                elif state in ACTIVE_STATES and hf_uploaded:
                    classification = "active_raw_missing_hf_uploaded"
                elif state in ACTIVE_STATES:
                    classification = "active_raw_missing_not_uploaded"
                else:
                    classification = "completed_legacy"
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
            yield LegacyQueueItem(
                state=state,
                path=path,
                task=task,
                archive_date=archive_date,
                receipt_log_path=receipt_log_path,
                raw_exists=raw_exists,
                hf_uploaded=hf_uploaded,
                classification=classification,
                error=error,
            )


def backup_legacy_files(config: AppConfig, backup_path: Path, items: list[LegacyQueueItem]) -> dict[str, Any]:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    added = 0
    with tarfile.open(backup_path, "w:gz") as archive_handle:
        for item in items:
            if not item.path.exists():
                continue
            archive_handle.add(item.path, arcname=str(item.path.relative_to(config.queue_dir)))
            added += 1
    return {"backup_path": str(backup_path), "backup_file_count": added}


def migrate_active_items(queue: ReceiptQueue, items: list[LegacyQueueItem], *, apply: bool) -> dict[str, int]:
    migrated = 0
    skipped = 0
    for item in items:
        if item.classification != "active_raw_present" or item.task is None:
            continue
        if apply:
            try:
                queue.enqueue(item.task)
            except sqlite3.IntegrityError:
                skipped += 1
                continue
            item.path.unlink(missing_ok=True)
        migrated += 1
    return {"legacy_active_migrated": migrated, "legacy_active_migration_skipped": skipped}


def delete_stale_uploaded_items(items: list[LegacyQueueItem], *, apply: bool) -> dict[str, int]:
    deleted = 0
    for item in items:
        if item.classification != "active_raw_missing_hf_uploaded":
            continue
        if apply:
            item.path.unlink(missing_ok=True)
        deleted += 1
    return {"legacy_stale_uploaded_deleted": deleted}


def summarize_items(items: list[LegacyQueueItem]) -> dict[str, Any]:
    by_state: dict[str, int] = {}
    by_classification: dict[str, int] = {}
    for item in items:
        by_state[item.state] = by_state.get(item.state, 0) + 1
        by_classification[item.classification] = by_classification.get(item.classification, 0) + 1
    sample_items = [item.to_dict() for item in items[:20]]
    return {
        "legacy_file_count": len(items),
        "by_state": by_state,
        "by_classification": by_classification,
        "samples": sample_items,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit, back up, migrate, or clean legacy live receipt queue files")
    parser.add_argument("--env-file", type=Path, default=None, help="Optional runtime env file")
    parser.add_argument("--apply", action="store_true", help="Perform migrations/deletions. Default is dry-run.")
    parser.add_argument("--backup-path", type=Path, default=None, help="Legacy queue backup tgz path")
    parser.add_argument(
        "--migrate-active",
        action="store_true",
        help="Migrate active legacy tasks whose raw payload file still exists into the SQLite queue",
    )
    parser.add_argument(
        "--delete-stale-uploaded",
        action="store_true",
        help="Delete active legacy tasks whose raw payload is missing but whose archive date is uploaded to Hugging Face",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.env_file is not None:
        load_env_file(args.env_file, allowed_keys=QUEUE_ENV_FILE_KEYS)
    config = AppConfig()
    queue = ReceiptQueue(config)
    items = list(iter_legacy_queue_items(config))
    result: dict[str, Any] = {
        "mode": "apply" if args.apply else "dry_run",
        "summary": summarize_items(items),
    }
    if args.backup_path is not None and args.apply and (args.migrate_active or args.delete_stale_uploaded):
        result["backup"] = backup_legacy_files(config, args.backup_path, items)
    elif args.apply and (args.migrate_active or args.delete_stale_uploaded):
        raise SystemExit("--backup-path is required when --apply mutates legacy queue files")

    if args.migrate_active:
        if args.apply:
            queue.initialize()
        result["migration"] = migrate_active_items(queue, items, apply=args.apply)
    if args.delete_stale_uploaded:
        result["stale_cleanup"] = delete_stale_uploaded_items(items, apply=args.apply)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
