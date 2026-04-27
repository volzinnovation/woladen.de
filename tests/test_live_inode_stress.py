from __future__ import annotations

import json
import tarfile
from datetime import date
from pathlib import Path

from scripts.live_inode_stress import run_legacy_file_probe, write_journal_and_queue_records


def test_live_inode_stress_uses_journal_and_sqlite_queue(app_config):
    target_date = date(2026, 4, 19)

    result = write_journal_and_queue_records(
        app_config,
        record_count=12,
        provider_uid="stress_provider",
        target_date=target_date,
    )

    journal_path = Path(result["journal_path"])
    assert journal_path == app_config.raw_payload_dir / "stress_provider" / "2026-04-19" / "records.jsonl"
    assert journal_path.exists()
    records = [json.loads(line) for line in journal_path.read_text(encoding="utf-8").splitlines()]
    assert len(records) == 12
    assert {record["kind"] for record in records} == {"http_response"}
    assert result["queue_stats"]["pending_count"] == 12
    assert result["archive_result"]["result"] == "archived_local_only"
    assert result["archive_result"]["file_count"] == 1

    archive_path = Path(result["archive_result"]["archive_path"])
    with tarfile.open(archive_path, mode="r:gz") as archive_handle:
        names = archive_handle.getnames()
        archived_journal = archive_handle.extractfile("stress_provider/2026-04-19/records.jsonl")
        assert archived_journal is not None
        archived_records = [
            json.loads(line)
            for line in archived_journal.read().decode("utf-8").splitlines()
            if line.strip()
        ]
    assert "manifest.json" in names
    assert "stress_provider/2026-04-19/records.jsonl" in names
    assert sum(1 for name in names if name.endswith(".json") and name != "manifest.json") == 0
    assert len(archived_records) == 12
    assert sorted(path.name for path in journal_path.parent.iterdir()) == ["records.jsonl"]


def test_legacy_file_probe_can_clean_up_probe_files(app_config):
    result = run_legacy_file_probe(
        app_config,
        target_date=date(2026, 4, 19),
        limit=3,
        bytes_per_file=16,
        cleanup=True,
    )

    assert result["result"] == "limit_reached"
    assert result["created_files"] == 3
    assert result["cleaned_up"] is True
    assert not (app_config.raw_payload_dir / "_legacy_inode_probe").exists()
