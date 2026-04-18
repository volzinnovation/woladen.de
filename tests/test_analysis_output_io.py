from __future__ import annotations

import csv
import json
from pathlib import Path

from analysis.output_io import (
    publish_staged_directory,
    staged_output_directory,
    write_csv,
    write_json_atomic,
    write_text_atomic,
)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_publish_staged_directory_keeps_old_snapshot_until_publish(tmp_path: Path):
    output_dir = tmp_path / "analysis-output"
    output_dir.mkdir()
    old_summary_path = output_dir / "provider_daily_summary.csv"
    write_csv(
        old_summary_path,
        ["archive_date", "provider_uid", "messages_total"],
        [{"archive_date": "2026-04-16", "provider_uid": "provider-a", "messages_total": 10}],
    )

    with staged_output_directory(output_dir) as staged_dir:
        staged_summary_path = staged_dir / "provider_daily_summary.csv"
        write_csv(
            staged_summary_path,
            ["archive_date", "provider_uid", "messages_total"],
            [{"archive_date": "2026-04-17", "provider_uid": "provider-a", "messages_total": 12}],
        )
        assert _read_csv(old_summary_path) == [
            {"archive_date": "2026-04-16", "provider_uid": "provider-a", "messages_total": "10"}
        ]

        publish_staged_directory(staged_dir, output_dir)
        assert _read_csv(old_summary_path) == [
            {"archive_date": "2026-04-17", "provider_uid": "provider-a", "messages_total": "12"}
        ]


def test_write_text_atomic_replaces_markdown_in_place(tmp_path: Path):
    report_path = tmp_path / "reports" / "provider_quality_2026-04-17.md"
    report_path.parent.mkdir(parents=True)
    report_path.write_text("old report\n", encoding="utf-8")

    write_text_atomic(report_path, "new report\n")

    assert report_path.read_text(encoding="utf-8") == "new report\n"
    temp_paths = list(report_path.parent.glob(f".{report_path.name}.tmp-*"))
    assert temp_paths == []


def test_publish_staged_directory_preserves_nested_relative_paths(tmp_path: Path):
    output_dir = tmp_path / "management"

    with staged_output_directory(output_dir) as staged_dir:
        nested_path = staged_dir / "days" / "2026" / "04" / "17" / "snapshot.json"
        nested_path.parent.mkdir(parents=True, exist_ok=True)
        nested_path.write_text('{"snapshot_date":"2026-04-17"}\n', encoding="utf-8")
        publish_staged_directory(staged_dir, output_dir)

    assert (output_dir / "days" / "2026" / "04" / "17" / "snapshot.json").exists()


def test_write_json_atomic_replaces_json_in_place(tmp_path: Path):
    json_path = tmp_path / "management" / "index.json"
    write_json_atomic(json_path, {"latest_date": "2026-04-17"}, pretty=True)

    assert json.loads(json_path.read_text(encoding="utf-8")) == {"latest_date": "2026-04-17"}
