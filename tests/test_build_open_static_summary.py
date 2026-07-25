from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from scripts.build_open_static_summary import build_summary


def _write_bundle(path: Path, build_metadata: dict[str, object]) -> None:
    with sqlite3.connect(path) as connection:
        connection.execute("create table bundle_metadata (key text primary key, json_value text not null)")
        connection.execute("create table stations (country_code text, max_power_kw real)")
        connection.execute("create table chargers (country_code text)")
        connection.execute(
            """
            create table sources (
                source_uid text,
                country_code text,
                source_url text,
                license text,
                attribution_json text
            )
            """
        )
        connection.execute(
            "insert into bundle_metadata (key, json_value) values (?, ?)",
            ("build", json.dumps(build_metadata)),
        )
        connection.execute("insert into stations values ('BE', 150)")
        connection.execute("insert into chargers values ('BE')")
        connection.execute(
            "insert into sources values ('be_source', 'BE', 'https://example.test', 'CC0', '{}')"
        )


def test_catalog_update_uses_latest_valid_patch_without_overwriting_base_build(tmp_path: Path):
    bundle_path = tmp_path / "open_static.sqlite3"
    _write_bundle(
        bundle_path,
        {
            "generated_at": "2026-06-26T22:14:56+00:00",
            "patches": [
                {"patched_at": "not-a-date"},
                {"patched_at": "2026-07-24T20:33:52.141603+00:00"},
            ],
        },
    )

    summary = build_summary(bundle_path)

    assert summary["generated_at"] == "2026-06-26T22:14:56+00:00"
    assert summary["bundle_generated_at"] == "2026-06-26T22:14:56+00:00"
    assert summary["catalog_updated_at"] == "2026-07-24T20:33:52.141603+00:00"
    assert summary["bundle"]["station_count"] == 1
    assert summary["bundle"]["charger_count"] == 1


def test_catalog_update_falls_back_to_base_build_for_unpatched_bundle(tmp_path: Path):
    bundle_path = tmp_path / "open_static.sqlite3"
    _write_bundle(bundle_path, {"generated_at": "2026-06-26T22:14:56Z"})

    summary = build_summary(bundle_path)

    assert summary["catalog_updated_at"] == "2026-06-26T22:14:56Z"
