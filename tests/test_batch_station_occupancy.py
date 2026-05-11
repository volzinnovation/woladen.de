from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
from datetime import date
from pathlib import Path

from analysis.batch_station_occupancy import (
    build_evse_station_maps,
    build_payload,
    build_provider_scopes,
    build_site_station_maps,
    generate_batch_station_occupancy,
    load_station_scope,
    process_archive,
)
from analysis.export_station_occupancy_from_db import EmptyArchiveStats, _load_aggregates, write_public_files
from analysis.occupancy_store import (
    OccupancyStore,
    daily_rows_from_provider_station,
    hourly_rows_from_provider_station,
)
from backend.config import AppConfig


REPO_ROOT = Path(__file__).resolve().parents[1]


def _dynamic_payload(
    *,
    first_evse_status: str,
    second_evse_status: str,
    timestamp: str,
) -> dict:
    return {
        "messageContainer": {
            "payload": [
                {
                    "aegiEnergyInfrastructureStatusPublication": {
                        "energyInfrastructureSiteStatus": [
                            {
                                "reference": {"idG": "SITE-1"},
                                "lastUpdated": timestamp,
                                "energyInfrastructureStationStatus": [
                                    {
                                        "reference": {"idG": "STATION-REF-1"},
                                        "lastUpdated": timestamp,
                                        "refillPointStatus": [
                                            {
                                                "aegiElectricChargingPointStatus": {
                                                    "reference": {"idG": "DE*QWE*E1"},
                                                    "status": {"value": first_evse_status},
                                                    "lastUpdated": timestamp,
                                                }
                                            },
                                            {
                                                "aegiElectricChargingPointStatus": {
                                                    "reference": {"idG": "DE*QWE*E2"},
                                                    "status": {"value": second_evse_status},
                                                    "lastUpdated": timestamp,
                                                }
                                            },
                                        ],
                                    }
                                ],
                            }
                        ]
                    }
                }
            ]
        }
    }


def _write_chargers_csv(path: Path) -> None:
    rows = [
        {
            "station_id": "station-1",
            "operator": "Qwello",
            "status": "In Betrieb",
            "max_power_kw": "150",
            "charging_points_count": "2",
            "lat": "52.5",
            "lon": "13.4",
            "postcode": "10115",
            "city": "Berlin",
            "address": "Example Street 1",
            "datex_site_id": "SITE-1",
            "datex_station_ids": "STATION-REF-1",
            "datex_charge_point_ids": "DEQWEE1|DEQWEE2",
            "detail_source_uid": "",
        }
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_site_matches_csv(path: Path) -> None:
    rows = [
        {
            "provider_uid": "qwello",
            "site_id": "SITE-1",
            "station_id": "station-1",
            "datex_station_ids": "STATION-REF-1",
            "datex_charge_point_ids": "DEQWEE1|DEQWEE2",
            "score": "-100.0",
        }
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _build_record(*, fetched_at: str, body: dict, archive_date: str) -> dict:
    body_text = json.dumps(body, ensure_ascii=False)
    return {
        "kind": "http_response",
        "provider_uid": "qwello",
        "fetched_at": fetched_at,
        "logged_at": fetched_at,
        "archive_date": archive_date,
        "http_status": 200,
        "content_type": "application/json",
        "headers_text": "",
        "payload_sha256": hashlib.sha256(body_text.encode("utf-8")).hexdigest(),
        "payload_byte_length": len(body_text.encode("utf-8")),
        "payload_is_gzip": False,
        "body_text": body_text,
    }


def _write_archive(path: Path) -> None:
    records = [
        (
            "qwello/2026-04-15/20260415T080000000000Z-200-a.json",
            _build_record(
                fetched_at="2026-04-15T08:00:00+00:00",
                body=_dynamic_payload(
                    first_evse_status="AVAILABLE",
                    second_evse_status="OCCUPIED",
                    timestamp="2026-04-15T08:00:00+00:00",
                ),
                archive_date="2026-04-15",
            ),
        ),
        (
            "qwello/2026-04-15/20260415T091500000000Z-200-b.json",
            _build_record(
                fetched_at="2026-04-15T09:15:00+00:00",
                body=_dynamic_payload(
                    first_evse_status="OCCUPIED",
                    second_evse_status="OCCUPIED",
                    timestamp="2026-04-15T09:15:00+00:00",
                ),
                archive_date="2026-04-15",
            ),
        ),
        (
            "qwello/2026-04-15/20260415T103000000000Z-200-c.json",
            _build_record(
                fetched_at="2026-04-15T10:30:00+00:00",
                body=_dynamic_payload(
                    first_evse_status="FAULTED",
                    second_evse_status="OCCUPIED",
                    timestamp="2026-04-15T10:30:00+00:00",
                ),
                archive_date="2026-04-15",
            ),
        ),
    ]
    with tarfile.open(path, "w:gz") as archive_handle:
        for name, record in records:
            payload = json.dumps(record, ensure_ascii=False, indent=2).encode("utf-8")
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            archive_handle.addfile(info, io.BytesIO(payload))


def test_generate_batch_station_occupancy_writes_compact_hourly_json(tmp_path: Path):
    chargers_csv_path = tmp_path / "chargers.csv"
    site_match_path = tmp_path / "site_matches.csv"
    archive_dir = tmp_path / "archives"
    output_dir = tmp_path / "station-occupancy"
    archive_dir.mkdir()
    archive_path = archive_dir / "live-provider-responses-2026-04-15.tgz"

    _write_chargers_csv(chargers_csv_path)
    _write_site_matches_csv(site_match_path)
    _write_archive(archive_path)

    config = AppConfig(
        chargers_csv_path=chargers_csv_path,
        full_chargers_csv_path=chargers_csv_path,
        site_match_path=site_match_path,
        archive_dir=archive_dir,
        archive_timezone_name="UTC",
    )

    result = generate_batch_station_occupancy(
        end_date=date(2026, 4, 15),
        days=1,
        output_dir=output_dir,
        config=config,
        pretty=True,
        quiet=True,
    )

    assert result["station_count"] == 1
    assert result["provider_station_count"] == 1
    assert result["matching_observations"] == 6
    assert result["status_changes"] == 4

    payload = json.loads(Path(result["json"]).read_text(encoding="utf-8"))
    assert payload["start_date"] == "2026-04-15"
    assert payload["summary"]["station_count"] == 1
    assert payload["summary"]["matching_observations"] == 6
    assert (output_dir / "station-occupancy-latest.json").exists()

    station = payload["stations"][0]
    assert station["station_id"] == "station-1"
    assert station["provider_uid"] == "qwello"
    assert station["observed_days"] == 1
    assert station["observed_evses"] == 2
    assert station["hourly_average_occupied"][7] == 0.0
    assert station["hourly_average_occupied"][8] == 1.0
    assert station["hourly_average_occupied"][9] == 1.75
    assert station["hourly_average_occupied"][10] == 1.5
    assert station["hourly_average_occupied"][11] == 1.0


def test_occupancy_store_round_trips_hourly_chart_data(tmp_path: Path):
    chargers_csv_path = tmp_path / "chargers.csv"
    site_match_path = tmp_path / "site_matches.csv"
    archive_dir = tmp_path / "archives"
    output_dir = tmp_path / "web" / "station-occupancy"
    archive_dir.mkdir()
    archive_path = archive_dir / "live-provider-responses-2026-04-15.tgz"

    _write_chargers_csv(chargers_csv_path)
    _write_site_matches_csv(site_match_path)
    _write_archive(archive_path)

    config = AppConfig(
        chargers_csv_path=chargers_csv_path,
        full_chargers_csv_path=chargers_csv_path,
        site_match_path=site_match_path,
        archive_dir=archive_dir,
        archive_timezone_name="UTC",
    )
    station_catalog = load_station_scope(config, "fast")
    station_ids = set(station_catalog)
    day = process_archive(
        archive_path,
        target_date=date(2026, 4, 15),
        config=config,
        station_ids=station_ids,
        site_station_maps=build_site_station_maps(config),
        evse_station_maps=build_evse_station_maps(config),
        provider_scopes=build_provider_scopes(config, station_ids),
        provider_prefilters={},
        quiet=True,
    )
    assert day.status_events == []

    store = OccupancyStore(tmp_path / "occupancy.sqlite3")
    store.initialize()
    store.replace_archive_day(
        archive_date="2026-04-15",
        archive_path=archive_path,
        archive_sha256="test-sha",
        record_count=day.stats.records_seen,
        mapped_event_count=sum(
            daily.matching_observations for daily in day.provider_station.values()
        ),
        stored_event_count=0,
        provider_station_count=len(day.provider_station),
        status_change_count=sum(daily.status_changes for daily in day.provider_station.values()),
        events=[],
        daily_rows=daily_rows_from_provider_station("2026-04-15", day.provider_station),
        hourly_rows=hourly_rows_from_provider_station("2026-04-15", day.provider_station),
    )

    assert store.archive_day("2026-04-15")["archive_sha256"] == "test-sha"
    assert store.available_dates(start_date="2026-04-15", end_date="2026-04-15") == ["2026-04-15"]
    assert store.archive_day("2026-04-15")["stored_event_count"] == 0
    with store.connection() as conn:
        assert conn.execute("SELECT COUNT(*) FROM evse_status_events").fetchone()[0] == 0

    store.replace_archive_day(
        archive_date="2026-04-14",
        archive_path=archive_path,
        archive_sha256="old-test-sha",
        record_count=day.stats.records_seen,
        mapped_event_count=sum(
            daily.matching_observations for daily in day.provider_station.values()
        ),
        stored_event_count=0,
        provider_station_count=len(day.provider_station),
        status_change_count=sum(daily.status_changes for daily in day.provider_station.values()),
        events=[],
        daily_rows=daily_rows_from_provider_station("2026-04-14", day.provider_station),
        hourly_rows=hourly_rows_from_provider_station("2026-04-14", day.provider_station),
    )
    assert store.available_dates(start_date="2026-04-14", end_date="2026-04-15") == [
        "2026-04-14",
        "2026-04-15",
    ]
    assert store.prune_archive_window(start_date="2026-04-15", end_date="2026-04-15") == 1
    assert store.available_dates(start_date="2026-04-14", end_date="2026-04-15") == ["2026-04-15"]

    aggregates = _load_aggregates(
        store,
        start_date=date(2026, 4, 15),
        end_date=date(2026, 4, 15),
    )
    payload = build_payload(
        start_date=date(2026, 4, 15),
        end_date=date(2026, 4, 15),
        requested_days=1,
        included_dates=[date(2026, 4, 15)],
        missing_archives=[],
        station_scope="fast",
        denominator_mode="included-days",
        raw_prefilter=False,
        station_catalog=station_catalog,
        aggregates=aggregates,
        stats=EmptyArchiveStats(),
    )
    stale_path = output_dir / "stale-station.json"
    stale_path.parent.mkdir(parents=True, exist_ok=True)
    stale_path.write_text("{}", encoding="utf-8")
    index = write_public_files(payload, output_dir, pretty=True, quiet=True)

    assert index["station_count"] == 1
    assert index["stations"]["station-1"] == "station-1.json"
    assert not stale_path.exists()
    station_payload = json.loads((output_dir / "station-1.json").read_text(encoding="utf-8"))
    assert station_payload["hourly_average_occupied"]["08:00"] == 1.0
    assert station_payload["hourly_average_occupied"]["09:00"] == 1.75
    assert station_payload["hourly_average_occupied"]["10:00"] == 1.5


def test_build_occupancy_db_accepts_existing_day_without_local_archive(tmp_path: Path):
    chargers_csv_path = tmp_path / "chargers.csv"
    site_match_path = tmp_path / "site_matches.csv"
    archive_dir = tmp_path / "archives"
    db_path = tmp_path / "occupancy.sqlite3"
    archive_dir.mkdir()

    _write_chargers_csv(chargers_csv_path)
    _write_site_matches_csv(site_match_path)

    store = OccupancyStore(db_path)
    store.initialize()
    store.replace_archive_day(
        archive_date="2026-04-15",
        archive_path=archive_dir / "live-provider-responses-2026-04-15.tgz",
        archive_sha256="already-imported",
        record_count=3,
        mapped_event_count=6,
        stored_event_count=0,
        provider_station_count=1,
        status_change_count=4,
        events=[],
        daily_rows=[
            {
                "station_id": "station-1",
                "provider_uid": "qwello",
                "observed_evse_ids_json": '["DEQWEE1","DEQWEE2"]',
                "observed_evses": 2,
                "matching_observations": 6,
                "occupied_observations": 4,
                "status_changes": 4,
                "latest_event_timestamp": "2026-04-15T10:30:00+00:00",
            }
        ],
        hourly_rows=[
            {
                "station_id": "station-1",
                "provider_uid": "qwello",
                "hour": hour,
                "occupied_seconds": 0,
            }
            for hour in range(24)
        ],
    )

    env = os.environ.copy()
    env.update(
        {
            "WOLADEN_LIVE_CHARGERS_CSV_PATH": str(chargers_csv_path),
            "WOLADEN_LIVE_FULL_CHARGERS_CSV_PATH": str(chargers_csv_path),
            "WOLADEN_LIVE_SITE_MATCH_PATH": str(site_match_path),
            "WOLADEN_LIVE_ARCHIVE_TIMEZONE": "UTC",
        }
    )
    completed = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "analysis" / "build_occupancy_db.py"),
            "--date",
            "2026-04-15",
            "--archive-dir",
            str(archive_dir),
            "--db",
            str(db_path),
            "--require-complete",
            "--quiet",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    summary = json.loads(completed.stdout)
    assert summary["imported"] == []
    assert summary["missing"] == []
    assert summary["incomplete_dates"] == []
    assert summary["available_dates"] == ["2026-04-15"]
    assert summary["skipped"] == [
        {"archive_date": "2026-04-15", "reason": "existing_without_local_archive"}
    ]

    forced_completed = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "analysis" / "build_occupancy_db.py"),
            "--date",
            "2026-04-15",
            "--archive-dir",
            str(archive_dir),
            "--db",
            str(db_path),
            "--require-complete",
            "--force",
            "--quiet",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert forced_completed.returncode != 0
    assert "Missing occupancy DB days after import: 2026-04-15" in forced_completed.stderr

    missing_completed = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "analysis" / "build_occupancy_db.py"),
            "--date",
            "2026-04-16",
            "--archive-dir",
            str(archive_dir),
            "--db",
            str(db_path),
            "--require-complete",
            "--quiet",
        ],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert missing_completed.returncode != 0
    assert "Missing occupancy DB days after import: 2026-04-16" in missing_completed.stderr
