from __future__ import annotations

import csv
import hashlib
import io
import json
import tarfile
from datetime import date
from pathlib import Path

from analysis.batch_station_occupancy import generate_batch_station_occupancy
from backend.config import AppConfig


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
