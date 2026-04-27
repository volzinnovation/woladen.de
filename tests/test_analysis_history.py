from __future__ import annotations

import csv
import hashlib
import io
import json
import tarfile
from pathlib import Path

from analysis.afir_history import _competitive_analysis_status, _iter_archive_members, run_analysis
from backend.config import AppConfig


def _dynamic_payload(
    *,
    status: str,
    second_evse_status: str = "OCCUPIED",
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
                                                    "status": {"value": status},
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


def _write_provider_config(path: Path) -> None:
    payload = {
        "providers": [
            {
                "uid": "qwello",
                "display_name": "Qwello",
                "publisher": "Qwello Deutschland GmbH",
                "feeds": {
                    "static": {
                        "publication_id": "static-pub-1",
                        "access_mode": "noauth",
                    },
                    "dynamic": {
                        "publication_id": "dynamic-pub-1",
                        "access_mode": "noauth",
                        "delta_delivery": False,
                        "content_data": {
                            "deltaDelivery": False,
                            "retentionPeriod": 15,
                        },
                    },
                },
            }
        ]
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_chargers_csv(path: Path) -> None:
    rows = [
        {
            "station_id": "station-1",
            "operator": "Qwello",
            "status": "In Betrieb",
            "max_power_kw": "22",
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
            "score": "-100.0",
        }
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _build_record(*, provider_uid: str, fetched_at: str, body: dict, archive_date: str) -> dict:
    body_text = json.dumps(body, ensure_ascii=False)
    return {
        "kind": "http_response",
        "provider_uid": provider_uid,
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
                provider_uid="qwello",
                fetched_at="2026-04-15T08:00:00+00:00",
                body=_dynamic_payload(status="AVAILABLE", timestamp="2026-04-15T08:00:00+00:00"),
                archive_date="2026-04-15",
            ),
        ),
        (
            "qwello/2026-04-15/20260415T091500000000Z-200-b.json",
            _build_record(
                provider_uid="qwello",
                fetched_at="2026-04-15T09:15:00+00:00",
                body=_dynamic_payload(status="OCCUPIED", timestamp="2026-04-15T09:15:00+00:00"),
                archive_date="2026-04-15",
            ),
        ),
        (
            "qwello/2026-04-15/20260415T103000000000Z-200-c.json",
            _build_record(
                provider_uid="qwello",
                fetched_at="2026-04-15T10:30:00+00:00",
                body=_dynamic_payload(status="FAULTED", timestamp="2026-04-15T10:30:00+00:00"),
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


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_iter_archive_members_streams_gzip_archive_without_member_index(tmp_path, monkeypatch):
    archive_path = tmp_path / "live-provider-responses-2026-04-15.tgz"
    _write_archive(archive_path)

    def fail_getmembers(self):
        raise AssertionError("gzip archive reader must not build a seek-heavy member index")

    monkeypatch.setattr(tarfile.TarFile, "getmembers", fail_getmembers)

    members = list(_iter_archive_members(archive_path))

    assert [name for name, _record in members] == [
        "qwello/2026-04-15/20260415T080000000000Z-200-a.json",
        "qwello/2026-04-15/20260415T091500000000Z-200-b.json",
        "qwello/2026-04-15/20260415T103000000000Z-200-c.json",
    ]
    assert [record["fetched_at"] for _name, record in members] == [
        "2026-04-15T08:00:00+00:00",
        "2026-04-15T09:15:00+00:00",
        "2026-04-15T10:30:00+00:00",
    ]


def test_iter_archive_members_streams_jsonl_archive_members(tmp_path, monkeypatch):
    archive_path = tmp_path / "live-provider-responses-2026-04-15.tgz"
    records = [
        _build_record(
            provider_uid="qwello",
            fetched_at="2026-04-15T08:00:00+00:00",
            body=_dynamic_payload(status="AVAILABLE", timestamp="2026-04-15T08:00:00+00:00"),
            archive_date="2026-04-15",
        ),
        _build_record(
            provider_uid="qwello",
            fetched_at="2026-04-15T09:15:00+00:00",
            body=_dynamic_payload(status="OCCUPIED", timestamp="2026-04-15T09:15:00+00:00"),
            archive_date="2026-04-15",
        ),
    ]
    payload = b"\n".join(json.dumps(record, ensure_ascii=False).encode("utf-8") for record in records) + b"\n"
    with tarfile.open(archive_path, "w:gz") as archive_handle:
        info = tarfile.TarInfo(name="qwello/2026-04-15/records.jsonl")
        info.size = len(payload)
        archive_handle.addfile(info, io.BytesIO(payload))

    def fail_getmembers(self):
        raise AssertionError("gzip archive reader must not build a seek-heavy member index")

    monkeypatch.setattr(tarfile.TarFile, "getmembers", fail_getmembers)

    members = list(_iter_archive_members(archive_path))

    assert [name for name, _record in members] == [
        "qwello/2026-04-15/records.jsonl#1",
        "qwello/2026-04-15/records.jsonl#2",
    ]
    assert [record["fetched_at"] for _name, record in members] == [
        "2026-04-15T08:00:00+00:00",
        "2026-04-15T09:15:00+00:00",
    ]


def test_run_analysis_builds_history_csvs_from_archives(tmp_path):
    provider_config_path = tmp_path / "provider_config.json"
    chargers_csv_path = tmp_path / "chargers.csv"
    site_match_path = tmp_path / "site_matches.csv"
    archive_path = tmp_path / "live-provider-responses-2026-04-15.tgz"
    output_dir = tmp_path / "analysis-output"

    _write_provider_config(provider_config_path)
    _write_chargers_csv(chargers_csv_path)
    _write_site_matches_csv(site_match_path)
    _write_archive(archive_path)

    config = AppConfig(
        provider_config_path=provider_config_path,
        chargers_csv_path=chargers_csv_path,
        site_match_path=site_match_path,
        provider_override_path=tmp_path / "missing-overrides.json",
        subscription_registry_path=tmp_path / "missing-subscriptions.json",
        archive_timezone_name="UTC",
    )
    result = run_analysis(
        archive_paths=[archive_path],
        output_dir=output_dir,
        config=config,
    )

    assert result["archive_count"] == 1
    assert result["archive_dates"] == ["2026-04-15"]
    assert result["message_row_count"] == 3
    assert result["observation_row_count"] == 6
    assert result["status_change_row_count"] == 4
    assert result["station_daily_row_count"] == 1
    assert result["provider_daily_row_count"] == 1

    provider_catalog_rows = _read_csv(output_dir / "provider_catalog.csv")
    assert provider_catalog_rows[0]["provider_uid"] == "qwello"
    assert provider_catalog_rows[0]["static_matched_station_count"] == "1"
    assert provider_catalog_rows[0]["static_matched_station_count_in_bundle"] == "1"

    observation_rows = _read_csv(output_dir / "evse_observations.csv")
    evse1_rows = [row for row in observation_rows if row["provider_evse_id"] == "DEQWEE1"]
    assert [row["availability_status"] for row in evse1_rows] == ["free", "occupied", "out_of_order"]

    status_change_rows = _read_csv(output_dir / "evse_status_changes.csv")
    evse1_changes = [row for row in status_change_rows if row["provider_evse_id"] == "DEQWEE1"]
    assert [row["availability_status"] for row in evse1_changes] == ["free", "occupied", "out_of_order"]
    assert [row["duration_seconds"] for row in evse1_changes] == ["4500", "4500", "48600"]
    assert evse1_changes[-1]["is_open_interval"] == "1"

    station_rows = _read_csv(output_dir / "station_daily_summary.csv")
    assert station_rows[0]["station_id"] == "station-1"
    assert station_rows[0]["evses_observed"] == "2"
    assert station_rows[0]["occupied_evses"] == "1"
    assert station_rows[0]["out_of_order_evses"] == "1"
    assert station_rows[0]["station_any_out_of_order"] == "1"
    assert station_rows[0]["station_all_evses_out_of_order"] == "0"

    provider_rows = _read_csv(output_dir / "provider_daily_summary.csv")
    assert provider_rows[0]["provider_uid"] == "qwello"
    assert provider_rows[0]["messages_total"] == "3"
    assert provider_rows[0]["parseable_messages_total"] == "3"
    assert provider_rows[0]["extracted_observation_count_total"] == "6"
    assert provider_rows[0]["extracted_mapped_observation_count_total"] == "6"
    assert provider_rows[0]["extracted_unmapped_observation_count_total"] == "0"
    assert provider_rows[0]["mapped_observation_ratio"] == "1.0"
    assert provider_rows[0]["competitive_analysis_eligible"] == "1"
    assert provider_rows[0]["competitive_analysis_tier"] == "eligible"
    assert provider_rows[0]["competitive_analysis_reason"] == "ratio_ge_0_5"
    assert provider_rows[0]["evses_observed"] == "2"
    assert provider_rows[0]["mapped_stations_observed"] == "1"
    assert provider_rows[0]["mapped_stations_observed_in_bundle"] == "1"
    assert provider_rows[0]["out_of_order_evses_end_of_day"] == "1"
    assert provider_rows[0]["dynamic_station_coverage_ratio"] == "1.0"
    assert provider_rows[0]["dynamic_station_coverage_ratio_in_bundle"] == "1.0"


def test_competitive_analysis_status_uses_conservative_thresholds():
    assert _competitive_analysis_status(
        messages_total=12,
        parseable_messages_total=12,
        static_matched_station_count=5,
        mapped_observation_ratio=0.7,
    ) == {
        "competitive_analysis_eligible": 1,
        "competitive_analysis_tier": "eligible",
        "competitive_analysis_reason": "ratio_ge_0_5",
    }
    assert _competitive_analysis_status(
        messages_total=12,
        parseable_messages_total=12,
        static_matched_station_count=5,
        mapped_observation_ratio=0.3,
    ) == {
        "competitive_analysis_eligible": 0,
        "competitive_analysis_tier": "review",
        "competitive_analysis_reason": "ratio_ge_0_2",
    }
    assert _competitive_analysis_status(
        messages_total=12,
        parseable_messages_total=12,
        static_matched_station_count=5,
        mapped_observation_ratio=0.1,
    ) == {
        "competitive_analysis_eligible": 0,
        "competitive_analysis_tier": "exclude",
        "competitive_analysis_reason": "ratio_lt_0_2",
    }
    assert _competitive_analysis_status(
        messages_total=12,
        parseable_messages_total=0,
        static_matched_station_count=5,
        mapped_observation_ratio=0.8,
    ) == {
        "competitive_analysis_eligible": 0,
        "competitive_analysis_tier": "exclude",
        "competitive_analysis_reason": "no_parseable_messages",
    }
    assert _competitive_analysis_status(
        messages_total=12,
        parseable_messages_total=12,
        static_matched_station_count=0,
        mapped_observation_ratio=0.8,
    ) == {
        "competitive_analysis_eligible": 0,
        "competitive_analysis_tier": "exclude",
        "competitive_analysis_reason": "no_static_matches",
    }
