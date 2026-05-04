from __future__ import annotations

import csv
import json
from datetime import date
from pathlib import Path

from analysis.management_snapshot import (
    SNAPSHOT_PROVIDER_LIMIT,
    SNAPSHOT_TOP_LIMIT,
    _public_snapshot_payload,
    build_management_snapshot_from_analysis_outputs,
    rebuild_management_indexes,
)
from backend.config import AppConfig


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_station_csv(path: Path, rows: list[dict[str, object]]) -> None:
    _write_csv(
        path,
        [
            "station_id",
            "operator",
            "address",
            "postcode",
            "city",
            "lat",
            "lon",
            "charging_points_count",
            "max_power_kw",
        ],
        rows,
    )


def test_build_management_snapshot_from_analysis_outputs_derives_station_rankings(tmp_path: Path):
    analysis_output_dir = tmp_path / "analysis-output"
    output_root = tmp_path / "management"
    bundle_csv_path = tmp_path / "chargers_fast.csv"
    full_csv_path = tmp_path / "chargers_full.csv"
    _write_station_csv(
        bundle_csv_path,
        [
            {
                "station_id": "station-1",
                "operator": "Bundle Operator",
                "address": "Example Street 1",
                "postcode": "10115",
                "city": "Berlin",
                "lat": "52.5",
                "lon": "13.4",
                "charging_points_count": "4",
                "max_power_kw": "300",
            },
            {
                "station_id": "station-2",
                "operator": "Second Operator",
                "address": "Example Street 2",
                "postcode": "80331",
                "city": "München",
                "lat": "48.1",
                "lon": "11.5",
                "charging_points_count": "2",
                "max_power_kw": "150",
            },
        ],
    )
    _write_station_csv(
        full_csv_path,
        [
            {
                "station_id": "station-1",
                "operator": "Bundle Operator",
                "address": "Example Street 1",
                "postcode": "10115",
                "city": "Berlin",
                "lat": "52.5",
                "lon": "13.4",
                "charging_points_count": "4",
                "max_power_kw": "300",
            },
            {
                "station_id": "station-2",
                "operator": "Second Operator",
                "address": "Example Street 2",
                "postcode": "80331",
                "city": "München",
                "lat": "48.1",
                "lon": "11.5",
                "charging_points_count": "2",
                "max_power_kw": "150",
            },
            {
                "station_id": "station-3",
                "operator": "Third Operator",
                "address": "Example Street 3",
                "postcode": "20095",
                "city": "Hamburg",
                "lat": "53.5",
                "lon": "10.0",
                "charging_points_count": "1",
                "max_power_kw": "50",
            },
        ],
    )

    _write_csv(
        analysis_output_dir / "station_daily_summary.csv",
        [
            "archive_date",
            "provider_uid",
            "station_id",
            "station_operator",
            "station_city",
            "station_catalog_charging_points_count",
            "evses_observed",
            "free_evses",
            "occupied_evses",
            "out_of_order_evses",
            "unknown_evses",
            "station_availability_status",
            "station_any_out_of_order",
            "station_all_evses_out_of_order",
            "station_coverage_vs_catalog",
            "latest_event_timestamp",
        ],
        [
            {
                "archive_date": "2026-04-17",
                "provider_uid": "provider-a",
                "station_id": "station-1",
                "station_operator": "Bundle Operator",
                "station_city": "Berlin",
                "station_catalog_charging_points_count": "4",
                "evses_observed": "4",
                "free_evses": "1",
                "occupied_evses": "1",
                "out_of_order_evses": "2",
                "unknown_evses": "0",
                "station_availability_status": "out_of_order",
                "station_any_out_of_order": "1",
                "station_all_evses_out_of_order": "0",
                "station_coverage_vs_catalog": "1.0",
                "latest_event_timestamp": "2026-04-17T23:00:00+00:00",
            },
            {
                "archive_date": "2026-04-17",
                "provider_uid": "provider-b",
                "station_id": "station-1",
                "station_operator": "Bundle Operator",
                "station_city": "Berlin",
                "station_catalog_charging_points_count": "4",
                "evses_observed": "2",
                "free_evses": "2",
                "occupied_evses": "0",
                "out_of_order_evses": "0",
                "unknown_evses": "0",
                "station_availability_status": "free",
                "station_any_out_of_order": "0",
                "station_all_evses_out_of_order": "0",
                "station_coverage_vs_catalog": "0.5",
                "latest_event_timestamp": "2026-04-17T23:05:00+00:00",
            },
            {
                "archive_date": "2026-04-17",
                "provider_uid": "provider-a",
                "station_id": "station-2",
                "station_operator": "Second Operator",
                "station_city": "München",
                "station_catalog_charging_points_count": "2",
                "evses_observed": "2",
                "free_evses": "1",
                "occupied_evses": "1",
                "out_of_order_evses": "0",
                "unknown_evses": "0",
                "station_availability_status": "occupied",
                "station_any_out_of_order": "0",
                "station_all_evses_out_of_order": "0",
                "station_coverage_vs_catalog": "1.0",
                "latest_event_timestamp": "2026-04-17T22:00:00+00:00",
            },
            {
                "archive_date": "2026-04-17",
                "provider_uid": "provider-a",
                "station_id": "station-3",
                "station_operator": "Third Operator",
                "station_city": "Hamburg",
                "station_catalog_charging_points_count": "1",
                "evses_observed": "1",
                "free_evses": "0",
                "occupied_evses": "0",
                "out_of_order_evses": "1",
                "unknown_evses": "0",
                "station_availability_status": "out_of_order",
                "station_any_out_of_order": "1",
                "station_all_evses_out_of_order": "1",
                "station_coverage_vs_catalog": "1.0",
                "latest_event_timestamp": "2026-04-17T21:00:00+00:00",
            },
        ],
    )
    _write_csv(
        analysis_output_dir / "evse_status_changes.csv",
        [
            "provider_uid",
            "provider_evse_id",
            "station_id",
            "archive_date",
            "change_rank",
            "duration_seconds",
            "availability_status",
        ],
        [
            {
                "provider_uid": "provider-a",
                "provider_evse_id": "EVSE-1",
                "station_id": "station-1",
                "archive_date": "2026-04-17",
                "change_rank": "1",
                "duration_seconds": "300",
                "availability_status": "free",
            },
            {
                "provider_uid": "provider-a",
                "provider_evse_id": "EVSE-1",
                "station_id": "station-1",
                "archive_date": "2026-04-17",
                "change_rank": "2",
                "duration_seconds": "300",
                "availability_status": "occupied",
            },
            {
                "provider_uid": "provider-a",
                "provider_evse_id": "EVSE-1",
                "station_id": "station-1",
                "archive_date": "2026-04-17",
                "change_rank": "3",
                "duration_seconds": "300",
                "availability_status": "free",
            },
            {
                "provider_uid": "provider-a",
                "provider_evse_id": "EVSE-2",
                "station_id": "station-1",
                "archive_date": "2026-04-17",
                "change_rank": "1",
                "duration_seconds": "1200",
                "availability_status": "out_of_order",
            },
            {
                "provider_uid": "provider-a",
                "provider_evse_id": "EVSE-3",
                "station_id": "station-1",
                "archive_date": "2026-04-17",
                "change_rank": "1",
                "duration_seconds": "600",
                "availability_status": "out_of_order",
            },
            {
                "provider_uid": "provider-b",
                "provider_evse_id": "EVSE-IGNORED",
                "station_id": "station-1",
                "archive_date": "2026-04-17",
                "change_rank": "1",
                "duration_seconds": "9999",
                "availability_status": "out_of_order",
            },
            {
                "provider_uid": "provider-a",
                "provider_evse_id": "EVSE-4",
                "station_id": "station-2",
                "archive_date": "2026-04-17",
                "change_rank": "1",
                "duration_seconds": "300",
                "availability_status": "free",
            },
            {
                "provider_uid": "provider-a",
                "provider_evse_id": "EVSE-4",
                "station_id": "station-2",
                "archive_date": "2026-04-17",
                "change_rank": "2",
                "duration_seconds": "300",
                "availability_status": "occupied",
            },
            {
                "provider_uid": "provider-a",
                "provider_evse_id": "EVSE-5",
                "station_id": "station-3",
                "archive_date": "2026-04-17",
                "change_rank": "1",
                "duration_seconds": "1800",
                "availability_status": "out_of_order",
            },
        ],
    )
    _write_csv(
        analysis_output_dir / "provider_daily_summary.csv",
        [
            "archive_date",
            "provider_uid",
            "display_name",
            "publisher",
            "messages_total",
            "parseable_messages_total",
            "extracted_observation_count_total",
            "extracted_mapped_observation_count_total",
            "mapped_observation_ratio",
            "evses_observed",
            "mapped_evses_observed",
            "mapped_stations_observed",
            "mapped_stations_observed_in_bundle",
            "out_of_order_evses_end_of_day",
            "stations_all_evses_out_of_order",
            "dynamic_station_coverage_ratio",
            "dynamic_station_coverage_ratio_in_bundle",
            "competitive_analysis_tier",
            "latest_event_timestamp",
        ],
        [
            {
                "archive_date": "2026-04-17",
                "provider_uid": "provider-a",
                "display_name": "Provider A",
                "publisher": "Publisher A",
                "messages_total": "12",
                "parseable_messages_total": "11",
                "extracted_observation_count_total": "120",
                "extracted_mapped_observation_count_total": "100",
                "mapped_observation_ratio": "0.833333",
                "evses_observed": "7",
                "mapped_evses_observed": "7",
                "mapped_stations_observed": "3",
                "mapped_stations_observed_in_bundle": "2",
                "out_of_order_evses_end_of_day": "3",
                "stations_all_evses_out_of_order": "1",
                "dynamic_station_coverage_ratio": "1.0",
                "dynamic_station_coverage_ratio_in_bundle": "1.0",
                "competitive_analysis_tier": "eligible",
                "latest_event_timestamp": "2026-04-17T23:00:00+00:00",
            },
            {
                "archive_date": "2026-04-17",
                "provider_uid": "provider-b",
                "display_name": "Provider B",
                "publisher": "Publisher B",
                "messages_total": "3",
                "parseable_messages_total": "3",
                "extracted_observation_count_total": "20",
                "extracted_mapped_observation_count_total": "10",
                "mapped_observation_ratio": "0.5",
                "evses_observed": "2",
                "mapped_evses_observed": "2",
                "mapped_stations_observed": "1",
                "mapped_stations_observed_in_bundle": "1",
                "out_of_order_evses_end_of_day": "0",
                "stations_all_evses_out_of_order": "0",
                "dynamic_station_coverage_ratio": "0.5",
                "dynamic_station_coverage_ratio_in_bundle": "0.5",
                "competitive_analysis_tier": "review",
                "latest_event_timestamp": "2026-04-17T23:05:00+00:00",
            },
        ],
    )
    _write_csv(
        analysis_output_dir / "archive_messages.csv",
        [
            "archive_date",
            "provider_uid",
            "record_kind",
            "message_timestamp",
            "http_status",
            "payload_byte_length",
        ],
        [
            *[
                {
                    "archive_date": "2026-04-17",
                    "provider_uid": "provider-a",
                    "record_kind": "push_request",
                    "message_timestamp": f"2026-04-17T12:{index:02d}:00+00:00",
                    "http_status": "",
                    "payload_byte_length": "100",
                }
                for index in range(10)
            ],
            {
                "archive_date": "2026-04-17",
                "provider_uid": "provider-a",
                "record_kind": "http_response",
                "message_timestamp": "2026-04-17T13:00:00+00:00",
                "http_status": "200",
                "payload_byte_length": "250",
            },
            {
                "archive_date": "2026-04-17",
                "provider_uid": "provider-a",
                "record_kind": "fetch_failure",
                "message_timestamp": "2026-04-17T13:05:00+00:00",
                "http_status": "",
                "payload_byte_length": "0",
            },
            *[
                {
                    "archive_date": "2026-04-17",
                    "provider_uid": "provider-b",
                    "record_kind": "push_request",
                    "message_timestamp": f"2026-04-17T10:{index:02d}:00+00:00",
                    "http_status": "",
                    "payload_byte_length": "50",
                }
                for index in range(3)
            ],
        ],
    )

    result = build_management_snapshot_from_analysis_outputs(
        target_date=date(2026, 4, 17),
        analysis_output_dir=analysis_output_dir,
        output_root=output_root,
        config=AppConfig(chargers_csv_path=bundle_csv_path, full_chargers_csv_path=full_csv_path),
        analysis_result={
            "message_row_count": 12,
            "observation_row_count": 120,
        },
    )

    assert result["summary"]["afir_stations_observed"] == 3
    assert result["summary"]["bundle_stations_observed"] == 2
    assert result["summary"]["busy_transition_count"] == 3
    assert result["summary"]["high_utilization_stations"] == 2
    assert result["summary"]["stations_with_disruptions"] == 2
    assert result["summary"]["disruptions_at_end_of_day"] == 2
    assert result["summary"]["current_out_of_order_evses"] == 3
    assert result["summary"]["archive_messages_total"] == 12
    assert result["summary"]["out_of_order_duration_seconds_total"] == 3600
    assert result["broken_stations"][0]["station_id"] == "station-3"
    assert result["broken_stations"][0]["status_label"] == "Komplett gestört"
    assert result["broken_stations"][0]["station_url"] == ""
    assert result["broken_stations"][1]["station_id"] == "station-1"
    assert result["broken_stations"][1]["affected_charger_count"] == 2
    assert result["broken_stations"][1]["current_broken_charger_count"] == 2
    assert result["broken_stations"][1]["status_label"] == "Derzeit eingeschränkt"
    assert result["busiest_stations"][0]["station_id"] == "station-1"
    assert result["busiest_stations"][0]["busy_transition_count"] == 2
    assert result["provider_reports"][0]["provider_uid"] == "provider-a"
    assert result["provider_reports"][0]["messages_total"] == 12
    assert result["provider_reports"][0]["push_messages_total"] == 10
    assert result["provider_reports"][0]["http_response_messages_total"] == 1
    assert result["provider_reports"][0]["fetch_failure_messages_total"] == 1
    assert result["provider_reports"][0]["payload_byte_length_total"] == 1250
    assert result["provider_reports"][0]["mapped_observation_ratio"] == 0.833333
    assert result["provider_reports"][1]["provider_uid"] == "provider-b"
    snapshot_path = output_root / "days" / "2026" / "04" / "17" / "snapshot.json"
    assert snapshot_path.exists()


def test_rebuild_management_indexes_builds_available_dates_and_summary_series(tmp_path: Path):
    output_root = tmp_path / "management"
    (output_root / "days" / "2026" / "04" / "16").mkdir(parents=True)
    (output_root / "days" / "2026" / "04" / "17").mkdir(parents=True)

    for snapshot_date, busy_count in [("2026-04-16", 44), ("2026-04-17", 52)]:
        snapshot_path = output_root / "days" / snapshot_date[:4] / snapshot_date[5:7] / snapshot_date[8:10] / "snapshot.json"
        snapshot_path.write_text(
            json.dumps(
                {
                    "snapshot_date": snapshot_date,
                    "summary": {
                        "afir_stations_observed": 100,
                        "stations_with_disruptions": 8,
                        "disruptions_at_end_of_day": 5,
                        "high_utilization_stations": busy_count,
                        "archive_messages_total": 200,
                    },
                    "busiest_stations": [],
                    "broken_stations": [],
                    "provider_reports": [{"provider_uid": "provider-a"}],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    result = rebuild_management_indexes(output_root)

    assert result["index"]["latest_date"] == "2026-04-17"
    assert result["index"]["available_dates"] == ["2026-04-16", "2026-04-17"]
    assert result["trends"]["summary_series"][0]["snapshot_date"] == "2026-04-16"
    assert [row["high_utilization_stations"] for row in result["trends"]["summary_series"]] == [44, 52]
    assert (output_root / "index.json").exists()
    assert (output_root / "trends.json").exists()


def test_public_snapshot_payload_trims_rankings_and_drops_extra_fields():
    payload = {
        "snapshot_date": "2026-04-17",
        "generated_at": "2026-04-18T00:00:00+00:00",
        "source": {"archive_name": "live-provider-responses-2026-04-17.tgz"},
        "summary": {"afir_stations_observed": 14032},
        "busiest_stations": [{"station_id": f"busy-{index}"} for index in range(SNAPSHOT_TOP_LIMIT + 3)],
        "broken_stations": [{"station_id": f"broken-{index}"} for index in range(SNAPSHOT_TOP_LIMIT + 5)],
        "provider_reports": [{"provider_uid": f"provider-{index}"} for index in range(SNAPSHOT_PROVIDER_LIMIT + 2)],
        "provider_rows": [{"provider_uid": "legacy-private"}],
    }

    result = _public_snapshot_payload(payload)

    assert len(result["busiest_stations"]) == SNAPSHOT_TOP_LIMIT
    assert len(result["broken_stations"]) == SNAPSHOT_TOP_LIMIT
    assert len(result["provider_reports"]) == SNAPSHOT_PROVIDER_LIMIT
    assert result["provider_reports"][0]["provider_uid"] == "provider-0"
    assert "provider_rows" not in result
