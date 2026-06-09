from __future__ import annotations

import gzip
import io
import json

from commercial_backend.ch_ladestationen import (
    iter_static_rows_from_binary_stream,
    iter_status_rows_from_binary_stream,
    parse_static_payload,
    parse_status_payload,
)
from scripts.probe_ch_ladestationen import _build_summary, _probe_alerts


def test_parses_swiss_static_oicp_payload_and_join_key():
    payload = {
        "EVSEData": [
            {
                "OperatorID": "CH*CCI",
                "OperatorName": "SIG",
                "EVSEDataRecord": [
                    {
                        "Accessibility": "Paying publicly accessible",
                        "AccessibilityLocation": "OnStreet",
                        "Address": {
                            "City": "Meyrin",
                            "Country": "CHE",
                            "ParkingFacility": "PublicParking",
                            "PostalCode": "1217",
                            "Region": "GE",
                            "Street": "SIG CERN Esplanade des Particules",
                        },
                        "AuthenticationModes": ["NFC RFID Classic", "Direct Payment", "REMOTE"],
                        "ChargingFacilities": [{"Amperage": "32", "Voltage": "230", "power": "22.0", "powertype": "AC_3_PHASE"}],
                        "ChargingStationId": "CH*CCI*E22078",
                        "ChargingStationNames": [{"lang": "de", "value": "SIG CERN"}],
                        "DynamicInfoAvailable": "true",
                        "EvseID": "CH*CCI*E22078",
                        "GeoCoordinates": {"Google": "46.23432 6.055602"},
                        "HotlinePhoneNumber": "+41800292929",
                        "IsOpen24Hours": True,
                        "Plugs": ["Type 2 Outlet"],
                        "RenewableEnergy": False,
                        "ValueAddedServices": ["Reservation"],
                    }
                ],
            }
        ]
    }

    rows = parse_static_payload(payload)
    row = rows[0]

    assert row["country_code"] == "CH"
    assert row["source_uid"] == "ch_bfe_ladestationen"
    assert row["operator_id"] == "CH*CCI"
    assert row["operator_name"] == "SIG"
    assert row["charger_id"] == "ch:oicp:CH*CCI*E22078"
    assert row["station_id"] == "ch:station:CH*CCI*E22078"
    assert row["source_evse_id"] == "CH*CCI*E22078"
    assert row["source_station_id"] == "CH*CCI*E22078"
    assert row["dynamic_info_available"] == "true"
    assert row["latitude"] == 46.23432
    assert row["longitude"] == 6.055602
    assert row["city"] == "Meyrin"
    assert row["postal_code"] == "1217"
    assert row["street"] == "SIG CERN Esplanade des Particules"
    assert row["address_country"] == "CHE"
    assert row["address_region"] == "GE"
    assert row["parking_facility"] == "PublicParking"
    assert row["accessibility"] == "Paying publicly accessible"
    assert row["accessibility_location"] == "OnStreet"
    assert row["is_open_24_hours"] is True
    assert row["plugs"] == "Type 2 Outlet"
    assert row["power_types"] == "AC_3_PHASE"
    assert row["max_power_kw"] == 22.0
    assert row["authentication_modes"] == "NFC RFID Classic|Direct Payment|REMOTE"
    assert row["renewable_energy"] is False
    assert row["hotline_phone_number"] == "+41800292929"
    assert row["charging_station_names"] == "SIG CERN"
    assert row["value_added_services"] == "Reservation"


def test_parses_swiss_status_payload_and_keeps_same_join_key():
    payload = {
        "EVSEStatuses": [
            {
                "OperatorID": "CH*CCI",
                "OperatorName": "SIG",
                "EVSEStatusRecord": [
                    {"EvseID": "CH*CCI*E22078", "EVSEStatus": "Available"},
                    {"EvseID": "CH*CCI*E22081", "EVSEStatus": "Occupied"},
                    {"EvseID": "CH*CCI*E22082", "EVSEStatus": "Unknown"},
                    {"EvseID": "CH*CCI*E22079", "EVSEStatus": "OutOfService"},
                    {"EvseID": "CH*CCI*E22080", "EVSEStatus": "Reserved"},
                ],
            }
        ]
    }

    rows = parse_status_payload(payload)

    assert [row["charger_id"] for row in rows] == [
        "ch:oicp:CH*CCI*E22078",
        "ch:oicp:CH*CCI*E22081",
        "ch:oicp:CH*CCI*E22082",
        "ch:oicp:CH*CCI*E22079",
        "ch:oicp:CH*CCI*E22080",
    ]
    assert [row["availability_status"] for row in rows] == [
        "free",
        "occupied",
        "unknown",
        "out_of_order",
        "reserved",
    ]


def test_streams_swiss_static_and_status_payloads_from_gzip_bytes():
    static_payload = {
        "EVSEData": [
            {
                "OperatorID": "CH*CCI",
                "OperatorName": "SIG",
                "EVSEDataRecord": [
                    {
                        "Address": {"City": "Meyrin"},
                        "ChargingStationId": "CH*CCI*E22078",
                        "EvseID": "CH*CCI*E22078",
                    }
                ],
            }
        ]
    }
    status_payload = {
        "EVSEStatuses": [
            {
                "OperatorID": "CH*CCI",
                "OperatorName": "SIG",
                "EVSEStatusRecord": [{"EvseID": "CH*CCI*E22078", "EVSEStatus": "Available"}],
            }
        ]
    }

    static_rows = list(
        iter_static_rows_from_binary_stream(
            io.BufferedReader(io.BytesIO(gzip.compress(json.dumps(static_payload).encode("utf-8"))))
        )
    )
    status_rows = list(
        iter_status_rows_from_binary_stream(
            io.BufferedReader(io.BytesIO(gzip.compress(json.dumps(status_payload).encode("utf-8"))))
        )
    )

    assert static_rows[0]["station_id"] == "ch:station:CH*CCI*E22078"
    assert static_rows[0]["operator_name"] == "SIG"
    assert status_rows[0]["availability_status"] == "free"
    assert status_rows[0]["operator_id"] == "CH*CCI"


def test_probe_summary_alerts_on_record_drops_and_join_gaps():
    static_payload = {
        "EVSEData": [
            {
                "OperatorID": "CH*CCI",
                "OperatorName": "SIG",
                "EVSEDataRecord": [
                    {"ChargingStationId": "CH*CCI*E22078", "EvseID": "CH*CCI*E22078"},
                    {"ChargingStationId": "CH*CCI*E22079", "EvseID": "CH*CCI*E22079"},
                ],
            }
        ]
    }
    status_payload = {
        "EVSEStatuses": [
            {
                "OperatorID": "CH*CCI",
                "OperatorName": "SIG",
                "EVSEStatusRecord": [
                    {"EvseID": "CH*CCI*E22078", "EVSEStatus": "Available"},
                    {"EvseID": "CH*CCI*E99999", "EVSEStatus": "Unknown"},
                ],
            }
        ]
    }

    summary = _build_summary(static_payload, status_payload)
    alerts = _probe_alerts(
        summary,
        min_static_records=3,
        min_status_records=3,
        max_static_without_status=0,
        max_status_without_static=0,
    )

    assert summary["static_without_status_count"] == 1
    assert summary["status_without_static_count"] == 1
    assert alerts == [
        "static_record_count_below_threshold:2<3",
        "status_record_count_below_threshold:2<3",
        "static_without_status_above_threshold:1>0",
        "status_without_static_above_threshold:1>0",
    ]
