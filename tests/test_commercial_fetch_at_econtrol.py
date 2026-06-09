from __future__ import annotations

import base64
from dataclasses import replace

from commercial_backend.config import AppConfig
from commercial_backend.at_econtrol import iter_api_search_station_rows, iter_datex_status_rows, iter_datex_table_station_rows
from scripts import commercial_fetch_at_econtrol as fetch_at


def test_at_fetch_defaults_to_open_metadata_sources():
    specs = fetch_at._selected_sources("all-open")

    assert [spec.key for spec in specs] == ["metadata", "technical-info"]
    assert [spec.source_uid for spec in specs] == [
        "at_econtrol_mobilitydata_dataset_page",
        "at_econtrol_technical_info_page",
    ]
    assert all(spec.task_kind == "archive_only_payload" for spec in specs)


def test_at_api_search_requires_explicit_api_key():
    spec = fetch_at.SOURCES["api-search"]

    assert spec.requires_api_key is True
    assert spec.requires_basic_auth is True
    assert spec.task_kind == "parse_dynamic_payload"
    assert fetch_at._api_key_from_env({"AT_ECONTROL_API_KEY": "key"}) == "key"


def test_at_api_docs_is_credentialed_archive_source():
    spec = fetch_at.SOURCES["api-docs"]

    assert spec.source_uid == "at_econtrol_public_api_docs"
    assert spec.requires_api_key is True
    assert spec.requires_basic_auth is True
    assert spec.task_kind == "archive_only_payload"


def test_at_credentialed_sources_use_datex_table_and_status():
    specs = fetch_at._selected_sources("all-credentialed")

    assert [spec.key for spec in specs] == ["api-docs", "datex-table", "datex-status"]
    assert [spec.source_uid for spec in specs] == [
        "at_econtrol_public_api_docs",
        "at_econtrol_public_api_datex_table",
        "at_econtrol_public_api_datex_status",
    ]
    assert [spec.task_kind for spec in specs] == [
        "archive_only_payload",
        "parse_static_payload",
        "parse_dynamic_payload",
    ]


def test_at_request_headers_use_woladen_referer_and_basic_auth():
    spec = fetch_at.SOURCES["api-search"]

    headers = fetch_at._request_headers(
        spec,
        {
            "AT_ECONTROL_API_KEY": "key",
            "AT_ECONTROL_REFERER": "https://woladen.de",
            "AT_LADESTELLEN_USER": "woladen.de",
            "AT_LADESTELLEN_PASSWORD": "password",
        },
    )

    assert headers["Apikey"] == "key"
    assert headers["Referer"] == "https://woladen.de"
    scheme, token = headers["Authorization"].split(" ", 1)
    assert scheme == "Basic"
    assert base64.b64decode(token).decode("utf-8") == "woladen.de:password"


def test_at_api_search_can_read_key_from_secret_descriptor(tmp_path):
    key_path = tmp_path / "at_econtrol_api_key.txt"
    descriptor_path = tmp_path / "at_nap_credentials.json"
    key_path.write_text("descriptor-key\n", encoding="utf-8")
    descriptor_path.write_text(
        '{"api_key_file": "' + str(key_path) + '", "referer_file": ""}',
        encoding="utf-8",
    )

    assert (
        fetch_at._api_key_from_env({"AT_ECONTROL_SECRET_FILE": str(descriptor_path)})
        == "descriptor-key"
    )


def test_at_api_search_parser_maps_cent_price_fields():
    rows = list(
        iter_api_search_station_rows(
            [
                {
                    "stationId": "EAT0303196",
                    "operatorName": "SMATRICS",
                    "phoneCountryCode": "+43",
                    "regionCode": "0",
                    "phoneNumber": "5031351855",
                    "location": {"lat": 48.0, "lon": 16.0},
                    "points": [
                        {
                            "evseId": "AT*SMA*EAT0303196*001AC",
                            "capacityKw": 43.6,
                            "freeOfCharge": False,
                            "priceCentKwh": 65,
                            "priceCentMin": 0,
                            "blockingFeeCentMin": 15,
                            "blockingFeeFromMinute": 301,
                            "startFeeCent": 0,
                        }
                    ],
                }
            ]
        )
    )

    assert rows[0]["price_display"] == "ab 0,65 €/kWh"
    assert rows[0]["price_energy_eur_kwh_min"] == "0.65"
    assert rows[0]["price_energy_eur_kwh_max"] == "0.65"
    assert rows[0]["price_time_eur_min_min"] == 0.15
    assert rows[0]["price_complex"] is True
    assert rows[0]["helpdesk_phone"] == "+435031351855"


def test_at_datex_table_and_status_parsers_use_all_country_ids():
    table_payload = {
        "energyInfrastructureTable": [
            {
                "energyInfrastructureSite": [
                    {
                        "id": "AT-006-EPI001",
                        "name": {"values": {"value": [{"value": "Site Name", "lang": "de"}]}},
                        "operator": {"name": {"values": {"value": [{"value": "Operator", "lang": "de"}]}}},
                        "locationReference": {
                            "_LocationReferenceExtension": {
                                "facilityLocation": {
                                    "address": {
                                        "postcode": "1010",
                                        "city": {"values": {"value": [{"value": "Wien", "lang": "de"}]}},
                                    },
                                    "addressLine": [
                                        {
                                            "order": 1,
                                            "text": {"values": {"value": [{"value": "Ring", "lang": "de"}]}},
                                        },
                                        {
                                            "order": 2,
                                            "text": {"values": {"value": [{"value": "1", "lang": "de"}]}},
                                        },
                                    ],
                                }
                            },
                            "coordinatesForDisplay": {"latitude": 48.2, "longitude": 16.3},
                        },
                        "energyInfrastructureStation": [
                            {
                                "id": "AT-006-EPI001-AT*006*E01",
                                "locationReference": {
                                    "coordinatesForDisplay": {"latitude": 48.21, "longitude": 16.31}
                                },
                                "refillPoint": [
                                    {
                                        "id": "AT-006-EPI001-AT*006*E01-rp",
                                        "deliveryUnit": {"value": "K_WH"},
                                        "connector": [
                                            {
                                                "connectorType": {"value": "IEC_62196_T_2"},
                                                "maxPowerAtSocket": 22000,
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        ]
    }
    status_payload = {
        "publicationTime": 1777813903551,
        "energyInfrastructureSiteStatus": [
            {
                "reference": {"id": "AT-006-EPI001"},
                "energyInfrastructureStationStatus": [
                    {
                        "reference": {"id": "AT-006-EPI001-AT*006*E01"},
                        "refillPointStatus": [
                            {
                                "reference": {"id": "AT-006-EPI001-AT*006*E01-rp"},
                                "status": {"value": "AVAILABLE"},
                                "newRates": {
                                    "applicableCurrency": [],
                                    "energyPricingPolicy": {
                                        "pricingPolicy": [
                                            {"value": "PRICE_PER_DELIVERY_UNIT"},
                                            {"value": "PRICE_PER_CHARGING_TIME"},
                                        ]
                                    },
                                    "rateLineCollection": [
                                        {
                                            "applicableCurrency": "Euro",
                                            "rateLine": [
                                                {"rateLineType": {"value": "PER_UNIT"}, "value": 0.35}
                                            ],
                                        },
                                        {
                                            "applicableCurrency": "Euro",
                                            "rateLine": [
                                                {
                                                    "rateLineType": {"value": "INCREMENTING_RATE"},
                                                    "incrementPeriod": "PT1M",
                                                    "value": 0.02,
                                                }
                                            ],
                                        },
                                    ],
                                },
                            }
                        ],
                    }
                ],
            }
        ],
    }

    static_rows = list(iter_datex_table_station_rows(table_payload))
    status_rows = list(iter_datex_status_rows(status_payload))

    assert static_rows[0]["source_uid"] == "at_econtrol_public_api_datex_table"
    assert static_rows[0]["station_id"] == "at:econtrol:at-006-epi001"
    assert static_rows[0]["source_evse_id"] == "AT*006*E01"
    assert static_rows[0]["connector_types"] == "IEC_62196_T_2"
    assert static_rows[0]["max_power_kw"] == 22.0
    assert status_rows[0]["source_uid"] == "at_econtrol_public_api_datex_status"
    assert status_rows[0]["station_id"] == "at:econtrol:at-006-epi001"
    assert status_rows[0]["source_evse_id"] == "AT*006*E01"
    assert status_rows[0]["availability_status"] == "free"
    assert status_rows[0]["price_display"] == "ab 0,35 €/kWh"
    assert status_rows[0]["price_time_eur_min_min"] == 0.02


def test_at_truncated_json_is_quarantined_before_queueing(tmp_path):
    config = replace(AppConfig(), raw_payload_dir=tmp_path / "raw")
    temp_path = config.raw_payload_dir / "_incoming" / "broken.json"
    temp_path.parent.mkdir(parents=True)
    payload = b'{"energyInfrastructureSiteStatus": ["unterminated'
    temp_path.write_bytes(payload)

    try:
        fetch_at._validate_json_container_shape(
            config=config,
            spec=fetch_at.SOURCES["datex-status"],
            temp_path=temp_path,
            payload_sha256="b" * 64,
            byte_length=len(payload),
        )
    except RuntimeError as exc:
        assert "invalid_at_econtrol_payload:datex-status" in str(exc)
    else:
        raise AssertionError("truncated AT JSON should fail validation")

    assert not temp_path.exists()
    invalid_files = list((config.raw_payload_dir / "_invalid").rglob("*.json"))
    assert len(invalid_files) == 1
    manifest_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (config.raw_payload_dir / "_invalid").rglob("invalid_payloads.ndjson")
    )
    assert "json_payload_truncated_or_unexpected_end" in manifest_text
