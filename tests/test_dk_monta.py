from __future__ import annotations

import io
import json

from commercial_backend.dk_monta import (
    BE_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
    BE_MONTA_AFIR_EVSE_STATUS_SOURCE_UID,
    BE_MONTA_PROVIDER_UID,
    MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
    MONTA_AFIR_EVSE_STATUS_SOURCE_UID,
    count_table_payload,
    extract_evse_ids_from_table_payload,
    iter_static_rows_from_binary_stream,
    iter_status_rows_from_binary_stream,
    parse_static_payload,
    parse_status_payload,
)


def _table_payload() -> dict:
    return {
        "publicationTime": "2026-06-08T10:00:00Z",
        "energyInfrastructureTable": [
            {
                "idG": "table-1",
                "energyInfrastructureSite": [
                    {
                        "idG": "site-1",
                        "name": {"values": [{"lang": "da", "value": "Monta Test Site"}]},
                        "operator": {
                            "afacAnOrganisation": {
                                "name": {"values": [{"lang": "en", "value": "Monta Operator"}]}
                            }
                        },
                        "owner": {
                            "afacAnOrganisation": {
                                "name": {"values": [{"lang": "en", "value": "Monta Owner"}]}
                            }
                        },
                        "locationReference": {
                            "locPointLocation": {
                                "pointByCoordinates": {
                                    "pointCoordinates": {"latitude": 55.6761, "longitude": 12.5683}
                                },
                                "locLocationExtensionG": {
                                    "FacilityLocation": {
                                        "address": {
                                            "postcode": "2100",
                                            "city": {"values": [{"lang": "da", "value": "Kobenhavn"}]},
                                            "countryCode": "DK",
                                            "addressLine": [
                                                {
                                                    "order": 1,
                                                    "type": {"value": "streetName"},
                                                    "text": {"values": [{"lang": "da", "value": "Testvej 1"}]},
                                                }
                                            ],
                                        }
                                    }
                                },
                            }
                        },
                        "energyInfrastructureStation": [
                            {
                                "idG": "station-1",
                                "numberOfRefillPoints": 1,
                                "totalMaximumPower": 50000,
                                "serviceType": [{"serviceType": {"value": "charging"}}],
                                "refillPoint": [
                                    {
                                        "idG": "refill-1",
                                        "currentType": {"value": "directCurrent"},
                                        "deliveryUnit": {"value": "kWh"},
                                        "electricEnergyMix": [{"energyMixIndex": 1, "isGreenEnergy": True}],
                                        "externalIdentifier": [
                                            {
                                                "identifier": "DK*MON*E100001",
                                                "typeOfIdentifier": {"value": "evseId"},
                                            }
                                        ],
                                        "connector": [
                                            {
                                                "connectorType": {"value": "iec62196T2COMBO"},
                                                "connectorFormat": {"value": "socket"},
                                                "chargingMode": {"value": "mode4DC"},
                                                "maxPowerAtSocket": 50000,
                                                "maximumCurrent": 125,
                                            }
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
        "meta": {"page": 1, "perPage": 1, "total": 1},
    }


def test_parse_static_payload_reads_datex_table_site_station_refill_point():
    rows = parse_static_payload(_table_payload())

    assert len(rows) == 1
    row = rows[0]
    assert row["country_code"] == "DK"
    assert row["source_uid"] == MONTA_AFIR_CHARGE_POINTS_SOURCE_UID
    assert row["provider_uid"] == "dk_monta"
    assert row["station_id"] == "dk:monta:site-1"
    assert row["charger_id"] == "dk:monta:evse:dk-mon-e100001"
    assert row["source_station_id"] == "site-1"
    assert row["source_station_ref"] == "station-1"
    assert row["source_evse_id"] == "DK*MON*E100001"
    assert row["source_evse_alias_ids"] == ["refill-1"]
    assert row["operator_name"] == "Monta Operator"
    assert row["owner_name"] == "Monta Owner"
    assert row["station_name"] == "Monta Test Site"
    assert row["address"] == "Testvej 1"
    assert row["city"] == "Kobenhavn"
    assert row["postal_code"] == "2100"
    assert row["latitude"] == 55.6761
    assert row["longitude"] == 12.5683
    assert row["max_power_kw"] == 50.0
    assert row["connector_types"] == "iec62196T2COMBO"
    assert row["renewable_energy"] is True


def test_parse_static_payload_can_emit_be_monta_source_ids():
    rows = parse_static_payload(_table_payload(), country_code="BE")

    assert len(rows) == 1
    row = rows[0]
    assert row["country_code"] == "BE"
    assert row["source_uid"] == BE_MONTA_AFIR_CHARGE_POINTS_SOURCE_UID
    assert row["provider_uid"] == BE_MONTA_PROVIDER_UID
    assert row["station_id"] == "be:monta:site-1"
    assert row["charger_id"] == "be:monta:evse:dk-mon-e100001"


def test_static_streaming_parser_and_summary_extract_evse_ids():
    payload = _table_payload()
    rows = list(iter_static_rows_from_binary_stream(io.BytesIO(json.dumps(payload).encode("utf-8"))))

    assert [row["source_evse_id"] for row in rows] == ["DK*MON*E100001"]
    assert extract_evse_ids_from_table_payload(payload) == ["DK*MON*E100001"]
    assert count_table_payload(payload) == {
        "publication_time": "2026-06-08T10:00:00Z",
        "table_count": 1,
        "site_count": 1,
        "station_count": 1,
        "refill_point_count": 1,
        "connector_count": 1,
        "page": 1,
        "per_page": 1,
        "total": 1,
    }


def test_parse_status_payload_maps_availability_and_dkk_price():
    payload = {
        "publicationTime": "2026-06-08T10:01:00Z",
        "electricChargingPointStatus": {
            "evseId": "DK*MON*E100001",
            "availabilityStatus": "outOfService",
            "lastUpdated": "2026-06-08T10:00:30Z",
            "energyRateUpdate": [
                {
                    "idG": "rate-1",
                    "ratePolicy": "adHoc",
                    "applicableCurrency": "DKK",
                    "energyRate": [
                        {
                            "price": 2.5,
                            "priceType": "unitPrice",
                            "unitType": "perKilowattHour",
                            "applicableQuantity": "energy",
                            "taxIncluded": True,
                            "taxRate": 25,
                        }
                    ],
                }
            ],
        },
    }

    rows = parse_status_payload(payload)

    assert len(rows) == 1
    row = rows[0]
    assert row["source_uid"] == MONTA_AFIR_EVSE_STATUS_SOURCE_UID
    assert row["source_evse_id"] == "DK*MON*E100001"
    assert row["source_status"] == "outOfService"
    assert row["availability_status"] == "out_of_order"
    assert row["source_observed_at"] == "2026-06-08T10:00:30Z"
    assert row["price_display"] == "2.5 DKK/kWh"
    assert row["price_currency"] == "DKK"
    assert row["price_quality"] == "source_dk_monta_afir_status_price"


def test_parse_status_payload_can_emit_be_monta_source_ids_and_eur_price():
    payload = {
        "publicationTime": "2026-06-08T10:01:00Z",
        "electricChargingPointStatus": {
            "evseId": "BE*MON*E100001",
            "availabilityStatus": "available",
            "lastUpdated": "2026-06-08T10:00:30Z",
            "energyRateUpdate": [
                {
                    "applicableCurrency": "EUR",
                    "energyRate": [
                        {
                            "price": 0.42,
                            "unitType": "perKilowattHour",
                            "applicableQuantity": "energy",
                        }
                    ],
                }
            ],
        },
    }

    rows = parse_status_payload(payload, country_code="BE")

    assert len(rows) == 1
    row = rows[0]
    assert row["country_code"] == "BE"
    assert row["source_uid"] == BE_MONTA_AFIR_EVSE_STATUS_SOURCE_UID
    assert row["provider_uid"] == BE_MONTA_PROVIDER_UID
    assert row["charger_id"] == "be:monta:evse:be-mon-e100001"
    assert row["availability_status"] == "free"
    assert row["price_display"] == "0.42 EUR/kWh"
    assert row["price_quality"] == "source_be_monta_afir_status_price"


def test_status_streaming_parser_accepts_single_status_document():
    payload = {
        "electricChargingPointStatus": {
            "evseId": "DK*MON*E100002",
            "availabilityStatus": "available",
            "lastUpdated": "2026-06-08T10:02:00Z",
        }
    }

    rows = list(iter_status_rows_from_binary_stream(io.BytesIO(json.dumps(payload).encode("utf-8"))))

    assert len(rows) == 1
    assert rows[0]["availability_status"] == "free"
