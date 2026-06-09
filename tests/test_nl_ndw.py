from __future__ import annotations

import gzip
import io
import json

from commercial_backend.nl_ndw import (
    OCPI_TARIFFS_SOURCE_UID,
    iter_location_rows_from_binary_stream,
    iter_tariff_rows_from_binary_stream,
    parse_location_payload,
    parse_tariff_payload,
    station_id_from_location,
)


def test_parses_nl_ocpi_locations_status_rows():
    payload = [
        {
            "id": "696609d31bfcda4cdd955c92",
            "party_id": "EFL",
            "last_updated": "2026-04-30T10:28:46.045Z",
            "operator": {"name": "Equans"},
            "evses": [
                {
                    "uid": "62e7a7aecab20a95e3a5323c",
                    "evse_id": "NL*EFL*EV1749452*C1",
                    "status": "AVAILABLE",
                    "last_updated": "2026-04-30T10:28:41.057Z",
                    "connectors": [{"id": "1", "tariff_ids": ["tariff-a", "tariff-a", "tariff-b"]}],
                },
                {
                    "uid": "62e7a7aecab20a2be2a5323d",
                    "evse_id": "NL*EFL*EV1749452*C2",
                    "status": "CHARGING",
                },
            ],
        }
    ]

    rows = parse_location_payload(payload)

    assert [row["station_id"] for row in rows] == [
        "nl:ocpi:efl:696609d31bfcda4cdd955c92",
        "nl:ocpi:efl:696609d31bfcda4cdd955c92",
    ]
    assert [row["charger_id"] for row in rows] == [
        "nl:ocpi:nl-efl-ev1749452-c1",
        "nl:ocpi:nl-efl-ev1749452-c2",
    ]
    assert [row["availability_status"] for row in rows] == ["free", "occupied"]
    assert rows[0]["connector_count"] == 1
    assert rows[0]["tariff_ids"] == "tariff-a|tariff-b"
    assert rows[1]["source_observed_at"] == "2026-04-30T10:28:46.045Z"


def test_nl_station_id_matches_onboarded_catalog_rule():
    assert (
        station_id_from_location(party_id="TNM", location_id="6ae2b8fa-3b1e-11ef-a346-42010aa400b8")
        == "nl:ocpi:tnm:6ae2b8fa-3b1e-11ef-a346-42010aa400b8"
    )


def test_streams_nl_ocpi_locations_from_gzip_bytes():
    payload = [
        {
            "id": "station-1",
            "party_id": "TNM",
            "operator": {"name": "Shell"},
            "evses": [
                {
                    "evse_id": "NL*TNM*E1",
                    "status": "AVAILABLE",
                    "last_updated": "2026-04-30T10:28:41Z",
                }
            ],
        },
        {
            "id": "station-2",
            "party_id": "TNM",
            "operator": {"name": "Shell"},
            "evses": [{"evse_id": "NL*TNM*E2", "status": "OUTOFORDER"}],
        },
    ]
    stream = io.BufferedReader(io.BytesIO(gzip.compress(json.dumps(payload).encode("utf-8"))))

    rows = list(iter_location_rows_from_binary_stream(stream))

    assert [row["station_id"] for row in rows] == ["nl:ocpi:tnm:station-1", "nl:ocpi:tnm:station-2"]
    assert [row["availability_status"] for row in rows] == ["free", "out_of_order"]


def test_parses_nl_ocpi_tariffs_with_price_fields():
    payload = {
        "data": [
            {
                "id": "NL-FAST-001",
                "country_code": "NL",
                "party_id": "FAS",
                "currency": "EUR",
                "type": "REGULAR",
                "last_updated": "2026-05-07T16:05:00Z",
                "elements": [
                    {
                        "price_components": [
                            {"type": "ENERGY", "price": 0.59, "vat": 21.0},
                            {"type": "TIME", "price": 6.0},
                        ],
                        "restrictions": {"start_time": "08:00", "end_time": "20:00"},
                    }
                ],
                "tariff_alt_text": [{"language": "nl", "text": "Snelladen"}],
                "tariff_alt_url": "https://example.test/tariffs/NL-FAST-001",
            }
        ]
    }

    rows = parse_tariff_payload(payload)

    assert rows == [
        {
            "country_code": "NL",
            "source_uid": OCPI_TARIFFS_SOURCE_UID,
            "tariff_id": "NL-FAST-001",
            "country_code_source": "NL",
            "party_id": "FAS",
            "currency": "EUR",
            "type": "REGULAR",
            "last_updated": "2026-05-07T16:05:00Z",
            "elements": (
                '[{"price_components":[{"price":0.59,"type":"ENERGY","vat":21.0},'
                '{"price":6.0,"type":"TIME"}],"restrictions":{"end_time":"20:00","start_time":"08:00"}}]'
            ),
            "tariff_alt_text": '[{"language":"nl","text":"Snelladen"}]',
            "tariff_alt_url": "https://example.test/tariffs/NL-FAST-001",
            "raw_static": (
                '{"country_code":"NL","currency":"EUR","elements":[{"price_components":'
                '[{"price":0.59,"type":"ENERGY","vat":21.0},{"price":6.0,"type":"TIME"}],'
                '"restrictions":{"end_time":"20:00","start_time":"08:00"}}],"id":"NL-FAST-001",'
                '"last_updated":"2026-05-07T16:05:00Z","party_id":"FAS",'
                '"tariff_alt_text":[{"language":"nl","text":"Snelladen"}],'
                '"tariff_alt_url":"https://example.test/tariffs/NL-FAST-001","type":"REGULAR"}'
            ),
            "price_display": "ab 0,59 €/kWh",
            "price_currency": "EUR",
            "price_energy_eur_kwh_min": "0.59",
            "price_energy_eur_kwh_max": "0.59",
            "price_time_eur_min_min": 0.1,
            "price_time_eur_min_max": 0.1,
            "price_quality": "source_ocpi_tariff_complex",
            "price_complex": True,
            "price_source_text": "NL-FAST-001",
        }
    ]


def test_streams_nl_ocpi_tariffs_from_gzip_bytes():
    payload = [
        {
            "id": "tariff-a",
            "currency": "EUR",
            "elements": [{"price_components": [{"type": "ENERGY", "price": 0.42}]}],
        },
        {
            "id": "tariff-b",
            "currency": "EUR",
            "elements": [{"price_components": [{"type": "PARKING_TIME", "price": 3.0}]}],
        },
    ]
    stream = io.BufferedReader(io.BytesIO(gzip.compress(json.dumps(payload).encode("utf-8"))))

    rows = list(iter_tariff_rows_from_binary_stream(stream))

    assert [row["tariff_id"] for row in rows] == ["tariff-a", "tariff-b"]
    assert rows[0]["price_display"] == "0,42 €/kWh"
    assert rows[1]["price_display"] == "ab 0,05 €/min"
