from __future__ import annotations

from commercial_backend.pl_eipa import (
    count_eipa_browser_rows,
    extract_eipa_provinces,
    iter_reader_dynamic_rows,
    iter_reader_static_rows,
    max_eipa_browser_page,
    summarize_dane_dataset_search_payload,
)
from scripts import commercial_fetch_pl_eipa as fetch_pl


def test_pl_fetch_defaults_to_eipa_data_point_sources():
    keys = fetch_pl._selected_source_keys("all-open")

    assert keys == [
        "eipa-home",
        "eipa-docs",
        "eipa-stats",
        "eipa-browser-provinces",
    ]
    assert fetch_pl.SOURCES["eipa-home"].source_uid == "pl_eipa_home"
    assert fetch_pl.SOURCES["eipa-docs"].source_uid == "pl_eipa_reader_docs"


def test_pl_fetch_keeps_dane_gov_as_optional_discovery_metadata():
    keys = fetch_pl._selected_source_keys("all-discovery")

    assert keys == [
        "eipa-home",
        "eipa-docs",
        "eipa-stats",
        "eipa-browser-provinces",
        "dane-doc",
        "dane-spec",
        "dane-afir-search",
        "dane-charging-search",
    ]
    assert fetch_pl.SOURCES["dane-charging-search"].source_uid == "pl_dane_gov_charging_search"


def test_pl_fetch_can_select_authenticated_reader_json_files():
    assert fetch_pl._selected_source_keys("all-reader-static") == [
        "eipa-reader-operator",
        "eipa-reader-pool",
        "eipa-reader-station",
        "eipa-reader-point",
        "eipa-reader-dictionary",
    ]
    assert fetch_pl._selected_source_keys("all-reader-json") == [
        "eipa-reader-operator",
        "eipa-reader-pool",
        "eipa-reader-station",
        "eipa-reader-point",
        "eipa-reader-dictionary",
        "eipa-reader-dynamic",
    ]
    assert fetch_pl.SOURCES["eipa-reader-point"].source_uid == "pl_eipa_reader_point_json"
    assert "<token-redacted>" in fetch_pl.SOURCES["eipa-reader-point"].url


def test_pl_dane_search_summary_strips_highlight_markup():
    summary = summarize_dane_dataset_search_payload(
        {
            "meta": {"count": 1, "server_time": "2026-05-03T11:16:20Z"},
            "data": [
                {
                    "id": "4478",
                    "attributes": {
                        "title": "B.4.10 - Liczba ogolnodostepnych punktow <mark>ladowania</mark>",
                        "url": "liczba-ogolnodostepnych-punktow-ladowania-pojazdow",
                    },
                }
            ],
        }
    )

    assert summary["total"] == 1
    assert summary["shown_dataset_candidates"][0]["title"].endswith("punktow ladowania")


def test_pl_eipa_browser_helpers_extract_provinces_rows_and_pages():
    html = """
    <select id="browser_filter_province">
      <option value=""></option>
      <option value="mazowieckie">mazowieckie</option>
      <option value="slaskie">slaskie</option>
    </select>
    <a class="browser_show_details">show</a>
    <a class="browser_show_details">show</a>
    <a href="/browser/page/12?filter_type=province&amp;filter_value=mazowieckie">12</a>
    """

    assert extract_eipa_provinces(html) == ["mazowieckie", "slaskie"]
    assert count_eipa_browser_rows(html) == 2
    assert max_eipa_browser_page(html) == 12


def test_pl_eipa_reader_static_rows_join_static_files():
    rows = list(
        iter_reader_static_rows(
            operator_payload={
                "data": [
                    {
                        "id": 4,
                        "name": "EV PLUS Sp. z o.o.",
                        "code": "PL-GJC",
                        "phone": "780070766",
                    }
                ]
            },
            pool_payload={
                "data": [
                    {
                        "id": 240,
                        "operator_id": 4,
                        "charging": True,
                        "code": "PL-GJC-PEVP01001",
                        "name": "Business Garden Wroclaw EV+",
                        "latitude": 51.116886,
                        "longitude": 16.99725,
                        "street": "ul. Legnicka",
                        "house_number": "48G-H",
                        "postal_code": "54-202",
                        "city": "Wroclaw",
                        "operating_hours": [
                            {"weekday": day, "from_time": "00:00", "to_time": "23:59"}
                            for day in range(1, 8)
                        ],
                        "ts": "2026-01-01T00:00:00+01:00",
                    }
                ]
            },
            station_payload={
                "data": [
                    {
                        "id": 1153,
                        "pool_id": 240,
                        "latitude": 51.116886,
                        "longitude": 16.99725,
                        "authentication_methods": [0, 32],
                        "payment_methods": [1, 2],
                        "location": {"city": "Wroclaw"},
                        "ts": "2026-02-01T00:00:00+01:00",
                    }
                ]
            },
            point_payload={
                "data": [
                    {
                        "id": 13477,
                        "station_id": 1153,
                        "code": "PL-GJC-EEVP01001",
                        "charging_solutions": [{"mode": 7, "power": 60}],
                        "connectors": [{"interfaces": [29], "power": 60}],
                        "ts": "2026-03-01T00:00:00+01:00",
                    }
                ]
            },
            dictionary_payload={
                "charging_mode": [{"id": 7, "name": "Mode4-DC"}],
                "connector_interface": [{"id": 29, "name": "IEC-62196-T2-COMBO"}],
                "station_authentication_method": [
                    {"id": 0, "description": "Open access"},
                    {"id": 32, "description": "App"},
                ],
                "station_payment_method": [
                    {"id": 1, "description": "Free"},
                    {"id": 2, "description": "Operator contract"},
                ],
                "weekday": [{"id": day, "name": str(day)} for day in range(1, 8)],
            },
        )
    )

    assert rows == [
        {
            "country_code": "PL",
            "source_uid": "pl_eipa_reader_point_json",
            "provider_uid": "PL-GJC",
            "station_id": "pl:eipa:station:1153",
            "charger_id": "pl:eipa:point:pl-gjc-eevp01001",
            "source_station_id": "1153",
            "source_evse_id": "PL-GJC-EEVP01001",
            "connector_id": "PL-GJC-EEVP01001",
            "operator_name": "EV PLUS Sp. z o.o.",
            "station_name": "Business Garden Wroclaw EV+",
            "address": "ul. Legnicka 48G-H, 54-202 Wroclaw",
            "city": "Wroclaw",
            "postal_code": "54-202",
            "latitude": 51.116886,
            "longitude": 16.99725,
            "connector_count": 1,
            "connector_types": "IEC-62196-T2-COMBO",
            "current_type": "DC",
            "max_power_kw": 60.0,
            "opening_hours": "24/7",
            "payment_methods": "Free|Operator contract",
            "auth_methods": "Open access|App",
            "helpdesk_phone": "780070766",
            "date_updated": "2026-03-01T00:00:00+01:00",
        }
    ]


def test_pl_eipa_reader_dynamic_rows_map_status_codes():
    rows = list(
        iter_reader_dynamic_rows(
            {
                "data": [
                    {
                        "point_id": 13477,
                        "code": "PL-GJC-EEVP01001",
                        "status": {"availability": 1, "status": 1, "ts": "2026-05-05T10:00:00+02:00"},
                        "prices": [{"literal": "PL*EVP*EPL105*105A", "price": "1.00", "unit": "kWh"}],
                    },
                    {
                        "point_id": 13478,
                        "code": "PL-GJC-EEVP01002",
                        "status": {"availability": 0, "status": 1, "ts": "2026-05-05T10:01:00+02:00"},
                    },
                    {
                        "point_id": 13479,
                        "code": "PL-GJC-EEVP01003",
                        "status": {"availability": 1, "status": 0, "ts": "2026-05-05T10:02:00+02:00"},
                    },
                ]
            }
        )
    )

    assert [row["source_evse_id"] for row in rows] == [
        "PL-GJC-EEVP01001",
        "PL-GJC-EEVP01002",
        "PL-GJC-EEVP01003",
    ]
    assert [row["availability_status"] for row in rows] == ["free", "occupied", "out_of_order"]
    assert {row["provider_uid"] for row in rows} == {"pl_eipa_reader_json"}
    assert rows[0]["price_display"] == "1,00 €/kWh"
    assert rows[0]["price_quality"] == "private_pl_eipa_dynamic_price"
