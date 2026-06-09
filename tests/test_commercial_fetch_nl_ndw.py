from __future__ import annotations

from scripts import commercial_fetch_nl_ndw as fetch_nl


def test_nl_fetch_defaults_to_all_approved_no_auth_sources():
    specs = fetch_nl._selected_sources("all")

    assert [spec.key for spec in specs] == ["locations", "geojson", "tariffs"]
    assert [spec.source_uid for spec in specs] == [
        "nl_ndw_dotnl_ocpi_locations",
        "nl_ndw_dotnl_geojson_locations",
        "nl_ndw_dotnl_ocpi_tariffs",
    ]


def test_nl_fetch_parses_locations_and_tariff_metadata():
    locations = fetch_nl.SOURCES["locations"]
    geojson = fetch_nl.SOURCES["geojson"]
    tariffs = fetch_nl.SOURCES["tariffs"]

    assert locations.task_kind == "parse_dynamic_payload"
    assert locations.stream_parser is not None
    assert geojson.task_kind == "archive_only_payload"
    assert geojson.stream_parser is None
    assert tariffs.task_kind == "parse_metadata_payload"
    assert tariffs.stream_parser is not None
