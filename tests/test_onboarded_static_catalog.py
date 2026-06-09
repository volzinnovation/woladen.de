from __future__ import annotations

import json

import scripts.build_onboarded_static_catalog as catalog
from scripts.build_onboarded_static_catalog import _load_amenity_points


def test_load_amenity_points_accepts_geojson_sequence_record_separators(tmp_path):
    path = tmp_path / "amenities.geojsonseq"
    feature = {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [8.54, 47.37]},
        "properties": {"amenity": "cafe", "name": "Charge Coffee"},
    }
    path.write_text(f"\x1e{json.dumps(feature)}\n", encoding="utf-8")

    grid = _load_amenity_points(path, cell_degrees=0.01)

    points = [point for bucket in grid.values() for point in bucket]
    assert points == [
        {
            "category": "cafe",
            "latitude": 47.37,
            "longitude": 8.54,
            "kind": "amenity:cafe",
            "name": "Charge Coffee",
            "opening_hours": "",
        }
    ]


def test_load_amenity_points_accepts_feature_collection(tmp_path):
    path = tmp_path / "amenities.geojson"
    document = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [4.35, 50.85]},
                "properties": {"shop": "supermarket", "name": "Market"},
            }
        ],
    }
    path.write_text(json.dumps(document, indent=2), encoding="utf-8")

    grid = _load_amenity_points(path, cell_degrees=0.01)

    points = [point for bucket in grid.values() for point in bucket]
    assert points == [
        {
            "category": "supermarket",
            "latitude": 50.85,
            "longitude": 4.35,
            "kind": "shop:supermarket",
            "name": "Market",
            "opening_hours": "",
        }
    ]


def test_parse_nl_catalog_uses_canonical_charger_ids_and_alias_dedupe(monkeypatch):
    payload = {
        "data": [
            {
                "id": "loc-a",
                "party_id": "FAS",
                "name": "Canonical Station",
                "coordinates": {"latitude": "52.1", "longitude": "5.1"},
                "operator": {"name": "Fastned"},
                "evses": [
                    {
                        "evse_id": "NL*FAS*E1",
                        "status": "AVAILABLE",
                        "connectors": [
                            {
                                "id": "1",
                                "standard": "IEC_62196_T2",
                                "power_type": "AC_3_PHASE",
                                "max_voltage": 230,
                                "max_amperage": 32,
                            },
                            {
                                "id": "2",
                                "standard": "IEC_62196_T2_COMBO",
                                "power_type": "DC",
                                "max_electric_power": 150000,
                            },
                        ],
                    }
                ],
            },
            {
                "id": "loc-alias",
                "party_id": "FAS",
                "name": "Alias Station",
                "coordinates": {"latitude": "52.1", "longitude": "5.1"},
                "operator": {"name": "Fastned"},
                "evses": [
                    {
                        "evse_id": "NL*FAS*E1",
                        "status": "AVAILABLE",
                        "connectors": [{"id": "2", "standard": "IEC_62196_T2_COMBO", "power_type": "DC"}],
                    }
                ],
            },
            {
                "id": "loc-b",
                "party_id": "TNM",
                "name": "Second Station",
                "coordinates": {"latitude": "52.2", "longitude": "5.2"},
                "operator": {"name": "The New Motion"},
                "evses": [{"evse_id": "NL*TNM*E2", "status": "AVAILABLE"}],
            },
        ]
    }
    monkeypatch.setattr(catalog, "_download_json", lambda *_args, **_kwargs: payload)
    monkeypatch.setattr(catalog, "_load_nl_tariff_lookup", lambda: {})

    stations, chargers, dedupe, summary = catalog._parse_nl_catalog()

    assert [station.station_id for station in stations] == [
        "nl:ocpi:fas:loc-a",
        "nl:ocpi:tnm:loc-b",
    ]
    assert [station.charger_count for station in stations] == [1, 1]
    assert [charger.charger_id for charger in chargers] == [
        "nl:ocpi:nl-fas-e1",
        "nl:ocpi:nl-tnm-e2",
    ]
    assert chargers[0].connector_id == "1;2"
    assert chargers[0].connector_type == "IEC_62196_T2;IEC_62196_T2_COMBO"
    assert chargers[0].current_type == "DC"
    assert any(item["issue"] == "duplicate_nl_source_evse_location_alias" for item in dedupe)
    assert summary["station_count"] == 2
    assert summary["charger_count"] == 2


def test_nl_unique_charger_id_suffixes_sanitized_collisions():
    used = {"nl:ocpi:nl-col-e3", "nl:ocpi:nl-col-e3:loc-d:2"}

    charger_id = catalog._nl_unique_charger_id("nl:ocpi:nl-col-e3", "loc-d", used)

    assert charger_id == "nl:ocpi:nl-col-e3:loc-d:3"


def test_parse_ch_catalog_groups_same_coordinate_chargers_into_one_station(monkeypatch):
    payload = {
        "EVSEData": [
            {
                "OperatorID": "CH*CCI",
                "OperatorName": "SIG",
                "EVSEDataRecord": [
                    {
                        "ChargingStationId": "CH*CCI*E22078",
                        "ChargingStationNames": [{"value": "SIG CERN"}],
                        "EvseID": "CH*CCI*E22078",
                        "GeoCoordinates": {"Google": "46.23432 6.055602"},
                        "ChargingFacilities": [{"power": "22.0", "powertype": "AC_3_PHASE"}],
                        "Plugs": ["Type 2 Outlet"],
                    },
                    {
                        "ChargingStationId": "CH*CCI*E22079",
                        "ChargingStationNames": [{"value": "SIG CERN"}],
                        "EvseID": "CH*CCI*E22079",
                        "GeoCoordinates": {"Google": "46.23432 6.055602"},
                        "ChargingFacilities": [{"power": "150.0", "powertype": "DC"}],
                        "Plugs": ["CCS"],
                    },
                ],
            }
        ]
    }
    monkeypatch.setattr(catalog, "_download_json", lambda *_args, **_kwargs: payload)

    stations, chargers, dedupe, summary = catalog._parse_ch_catalog()

    assert len(stations) == 1
    assert stations[0].station_id == "ch:coord:ch-cci:46.23432:6.055602"
    assert stations[0].source_station_id == "CH*CCI*E22078|CH*CCI*E22079"
    assert stations[0].source_uid == catalog.CH_STATIC_SOURCE_UID
    assert stations[0].charger_count == 2
    assert stations[0].max_power_kw == 150.0
    assert {charger.station_id for charger in chargers} == {stations[0].station_id}
    assert {charger.source_uid for charger in chargers} == {catalog.CH_STATIC_SOURCE_UID}
    assert any(
        item["issue"] == "ch_coordinate_station_merge" and item["source_uid"] == catalog.CH_STATIC_SOURCE_UID
        for item in dedupe
    )
    assert summary["station_count"] == 1
    assert summary["charger_count"] == 2


def test_parse_ch_catalog_keeps_same_coordinate_different_provider_stations(monkeypatch):
    payload = {
        "EVSEData": [
            {
                "OperatorID": "CH*AAA",
                "OperatorName": "Provider A",
                "EVSEDataRecord": [
                    {
                        "ChargingStationId": "CH*AAA*E1",
                        "EvseID": "CH*AAA*E1",
                        "GeoCoordinates": {"Google": "46.23432 6.055602"},
                    }
                ],
            },
            {
                "OperatorID": "CH*BBB",
                "OperatorName": "Provider B",
                "EVSEDataRecord": [
                    {
                        "ChargingStationId": "CH*BBB*E1",
                        "EvseID": "CH*BBB*E1",
                        "GeoCoordinates": {"Google": "46.23432 6.055602"},
                    }
                ],
            },
        ]
    }
    monkeypatch.setattr(catalog, "_download_json", lambda *_args, **_kwargs: payload)

    stations, chargers, dedupe, summary = catalog._parse_ch_catalog()

    assert [station.station_id for station in stations] == [
        "ch:coord:ch-aaa:46.23432:6.055602",
        "ch:coord:ch-bbb:46.23432:6.055602",
    ]
    assert [station.charger_count for station in stations] == [1, 1]
    assert {station.source_uid for station in stations} == {catalog.CH_STATIC_SOURCE_UID}
    assert {charger.station_id for charger in chargers} == {station.station_id for station in stations}
    assert {charger.source_uid for charger in chargers} == {catalog.CH_STATIC_SOURCE_UID}
    assert not any(item["issue"] == "ch_coordinate_station_merge" for item in dedupe)
    assert summary["station_count"] == 2


def test_build_catalog_writes_woladen_compatible_bundle_shape(tmp_path, monkeypatch):
    station = catalog.StationRow(
        country_code="CH",
        station_id="ch:station:test",
        source_uid="ch_bfe_ladestationen_static",
        source_station_id="CH-1",
        operator_name="Test Operator",
        address="Teststrasse 1 8000 Zürich",
        postal_code="8000",
        city="Zürich",
        latitude=47.37,
        longitude=8.54,
        charger_count=2,
        max_power_kw=150.0,
        source_url="https://example.test/static.json",
        license="test",
        id_rule="test_id",
    )
    charger = catalog.ChargerRow(
        country_code="CH",
        station_id=station.station_id,
        charger_id="ch:charger:test",
        source_uid=station.source_uid,
        source_station_id=station.source_station_id,
        source_evse_id="CH*1",
        connector_id="1",
        connector_type="IEC_62196_T2_COMBO",
        current_type="DC",
        max_power_kw=150.0,
        operator_name=station.operator_name,
    )

    monkeypatch.setattr(
        catalog,
        "_parse_ch_catalog",
        lambda: ([station], [charger], [], {"source": "test", "station_count": 1, "charger_count": 1}),
    )
    monkeypatch.setattr(
        catalog,
        "_build_amenity_rows",
        lambda **_: [
            {
                "country_code": "CH",
                "station_id": station.station_id,
                "amenity_count": "2",
                "amenity_summary": "amenity:restaurant=2",
                "amenity_category_counts": {"restaurant": 2},
                "amenity_examples": [{"category": "restaurant", "name": "Lunch", "distance_m": 20}],
                "nearest_amenity_kind": "amenity:restaurant",
                "nearest_amenity_name": "Lunch",
                "nearest_amenity_distance_m": "20",
                "osm_pbf_url": "",
                "osm_extraction_status": "extracted_from_pbf",
            }
        ],
    )

    summary = catalog.build_catalog(
        output_dir=tmp_path,
        countries=["CH"],
        include_osm=True,
        download_osm_pbf=False,
        pbf_cache_dir=tmp_path / "pbf",
        amenity_radius_m=250.0,
        operator_min_stations=1,
    )

    assert not (tmp_path / "live_seed").exists()
    assert {
        "stations.csv",
        "chargers.csv",
        "chargers_full.csv",
        "chargers_fast.csv",
        "chargers_fast.geojson",
        "operators.json",
        "summary.json",
    }.issubset({path.name for path in tmp_path.iterdir()})

    full_header = (tmp_path / "chargers_full.csv").read_text(encoding="utf-8").splitlines()[0].split(",")
    fast_header = (tmp_path / "chargers_fast.csv").read_text(encoding="utf-8").splitlines()[0].split(",")
    assert tuple(full_header) == catalog.FULL_BUNDLE_FIELDS
    assert tuple(fast_header) == catalog.FAST_BUNDLE_FIELDS
    assert (tmp_path / "stations.csv").read_text(encoding="utf-8").splitlines()[0].split(",") == list(catalog.STATION_FIELDS)
    assert (tmp_path / "chargers.csv").read_text(encoding="utf-8").splitlines()[0].split(",") == list(catalog.CHARGER_FIELDS)
    source_attribution = json.loads((tmp_path / "source_attribution.json").read_text(encoding="utf-8"))
    assert source_attribution["sources"]["CH"]["source_uid"] == catalog.CH_STATIC_SOURCE_UID

    geojson = json.loads((tmp_path / "chargers_fast.geojson").read_text(encoding="utf-8"))
    assert geojson["type"] == "FeatureCollection"
    assert len(geojson["features"]) == 1
    properties = geojson["features"][0]["properties"]
    assert properties["operator"] == "Test Operator"
    assert properties["amenity_restaurant"] == 2
    assert "details_json" not in properties
    assert summary["fast_charger_count"] == 1
