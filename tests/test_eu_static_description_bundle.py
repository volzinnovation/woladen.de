from __future__ import annotations

import json

from scripts import build_eu_static_description_bundle as bundle


def test_unique_charger_rows_rekeys_duplicate_ids_with_source_context():
    rows, dedupe = bundle._unique_charger_rows(
        [
            {
                "country_code": "CH",
                "station_id": "ch:station:first",
                "charger_id": "ch:oicp:CH*SCH*E2OVB556962SCH",
                "source_uid": "ch_bfe_ladestationen_static",
                "source_station_id": "TMHAG_952",
                "connector_id": "",
            },
            {
                "country_code": "CH",
                "station_id": "ch:station:second",
                "charger_id": "ch:oicp:CH*SCH*E2OVB556962SCH",
                "source_uid": "ch_bfe_ladestationen_static",
                "source_station_id": "7c9fdae0-b7df-11e9-adf2-42010a840003",
                "connector_id": "",
            },
        ]
    )

    assert rows[0]["charger_id"] == "ch:oicp:CH*SCH*E2OVB556962SCH"
    assert rows[1]["charger_id"].startswith(
        "ch:oicp:CH*SCH*E2OVB556962SCH:7c9fdae0-b7df-11e9-adf2-42010a840003"
    )
    assert dedupe == [
        {
            "issue": "duplicate_normalized_charger_id_rekeyed",
            "country_code": "CH",
            "station_id": "ch:station:second",
            "source_uid": "ch_bfe_ladestationen_static",
            "details": f"ch:oicp:CH*SCH*E2OVB556962SCH rekeyed to {rows[1]['charger_id']}",
        }
    ]


def test_flat_rows_to_station_rows_preserves_price_fields():
    stations, chargers, dedupe = bundle._flat_rows_to_station_charger_rows(
        [
            {
                "country_code": "FR",
                "station_id": "fr:irve:frtestp1",
                "charger_id": "fr:irve:evse:frteste1",
                "source_uid": "fr_base_nationale_irve_static",
                "provider_uid": "fr_base_nationale_irve",
                "source_station_id": "FRTESTP1",
                "source_evse_id": "FRTESTE1",
                "operator_name": "Test Operator",
                "station_name": "Test Station",
                "address": "1 Rue Test 75001 Paris",
                "latitude": "48.8566",
                "longitude": "2.3522",
                "connector_types": "CCS",
                "max_power_kw": "150",
                "price_display": "0,30 \u20ac/kWh",
                "price_energy_eur_kwh_min": "0.3",
                "price_energy_eur_kwh_max": "0.3",
                "price_currency": "EUR",
                "price_quality": "source_tarification_exact",
                "price_source_text": "0,30 \u20ac/kWh",
            }
        ]
    )

    assert dedupe == []
    assert len(chargers) == 1
    assert len(stations) == 1
    assert stations[0].price_display == "0,30 \u20ac/kWh"
    assert stations[0].price_energy_eur_kwh_min == "0.3"
    assert stations[0].price_energy_eur_kwh_max == "0.3"
    assert stations[0].price_currency == "EUR"
    assert stations[0].price_quality == "source_tarification_exact"
    assert stations[0].details["price_source_texts"] == ["0,30 \u20ac/kWh"]


def test_de_station_projection_uses_public_prefix_and_keeps_legacy_source_id(tmp_path):
    (tmp_path / "summary.json").write_text(
        json.dumps(
            {
                "source": {
                    "source_url": "https://example.test/de",
                    "fetched_at": "2026-05-07T12:00:00+00:00",
                }
            }
        ),
        encoding="utf-8",
    )
    rows = bundle._project_de_station_ids(
        [
            {
                "station_id": "b861b10172d72c0d",
                "operator": "Test Operator",
                "address": "Musterstrasse 1",
                "postcode": "10115",
                "city": "Berlin",
                "lat": "52.52",
                "lon": "13.405",
                "charging_points_count": "2",
                "max_power_kw": "150",
                "connector_types": "CCS",
            }
        ]
    )

    stations, chargers, metadata = bundle._de_station_charger_rows(rows, tmp_path)

    assert metadata["de_normalized_charger_derivation"]["status"] == "bnetza_register_missing"
    assert rows[0]["station_id"] == "DE:b861b10172d72c0d"
    assert stations[0]["station_id"] == "DE:b861b10172d72c0d"
    assert stations[0]["source_station_id"] == "b861b10172d72c0d"
    assert stations[0]["id_rule"] == "woladen_de_public_station_id"
    assert chargers[0]["station_id"] == "DE:b861b10172d72c0d"
    assert chargers[0]["source_station_id"] == "b861b10172d72c0d"


def test_station_amenity_rows_cover_all_normalized_stations():
    rows = bundle._station_amenity_rows_from_bundle(
        stations=[
            {"country_code": "FR", "station_id": "fr:station:1"},
            {"country_code": "DE", "station_id": "de:station:1"},
        ],
        fast_rows=[
            {
                "station_id": "de:station:1",
                "amenity_restaurant": "1",
                "amenities_source": "osm-pbf",
                "amenity_examples": '[{"category":"restaurant","distance_m":20}]',
            }
        ],
        static_amenity_rows=[
            {
                "country_code": "FR",
                "station_id": "fr:station:1",
                "amenity_radius_m": "250",
                "amenities_total": "0",
                "amenity_category_counts": {},
                "amenity_examples": [],
                "nearest_amenity_kind": "",
                "nearest_amenity_name": "",
                "nearest_amenity_distance_m": "",
                "osm_pbf_url": bundle.EU_OSM_PBF_URLS["FR"],
                "osm_pbf_sha256": "",
                "osm_extracted_at": "",
                "osm_extraction_status": "pbf_missing",
            }
        ],
        amenity_radius_m=250.0,
    )

    assert [row["station_id"] for row in rows] == ["de:station:1", "fr:station:1"]
    assert rows[0]["amenities_total"] == "1"
    assert rows[0]["osm_extraction_status"] == "osm-pbf"
    assert rows[1]["amenities_total"] == "0"
    assert rows[1]["osm_extraction_status"] == "pbf_missing"


def test_delta_source_provider_mappings_include_be_additions_lt_migration_and_dk_monta():
    assert "DK" in bundle.COUNTRIES
    assert "DK" in bundle.STATIC_DESCRIPTION_COUNTRIES
    assert bundle.SOURCE_PROVIDER_UIDS["be_road_ocpi_locations"] == "be_road"
    assert bundle.SOURCE_PROVIDER_UIDS["be_group_indigo_datex_static"] == "be_group_indigo"
    assert bundle.SOURCE_PROVIDER_UIDS["be_monta_afir_charge_points"] == "be_monta"
    assert bundle.SOURCE_PROVIDER_UIDS["dk_monta_afir_charge_points"] == "dk_monta"
    assert bundle.SOURCE_PROVIDER_UIDS["lt_vialietuva_datex_static"] == "lt_vialietuva_datex"
    assert bundle._row_source("be_road_ocpi_locations")["country_code"] == "BE"
    assert bundle._row_source("be_group_indigo_datex_static")["country_code"] == "BE"
    assert bundle._row_source("be_monta_afir_charge_points")["country_code"] == "BE"
    assert bundle._row_source("dk_monta_afir_charge_points")["country_code"] == "DK"
    assert bundle._row_source("dk_monta_afir_charge_points")["license"].startswith("source_terms_pending")


def test_lt_source_attribution_documents_static_fallback():
    assert bundle.LT_VIA_LIETUVA_STATIC_FALLBACK_FILE == "data/lt-EnergyInfrastructureTablePublication.xml"
    assert (
        bundle.LT_VIA_LIETUVA_STATIC_FALLBACK_SHA256
        == "6f3fa1ab3ea0ea8a782c88a088ba03ea1c3d6bdaee5b59e66f976a16ab219f8d"
    )
