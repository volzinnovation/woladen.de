from __future__ import annotations

from commercial_backend.fr_datagouv import static_row_from_csv_record, summarize_afir_search_payload
from scripts import commercial_fetch_fr_datagouv as fetch_fr


def test_fr_fetch_defaults_to_search_and_ecosystem_resources():
    specs = fetch_fr._selected_sources("all")

    assert [spec.key for spec in specs] == ["search", "base-static", "eco-dynamic", "eco-static"]
    assert [spec.source_uid for spec in specs] == [
        "fr_data_gouv_afir_search",
        "fr_base_nationale_irve_static",
        "fr_eco_movement_afir_irve_dynamic",
        "fr_eco_movement_afir_irve_static",
    ]
    assert [spec.task_kind for spec in specs] == [
        "archive_only_payload",
        "parse_static_payload",
        "parse_dynamic_payload",
        "parse_static_payload",
    ]


def test_fr_afir_search_summary_lists_provider_candidates():
    summary = summarize_afir_search_payload(
        {
            "total": 481,
            "page": 1,
            "page_size": 20,
            "data": [
                {
                    "id": "dataset-id",
                    "slug": "public-charging-stations",
                    "title": "Public charging stations for electric cars from several CPOs",
                    "organization": {"name": "Eco-Movement"},
                    "access_type": "open",
                    "license": "notspecified",
                    "resources": {"total": 2},
                    "page": "https://example.test/datasets/public-charging-stations",
                }
            ],
        }
    )

    assert summary["total"] == 481
    assert summary["shown_provider_candidates"][0]["organization"] == "Eco-Movement"
    assert summary["shown_provider_candidates"][0]["resource_count"] == 2


def _base_static_record(**overrides):
    record = {
        "id_pdc_itinerance": "FRTESTE1",
        "id_station_itinerance": "FRTESTP1",
        "coordonneesXY": "[2.3522,48.8566]",
        "prise_type_combo_ccs": "true",
        "puissance_nominale": "150",
        "nom_operateur": "Test Operator",
        "nom_station": "Test Station",
        "adresse_station": "1 Rue Test 75001 Paris",
        "paiement_cb": "true",
        "paiement_acte": "true",
        "horaires": "24/7",
        "date_maj": "2026-05-01",
        "gratuit": "false",
        "tarification": "",
    }
    record.update(overrides)
    return record


def test_fr_static_parser_extracts_simple_kwh_tarification():
    row = static_row_from_csv_record(_base_static_record(tarification="0,30 \u20ac/kWh"))

    assert row is not None
    assert row["price_display"] == "0,30 \u20ac/kWh"
    assert row["price_currency"] == "EUR"
    assert row["price_energy_eur_kwh_min"] == "0.3"
    assert row["price_energy_eur_kwh_max"] == "0.3"
    assert row["price_quality"] == "source_tarification_exact"
    assert row["price_source_text"] == "0,30 \u20ac/kWh"


def test_fr_static_parser_extracts_free_tarification():
    row = static_row_from_csv_record(_base_static_record(gratuit="true", tarification=""))

    assert row is not None
    assert row["price_display"] == "gratuit"
    assert row["price_currency"] == "EUR"
    assert row["price_energy_eur_kwh_min"] == "0"
    assert row["price_energy_eur_kwh_max"] == "0"
    assert row["price_quality"] == "source_tarification_free"


def test_fr_static_parser_extracts_complex_kwh_and_time_tarification():
    row = static_row_from_csv_record(
        _base_static_record(
            tarification=(
                "La premiere heure est gratuite puis 0,20 \u20ac par kWh "
                "et 0,01 \u20ac par minute"
            )
        )
    )

    assert row is not None
    assert row["price_display"] == "ab 0,20 \u20ac/kWh"
    assert row["price_energy_eur_kwh_min"] == "0.2"
    assert row["price_energy_eur_kwh_max"] == "0.2"
    assert row["price_time_eur_min_min"] == 0.01
    assert row["price_time_eur_min_max"] == 0.01
    assert row["price_quality"] == "source_tarification_complex"
    assert row["price_complex"] is True


def test_fr_static_parser_extracts_energy_price_from_json_tarification():
    row = static_row_from_csv_record(
        _base_static_record(
            tarification=(
                '{"fixedPrice":0,"energyPrice":0.51,"minimumBilling":0,'
                '"matrix":[],"matrixOSF":[{"duration":0,"interval":1,"price":0.2}]}'
            )
        )
    )

    assert row is not None
    assert row["price_display"] == "ab 0,51 \u20ac/kWh"
    assert row["price_energy_eur_kwh_min"] == "0.51"
    assert row["price_energy_eur_kwh_max"] == "0.51"
    assert row["price_quality"] == "source_tarification_json_complex"
    assert row["price_complex"] is True
