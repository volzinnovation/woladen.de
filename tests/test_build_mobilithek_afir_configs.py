from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd


def _load_configs_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "build_mobilithek_afir_configs.py"
    spec = importlib.util.spec_from_file_location("build_mobilithek_afir_configs_module", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


build_configs = _load_configs_module()


def test_mobilithek_offer_search_uses_bearer_token():
    captured: dict[str, object] = {}

    class DummyResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"dataOffers": {"content": []}}

    class DummySession:
        def post(self, url, **kwargs):
            captured["url"] = url
            captured["headers"] = kwargs.get("headers")
            return DummyResponse()

    payload = build_configs.search_mobilithek_offers(
        DummySession(),
        search_term="AFIR",
        page=0,
        size=200,
        access_token="token-123",
    )

    assert payload == {"content": []}
    assert captured["url"] == build_configs.METADATA_SEARCH_URL
    assert captured["headers"] == {"Authorization": "Bearer token-123"}


def test_mobilithek_offer_metadata_uses_bearer_token():
    captured: dict[str, object] = {}

    class DummyResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"publicationId": "988133177339846656"}

    class DummySession:
        def get(self, url, **kwargs):
            captured["url"] = url
            captured["headers"] = kwargs.get("headers")
            return DummyResponse()

    payload = build_configs.fetch_offer_metadata(
        DummySession(),
        "988133177339846656",
        access_token="token-123",
    )

    assert payload == {"publicationId": "988133177339846656"}
    assert captured["url"] == build_configs.METADATA_OFFER_URL.format(
        publication_id="988133177339846656"
    )
    assert captured["headers"] == {"Authorization": "Bearer token-123"}


def test_charging_related_offer_accepts_schema_even_when_search_category_is_missing():
    metadata = {
        "title": "AFIR-recharging-dyn-Eulektro",
        "contentData": [
            {
                "schemaProfileName": "AFIR-Recharging-Dynamic-01-00-00_Delta",
                "dataModel": build_configs.DATEX_V3_DATA_MODEL,
            }
        ],
    }

    assert build_configs.is_charging_related_offer(metadata, search_offer={}) is True


def test_is_test_offer_filters_obvious_mobilithek_test_feeds():
    assert build_configs.is_test_offer({"title": "Test-AFIR-recharging-stat-VolkswagenChargingGroup"})
    assert build_configs.is_test_offer({"title": "AFIR-recharging-stat-SMATRICSTEST"})
    assert not build_configs.is_test_offer({"title": "AFIR-recharging-stat-Ladesonne GmbH & Co. KG"})


def test_load_dynamic_subscription_ids_reads_registry(tmp_path: Path):
    registry_path = tmp_path / "mobilithek_subscriptions.json"
    registry_path.write_text(
        json.dumps(
            {
                "edri": {
                    "subscription_id": "980986189821227008",
                    "static_subscription_id": "980986204027498496",
                },
                "invalid": [],
                "blank": {"subscription_id": ""},
            }
        ),
        encoding="utf-8",
    )

    assert build_configs.load_dynamic_subscription_ids(registry_path) == {
        "edri": "980986189821227008"
    }


def test_fetch_static_payload_with_probe_passes_subscription_id(monkeypatch):
    captured: dict[str, object] = {}

    def fake_fetch(session, *, publication_id, preferred_access_mode, access_token, subscription_id=""):
        captured.update(
            {
                "session": session,
                "publication_id": publication_id,
                "preferred_access_mode": preferred_access_mode,
                "access_token": access_token,
                "subscription_id": subscription_id,
            }
        )
        return {"source": "mtls"}, "mtls_subscription", None

    monkeypatch.setattr(build_configs, "fetch_datex_payload_with_probe", fake_fetch)

    session = object()
    payload, access_mode, error = build_configs.fetch_static_payload_with_probe(
        session,
        publication_id="972837891969273856",
        preferred_access_mode="auth",
        access_token="token",
        subscription_id="980986204027498496",
    )

    assert payload == {"source": "mtls"}
    assert access_mode == "mtls_subscription"
    assert error is None
    assert captured == {
        "session": session,
        "publication_id": "972837891969273856",
        "preferred_access_mode": "auth",
        "access_token": "token",
        "subscription_id": "980986204027498496",
    }


def test_summarize_static_coverage_reports_full_registry_and_bundle_counters():
    chargers_df = pd.DataFrame(
        [
            {"station_id": "station-a", "charging_points_count": 2},
            {"station_id": "station-b", "charging_points_count": 4},
            {"station_id": "station-c", "charging_points_count": 2},
            {"station_id": "station-z", "charging_points_count": 2},
        ]
    )
    bundle_df = pd.DataFrame(
        [
            {"station_id": "station-a", "charging_points_count": 2},
            {"station_id": "station-b", "charging_points_count": 4},
            {"station_id": "station-c", "charging_points_count": 2},
        ]
    )

    summary = build_configs.summarize_static_coverage(
        chargers_df,
        bundle_df,
        matches={"site-a": "station-a", "site-b": "station-z"},
        total_sites=2,
        fetch_status="ok",
        access_mode="noauth",
        site_operator_samples=["Example Operator"],
    )

    assert summary["matched_stations"] == 2
    assert summary["matched_charging_points"] == 4
    assert summary["station_coverage_ratio"] == 0.5
    assert summary["charging_point_coverage_ratio"] == 0.4
    assert summary["bundle_matched_stations"] == 1
    assert summary["bundle_matched_charging_points"] == 2
    assert summary["bundle_station_coverage_ratio"] == 0.333333
    assert summary["bundle_charging_point_coverage_ratio"] == 0.25


def test_supports_eliso_generic_json_feed_accepts_model_other_application_json():
    assert (
        build_configs.supports_eliso_generic_json_feed(
            "eliso",
            {
                "data_model": "https://w3id.org/mdp/schema/data_model#MODEL_OTHER",
                "content_data": {"mediaType": "application/json"},
            },
        )
        is True
    )
    assert (
        build_configs.supports_eliso_generic_json_feed(
            "edri",
            {
                "data_model": "https://w3id.org/mdp/schema/data_model#MODEL_OTHER",
                "content_data": {"mediaType": "application/json"},
            },
        )
        is False
    )
    assert (
        build_configs.supports_eliso_generic_json_feed(
            "eliso",
            {
                "data_model": "https://w3id.org/mdp/schema/data_model#MODEL_OTHER",
                "content_data": {
                    "mediaType": "https://www.iana.org/assignments/media-types/application/json"
                },
            },
        )
        is True
    )


def test_parse_static_sites_with_operator_parses_eliso_generic_static_payload():
    payload = [
        {
            "operator_name": "eliso GmbH",
            "address": "Gutshofstraße 26",
            "postalCode": "26871",
            "city": "Papenburg",
            "chargepoints_count": 2,
            "coordinates": {"latitude": 53.08, "longitude": 7.39},
            "evses": [
                {"evseId": "DE*ELI*E3603098"},
                {"evseId": "DE*ELI*E3603099"},
            ],
        }
    ]

    sites = build_configs.parse_static_sites_with_operator(payload, provider_uid="eliso")

    assert len(sites) == 1
    assert sites[0].site_id == "Gutshofstraße 26 | 26871 | Papenburg"
    assert sites[0].operator_name == "eliso GmbH"
    assert sites[0].total_evses == 2
    assert sites[0].evse_ids == ("DEELIE3603098", "DEELIE3603099")
    assert sites[0].station_ids == ("DEELIE3603098", "DEELIE3603099")


def test_summarize_dynamic_probe_supports_eliso_generic_payload():
    payload = {
        "evses": [
            {
                "evseId": "DE*ELI*E3603098",
                "availability_status": "Not in use",
                "operational_status": "Operational",
                "mobilithek_last_updated_dts": "2026-04-21T18:00:00+00:00",
            },
            {
                "evseId": "DE*ELI*E3603099",
                "availability_status": "In use",
                "operational_status": "Operational",
                "mobilithek_last_updated_dts": "2026-04-21T18:05:00+00:00",
            },
            {
                "evseId": "DE*ELI*E3603100",
                "availability_status": "",
                "operational_status": "Non-operational",
                "mobilithek_last_updated_dts": "2026-04-21T17:59:00+00:00",
            },
        ]
    }

    summary = build_configs.summarize_dynamic_probe(
        payload=payload,
        fetch_status="ok",
        access_mode="auth",
        delta_delivery=False,
        provider_uid="eliso",
    )

    assert summary["fetch_status"] == "ok"
    assert summary["evse_status_count"] == 3
    assert summary["available_evses"] == 1
    assert summary["occupied_evses"] == 1
    assert summary["out_of_order_evses"] == 1
    assert summary["unknown_evses"] == 0
    assert summary["latest_last_updated"] == "2026-04-21T18:05:00+00:00"


def test_score_site_to_station_rejects_close_candidate_with_postcode_conflict_only():
    site = build_configs.StaticSiteRecord(
        site_id="site-enio-rosenheim",
        station_ids=("KathreinECSim02", "KathreinECSim03", "KathreinECSim01"),
        evse_ids=(),
        lat=47.85713,
        lon=12.11810,
        postcode="83022",
        city="Rosenheim",
        address="Wittelsbacherstraße",
        total_evses=6,
        operator_name="",
    )
    station_row = pd.Series(
        {
            "station_id": "station-1",
            "lat": 47.857127,
            "lon": 12.118105,
            "postcode": "83026",
            "city": "Rosenheim",
            "address": "Äußere Münchenerstraße 70a 83026 Rosenheim",
            "operator": "Erich Vinzenz KFZ-Werkstatt",
            "charging_points_count": 1,
            "in_bundle": False,
        }
    )

    accepted, _, _, details = build_configs.score_site_to_station(
        site,
        station_row,
        publisher="ENIO GmbH",
    )

    assert accepted is False
    assert details["postcode_match"] is False
    assert details["city_match"] is True
    assert details["address_match"] is False
    assert details["operator_similarity"] == 0.0
