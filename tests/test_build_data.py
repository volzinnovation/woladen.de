from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_build_data_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "build_data.py"
    spec = importlib.util.spec_from_file_location("build_data_module", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


build_data = _load_build_data_module()


def test_load_static_subscription_ids_reads_registry(tmp_path: Path):
    registry_path = tmp_path / "subscriptions.json"
    registry_path.write_text(
        json.dumps(
            {
                "m8mit": {
                    "subscription_id": "980986232691372032",
                    "static_subscription_id": "980986244745637888",
                },
                "wirelane": {
                    "subscription_id": "980986434407878656",
                },
            }
        ),
        encoding="utf-8",
    )

    assert build_data.load_static_subscription_ids(registry_path) == {
        "m8mit": "980986244745637888",
    }


def test_load_direct_datex_sources_reads_external_registry_entries(tmp_path: Path):
    registry_path = tmp_path / "subscriptions.json"
    registry_path.write_text(
        json.dumps(
            {
                "mobidata_bw_datex": {
                    "display_name": "MobiData BW DATEX II",
                    "publisher": "MobiData BW",
                    "fetch_kind": "direct_url",
                    "fetch_url": "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/realtime",
                    "offer_title": "MobiData BW DATEX II realtime",
                    "static_fetch_url": "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/static",
                    "static_offer_title": "MobiData BW DATEX II static",
                },
                "m8mit": {
                    "subscription_id": "980986232691372032",
                    "static_subscription_id": "980986244745637888",
                },
            }
        ),
        encoding="utf-8",
    )

    sources = build_data.load_direct_datex_sources(registry_path)

    assert len(sources) == 1
    assert sources[0].provider_uid == "mobidata_bw_datex"
    assert sources[0].dynamic_url == "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/realtime"
    assert sources[0].static_url == "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/static"


def test_load_direct_datex_sources_skips_disabled_entries(tmp_path: Path):
    registry_path = tmp_path / "subscriptions.json"
    registry_path.write_text(
        json.dumps(
            {
                "mobidata_bw_datex": {
                    "display_name": "MobiData BW DATEX II",
                    "publisher": "MobiData BW",
                    "enabled": False,
                    "fetch_kind": "direct_url",
                    "fetch_url": "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/realtime",
                    "offer_title": "MobiData BW DATEX II realtime",
                    "static_fetch_url": "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/static",
                    "static_offer_title": "MobiData BW DATEX II static",
                }
            }
        ),
        encoding="utf-8",
    )

    sources = build_data.load_direct_datex_sources(registry_path)

    assert sources == []


def test_fetch_mobilithek_static_payload_with_probe_falls_back_to_mtls_subscription(monkeypatch):
    attempted_urls: list[str] = []

    def fake_request_with_retries(method, url, session, **kwargs):
        attempted_urls.append(url)
        raise RuntimeError("fetch_failed")

    def fake_fetch_mtls_subscription_payload(*, subscription_id: str):
        assert subscription_id == "980986244745637888"
        return {"payload": {"source": "mtls"}}

    monkeypatch.setattr(build_data, "request_with_retries", fake_request_with_retries)
    monkeypatch.setattr(
        build_data,
        "fetch_mobilithek_subscription_payload_with_mtls",
        fake_fetch_mtls_subscription_payload,
    )

    payload, access_mode_used, fetch_error = build_data.fetch_mobilithek_static_payload_with_probe(
        session=object(),
        publication_id="970305056590979072",
        preferred_access_mode="auth",
        access_token="token",
        subscription_id="980986244745637888",
    )

    assert payload == {"payload": {"source": "mtls"}}
    assert access_mode_used == "mtls_subscription"
    assert fetch_error is None
    assert attempted_urls == [
        build_data.MOBILITHEK_PUBLICATION_FILE_URL.format(publication_id="970305056590979072"),
        build_data.MOBILITHEK_PUBLICATION_PUBLIC_FILE_URL.format(publication_id="970305056590979072"),
    ]


def test_derive_eliso_static_site_id_prefers_location_fields_over_operator_code():
    site = {
        "operator": "DEELI",
        "operator_name": "eliso GmbH",
        "address": "Gutshofstraße 26",
        "postalCode": "26871",
        "city": "Papenburg",
        "coordinates": {"latitude": 53.069105837584, "longitude": 7.4195209523674},
    }

    assert build_data.derive_eliso_static_site_id(site) == "Gutshofstraße 26 | 26871 | Papenburg"


def test_parse_eliso_static_sites_keeps_distinct_sites_with_same_operator_code():
    payload = [
        {
            "operator": "DEELI",
            "operator_name": "eliso GmbH",
            "address": "Gutshofstraße 26",
            "postalCode": "26871",
            "city": "Papenburg",
            "coordinates": {"latitude": 53.069105837584, "longitude": 7.4195209523674},
            "chargepoints_count": 2,
            "evses": [{"evseId": "DE*ELI*E3585539"}, {"evseId": "DE*ELI*E3585548"}],
        },
        {
            "operator": "DEELI",
            "operator_name": "eliso GmbH",
            "address": "Parkplatz Brandbühl",
            "postalCode": "78315",
            "city": "Radolfzell am Bodensee",
            "coordinates": {"latitude": 47.770667, "longitude": 8.9661674},
            "chargepoints_count": 2,
            "evses": [{"evseId": "DE*ELI*E3584554"}, {"evseId": "DE*ELI*E3584555"}],
        },
    ]

    sites = build_data.parse_eliso_static_sites(payload)

    assert [site.site_id for site in sites] == [
        "Gutshofstraße 26 | 26871 | Papenburg",
        "Parkplatz Brandbühl | 78315 | Radolfzell am Bodensee",
    ]


def test_summarize_price_display_keeps_numeric_kwh_bounds():
    summary = build_data.summarize_price_display(
        {
            "kwh_values": [0.65, 0.79],
            "minute_values": [],
            "currencies": ["EUR"],
            "complex_tariff": False,
        }
    )

    assert summary["price_display"] == "0,65–0,79 €/kWh"
    assert summary["price_energy_eur_kwh_min"] == 0.65
    assert summary["price_energy_eur_kwh_max"] == 0.79
    assert summary["price_quality"] == "range"


def test_static_detail_columns_accept_numeric_assignments_without_string_dtype_errors():
    enriched = build_data.pd.DataFrame({"station_id": ["station-1"]})

    for field in build_data.STATIC_DETAIL_FIELDS:
        if field in {"opening_hours_is_24_7"}:
            enriched[field] = False
        else:
            enriched[field] = build_data.pd.Series(
                [""] * len(enriched),
                index=enriched.index,
                dtype="object",
            )

    enriched.at[0, "price_energy_eur_kwh_min"] = 0.65
    enriched.at[0, "price_energy_eur_kwh_max"] = 0.79
    enriched.at[0, "connector_count"] = 4
    enriched.at[0, "green_energy"] = True

    assert enriched.at[0, "price_energy_eur_kwh_min"] == 0.65
    assert enriched.at[0, "price_energy_eur_kwh_max"] == 0.79
    assert enriched.at[0, "connector_count"] == 4
    assert enriched.at[0, "green_energy"] is True
