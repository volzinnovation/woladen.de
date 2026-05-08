from __future__ import annotations

import gzip
import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd


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


def _strict_json_loads(text: str):
    def _reject_constant(value: str):
        raise ValueError(value)

    return json.loads(text, parse_constant=_reject_constant)


def test_dumps_minified_json_replaces_non_finite_numbers():
    payload = {
        "top_level": float("nan"),
        "nested": [1, float("inf"), {"value": float("-inf")}],
    }

    text = build_data.dumps_minified_json(payload)

    assert "NaN" not in text
    assert "Infinity" not in text
    assert _strict_json_loads(text) == {
        "top_level": None,
        "nested": [1, None, {"value": None}],
    }


def test_dumps_minified_json_normalizes_nan_like_strings():
    payload = {
        "opening_hours_display": "nan",
        "nested": ["NaT", "ok"],
    }

    text = build_data.dumps_minified_json(payload)

    assert _strict_json_loads(text) == {
        "opening_hours_display": "",
        "nested": ["", "ok"],
    }


def _ladenetz_static_xml_payload() -> bytes:
    payload = """<?xml version="1.0" encoding="UTF-8"?>
<ns2:d2Payload
    xmlns="http://datex2.eu/schema/3/common"
    xmlns:ns1="http://datex2.eu/schema/3/facilities"
    xmlns:ns2="http://datex2.eu/schema/3/d2Payload"
    xmlns:ns3="http://datex2.eu/schema/3/energyInfrastructure"
    xmlns:ns10="http://datex2.eu/schema/3/locationExtension">
  <ns2:payload xsi:type="ns3:EnergyInfrastructureTablePublication" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <ns2:publicationTime>2026-04-16T00:00:00Z</ns2:publicationTime>
    <ns3:energyInfrastructureTable id="table-1">
      <ns3:energyInfrastructureSite id="DESTA">
        <ns1:locationReference xsi:type="ns1:PointLocation">
          <ns1:coordinatesForDisplay>
            <ns1:latitude>50.756725</ns1:latitude>
            <ns1:longitude>6.150348</ns1:longitude>
          </ns1:coordinatesForDisplay>
          <ns1:pointByCoordinates>
            <ns1:pointCoordinates>
              <ns1:latitude>50.756725</ns1:latitude>
              <ns1:longitude>6.150348</ns1:longitude>
            </ns1:pointCoordinates>
          </ns1:pointByCoordinates>
          <ns10:_locationReferenceExtension>
            <ns10:facilityLocation>
              <ns10:address>
                <ns10:postcode>52078</ns10:postcode>
                <ns10:city>
                  <ns10:values>
                    <ns10:value lang="de">Aachen</ns10:value>
                  </ns10:values>
                </ns10:city>
                <ns10:addressLine order="1" type="street">
                  <ns10:text>
                    <ns10:values>
                      <ns10:value lang="de">Trierer Str.</ns10:value>
                    </ns10:values>
                  </ns10:text>
                </ns10:addressLine>
                <ns10:addressLine order="2" type="houseNumber">
                  <ns10:text>
                    <ns10:values>
                      <ns10:value lang="de">501</ns10:value>
                    </ns10:values>
                  </ns10:text>
                </ns10:addressLine>
              </ns10:address>
            </ns10:facilityLocation>
          </ns10:_locationReferenceExtension>
        </ns1:locationReference>
        <ns3:operator id="DESTA">
          <ns3:name>
            <ns3:values>
              <ns3:value lang="en">DESTA</ns3:value>
            </ns3:values>
          </ns3:name>
        </ns3:operator>
        <ns3:energyInfrastructureStation id="DESTAS0101">
          <ns3:numberOfRefillPoints>2</ns3:numberOfRefillPoints>
          <ns3:refillPoint id="DESTAE010101" xsi:type="ns3:ElectricChargingPoint" />
          <ns3:refillPoint id="DESTAE010102" xsi:type="ns3:ElectricChargingPoint" />
        </ns3:energyInfrastructureStation>
      </ns3:energyInfrastructureSite>
    </ns3:energyInfrastructureTable>
  </ns2:payload>
</ns2:d2Payload>
"""
    return gzip.compress(payload.encode("utf-8"))


def _ladenetz_dynamic_xml_payload() -> bytes:
    payload = """<?xml version="1.0" encoding="UTF-8"?>
<ns2:messageContainer
    xmlns="http://datex2.eu/schema/3/common"
    xmlns:ns1="http://datex2.eu/schema/3/facilities"
    xmlns:ns2="http://datex2.eu/schema/3/messageContainer"
    xmlns:ns3="http://datex2.eu/schema/3/energyInfrastructure"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <ns2:payload>
    <ns2:dynamicInformation>
      <ns3:energyInfrastructureSiteStatus>
        <ns1:reference id="DESTA" />
        <ns3:energyInfrastructureStationStatus>
          <ns1:reference id="DESTAS0101" />
          <ns3:refillPointStatus xsi:type="ns3:ElectricChargingPointStatus">
            <ns1:reference id="DESTAE010101" />
            <ns1:lastUpdated>2026-04-16T08:00:00Z</ns1:lastUpdated>
            <ns3:status>charging</ns3:status>
          </ns3:refillPointStatus>
          <ns3:refillPointStatus xsi:type="ns3:ElectricChargingPointStatus">
            <ns1:reference id="DESTAE010102" />
            <ns1:lastUpdated>2026-04-16T08:00:00Z</ns1:lastUpdated>
            <ns3:status>available</ns3:status>
          </ns3:refillPointStatus>
        </ns3:energyInfrastructureStationStatus>
      </ns3:energyInfrastructureSiteStatus>
    </ns2:dynamicInformation>
  </ns2:payload>
</ns2:messageContainer>
"""
    return gzip.compress(payload.encode("utf-8"))


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


def test_load_dynamic_subscription_ids_reads_registry(tmp_path: Path):
    registry_path = tmp_path / "subscriptions.json"
    registry_path.write_text(
        json.dumps(
            {
                "m8mit": {
                    "subscription_id": "980986232691372032",
                    "static_subscription_id": "980986244745637888",
                },
                "wirelane": {
                    "static_subscription_id": "980986448760786944",
                },
            }
        ),
        encoding="utf-8",
    )

    assert build_data.load_dynamic_subscription_ids(registry_path) == {
        "m8mit": "980986232691372032",
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


def test_load_registry_datex_publications_reads_active_pairs(tmp_path: Path):
    registry_path = tmp_path / "subscriptions.json"
    registry_path.write_text(
        json.dumps(
            {
                "wirelane": {
                    "display_name": "wirelane",
                    "publisher": "Wirelane GmbH",
                    "subscription_id": "980986434407878656",
                    "publication_id": "876587237907525632",
                    "access_mode": "auth",
                    "static_subscription_id": "980986448760786944",
                    "static_publication_id": "869246425829892096",
                    "static_access_mode": "auth",
                },
                "eliso": {
                    "display_name": "eliso",
                    "publisher": "eliso GmbH",
                    "subscription_id": "980986474933399552",
                    "publication_id": "843502085052710912",
                    "access_mode": "auth",
                    "static_subscription_id": "980986489051262976",
                    "static_publication_id": "843477276990078976",
                    "static_access_mode": "auth",
                },
            }
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "provider-configs.json"
    config_path.write_text(
        json.dumps(
            {
                "providers": [
                    {
                        "uid": "wirelane",
                        "display_name": "wirelane",
                        "publisher": "Wirelane GmbH",
                        "feeds": {
                            "static": {
                                "publication_id": "869246425829892096",
                                "data_model": build_data.DATEX_V3_DATA_MODEL,
                                "access_mode": "auth",
                                "content_data": {"accessUrl": "https://wirelane.example/static"},
                            },
                            "dynamic": {
                                "publication_id": "876587237907525632",
                                "data_model": build_data.DATEX_V3_DATA_MODEL,
                                "access_mode": "auth",
                                "content_data": {"accessUrl": "https://wirelane.example/dynamic"},
                            },
                        },
                    },
                    {
                        "uid": "eliso",
                        "display_name": "eliso",
                        "publisher": "eliso GmbH",
                        "feeds": {
                            "static": {
                                "publication_id": "843477276990078976",
                                "data_model": "https://w3id.org/mdp/schema/data_model#MODEL_OTHER",
                                "access_mode": "auth",
                            },
                            "dynamic": {
                                "publication_id": "843502085052710912",
                                "data_model": "https://w3id.org/mdp/schema/data_model#MODEL_OTHER",
                                "access_mode": "auth",
                            },
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    publications = build_data.load_registry_datex_publications(
        subscription_path=registry_path,
        config_path=config_path,
    )

    assert publications == [
        {
            "uid": "wirelane",
            "name": "wirelane",
            "operator_patterns": tuple(sorted(build_data.operator_tokens("wirelane", "wirelane", "Wirelane GmbH"))),
            "static_publication_id": "869246425829892096",
            "dynamic_publication_id": "876587237907525632",
            "static_access_mode": "auth",
            "dynamic_access_mode": "auth",
            "static_subscription_id": "980986448760786944",
            "dynamic_subscription_id": "980986434407878656",
            "static_access_url": "https://wirelane.example/static",
            "dynamic_access_url": "https://wirelane.example/dynamic",
        }
    ]


def test_load_provider_context_by_static_publication_reads_display_name_and_publisher(tmp_path: Path):
    config_path = tmp_path / "provider-configs.json"
    config_path.write_text(
        json.dumps(
            {
                "providers": [
                    {
                        "uid": "audi_hub_energy_tables",
                        "display_name": "audi hub energy tables",
                        "publisher": "Audi AG",
                        "feeds": {
                            "static": {
                                "publication_id": "980858103788171264",
                            }
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    assert build_data.load_provider_context_by_static_publication(config_path) == {
        "980858103788171264": {
            "uid": "audi_hub_energy_tables",
            "display_name": "audi hub energy tables",
            "publisher": "Audi AG",
        }
    }


def test_resolve_content_access_url_uses_provider_description_query():
    content_data = {
        "accessUrl": "https://api.spirii.com/v2/afir/energy-infrastructure-tables",
        "description": (
            "Static data\n"
            "GET /v2/afir/energy-infrastructure-tables?customerIds=128650\n"
            "Returns a DATEX II v3 payload."
        ),
    }

    assert build_data.resolve_content_access_url(content_data) == (
        "https://api.spirii.com/v2/afir/energy-infrastructure-tables?customerIds=128650"
    )


def test_fetch_mobilithek_access_token_reads_secret_files_when_env_missing(tmp_path: Path, monkeypatch):
    user_file = tmp_path / "mobilithek_user.txt"
    password_file = tmp_path / "mobilithek_pwd.txt"
    user_file.write_text("raphael@example.com\n", encoding="utf-8")
    password_file.write_text("top-secret\n", encoding="utf-8")
    monkeypatch.delenv(build_data.MOBILITHEK_USERNAME_ENV, raising=False)
    monkeypatch.delenv(build_data.MOBILITHEK_PASSWORD_ENV, raising=False)

    captured: dict[str, object] = {}

    class DummyResponse:
        def json(self):
            return {"access_token": "token-from-file"}

    def fake_request_with_retries(method, url, session, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["data"] = kwargs.get("data")
        return DummyResponse()

    monkeypatch.setattr(build_data, "request_with_retries", fake_request_with_retries)

    token = build_data.fetch_mobilithek_access_token(
        session=object(),
        username_file=user_file,
        password_file=password_file,
    )

    assert token == "token-from-file"
    assert captured["method"] == "POST"
    assert captured["url"] == build_data.MOBILITHEK_TOKEN_URL
    assert captured["data"] == {
        "grant_type": "password",
        "client_id": "Platform",
        "username": "raphael@example.com",
        "password": "top-secret",
    }


def test_should_attempt_static_payload_fetch_accepts_direct_access_url_without_subscription():
    assert build_data.should_attempt_static_payload_fetch(
        {"status": "ok", "is_accessible": False},
        fallback_url="https://provider.example/static",
    )

    assert not build_data.should_attempt_static_payload_fetch(
        {"status": "ok", "is_accessible": False},
    )


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


def test_fetch_mobilithek_static_payload_with_probe_uses_direct_access_url_before_mtls(monkeypatch):
    attempted_urls: list[str] = []

    def fake_request_with_retries(method, url, session, **kwargs):
        attempted_urls.append(url)
        if url == "https://provider.example/static":
            class DummyResponse:
                content = json.dumps({"payload": {"source": "direct"}}).encode("utf-8")

            return DummyResponse()
        raise RuntimeError("fetch_failed")

    def fake_fetch_mtls_subscription_payload(*, subscription_id: str):
        raise AssertionError(f"mTLS should not be used: {subscription_id}")

    monkeypatch.setattr(build_data, "request_with_retries", fake_request_with_retries)
    monkeypatch.setattr(
        build_data,
        "fetch_mobilithek_subscription_payload_with_mtls",
        fake_fetch_mtls_subscription_payload,
    )

    payload, access_mode_used, fetch_error = build_data.fetch_mobilithek_static_payload_with_probe(
        session=object(),
        publication_id="980858103788171264",
        preferred_access_mode="auth",
        access_token="token",
        subscription_id="981605283809447936",
        fallback_url="https://provider.example/static",
    )

    assert payload == {"payload": {"source": "direct"}}
    assert access_mode_used == "direct_access_url"
    assert fetch_error is None
    assert attempted_urls == [
        build_data.MOBILITHEK_PUBLICATION_FILE_URL.format(publication_id="980858103788171264"),
        build_data.MOBILITHEK_PUBLICATION_PUBLIC_FILE_URL.format(publication_id="980858103788171264"),
        "https://provider.example/static",
    ]


def test_build_fast_charger_frame_aggregates_bnetza_api_operator_aliases():
    raw_df = pd.DataFrame(
        [
            {
                "Ladeeinrichtungs-ID": "1000001",
                "Betreiber": "BP Europa SE",
                "Anzeigename (Karte)": "",
                "Status": "In Betrieb",
                "Art der Ladeeinrichtung": "Schnellladeeinrichtung",
                "Anzahl Ladepunkte": "2",
                "Nennleistung Ladeeinrichtung [kW]": "300",
                "Inbetriebnahmedatum": "2025-01-01",
                "Straße": "Hauptstraße",
                "Hausnummer": "1",
                "Postleitzahl": "10115",
                "Ort": "Berlin",
                "Breitengrad": "52,5200",
                "Längengrad": "13,4050",
                "Steckertypen1": "DC Fahrzeugkupplung Typ Combo 2 (CCS)",
                "Nennleistung Stecker1": "300",
                "EVSE-ID1": "DEALLEGO001272*1",
                "EVSE-ID2": "DEALLEGO001272*2",
            }
        ]
    )

    fast_df = build_data.build_fast_charger_frame(
        raw_df,
        min_power_kw=50.0,
        bnetza_api_station_aliases={
            "1000001": ["BP Europa SE", "Aral Pulse", "BP Europa"],
        },
    )

    assert len(fast_df) == 1
    row = fast_df.iloc[0]
    assert row["bnetza_ladestation_ids"] == ["1000001"]
    assert row["operator_aliases"] == ["BP Europa SE", "Aral Pulse", "BP Europa"]


def test_build_full_registry_station_frame_reuses_legacy_fast_station_ids_and_keeps_full_point_count():
    raw_df = pd.DataFrame(
        [
            {
                "Ladeeinrichtungs-ID": "1000001",
                "Betreiber": "Example Operator",
                "Status": "In Betrieb",
                "Art der Ladeeinrichtung": "Schnellladeeinrichtung",
                "Anzahl Ladepunkte": "2",
                "Nennleistung Ladeeinrichtung [kW]": "150",
                "Straße": "Musterstraße",
                "Hausnummer": "1",
                "Postleitzahl": "10115",
                "Ort": "Berlin",
                "Breitengrad": "52,5200",
                "Längengrad": "13,4050",
            },
            {
                "Ladeeinrichtungs-ID": "1000002",
                "Betreiber": "Example Operator",
                "Status": "Außer Betrieb",
                "Art der Ladeeinrichtung": "Normalladeeinrichtung",
                "Anzahl Ladepunkte": "3",
                "Nennleistung Ladeeinrichtung [kW]": "22",
                "Straße": "Musterstraße",
                "Hausnummer": "1",
                "Postleitzahl": "10115",
                "Ort": "Berlin",
                "Breitengrad": "52,5200",
                "Längengrad": "13,4050",
            },
        ]
    )

    legacy_fast_df = build_data.build_fast_charger_frame(raw_df, min_power_kw=50.0)
    legacy_station_id = str(legacy_fast_df.iloc[0]["station_id"])
    legacy_group_key = build_data._station_group_key(
        legacy_fast_df.iloc[0]["lat"],
        legacy_fast_df.iloc[0]["lon"],
        legacy_fast_df.iloc[0]["operator"],
    )

    full_df = build_data.build_full_registry_station_frame(
        raw_df,
        legacy_station_ids_by_group_key={legacy_group_key: legacy_station_id},
    )
    fast_projection_df = build_data.build_fast_projection_from_full_registry(
        full_df,
        min_power_kw=50.0,
    )

    assert len(full_df) == 1
    assert full_df.iloc[0]["station_id"] == legacy_station_id
    assert bool(full_df.iloc[0]["has_active_record"]) is True
    assert int(full_df.iloc[0]["charging_points_count"]) == 5
    assert float(full_df.iloc[0]["max_power_kw"]) == 75.0

    assert len(fast_projection_df) == 1
    assert fast_projection_df.iloc[0]["station_id"] == legacy_station_id
    assert int(fast_projection_df.iloc[0]["charging_points_count"]) == 5


def test_build_under_power_projection_from_full_registry_keeps_active_sub_threshold_stations():
    full_df = pd.DataFrame(
        [
            {
                "station_id": "slow-active",
                "has_active_record": True,
                "max_power_kw": 22.0,
            },
            {
                "station_id": "fast-active",
                "has_active_record": True,
                "max_power_kw": 50.0,
            },
            {
                "station_id": "slow-inactive",
                "has_active_record": False,
                "max_power_kw": 11.0,
            },
        ]
    )

    projected = build_data.build_under_power_projection_from_full_registry(
        full_df,
        max_power_kw=50.0,
    )

    assert projected["station_id"].tolist() == ["slow-active"]
    assert "has_active_record" not in projected.columns


def test_filter_fast_chargers_with_amenities_keeps_only_rows_with_positive_amenity_count():
    df = pd.DataFrame(
        [
            {"station_id": "station-1", "amenities_total": 3, "max_power_kw": 150.0},
            {"station_id": "station-2", "amenities_total": 0, "max_power_kw": 300.0},
            {"station_id": "station-3", "amenities_total": "", "max_power_kw": 50.0},
            {"station_id": "station-4", "amenities_total": 1, "max_power_kw": 75.0},
        ]
    )

    filtered = build_data.filter_fast_chargers_with_amenities(df)

    assert filtered["station_id"].tolist() == ["station-1", "station-4"]


def test_score_static_site_to_station_uses_station_operator_aliases():
    site = build_data.DatexStaticSite(
        site_id="site-1",
        station_ids=("station-1",),
        lat=52.5200,
        lon=13.4050,
        postcode="10115",
        city="Berlin",
        address="Completely Different 99",
        operator_name="Aral Pulse",
        total_evses=2,
        evse_ids=(),
    )
    station_row = {
        "station_id": "station-1",
        "lat": 52.5209,
        "lon": 13.4050,
        "postcode": "10115",
        "city": "Berlin",
        "address": "Another Street 1",
        "operator": "BP Europa SE",
        "operator_aliases": ["Aral Pulse"],
        "charging_points_count": 2,
        "evse_ids": [],
        "bnetza_display_name": "",
    }

    accepted, _, _, details = build_data.score_static_site_to_station(
        site,
        station_row,
        publisher="Eco-Movement",
        max_distance_m=200.0,
    )

    assert accepted is True
    assert details["operator_similarity"] == 1.0

    station_row["operator_aliases"] = []
    accepted_without_alias, _, _, details_without_alias = build_data.score_static_site_to_station(
        site,
        station_row,
        publisher="Eco-Movement",
        max_distance_m=200.0,
    )

    assert accepted_without_alias is False
    assert details_without_alias["operator_similarity"] == 0.0


def test_score_static_site_to_station_rejects_close_candidate_with_postcode_conflict_only():
    site = build_data.DatexStaticSite(
        site_id="site-enio-rosenheim",
        station_ids=("KathreinECSim02", "KathreinECSim03", "KathreinECSim01"),
        lat=47.85713,
        lon=12.11810,
        postcode="83022",
        city="Rosenheim",
        address="Wittelsbacherstraße",
        operator_name="",
        total_evses=6,
        evse_ids=(),
    )
    station_row = {
        "station_id": "station-1",
        "lat": 47.857127,
        "lon": 12.118105,
        "postcode": "83026",
        "city": "Rosenheim",
        "address": "Äußere Münchenerstraße 70a 83026 Rosenheim",
        "operator": "Erich Vinzenz KFZ-Werkstatt",
        "operator_aliases": [],
        "charging_points_count": 1,
        "evse_ids": [],
        "bnetza_display_name": "",
    }

    accepted, _, _, details = build_data.score_static_site_to_station(
        site,
        station_row,
        publisher="ENIO GmbH",
        max_distance_m=200.0,
    )

    assert accepted is False
    assert details["postcode_match"] is False
    assert details["city_match"] is True
    assert details["address_match"] is False
    assert details["operator_similarity"] == 0.0


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


def test_parse_datex_static_sites_supports_ladenetz_xml_payload():
    payload = build_data.decode_json_bytes(_ladenetz_static_xml_payload())

    sites = build_data.parse_datex_static_sites(payload)

    assert len(sites) == 1
    assert sites[0].site_id == "DESTA"
    assert sites[0].station_ids == ("DESTAS0101",)
    assert sites[0].evse_ids == ("DESTAE010101", "DESTAE010102")
    assert sites[0].postcode == "52078"
    assert sites[0].city == "Aachen"
    assert sites[0].address == "Trierer Str. 501"
    assert sites[0].operator_name == "DESTA"
    assert sites[0].total_evses == 2


def test_parse_datex_static_sites_supports_message_container_wrapper():
    payload = {
        "messageContainer": {
            "payload": [
                {
                    "aegiEnergyInfrastructureTablePublication": {
                        "energyInfrastructureTable": [
                            {
                                "energyInfrastructureSite": [
                                    {
                                        "idG": "1302",
                                        "name": {"values": [{"lang": "de", "value": "Workspace A81 - New Office"}]},
                                        "locationReference": {
                                            "locPointLocation": {
                                                "pointByCoordinates": {
                                                    "pointCoordinates": {
                                                        "latitude": 51.4346197,
                                                        "longitude": 7.002928,
                                                    }
                                                },
                                                "locLocationExtensionG": {
                                                    "facilityLocation": {
                                                        "address": {
                                                            "postcode": "45130",
                                                            "city": {"values": [{"lang": "de", "value": "Essen"}]},
                                                            "addressLine": [
                                                                {
                                                                    "order": 0,
                                                                    "type": {"value": "street"},
                                                                    "text": {
                                                                        "values": [
                                                                            {"lang": "de", "value": "Alfredstraße 81"}
                                                                        ]
                                                                    },
                                                                }
                                                            ],
                                                        }
                                                    }
                                                },
                                            }
                                        },
                                        "energyInfrastructureStation": [
                                            {
                                                "idG": "7234",
                                                "numberOfRefillPoints": 2,
                                                "operator": {
                                                    "afacAnOrganisation": {
                                                        "name": {
                                                            "values": [
                                                                {
                                                                    "lang": "de",
                                                                    "value": "E.ON Drive Infrastructure GmbH",
                                                                }
                                                            ]
                                                        }
                                                    }
                                                },
                                                "refillPoint": [
                                                    {"aegiElectricChargingPoint": {"idG": "DE*UFC*E00159*1"}},
                                                    {"aegiElectricChargingPoint": {"idG": "DE*UFC*E00159*2"}},
                                                ],
                                            }
                                        ],
                                    }
                                ]
                            }
                        ]
                    }
                }
            ]
        }
    }

    sites = build_data.parse_datex_static_sites(payload)

    assert len(sites) == 1
    assert sites[0].site_id == "1302"
    assert sites[0].station_ids == ("7234",)
    assert sites[0].evse_ids == ("DEUFCE001591", "DEUFCE001592")
    assert sites[0].postcode == "45130"
    assert sites[0].city == "Essen"
    assert sites[0].address == "Alfredstraße 81"
    assert sites[0].operator_name == "E.ON Drive Infrastructure GmbH"
    assert sites[0].total_evses == 2


def test_parse_datex_dynamic_states_supports_ladenetz_xml_payload():
    payload = build_data.decode_json_bytes(_ladenetz_dynamic_xml_payload())

    states = build_data.parse_datex_dynamic_states(payload)

    assert sorted(states.keys()) == ["DESTA"]
    assert states["DESTA"]["station_refs"] == {"DESTAS0101"}
    assert states["DESTA"]["evses"]["DESTAE010101"]["status"] == "OCCUPIED"
    assert states["DESTA"]["evses"]["DESTAE010102"]["status"] == "AVAILABLE"


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
