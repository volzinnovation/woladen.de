from __future__ import annotations

import csv
import gzip
import json
import os
import sqlite3
import tarfile
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import backend.fetcher as fetcher_module

from fastapi.testclient import TestClient

from backend.api import create_app
from backend.archive import DailyResponseArchiver
from backend.config import AppConfig, load_env_file
from backend.datex import decode_json_payload, extract_dynamic_facts
from backend.fetcher import CurlFetcher
from backend.loaders import load_evse_matches, load_provider_targets, load_site_matches
from backend.models import FetchResponse
from backend.service import IngestionService
from backend.store import LiveStore
from backend.subscriptions import (
    SubscriptionOffer,
    build_subscription_registry,
    load_active_dyn_datex_subscription_offers,
)


def _parse_dt(value: str) -> datetime:
    text = value
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _write_provider_fixture(path: Path) -> None:
    payload = {
        "providers": [
            {
                "uid": "qwello",
                "display_name": "Qwello",
                "publisher": "Qwello Deutschland GmbH",
                "feeds": {
                    "dynamic": {
                        "publication_id": "972966368902897664",
                        "access_mode": "noauth",
                        "delta_delivery": False,
                        "content_data": {"accessUrl": None, "deltaDelivery": False, "retentionPeriod": 45},
                    }
                },
            },
            {
                "uid": "ampeco",
                "display_name": "AMPECO",
                "publisher": "AMPECO",
                "feeds": {
                    "dynamic": {
                        "publication_id": "973271761172537344",
                        "access_mode": "auth",
                        "delta_delivery": True,
                        "content_data": {"accessUrl": None, "deltaDelivery": True},
                    }
                },
            },
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _write_matches_fixture(path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "provider_uid",
                "site_id",
                "station_id",
                "score",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "provider_uid": "qwello",
                "site_id": "SITE-1",
                "station_id": "station-1",
                "score": "-30.0",
            }
        )


def _write_chargers_fixture(path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "station_id",
                "operator",
                "address",
                "postcode",
                "city",
                "lat",
                "lon",
                "charging_points_count",
                "max_power_kw",
                "detail_source_uid",
                "datex_site_id",
                "datex_station_ids",
                "datex_charge_point_ids",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "station_id": "station-1",
                "operator": "Qwello",
                "address": "Teststr. 1",
                "postcode": "10115",
                "city": "Berlin",
                "lat": "52.531",
                "lon": "13.3849",
                "charging_points_count": "2",
                "max_power_kw": "150",
                "detail_source_uid": "mobilithek_qwello_static",
                "datex_site_id": "SITE-1",
                "datex_station_ids": "",
                "datex_charge_point_ids": "",
            }
        )


def _write_geojson_fixture(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [13.3849, 52.531]},
                        "properties": {"station_id": "station-1"},
                    },
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [13.4, 52.54]},
                        "properties": {"station_id": "station-2"},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )


def _write_subscription_registry(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "ampeco": {
                    "enabled": True,
                    "fetch_kind": "mtls_subscription",
                    "subscription_id": "2000001",
                }
            }
        ),
        encoding="utf-8",
    )


def _write_active_subscription_provider_fixture(path: Path) -> None:
    payload = {
        "providers": [
            {
                "uid": "elu_mobility",
                "display_name": "ELU Mobility",
                "publisher": "ELU Mobility",
                "feeds": {
                    "dynamic": {
                        "publication_id": "971513500454850560",
                        "access_mode": "auth",
                        "data_model": "https://w3id.org/mdp/schema/data_model#DATEX_2_V3",
                        "title": "AFIR-recharging-dyn-elu-mobility",
                    }
                },
            },
            {
                "uid": "smatrics",
                "display_name": "SMATRICS",
                "publisher": "SMATRICS",
                "feeds": {
                    "dynamic": {
                        "publication_id": "961319990963605504",
                        "access_mode": "noauth",
                        "data_model": "https://w3id.org/mdp/schema/data_model#DATEX_2_V3",
                        "title": "AFIR-recharging-dyn-SMATRICS",
                    }
                },
            },
            {
                "uid": "eliso",
                "display_name": "eliso",
                "publisher": "eliso GmbH",
                "feeds": {
                    "dynamic": {
                        "publication_id": "843502085052710912",
                        "access_mode": "auth",
                        "data_model": "https://w3id.org/mdp/schema/data_model#MODEL_OTHER",
                        "title": "eliso AFIR Dynamic Data (Station & Point)",
                    }
                },
            },
            {
                "uid": "m8mit",
                "display_name": "m8mit",
                "publisher": "msu solutions GmbH",
                "feeds": {
                    "dynamic": {
                        "publication_id": "970388804493828096",
                        "access_mode": "auth",
                        "data_model": "https://w3id.org/mdp/schema/data_model#DATEX_2_V3",
                        "title": "AFIR-recharging-dyn-m8mit-v2",
                    }
                },
            },
            {
                "uid": "wirelane",
                "display_name": "Wirelane",
                "publisher": "Wirelane GmbH",
                "feeds": {
                    "dynamic": {
                        "publication_id": "876587237907525632",
                        "access_mode": "auth",
                        "data_model": "https://w3id.org/mdp/schema/data_model#DATEX_2_V3",
                        "title": "AFIR-recharging-dyn-Wirelane",
                    }
                },
            },
        ]
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _dynamic_payload(
    *,
    status: str = "AVAILABLE",
    operation_status: str | None = None,
    price_kwh: float | None = 0.59,
    second_evse_status: str = "OCCUPIED",
    second_evse_operation_status: str | None = None,
    timestamp: str = "2026-04-15T08:00:00+00:00",
    point_wrapper: str = "aegiElectricChargingPointStatus",
) -> dict:
    energy_rate = []
    if price_kwh is not None:
        energy_rate = [
            {
                "applicableCurrency": ["EUR"],
                "energyPrice": [
                    {
                        "value": price_kwh,
                        "priceType": {"value": "pricePerKwh"},
                    }
                ],
            }
        ]

    return {
        "messageContainer": {
            "payload": [
                {
                    "aegiEnergyInfrastructureStatusPublication": {
                        "energyInfrastructureSiteStatus": [
                            {
                                "reference": {"idG": "SITE-1"},
                                "lastUpdated": timestamp,
                                "energyInfrastructureStationStatus": [
                                    {
                                        "reference": {"idG": "STATION-REF-1"},
                                        "lastUpdated": timestamp,
                                        "energyRate": energy_rate,
                                        "refillPointStatus": [
                                            {
                                                point_wrapper: {
                                                    "reference": {"idG": "DE*QWE*E1"},
                                                    "status": {"value": status},
                                                    "operationStatus": (
                                                        {"value": operation_status} if operation_status is not None else None
                                                    ),
                                                    "lastUpdated": timestamp,
                                                }
                                            },
                                            {
                                                point_wrapper: {
                                                    "reference": {"idG": "DE*QWE*E2"},
                                                    "status": {"value": second_evse_status},
                                                    "operationStatus": (
                                                        {"value": second_evse_operation_status}
                                                        if second_evse_operation_status is not None
                                                        else None
                                                    ),
                                                    "lastUpdated": timestamp,
                                                }
                                            },
                                        ],
                                    }
                                ],
                            }
                        ]
                    }
                }
            ]
        }
    }


def _dynamic_payload_with_energy_rate_update(
    *,
    status: str = "AVAILABLE",
    price_kwh: float = 0.59,
    price_minute: float | None = None,
    next_slots: list[dict] | None = None,
    supplemental_status: list[str] | None = None,
    station_supplemental_status: list[str] | None = None,
    site_supplemental_status: list[str] | None = None,
    timestamp: str = "2026-04-15T08:00:00+00:00",
) -> dict:
    energy_price = [
        {
            "priceType": {"value": "pricePerKWh"},
            "value": price_kwh,
            "taxIncluded": True,
        }
    ]
    if price_minute is not None:
        energy_price.append(
            {
                "priceType": {"value": "pricePerMinute"},
                "value": price_minute,
                "taxIncluded": True,
            }
        )

    return {
        "messageContainer": {
            "payload": [
                {
                    "aegiEnergyInfrastructureStatusPublication": {
                        "energyInfrastructureSiteStatus": [
                            {
                                "reference": {"idG": "SITE-1"},
                                "lastUpdated": timestamp,
                                "supplementalFacilityStatus": site_supplemental_status or [],
                                "energyInfrastructureStationStatus": [
                                    {
                                        "reference": {"idG": "STATION-REF-1"},
                                        "lastUpdated": timestamp,
                                        "supplementalFacilityStatus": station_supplemental_status or [],
                                        "refillPointStatus": [
                                            {
                                                "aegiElectricChargingPointStatus": {
                                                    "reference": {"idG": "DE*QWE*E1"},
                                                    "status": {"value": status},
                                                    "lastUpdated": timestamp,
                                                    "nextAvailableChargingSlots": next_slots or [],
                                                    "supplementalFacilityStatus": supplemental_status or [],
                                                    "energyRateUpdate": [
                                                        {
                                                            "lastUpdated": timestamp,
                                                            "energyRateReference": {
                                                                "targetClass": "EnergyRate",
                                                                "idG": "RATE-1",
                                                            },
                                                            "energyPrice": energy_price,
                                                        }
                                                    ],
                                                }
                                            }
                                        ],
                                    }
                                ],
                            }
                        ]
                    }
                }
            ]
        }
    }


def _direct_payload_envelope(
    *,
    status: str = "charging",
    second_evse_status: str = "available",
    timestamp: str = "2026-04-15T08:00:00+00:00",
) -> dict:
    publication = json.loads(
        json.dumps(
            _dynamic_payload(
                status=status,
                second_evse_status=second_evse_status,
                timestamp=timestamp,
                point_wrapper="aegiRefillPointStatus",
            )["messageContainer"]["payload"][0]["aegiEnergyInfrastructureStatusPublication"]
        )
    )
    return {
        "payload": {
            "versionG": "3.5",
            "modelBaseVersionG": "3",
            "profileNameG": "AFIR Energy Infrastructure",
            "profileVersionG": "01-00-00",
            "aegiEnergyInfrastructureStatusPublication": publication,
        }
    }


def _ladenetz_xml_payload(timestamp: str = "2026-04-15T08:00:00Z") -> bytes:
    payload = f"""<?xml version="1.0" encoding="UTF-8"?>
<ns2:messageContainer
    xmlns="http://datex2.eu/schema/3/common"
    xmlns:ns1="http://datex2.eu/schema/3/facilities"
    xmlns:ns2="http://datex2.eu/schema/3/messageContainer"
    xmlns:ns3="http://datex2.eu/schema/3/energyInfrastructure"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <ns2:payload>
    <ns2:dynamicInformation>
      <ns3:energyInfrastructureSiteStatus>
        <ns1:reference id="DESTA" targetClass="fac:FacilityObject" version="2026-04-16" />
        <ns3:energyInfrastructureStationStatus>
          <ns1:reference id="DESTAS0101" targetClass="fac:FacilityObject" version="2026-04-16" />
          <ns3:isAvailable>true</ns3:isAvailable>
          <ns3:refillPointStatus xsi:type="ns3:ElectricChargingPointStatus">
            <ns1:reference id="DESTAE010101" targetClass="fac:FacilityObject" version="2026-04-16" />
            <ns1:lastUpdated>{timestamp}</ns1:lastUpdated>
            <ns3:status>charging</ns3:status>
          </ns3:refillPointStatus>
          <ns3:refillPointStatus xsi:type="ns3:ElectricChargingPointStatus">
            <ns1:reference id="DESTAE010102" targetClass="fac:FacilityObject" version="2026-04-16" />
            <ns1:lastUpdated>{timestamp}</ns1:lastUpdated>
            <ns3:status>available</ns3:status>
          </ns3:refillPointStatus>
        </ns3:energyInfrastructureStationStatus>
      </ns3:energyInfrastructureSiteStatus>
    </ns2:dynamicInformation>
  </ns2:payload>
</ns2:messageContainer>
"""
    return gzip.compress(payload.encode("utf-8"))


def _eliso_dynamic_payload() -> dict:
    return {
        "evses": [
            {
                "evseId": "DE*ELI*E3603098",
                "adhoc_price": 0.49,
                "blocking_fee": 0.1,
                "operator_name": "eliso GmbH",
                "operational_status": "Operational",
                "availability_status": "Not in use",
                "mobilithek_last_updated_dts": "2026-04-16T09:04:39.561456+00:00",
            },
            {
                "evseId": "DE*ELI*E3603099",
                "adhoc_price": 0.49,
                "blocking_fee": 0.1,
                "operator_name": "eliso GmbH",
                "operational_status": "Non-operational",
                "availability_status": "In use",
                "mobilithek_last_updated_dts": "2026-04-16T09:05:39.561456+00:00",
            },
        ]
    }


class MockFetcher:
    def __init__(self, responses):
        self.responses = responses

    def fetch(self, provider):
        response = self.responses[provider.provider_uid]
        if isinstance(response, Exception):
            raise response
        return response


def _build_service(app_config, fetcher):
    _write_provider_fixture(app_config.provider_config_path)
    _write_matches_fixture(app_config.site_match_path)
    _write_chargers_fixture(app_config.chargers_csv_path)
    _write_geojson_fixture(app_config.chargers_geojson_path)
    store = LiveStore(app_config)
    return IngestionService(app_config, store=store, fetcher=fetcher)


def test_load_provider_targets_defaults_to_noauth_enabled(app_config):
    _write_provider_fixture(app_config.provider_config_path)
    providers = load_provider_targets(app_config.provider_config_path)
    assert [provider.provider_uid for provider in providers] == ["ampeco", "qwello"]
    assert providers[0].enabled is False
    assert providers[0].delta_delivery is True
    assert providers[1].enabled is True
    assert providers[1].fetch_kind == "publication_file_noauth"
    assert providers[1].delta_delivery is False
    assert providers[1].retention_period_minutes == 45


def test_load_provider_targets_merges_subscription_registry(app_config):
    _write_provider_fixture(app_config.provider_config_path)
    _write_subscription_registry(app_config.subscription_registry_path)
    providers = load_provider_targets(
        app_config.provider_config_path,
        subscription_registry_path=app_config.subscription_registry_path,
    )
    ampeco = [provider for provider in providers if provider.provider_uid == "ampeco"][0]
    assert ampeco.enabled is True
    assert ampeco.fetch_kind == "mtls_subscription"
    assert ampeco.subscription_id == "2000001"
    assert ampeco.fetch_url.endswith("subscriptionID=2000001")


def test_load_provider_targets_adds_synthetic_direct_url_provider(app_config):
    app_config.provider_config_path.write_text(json.dumps({"providers": []}), encoding="utf-8")
    app_config.subscription_registry_path.write_text(
        json.dumps(
            {
                "mobidata_bw_datex": {
                    "display_name": "MobiData BW DATEX II",
                    "publisher": "MobiData BW",
                    "enabled": True,
                    "fetch_kind": "direct_url",
                    "fetch_url": "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/realtime",
                    "publication_id": "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/realtime",
                    "access_mode": "noauth",
                }
            }
        ),
        encoding="utf-8",
    )

    providers = load_provider_targets(
        app_config.provider_config_path,
        subscription_registry_path=app_config.subscription_registry_path,
    )

    assert len(providers) == 1
    provider = providers[0]
    assert provider.provider_uid == "mobidata_bw_datex"
    assert provider.fetch_kind == "direct_url"
    assert provider.fetch_url == "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/realtime"
    assert provider.enabled is True


def test_load_site_matches_derives_bundle_datex_site_matches(app_config):
    _write_matches_fixture(app_config.site_match_path)
    _write_chargers_fixture(app_config.chargers_csv_path)
    matches = load_site_matches(app_config.site_match_path, app_config.chargers_csv_path)
    by_key = {(item.provider_uid, item.site_id): item for item in matches}
    assert ("qwello", "SITE-1") in by_key
    assert by_key[("qwello", "SITE-1")].station_id == "station-1"
    assert by_key[("qwello", "SITE-1")].score == -30.0


def test_load_site_matches_derives_enbw_bundle_site_matches(app_config):
    app_config.site_match_path.write_text("provider_uid,site_id,station_id,score\n", encoding="utf-8")
    with app_config.chargers_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "station_id",
                "operator",
                "address",
                "postcode",
                "city",
                "lat",
                "lon",
                "charging_points_count",
                "max_power_kw",
                "detail_source_uid",
                "datex_site_id",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "station_id": "enbw-station-1",
                "operator": "EnBW mobility+ AG und Co.KG",
                "address": "Example 1",
                "postcode": "70173",
                "city": "Stuttgart",
                "lat": "48.7784",
                "lon": "9.1800",
                "charging_points_count": "2",
                "max_power_kw": "300",
                "detail_source_uid": "mobilithek_enbwmobility_static",
                "datex_site_id": "800018264",
            }
        )
    matches = load_site_matches(app_config.site_match_path, app_config.chargers_csv_path)
    by_key = {(item.provider_uid, item.site_id): item for item in matches}
    assert by_key[("enbwmobility", "800018264")].station_id == "enbw-station-1"


def test_load_evse_matches_infers_eliso_bundle_charge_point_aliases(app_config):
    app_config.chargers_csv_path.write_text(
        "\n".join(
            [
                "station_id,operator,address,postcode,city,lat,lon,charging_points_count,max_power_kw,detail_source_uid,datex_site_id,datex_station_ids,datex_charge_point_ids",
                "station-2,eliso GmbH,Example 2,70174,Stuttgart,48.779,9.181,4,300,mobilithek_monta_static,Eliso GmbH-s1076907,s1076907,3603098|3603099",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    matches = load_evse_matches(app_config.chargers_csv_path)
    by_key = {(item.provider_uid, item.evse_id): item for item in matches}

    assert by_key[("eliso", "3603098")].station_id == "station-2"
    assert by_key[("eliso", "3603098")].site_id == "Eliso GmbH-s1076907"
    assert by_key[("eliso", "3603098")].station_ref == "s1076907"


def test_load_evse_matches_reads_provider_specific_evse_ids_from_static_match_csv(app_config):
    app_config.chargers_csv_path.write_text(
        "\n".join(
            [
                "station_id,operator,address,postcode,city,lat,lon,charging_points_count,max_power_kw,detail_source_uid,datex_site_id,datex_station_ids,datex_charge_point_ids",
                "station-3,EnBW,Example 3,70175,Stuttgart,48.780,9.182,2,300,mobilithek_enbwmobility_static,800018264,ENBW-STATION-1,DEENBWE1|DEENBWE2",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    app_config.site_match_path.write_text(
        "\n".join(
            [
                "provider_uid,site_id,station_id,score,datex_station_ids,datex_charge_point_ids",
                "ladenetz_de_ladestationsdaten,DE1ESS0205,station-3,-30.0,DE1ESS0205,DE1ESE020501|DE1ESE020502",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    matches = load_evse_matches(app_config.chargers_csv_path, app_config.site_match_path)
    by_key = {(item.provider_uid, item.evse_id): item for item in matches}

    assert by_key[("ladenetz_de_ladestationsdaten", "DE1ESE020501")].station_id == "station-3"
    assert by_key[("ladenetz_de_ladestationsdaten", "DE1ESE020501")].site_id == "DE1ESS0205"
    assert by_key[("ladenetz_de_ladestationsdaten", "DE1ESE020501")].station_ref == "DE1ESS0205"


def test_load_active_dyn_datex_subscription_offers_filters_to_auth_datex_docs_subset(app_config):
    _write_active_subscription_provider_fixture(app_config.provider_config_path)
    offers = load_active_dyn_datex_subscription_offers(app_config.provider_config_path)
    assert [(offer.provider_uid, offer.publication_id) for offer in offers] == [
        ("elu_mobility", "971513500454850560"),
        ("m8mit", "970388804493828096"),
        ("wirelane", "876587237907525632"),
    ]


def test_build_subscription_registry_uses_only_active_contracts():
    offers = [
        SubscriptionOffer(
            provider_uid="elu_mobility",
            display_name="ELU Mobility",
            publisher="ELU Mobility",
            publication_id="971513500454850560",
            offer_title="AFIR-recharging-dyn-elu-mobility",
        ),
        SubscriptionOffer(
            provider_uid="wirelane",
            display_name="Wirelane",
            publisher="Wirelane GmbH",
            publication_id="876587237907525632",
            offer_title="AFIR-recharging-dyn-Wirelane",
        ),
    ]
    registry = build_subscription_registry(
        offers,
        [
            {
                "id": "3000001",
                "dataOfferId": "971513500454850560",
                "contractStatus": "ACTIVE",
                "dataOfferTitle": "AFIR-recharging-dyn-elu-mobility",
            },
            {
                "id": "3000002",
                "dataOfferId": "876587237907525632",
                "subscriptionStatus": "REQUESTED",
                "dataOfferTitle": "AFIR-recharging-dyn-Wirelane",
            },
        ],
    )
    assert registry["elu_mobility"]["enabled"] is True
    assert registry["elu_mobility"]["subscription_id"] == "3000001"
    assert registry["wirelane"]["enabled"] is False
    assert registry["wirelane"]["subscription_id"] == ""
    assert registry["mobidata_bw_datex"]["fetch_kind"] == "direct_url"
    assert registry["mobidata_bw_datex"]["static_fetch_url"] == "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/static"


def test_extract_dynamic_facts_parses_status_and_inherited_price():
    facts = extract_dynamic_facts(_dynamic_payload(), "qwello", {"SITE-1": "station-1"})
    assert len(facts) == 2
    assert facts[0].station_id == "station-1"
    assert facts[0].availability_status == "free"
    assert facts[0].operational_status == "AVAILABLE"
    assert facts[0].price.display == "0,59 €/kWh"
    assert facts[1].availability_status == "occupied"
    assert facts[1].operational_status == "CHARGING"


def test_extract_dynamic_facts_keeps_latest_duplicate_evse():
    payload = _dynamic_payload()
    duplicate = payload["messageContainer"]["payload"][0]["aegiEnergyInfrastructureStatusPublication"][
        "energyInfrastructureSiteStatus"
    ][0]["energyInfrastructureStationStatus"][0]["refillPointStatus"]
    duplicate.append(
        {
            "aegiElectricChargingPointStatus": {
                "reference": {"idG": "DE*QWE*E1"},
                "status": {"value": "FAULTED"},
                "lastUpdated": "2026-04-15T09:00:00+00:00",
            }
        }
    )
    facts = extract_dynamic_facts(payload, "qwello", {"SITE-1": "station-1"})
    assert len(facts) == 2
    first = [fact for fact in facts if fact.evse_id == "DEQWEE1"][0]
    assert first.availability_status == "out_of_order"


def test_extract_dynamic_facts_parses_refill_point_status_shape():
    facts = extract_dynamic_facts(
        _dynamic_payload(point_wrapper="aegiRefillPointStatus", status="charging"),
        "enbwmobility",
        {"SITE-1": "station-1"},
    )
    assert len(facts) == 2
    assert facts[0].station_id == "station-1"
    assert facts[0].availability_status == "occupied"


def test_extract_dynamic_facts_parses_direct_payload_envelope():
    facts = extract_dynamic_facts(_direct_payload_envelope(), "mobidata_bw_datex", {"SITE-1": "station-1"})
    assert len(facts) == 2
    assert facts[0].station_id == "station-1"
    assert facts[0].availability_status == "occupied"
    assert facts[0].operational_status == "CHARGING"
    assert facts[0].price.display == "0,59 €/kWh"
    assert facts[1].availability_status == "free"
    assert facts[1].operational_status == "AVAILABLE"


def test_extract_dynamic_facts_parses_ladenetz_xml_payload():
    payload = decode_json_payload(_ladenetz_xml_payload())
    facts = extract_dynamic_facts(payload, "ladenetz_de_ladestationsdaten", {"DESTA": "station-1"})

    assert len(facts) == 2
    by_evse_id = {fact.evse_id: fact for fact in facts}
    charging = by_evse_id["DESTAE010101"]
    available = by_evse_id["DESTAE010102"]
    assert charging.station_id == "station-1"
    assert charging.station_ref == "DESTAS0101"
    assert charging.availability_status == "occupied"
    assert charging.operational_status == "CHARGING"
    assert charging.source_observed_at == "2026-04-15T08:00:00Z"
    assert available.station_id == "station-1"
    assert available.availability_status == "free"
    assert available.operational_status == "AVAILABLE"


def test_extract_dynamic_facts_parses_eliso_generic_payload():
    facts = extract_dynamic_facts(
        _eliso_dynamic_payload(),
        "eliso",
        {},
        {
            "3603098": {
                "station_id": "station-2",
                "site_id": "Eliso GmbH-s1076907",
                "station_ref": "s1076907",
            }
        },
    )
    assert len(facts) == 2
    by_evse_id = {fact.evse_id: fact for fact in facts}
    matched = by_evse_id["DEELIE3603098"]
    unmatched = by_evse_id["DEELIE3603099"]
    assert matched.station_id == "station-2"
    assert matched.site_id == "Eliso GmbH-s1076907"
    assert matched.station_ref == "s1076907"
    assert matched.availability_status == "free"
    assert matched.operational_status == "AVAILABLE"
    assert matched.price.display == "ab 0,49 €/kWh"
    assert matched.price.energy_eur_kwh_min == "0.49"
    assert matched.price.time_eur_min_min == 0.1
    assert unmatched.station_id is None
    assert unmatched.availability_status == "out_of_order"
    assert unmatched.operational_status == "UNKNOWN"


def test_extract_dynamic_facts_prefers_operation_status_and_normalizes_case():
    facts = extract_dynamic_facts(
        _dynamic_payload(
            status="free",
            operation_status="aVaiLaBle",
            second_evse_status="occupied",
            second_evse_operation_status="cHaRgInG",
        ),
        "qwello",
        {"SITE-1": "station-1"},
    )
    assert len(facts) == 2
    assert facts[0].availability_status == "free"
    assert facts[0].operational_status == "AVAILABLE"
    assert facts[1].availability_status == "occupied"
    assert facts[1].operational_status == "CHARGING"


def test_extract_dynamic_facts_maps_unknown_operation_status_to_out_of_order():
    facts = extract_dynamic_facts(
        _dynamic_payload(
            status="unknown",
            operation_status="uNkNoW",
            second_evse_status="unknown",
            second_evse_operation_status="UNKNOW",
        ),
        "qwello",
        {"SITE-1": "station-1"},
    )
    assert len(facts) == 2
    assert facts[0].availability_status == "out_of_order"
    assert facts[0].operational_status == "UNKNOWN"
    assert facts[1].availability_status == "out_of_order"
    assert facts[1].operational_status == "UNKNOWN"


def test_extract_dynamic_facts_parses_energy_rate_update_prices():
    facts = extract_dynamic_facts(
        _dynamic_payload_with_energy_rate_update(price_kwh=0.7, price_minute=0.03),
        "wirelane",
        {"SITE-1": "station-1"},
    )
    assert len(facts) == 1
    assert facts[0].station_id == "station-1"
    assert facts[0].price.display == "ab 0,70 €/kWh"
    assert facts[0].price.currency == "EUR"
    assert facts[0].price.energy_eur_kwh_min == "0.7"
    assert facts[0].price.energy_eur_kwh_max == "0.7"
    assert facts[0].price.time_eur_min_min == 0.03
    assert facts[0].price.time_eur_min_max == 0.03
    assert facts[0].price.quality == "from"


def test_extract_dynamic_facts_parses_next_slots_and_supplemental_status():
    facts = extract_dynamic_facts(
        _dynamic_payload_with_energy_rate_update(
            next_slots=[
                {
                    "expectedAvailableFromTime": "2026-04-15T09:15:00+00:00",
                    "expectedAvailableUntilTime": "2026-04-15T09:45:00+00:00",
                }
            ],
            supplemental_status=["parkingRestricted"],
            station_supplemental_status=["covered"],
            site_supplemental_status=["wheelchairAccessible"],
        ),
        "wirelane",
        {"SITE-1": "station-1"},
    )
    assert len(facts) == 1
    assert facts[0].next_available_charging_slots == [
        {
            "expectedAvailableFromTime": "2026-04-15T09:15:00+00:00",
            "expectedAvailableUntilTime": "2026-04-15T09:45:00+00:00",
        }
    ]
    assert facts[0].supplemental_facility_status == [
        "wheelchairAccessible",
        "covered",
        "parkingRestricted",
    ]


def test_ingestion_persists_price_from_energy_rate_update_payload(app_config):
    payload = json.dumps(_dynamic_payload_with_energy_rate_update(price_kwh=0.5)).encode("utf-8")
    fetcher = MockFetcher({"qwello": FetchResponse(payload, "application/json", 200), "ampeco": TimeoutError("skip")})
    service = _build_service(app_config, fetcher)
    result = service.ingest_provider("qwello")

    assert result["result"] == "ok"
    store = LiveStore(app_config)
    detail = store.get_evse_detail("qwello", "DEQWEE1")
    assert detail is not None
    assert detail["current"]["price_display"] == "0,50 €/kWh"
    assert detail["current"]["price_currency"] == "EUR"
    assert detail["current"]["price_energy_eur_kwh_min"] == "0.5"


def test_ingestion_persists_dynamic_slot_and_supplemental_fields(app_config):
    payload = json.dumps(
        _dynamic_payload_with_energy_rate_update(
            price_kwh=0.5,
            next_slots=[
                {
                    "expectedAvailableFromTime": "2026-04-15T09:15:00+00:00",
                    "expectedAvailableUntilTime": "2026-04-15T09:45:00+00:00",
                }
            ],
            supplemental_status=["parkingRestricted"],
            station_supplemental_status=["covered"],
            site_supplemental_status=["wheelchairAccessible"],
        )
    ).encode("utf-8")
    fetcher = MockFetcher({"qwello": FetchResponse(payload, "application/json", 200), "ampeco": TimeoutError("skip")})
    service = _build_service(app_config, fetcher)
    result = service.ingest_provider("qwello")

    assert result["result"] == "ok"
    store = LiveStore(app_config)
    detail = store.get_evse_detail("qwello", "DEQWEE1")
    assert detail is not None
    assert detail["current"]["next_available_charging_slots"] == [
        {
            "expectedAvailableFromTime": "2026-04-15T09:15:00+00:00",
            "expectedAvailableUntilTime": "2026-04-15T09:45:00+00:00",
        }
    ]
    assert detail["current"]["supplemental_facility_status"] == [
        "wheelchairAccessible",
        "covered",
        "parkingRestricted",
    ]


def test_ingestion_persists_observations_current_state_and_change_flags(app_config):
    first_payload = json.dumps(_dynamic_payload()).encode("utf-8")
    second_payload = json.dumps(_dynamic_payload(status="AVAILABLE")).encode("utf-8")
    third_payload = json.dumps(_dynamic_payload(status="FAULTED", price_kwh=0.69)).encode("utf-8")
    fetcher = MockFetcher(
        {
            "qwello": FetchResponse(first_payload, "application/json", 200),
            "ampeco": TimeoutError("ampeco timeout"),
        }
    )
    service = _build_service(app_config, fetcher)
    first = service.ingest_provider("qwello")
    assert first["result"] == "ok"
    assert first["observation_count"] == 2
    assert first["mapped_observation_count"] == 2
    assert first["dropped_observation_count"] == 0
    assert first["changed_observation_count"] == 2
    assert first["changed_mapped_observation_count"] == 2
    assert first["changed_dropped_observation_count"] == 0

    fetcher.responses["qwello"] = FetchResponse(second_payload, "application/json", 200)
    second = service.ingest_provider("qwello")
    assert second["mapped_observation_count"] == 2
    assert second["dropped_observation_count"] == 0
    assert second["changed_observation_count"] == 0

    fetcher.responses["qwello"] = FetchResponse(third_payload, "application/json", 200)
    third = service.ingest_provider("qwello")
    assert third["mapped_observation_count"] == 2
    assert third["dropped_observation_count"] == 0
    assert third["changed_observation_count"] == 2
    assert third["changed_mapped_observation_count"] == 2
    assert third["changed_dropped_observation_count"] == 0

    store = LiveStore(app_config)
    detail = store.get_evse_detail("qwello", "DEQWEE1")
    assert detail is not None
    assert detail["current"]["availability_status"] == "out_of_order"
    assert detail["recent_observations"] == []


def test_ingestion_logs_timeout_poll_run(app_config):
    fetcher = MockFetcher({"qwello": TimeoutError("provider timed out"), "ampeco": TimeoutError("skip")})
    service = _build_service(app_config, fetcher)
    result = service.ingest_provider("qwello")
    assert result["result"] == "timeout"

    store = LiveStore(app_config)
    providers = store.list_providers()
    provider = [item for item in providers if item["provider_uid"] == "qwello"][0]
    assert provider["last_result"] == "timeout"
    assert provider["consecutive_error_count"] == 1


def test_poll_scheduler_waits_until_provider_is_due(app_config):
    fetcher = MockFetcher(
        {
            "qwello": FetchResponse(json.dumps(_dynamic_payload()).encode("utf-8"), "application/json", 200),
            "ampeco": TimeoutError("skip"),
        }
    )
    service = _build_service(app_config, fetcher)
    first = service.ingest_provider("qwello")
    assert first["result"] == "ok"
    assert service.store.get_next_provider_for_round_robin() is None
    delay = service.store.seconds_until_next_provider_due()
    assert delay is not None
    assert delay > 0


def test_ingestion_handles_no_data_http_204(app_config):
    _write_subscription_registry(app_config.subscription_registry_path)
    fetcher = MockFetcher(
        {
            "qwello": FetchResponse(json.dumps(_dynamic_payload()).encode("utf-8"), "application/json", 200),
            "ampeco": FetchResponse(b"", "application/json", 204),
        }
    )
    service = _build_service(app_config, fetcher)
    result = service.ingest_provider("ampeco")
    assert result["result"] == "no_data"
    provider = service.store.get_provider("ampeco")
    assert provider is not None
    assert provider["last_result"] == "no_data"
    assert provider["consecutive_no_data_count"] == 1
    last_polled = _parse_dt(provider["last_polled_at"])
    next_poll = _parse_dt(provider["next_poll_at"])
    assert int((next_poll - last_polled).total_seconds()) == 30


def test_ingestion_backs_off_unchanged_snapshot_provider(app_config):
    payload = json.dumps(_dynamic_payload()).encode("utf-8")
    fetcher = MockFetcher({"qwello": FetchResponse(payload, "application/json", 200), "ampeco": TimeoutError("skip")})
    service = _build_service(app_config, fetcher)

    first = service.ingest_provider("qwello")
    assert first["changed_observation_count"] == 2
    provider = service.store.get_provider("qwello")
    assert provider is not None
    first_interval = int((_parse_dt(provider["next_poll_at"]) - _parse_dt(provider["last_polled_at"])).total_seconds())
    assert first_interval == 30

    second = service.ingest_provider("qwello")
    assert second["changed_observation_count"] == 0
    provider = service.store.get_provider("qwello")
    assert provider is not None
    assert provider["consecutive_unchanged_count"] == 1
    second_interval = int((_parse_dt(provider["next_poll_at"]) - _parse_dt(provider["last_polled_at"])).total_seconds())
    assert second_interval == 60

    third = service.ingest_provider("qwello")
    assert third["changed_observation_count"] == 0
    provider = service.store.get_provider("qwello")
    assert provider is not None
    assert provider["consecutive_unchanged_count"] == 2
    third_interval = int((_parse_dt(provider["next_poll_at"]) - _parse_dt(provider["last_polled_at"])).total_seconds())
    assert third_interval == 90


def test_ingestion_logs_invalid_payload_error(app_config):
    fetcher = MockFetcher(
        {"qwello": FetchResponse(b'["not-an-object"]', "application/json", 200), "ampeco": TimeoutError("skip")}
    )
    service = _build_service(app_config, fetcher)
    result = service.ingest_provider("qwello")
    assert result["result"] == "error"
    assert "expected_json_object_payload" in result["error"]


def test_bootstrap_reconciles_existing_orphan_rows_from_bundle_site_matches(app_config):
    _write_provider_fixture(app_config.provider_config_path)
    app_config.site_match_path.write_text("provider_uid,site_id,station_id,score\n", encoding="utf-8")
    with app_config.chargers_csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "station_id",
                "operator",
                "address",
                "postcode",
                "city",
                "lat",
                "lon",
                "charging_points_count",
                "max_power_kw",
                "detail_source_uid",
                "datex_site_id",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "station_id": "station-1",
                "operator": "Qwello",
                "address": "Teststr. 1",
                "postcode": "10115",
                "city": "Berlin",
                "lat": "52.531",
                "lon": "13.3849",
                "charging_points_count": "2",
                "max_power_kw": "150",
                "detail_source_uid": "",
                "datex_site_id": "",
            }
        )

    payload = json.dumps(_dynamic_payload()).encode("utf-8")
    fetcher = MockFetcher({"qwello": FetchResponse(payload, "application/json", 200), "ampeco": TimeoutError("skip")})
    service = IngestionService(app_config, store=LiveStore(app_config), fetcher=fetcher)
    result = service.ingest_provider("qwello")
    assert result["observation_count"] == 2

    store = LiveStore(app_config)
    detail = store.get_evse_detail("qwello", "DEQWEE1")
    assert detail is not None
    assert detail["current"]["station_id"] == ""

    _write_chargers_fixture(app_config.chargers_csv_path)
    service.bootstrap()

    detail = store.get_evse_detail("qwello", "DEQWEE1")
    assert detail is not None
    assert detail["current"]["station_id"] == "station-1"

    station = store.get_station_detail("station-1")
    assert station is not None
    assert station["station"]["total_evses"] == 2


def test_initialize_drops_legacy_evse_observation_history(app_config):
    _write_provider_fixture(app_config.provider_config_path)
    _write_matches_fixture(app_config.site_match_path)
    _write_chargers_fixture(app_config.chargers_csv_path)

    conn = sqlite3.connect(app_config.db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE evse_observations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                provider_uid TEXT NOT NULL,
                provider_site_id TEXT NOT NULL DEFAULT '',
                provider_station_ref TEXT NOT NULL DEFAULT '',
                provider_evse_id TEXT NOT NULL DEFAULT '',
                station_id TEXT NOT NULL DEFAULT '',
                availability_status TEXT NOT NULL DEFAULT '',
                operational_status TEXT NOT NULL DEFAULT '',
                price_display TEXT NOT NULL DEFAULT '',
                price_currency TEXT NOT NULL DEFAULT '',
                price_energy_eur_kwh_min TEXT NOT NULL DEFAULT '',
                price_energy_eur_kwh_max TEXT NOT NULL DEFAULT '',
                price_time_eur_min_min REAL,
                price_time_eur_min_max REAL,
                price_quality TEXT NOT NULL DEFAULT '',
                price_complex INTEGER NOT NULL DEFAULT 0,
                next_available_charging_slots TEXT NOT NULL DEFAULT '',
                supplemental_facility_status TEXT NOT NULL DEFAULT '',
                source_observed_at TEXT NOT NULL DEFAULT '',
                fetched_at TEXT NOT NULL DEFAULT '',
                ingested_at TEXT NOT NULL DEFAULT '',
                changed_since_previous INTEGER NOT NULL DEFAULT 1,
                payload_sha256 TEXT NOT NULL DEFAULT ''
            );
            CREATE INDEX idx_evse_observations_station_id
                ON evse_observations (station_id, fetched_at DESC);
            """
        )
        conn.execute(
            """
            INSERT INTO evse_observations (
                provider_uid,
                provider_site_id,
                provider_station_ref,
                provider_evse_id,
                station_id,
                availability_status,
                operational_status,
                price_display,
                price_currency,
                price_quality,
                source_observed_at,
                fetched_at,
                ingested_at,
                payload_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "qwello",
                "SITE-1",
                "STATION-REF-1",
                "DEQWEE1",
                "station-1",
                "free",
                "AVAILABLE",
                "0,59 €/kWh",
                "EUR",
                "simple",
                "2026-04-15T08:00:00+00:00",
                "2026-04-15T08:00:01+00:00",
                "2026-04-15T08:00:02+00:00",
                "legacy-sha",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    service = _build_service(
        app_config,
        MockFetcher({"qwello": FetchResponse(json.dumps(_dynamic_payload()).encode("utf-8"), "application/json", 200)}),
    )
    service.bootstrap()

    conn = sqlite3.connect(app_config.db_path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'evse_observations'"
        ).fetchone()
    finally:
        conn.close()
    assert row is None


def test_initialize_migrates_descriptive_price_columns_to_text(app_config):
    conn = sqlite3.connect(app_config.db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE evse_current_state (
                provider_uid TEXT NOT NULL,
                provider_site_id TEXT NOT NULL,
                provider_station_ref TEXT NOT NULL,
                provider_evse_id TEXT NOT NULL,
                station_id TEXT NOT NULL DEFAULT '',
                availability_status TEXT NOT NULL,
                operational_status TEXT NOT NULL,
                price_display TEXT NOT NULL,
                price_currency TEXT NOT NULL,
                price_energy_eur_kwh_min REAL,
                price_energy_eur_kwh_max REAL,
                price_time_eur_min_min REAL,
                price_time_eur_min_max REAL,
                price_quality TEXT NOT NULL,
                price_complex INTEGER NOT NULL DEFAULT 0,
                source_observed_at TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                ingested_at TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                PRIMARY KEY (provider_uid, provider_evse_id)
            );
            CREATE TABLE station_current_state (
                station_id TEXT PRIMARY KEY,
                provider_uid TEXT NOT NULL,
                availability_status TEXT NOT NULL,
                available_evses INTEGER NOT NULL DEFAULT 0,
                occupied_evses INTEGER NOT NULL DEFAULT 0,
                out_of_order_evses INTEGER NOT NULL DEFAULT 0,
                unknown_evses INTEGER NOT NULL DEFAULT 0,
                total_evses INTEGER NOT NULL DEFAULT 0,
                price_display TEXT NOT NULL,
                price_currency TEXT NOT NULL,
                price_energy_eur_kwh_min REAL,
                price_energy_eur_kwh_max REAL,
                price_time_eur_min_min REAL,
                price_time_eur_min_max REAL,
                price_complex INTEGER NOT NULL DEFAULT 0,
                source_observed_at TEXT NOT NULL,
                fetched_at TEXT NOT NULL,
                ingested_at TEXT NOT NULL
            );
            """
        )
        conn.execute(
            """
            INSERT INTO evse_current_state (
                provider_uid,
                provider_site_id,
                provider_station_ref,
                provider_evse_id,
                station_id,
                availability_status,
                operational_status,
                price_display,
                price_currency,
                price_energy_eur_kwh_min,
                price_energy_eur_kwh_max,
                price_time_eur_min_min,
                price_time_eur_min_max,
                price_quality,
                price_complex,
                source_observed_at,
                fetched_at,
                ingested_at,
                payload_sha256
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "qwello",
                "SITE-1",
                "STATION-REF-1",
                "DEQWEE1",
                "station-1",
                "free",
                "AVAILABLE",
                "0,59 €/kWh",
                "EUR",
                0.59,
                0.79,
                None,
                None,
                "simple",
                0,
                "2026-04-15T08:00:00+00:00",
                "2026-04-15T08:00:01+00:00",
                "2026-04-15T08:00:02+00:00",
                "legacy-sha",
            ),
        )
        conn.execute(
            """
            INSERT INTO station_current_state (
                station_id,
                provider_uid,
                availability_status,
                available_evses,
                occupied_evses,
                out_of_order_evses,
                unknown_evses,
                total_evses,
                price_display,
                price_currency,
                price_energy_eur_kwh_min,
                price_energy_eur_kwh_max,
                price_time_eur_min_min,
                price_time_eur_min_max,
                price_complex,
                source_observed_at,
                fetched_at,
                ingested_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "station-1",
                "qwello",
                "free",
                1,
                0,
                0,
                0,
                1,
                "0,59 €/kWh",
                "EUR",
                0.59,
                0.79,
                None,
                None,
                0,
                "2026-04-15T08:00:00+00:00",
                "2026-04-15T08:00:01+00:00",
                "2026-04-15T08:00:02+00:00",
            ),
        )
        conn.commit()
    finally:
        conn.close()

    _write_provider_fixture(app_config.provider_config_path)
    _write_matches_fixture(app_config.site_match_path)
    _write_chargers_fixture(app_config.chargers_csv_path)

    store = LiveStore(app_config)
    store.initialize()

    with store.connection() as conn:
        conn.execute(
            """
            INSERT INTO stations (
                station_id,
                operator,
                address,
                postcode,
                city,
                lat,
                lon,
                charging_points_count,
                max_power_kw,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "station-1",
                "Qwello",
                "Teststr. 1",
                "10115",
                "Berlin",
                52.531,
                13.3849,
                2,
                150.0,
                "2026-04-15T08:00:03+00:00",
            ),
        )
        evse_columns = {
            str(row["name"]): str(row["type"] or "").upper()
            for row in conn.execute("PRAGMA table_info(evse_current_state)").fetchall()
        }
        station_columns = {
            str(row["name"]): str(row["type"] or "").upper()
            for row in conn.execute("PRAGMA table_info(station_current_state)").fetchall()
        }

    assert evse_columns["price_energy_eur_kwh_min"] == "TEXT"
    assert evse_columns["price_energy_eur_kwh_max"] == "TEXT"
    assert station_columns["price_energy_eur_kwh_min"] == "TEXT"
    assert station_columns["price_energy_eur_kwh_max"] == "TEXT"

    evse = store.get_evse_detail("qwello", "DEQWEE1")
    assert evse is not None
    assert evse["current"]["price_energy_eur_kwh_min"] == "0.59"
    assert evse["current"]["price_energy_eur_kwh_max"] == "0.79"

    station = store.get_station_detail("station-1")
    assert station is not None
    assert station["station"]["price_energy_eur_kwh_min"] == "0.59"
    assert station["station"]["price_energy_eur_kwh_max"] == "0.79"


def test_round_robin_picks_never_polled_provider_first(app_config):
    fetcher = MockFetcher(
        {
            "qwello": FetchResponse(json.dumps(_dynamic_payload()).encode("utf-8"), "application/json", 200),
            "ampeco": TimeoutError("skip"),
        }
    )
    service = _build_service(app_config, fetcher)
    service.bootstrap()
    provider = service.store.get_next_provider_for_round_robin()
    assert provider is not None
    assert provider["provider_uid"] == "qwello"


def test_curl_fetcher_uses_machine_certificate_and_gzip_header(app_config, monkeypatch):
    commands = []

    def fake_run(command, check, capture_output, timeout):
        commands.append(command)
        header_path = Path(command[command.index("-D") + 1])
        body_path = Path(command[command.index("-o") + 1])
        header_path.write_text("HTTP/1.1 200 OK\nContent-Type: application/json\n", encoding="utf-8")
        body_path.write_bytes(b"{}")
        return SimpleNamespace(returncode=0, stderr=b"")

    monkeypatch.setattr(fetcher_module.subprocess, "run", fake_run)
    fetcher = CurlFetcher(app_config)
    provider = SimpleNamespace(fetch_url="https://example.com/subscription?subscriptionID=2000001", fetch_kind="mtls_subscription")
    response = fetcher.fetch(provider)
    assert response.http_status == 200
    command = commands[0]
    assert "--cert-type" in command
    assert "P12" in command
    assert "--cert" in command
    assert f"{app_config.machine_cert_p12}:{app_config.cert_password()}" in command
    assert "Accept-Encoding: gzip" in command


def test_api_lists_stations_and_filters(app_config):
    payload = json.dumps(_dynamic_payload()).encode("utf-8")
    fetcher = MockFetcher({"qwello": FetchResponse(payload, "application/json", 200), "ampeco": TimeoutError("skip")})
    service = _build_service(app_config, fetcher)
    service.ingest_provider("qwello")

    client = TestClient(create_app(app_config))
    stations = client.get("/v1/stations").json()
    assert len(stations) == 1
    assert stations[0]["station_id"] == "station-1"
    assert stations[0]["availability_status"] == "free"
    for key in ("operator", "address", "postcode", "city", "lat", "lon", "charging_points_count", "max_power_kw", "provider_uid"):
        assert key not in stations[0]

    filtered = client.get("/v1/stations", params={"status": "occupied"}).json()
    assert filtered == []


def test_api_station_lookup_returns_requested_station_ids(app_config):
    payload = json.dumps(_dynamic_payload()).encode("utf-8")
    fetcher = MockFetcher({"qwello": FetchResponse(payload, "application/json", 200), "ampeco": TimeoutError("skip")})
    service = _build_service(app_config, fetcher)
    service.ingest_provider("qwello")

    client = TestClient(create_app(app_config))
    response = client.post(
        "/v1/stations/lookup",
        json={"station_ids": ["station-1", "missing", "station-1"]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert [station["station_id"] for station in payload["stations"]] == ["station-1"]
    assert payload["missing_station_ids"] == ["missing"]
    for key in ("operator", "address", "postcode", "city", "lat", "lon", "charging_points_count", "max_power_kw", "provider_uid"):
        assert key not in payload["stations"][0]


def test_api_clamps_station_list_limit_to_100(app_config):
    app = create_app(app_config)
    captured: dict[str, int] = {}

    def fake_list_station_summaries(*, provider_uid="", status="", limit=100, offset=0):
        captured["limit"] = limit
        return []

    app.state.store.list_station_summaries = fake_list_station_summaries

    client = TestClient(app)
    response = client.get("/v1/stations", params={"limit": 999})
    assert response.status_code == 200
    assert captured["limit"] == 100


def test_api_station_and_evse_details_return_current_state_only(app_config):
    first = json.dumps(_dynamic_payload()).encode("utf-8")
    second = json.dumps(
        _dynamic_payload_with_energy_rate_update(
            status="FAULTED",
            price_kwh=0.79,
            next_slots=[
                {
                    "expectedAvailableFromTime": "2026-04-15T09:15:00+00:00",
                    "expectedAvailableUntilTime": "2026-04-15T09:45:00+00:00",
                }
            ],
            supplemental_status=["parkingRestricted"],
        )
    ).encode("utf-8")
    fetcher = MockFetcher({"qwello": FetchResponse(first, "application/json", 200), "ampeco": TimeoutError("skip")})
    service = _build_service(app_config, fetcher)
    service.ingest_provider("qwello")
    fetcher.responses["qwello"] = FetchResponse(second, "application/json", 200)
    service.ingest_provider("qwello")

    client = TestClient(create_app(app_config))
    station = client.get("/v1/stations/station-1").json()
    assert station["station"]["price_display"] == "0,79 €/kWh"
    assert station["recent_observations"] == []
    for key in ("operator", "address", "postcode", "city", "lat", "lon", "charging_points_count", "max_power_kw", "provider_uid"):
        assert key not in station["station"]

    evse = client.get("/v1/evses/qwello/DEQWEE1").json()
    assert evse["current"]["availability_status"] == "out_of_order"
    assert evse["current"]["next_available_charging_slots"] == [
        {
            "expectedAvailableFromTime": "2026-04-15T09:15:00+00:00",
            "expectedAvailableUntilTime": "2026-04-15T09:45:00+00:00",
        }
    ]
    assert evse["current"]["supplemental_facility_status"] == ["parkingRestricted"]
    assert evse["recent_observations"] == []
    assert "provider_uid" not in evse["current"]


def test_api_returns_404_for_missing_records(app_config):
    _write_provider_fixture(app_config.provider_config_path)
    _write_matches_fixture(app_config.site_match_path)
    _write_chargers_fixture(app_config.chargers_csv_path)
    client = TestClient(create_app(app_config))
    assert client.get("/v1/stations/missing").status_code == 404
    assert client.get("/v1/evses/qwello/missing").status_code == 404


def test_ingestion_persists_eliso_generic_payload_with_bundle_evse_alias_matches(app_config):
    app_config.provider_config_path.write_text(
        json.dumps(
            {
                "providers": [
                    {
                        "uid": "eliso",
                        "display_name": "eliso",
                        "publisher": "eliso GmbH",
                        "feeds": {
                            "dynamic": {
                                "publication_id": "843502085052710912",
                                "access_mode": "auth",
                                "delta_delivery": False,
                                "content_data": {},
                            }
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    app_config.subscription_registry_path.write_text(
        json.dumps(
            {
                "eliso": {
                    "enabled": True,
                    "fetch_kind": "mtls_generic_subscription",
                    "fetch_url": "https://example.invalid/eliso",
                    "subscription_id": "980986474933399552",
                }
            }
        ),
        encoding="utf-8",
    )
    app_config.site_match_path.write_text("provider_uid,site_id,station_id,score\n", encoding="utf-8")
    app_config.chargers_csv_path.write_text(
        "\n".join(
            [
                "station_id,operator,address,postcode,city,lat,lon,charging_points_count,max_power_kw,detail_source_uid,datex_site_id,datex_station_ids,datex_charge_point_ids",
                "station-2,eliso GmbH,Example 2,70174,Stuttgart,48.779,9.181,4,300,mobilithek_monta_static,Eliso GmbH-s1076907,s1076907,3603098|3603100",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    app_config.chargers_geojson_path.write_text(
        json.dumps(
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [9.181, 48.779]},
                        "properties": {"station_id": "station-2"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    payload = json.dumps(_eliso_dynamic_payload()).encode("utf-8")
    service = IngestionService(
        app_config,
        store=LiveStore(app_config),
        fetcher=MockFetcher({"eliso": FetchResponse(payload, "application/json", 200)}),
    )

    result = service.ingest_provider("eliso")

    assert result["result"] == "ok"
    assert result["observation_count"] == 2

    evse = service.store.get_evse_detail("eliso", "DEELIE3603098")
    assert evse is not None
    assert evse["current"]["station_id"] == "station-2"
    assert evse["current"]["availability_status"] == "free"
    assert evse["current"]["price_display"] == "ab 0,49 €/kWh"
    assert evse["current"]["price_time_eur_min_min"] == 0.1

    station = service.store.get_station_detail("station-2")
    assert station is not None
    assert station["station"]["provider_uid"] == "eliso"
    assert station["station"]["availability_status"] == "free"
    assert station["station"]["available_evses"] == 1
    assert station["station"]["total_evses"] == 1


def test_api_allows_local_cors_origins(app_config):
    _write_provider_fixture(app_config.provider_config_path)
    _write_matches_fixture(app_config.site_match_path)
    _write_chargers_fixture(app_config.chargers_csv_path)
    client = TestClient(create_app(app_config))
    for origin in ("http://127.0.0.1:8000", "http://0.0.0.0:4173", "http://[::1]:4173"):
        response = client.get("/healthz", headers={"Origin": origin})
        assert response.status_code == 200
        assert response.headers["access-control-allow-origin"] == origin


def test_api_allows_configured_cors_origins(app_config):
    _write_provider_fixture(app_config.provider_config_path)
    _write_matches_fixture(app_config.site_match_path)
    _write_chargers_fixture(app_config.chargers_csv_path)
    client = TestClient(
        create_app(
            replace(
                app_config,
                api_cors_allowed_origins=("https://woladen.de", "https://www.woladen.de"),
            )
        )
    )
    response = client.get("/healthz", headers={"Origin": "https://woladen.de"})
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "https://woladen.de"


def test_api_status_reports_bundle_coverage_and_provider_timestamps(app_config):
    payload = json.dumps(
        _dynamic_payload_with_energy_rate_update(
            next_slots=[
                {
                    "expectedAvailableFromTime": "2026-04-15T09:15:00+00:00",
                    "expectedAvailableUntilTime": "2026-04-15T09:45:00+00:00",
                }
            ],
            supplemental_status=["parkingRestricted"],
        )
    ).encode("utf-8")
    fetcher = MockFetcher({"qwello": FetchResponse(payload, "application/json", 200), "ampeco": TimeoutError("skip")})
    service = _build_service(app_config, fetcher)
    result = service.ingest_provider("qwello")
    dropped_result = service.ingest_push(
        provider_uid="ampeco",
        payload_bytes=json.dumps(_dynamic_payload(status="AVAILABLE")).encode("utf-8"),
        content_type="application/json",
        request_path="/v1/push/ampeco",
    )

    client = TestClient(create_app(app_config))
    response = client.get("/status")
    assert response.status_code == 200

    status_payload = response.json()
    assert status_payload["bundle_station_count"] == 2
    assert status_payload["stations_with_any_live_observation"] == 1
    assert status_payload["stations_with_current_live_state"] == 1
    assert status_payload["coverage_ratio"] == 0.5
    assert status_payload["last_received_update_at"] == result["fetched_at"]
    assert status_payload["latest_updated_station_id"] == "station-1"
    assert status_payload["providers_with_any_live_observation"] == 1

    providers = {item["provider_uid"]: item for item in status_payload["providers"]}
    assert providers["qwello"]["stations_with_any_live_observation"] == 1
    assert providers["qwello"]["last_received_update_at"] == result["fetched_at"]
    assert providers["qwello"]["last_source_update_at"] == "2026-04-15T08:00:00+00:00"
    assert providers["qwello"]["latest_updated_station_id"] == "station-1"
    assert providers["qwello"]["latest_attribute_updates"]["availability_status"]["station_id"] == "station-1"
    assert providers["qwello"]["latest_attribute_updates"]["availability_status"]["value"] == "free"
    assert providers["qwello"]["latest_attribute_updates"]["price_display"]["value"] == "0,59 €/kWh"
    assert providers["qwello"]["latest_attribute_updates"]["price_energy_eur_kwh_min"]["value"] == "0.59"
    assert providers["qwello"]["latest_attribute_updates"]["next_available_charging_slots"]["value"] == [
        {
            "expectedAvailableFromTime": "2026-04-15T09:15:00+00:00",
            "expectedAvailableUntilTime": "2026-04-15T09:45:00+00:00",
        }
    ]
    assert providers["qwello"]["latest_attribute_updates"]["supplemental_facility_status"]["value"] == [
        "parkingRestricted"
    ]
    assert providers["qwello"]["last_polled_at"] == result["fetched_at"]
    assert providers["qwello"]["last_result"] == "ok"
    assert providers["qwello"]["recent_updates"][0]["update_kind"] == "poll"
    assert providers["qwello"]["recent_updates"][0]["observation_count"] == 1
    assert providers["qwello"]["recent_updates"][0]["mapped_observation_count"] == 1
    assert providers["qwello"]["recent_updates"][0]["dropped_observation_count"] == 0
    assert providers["qwello"]["recent_updates"][0]["changed_observation_count"] == 1
    assert providers["qwello"]["recent_updates"][0]["changed_mapped_observation_count"] == 1
    assert providers["qwello"]["recent_updates"][0]["changed_dropped_observation_count"] == 0
    assert providers["ampeco"]["stations_with_any_live_observation"] == 0
    assert providers["ampeco"]["last_received_update_at"] is None
    assert providers["ampeco"]["latest_updated_station_id"] is None
    assert providers["ampeco"]["latest_attribute_updates"] == {}
    assert providers["ampeco"]["recent_updates"][0]["update_kind"] == "push"
    assert providers["ampeco"]["recent_updates"][0]["received_at"] == dropped_result["received_at"]
    assert providers["ampeco"]["recent_updates"][0]["observation_count"] == 2
    assert providers["ampeco"]["recent_updates"][0]["mapped_observation_count"] == 0
    assert providers["ampeco"]["recent_updates"][0]["dropped_observation_count"] == 2
    assert providers["ampeco"]["recent_updates"][0]["changed_observation_count"] == 2
    assert providers["ampeco"]["recent_updates"][0]["changed_mapped_observation_count"] == 0
    assert providers["ampeco"]["recent_updates"][0]["changed_dropped_observation_count"] == 2

    versioned_response = client.get("/v1/status")
    assert versioned_response.status_code == 200
    assert versioned_response.json()["stations_with_any_live_observation"] == 1


def test_push_ingestion_persists_observations_from_provider_path(app_config):
    service = _build_service(app_config, MockFetcher({"qwello": TimeoutError("skip"), "ampeco": TimeoutError("skip")}))
    result = service.ingest_push(
        provider_uid="qwello",
        payload_bytes=json.dumps(_dynamic_payload(status="FAULTED", price_kwh=0.79)).encode("utf-8"),
        content_type="application/json",
        request_path="/v1/push/qwello",
    )

    assert result["result"] == "ok"
    assert result["provider_uid"] == "qwello"
    assert result["observation_count"] == 2
    assert result["mapped_observation_count"] == 2
    assert result["dropped_observation_count"] == 0
    assert result["changed_observation_count"] == 2
    assert result["changed_mapped_observation_count"] == 2
    assert result["changed_dropped_observation_count"] == 0

    provider = service.store.get_provider("qwello")
    assert provider is not None
    assert provider["last_push_result"] == "ok"
    assert provider["last_push_received_at"] == result["received_at"]

    station = service.store.get_station_detail("station-1")
    assert station is not None
    assert station["station"]["price_display"] == "0,79 €/kWh"


def test_push_ingestion_resolves_provider_from_subscription_id(app_config):
    _write_subscription_registry(app_config.subscription_registry_path)
    service = _build_service(app_config, MockFetcher({"qwello": TimeoutError("skip"), "ampeco": TimeoutError("skip")}))
    result = service.ingest_push(
        subscription_id="2000001",
        payload_bytes=json.dumps(_dynamic_payload(status="AVAILABLE")).encode("utf-8"),
        content_type="application/json",
        request_path="/v1/push",
    )

    assert result["result"] == "ok"
    assert result["provider_uid"] == "ampeco"
    assert result["observation_count"] == 2
    assert result["mapped_observation_count"] == 0
    assert result["dropped_observation_count"] == 2
    assert result["changed_observation_count"] == 2
    assert result["changed_mapped_observation_count"] == 0
    assert result["changed_dropped_observation_count"] == 2
    evse = service.store.get_evse_detail("ampeco", "DEQWEE1")
    assert evse is not None
    assert evse["current"]["availability_status"] == "free"


def test_pull_ingestion_normalizes_availability_and_operational_status_case(app_config):
    payload = json.dumps(
        _dynamic_payload(
            status="free",
            operation_status="aVaiLaBle",
            second_evse_status="occupied",
            second_evse_operation_status="cHaRgInG",
        )
    ).encode("utf-8")
    fetcher = MockFetcher({"qwello": FetchResponse(payload, "application/json", 200), "ampeco": TimeoutError("skip")})
    service = _build_service(app_config, fetcher)

    result = service.ingest_provider("qwello")
    assert result["result"] == "ok"

    first_evse = service.store.get_evse_detail("qwello", "DEQWEE1")
    second_evse = service.store.get_evse_detail("qwello", "DEQWEE2")
    assert first_evse is not None
    assert second_evse is not None
    assert first_evse["current"]["availability_status"] == "free"
    assert first_evse["current"]["operational_status"] == "AVAILABLE"
    assert second_evse["current"]["availability_status"] == "occupied"
    assert second_evse["current"]["operational_status"] == "CHARGING"


def test_push_ingestion_normalizes_unknown_status_to_out_of_order(app_config):
    service = _build_service(app_config, MockFetcher({"qwello": TimeoutError("skip"), "ampeco": TimeoutError("skip")}))
    result = service.ingest_push(
        provider_uid="qwello",
        payload_bytes=json.dumps(
            _dynamic_payload(
                status="unknown",
                operation_status="uNkNoW",
                second_evse_status="unknown",
                second_evse_operation_status="UNKNOW",
            )
        ).encode("utf-8"),
        content_type="application/json",
        request_path="/v1/push/qwello",
    )

    assert result["result"] == "ok"
    evse = service.store.get_evse_detail("qwello", "DEQWEE1")
    assert evse is not None
    assert evse["current"]["availability_status"] == "out_of_order"
    assert evse["current"]["operational_status"] == "UNKNOWN"


def test_api_push_endpoint_accepts_post_and_head(app_config):
    _write_subscription_registry(app_config.subscription_registry_path)
    _write_provider_fixture(app_config.provider_config_path)
    _write_matches_fixture(app_config.site_match_path)
    _write_chargers_fixture(app_config.chargers_csv_path)
    client = TestClient(create_app(app_config))

    head_response = client.head("/v1/push/ampeco")
    assert head_response.status_code == 200

    push_response = client.post(
        "/v1/push",
        content=json.dumps(_dynamic_payload(status="FAULTED", price_kwh=0.81)).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "subscriptionID": "2000001",
        },
    )
    assert push_response.status_code == 200
    assert push_response.content == b""

    store = LiveStore(app_config)
    evse = store.get_evse_detail("ampeco", "DEQWEE1")
    assert evse is not None
    assert evse["current"]["price_display"] == "0,81 €/kWh"


def test_push_ingestion_writes_timestamped_request_logs(app_config):
    service = _build_service(app_config, MockFetcher({"qwello": TimeoutError("skip"), "ampeco": TimeoutError("skip")}))
    result = service.ingest_push(
        provider_uid="qwello",
        subscription_id="sub-1",
        publication_id="pub-1",
        payload_bytes=json.dumps(_dynamic_payload()).encode("utf-8"),
        content_type="application/json",
        content_encoding="gzip",
        request_path="/v1/push/qwello",
        request_query="subscription_id=sub-1",
        request_headers={"subscriptionID": "sub-1", "content-type": "application/json"},
    )
    archive_date = _parse_dt(result["received_at"]).date().isoformat()

    request_logs = sorted(app_config.raw_payload_dir.glob(f"qwello/{archive_date}/*-push-*.json"))
    assert len(request_logs) == 1

    record = json.loads(request_logs[0].read_text(encoding="utf-8"))
    assert record["kind"] == "push_request"
    assert record["provider_uid"] == "qwello"
    assert record["subscription_id"] == "sub-1"
    assert record["publication_id"] == "pub-1"
    assert record["request_path"] == "/v1/push/qwello"
    assert "SITE-1" in record["body_text"]


def test_api_push_endpoint_returns_404_for_unknown_provider(app_config):
    _write_provider_fixture(app_config.provider_config_path)
    _write_matches_fixture(app_config.site_match_path)
    _write_chargers_fixture(app_config.chargers_csv_path)
    client = TestClient(create_app(app_config))

    response = client.post(
        "/v1/push/unknown-provider",
        content=json.dumps(_dynamic_payload()).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 404


def test_ingestion_writes_timestamped_provider_response_logs(app_config):
    payload = json.dumps(_dynamic_payload()).encode("utf-8")
    fetcher = MockFetcher({"qwello": FetchResponse(payload, "application/json", 200), "ampeco": TimeoutError("skip")})
    service = _build_service(app_config, fetcher)
    result = service.ingest_provider("qwello")
    archive_date = _parse_dt(result["fetched_at"]).date().isoformat()

    response_logs = sorted(app_config.raw_payload_dir.glob(f"qwello/{archive_date}/*.json"))
    assert len(response_logs) == 1

    record = json.loads(response_logs[0].read_text(encoding="utf-8"))
    assert record["kind"] == "http_response"
    assert record["provider_uid"] == "qwello"
    assert record["http_status"] == 200
    assert record["payload_byte_length"] == len(payload)
    assert "SITE-1" in record["body_text"]


def test_ingestion_keeps_http_error_body_in_response_logs(app_config):
    payload = json.dumps({"detail": "provider unavailable"}).encode("utf-8")
    fetcher = MockFetcher(
        {
            "qwello": FetchResponse(
                payload,
                "application/json",
                503,
                "HTTP/1.1 503 Service Unavailable\nContent-Type: application/json\n",
            ),
            "ampeco": TimeoutError("skip"),
        }
    )
    service = _build_service(app_config, fetcher)
    result = service.ingest_provider("qwello")
    archive_date = _parse_dt(result["fetched_at"]).date().isoformat()

    assert result["result"] == "error"
    assert result["http_status"] == 503

    response_logs = sorted(app_config.raw_payload_dir.glob(f"qwello/{archive_date}/*.json"))
    assert len(response_logs) == 1
    record = json.loads(response_logs[0].read_text(encoding="utf-8"))
    assert record["http_status"] == 503
    assert "provider unavailable" in record["body_text"]


def test_daily_response_archiver_creates_tgz_uploads_and_cleans_up_sources(app_config):
    target_date = date(2026, 4, 14)
    first_dir = app_config.raw_payload_dir / "qwello" / target_date.isoformat()
    second_dir = app_config.raw_payload_dir / "wirelane" / target_date.isoformat()
    first_dir.mkdir(parents=True, exist_ok=True)
    second_dir.mkdir(parents=True, exist_ok=True)
    (first_dir / "20260414T000000000000Z-200-aaaa.json").write_text(
        json.dumps({"provider_uid": "qwello", "body_text": "first"}, ensure_ascii=False),
        encoding="utf-8",
    )
    (second_dir / "20260414T010000000000Z-200-bbbb.json").write_text(
        json.dumps({"provider_uid": "wirelane", "body_text": "second"}, ensure_ascii=False),
        encoding="utf-8",
    )

    token_file = app_config.archive_dir / "huggingface.token"
    token_file.parent.mkdir(parents=True, exist_ok=True)
    token_file.write_text("secret-token\n", encoding="utf-8")

    class StubHfApi:
        def __init__(self):
            self.calls: list[dict] = []
            self.archive_names: list[str] = []

        def upload_file(self, **kwargs):
            self.calls.append(kwargs)
            with tarfile.open(kwargs["path_or_fileobj"], "r:gz") as archive_handle:
                self.archive_names = sorted(archive_handle.getnames())

    stub_api = StubHfApi()
    configured = replace(
        app_config,
        hf_archive_repo_id="raphaelvolz/woladen-live-archives",
        hf_archive_token_file=token_file,
        hf_archive_path_prefix="provider-response-archives",
    )
    result = DailyResponseArchiver(configured, hf_api=stub_api).archive_date(target_date)

    assert result["result"] == "uploaded"
    assert result["file_count"] == 2
    assert result["remote_path"] == "provider-response-archives/2026/04/live-provider-responses-2026-04-14.tgz"
    assert len(stub_api.calls) == 1
    assert stub_api.calls[0]["repo_id"] == "raphaelvolz/woladen-live-archives"
    assert stub_api.calls[0]["path_in_repo"] == result["remote_path"]
    assert "manifest.json" in stub_api.archive_names
    assert "qwello/2026-04-14/20260414T000000000000Z-200-aaaa.json" in stub_api.archive_names
    assert "wirelane/2026-04-14/20260414T010000000000Z-200-bbbb.json" in stub_api.archive_names

    archive_path = Path(result["archive_path"])
    assert not archive_path.exists()
    assert not first_dir.exists()
    assert not second_dir.exists()


def test_daily_response_archiver_local_only_keeps_tgz(app_config):
    target_date = date(2026, 4, 14)
    provider_dir = app_config.raw_payload_dir / "qwello" / target_date.isoformat()
    provider_dir.mkdir(parents=True, exist_ok=True)
    (provider_dir / "20260414T000000000000Z-200-aaaa.json").write_text(
        json.dumps({"provider_uid": "qwello", "body_text": "first"}, ensure_ascii=False),
        encoding="utf-8",
    )

    result = DailyResponseArchiver(app_config).archive_date(target_date, upload=False)

    assert result["result"] == "archived_local_only"
    assert result["file_count"] == 1
    archive_path = Path(result["archive_path"])
    assert archive_path.exists()
    assert not provider_dir.exists()


def test_load_env_file_can_filter_archive_settings(tmp_path, monkeypatch):
    env_file = tmp_path / "woladen-live.env"
    env_file.write_text(
        "\n".join(
            [
                "# runtime env",
                "WOLADEN_LIVE_RAW_PAYLOAD_DIR=/var/lib/woladen/live_raw",
                'WOLADEN_LIVE_ARCHIVE_DIR="/var/lib/woladen/live archives"',
                r"WOLADEN_LIVE_API_CORS_ALLOW_ORIGIN_REGEX=https?://(localhost|127\\.0\\.0\\.1|0\\.0\\.0\\.0|\\[::1\\])(\\:\\d+)?$",
                "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID=loffenauer/AFIR",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("WOLADEN_LIVE_RAW_PAYLOAD_DIR", raising=False)
    monkeypatch.delenv("WOLADEN_LIVE_ARCHIVE_DIR", raising=False)
    monkeypatch.delenv("WOLADEN_LIVE_HF_ARCHIVE_REPO_ID", raising=False)
    monkeypatch.delenv("WOLADEN_LIVE_API_CORS_ALLOW_ORIGIN_REGEX", raising=False)

    load_env_file(
        env_file,
        allowed_keys={
            "WOLADEN_LIVE_RAW_PAYLOAD_DIR",
            "WOLADEN_LIVE_ARCHIVE_DIR",
            "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID",
        },
    )

    assert os.environ["WOLADEN_LIVE_RAW_PAYLOAD_DIR"] == "/var/lib/woladen/live_raw"
    assert os.environ["WOLADEN_LIVE_ARCHIVE_DIR"] == "/var/lib/woladen/live archives"
    assert os.environ["WOLADEN_LIVE_HF_ARCHIVE_REPO_ID"] == "loffenauer/AFIR"
    assert "WOLADEN_LIVE_API_CORS_ALLOW_ORIGIN_REGEX" not in os.environ
