from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

from backend.subscriptions import SubscriptionOffer, build_live_subscription_registry, load_subscription_offers


def _load_sync_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "sync_mobilithek_subscriptions.py"
    spec = importlib.util.spec_from_file_location("sync_mobilithek_subscriptions_module", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


sync_module = _load_sync_module()


def test_load_subscription_offers_includes_static_dynamic_noauth_and_model_other(tmp_path: Path):
    config_path = tmp_path / "providers.json"
    config_path.write_text(
        json.dumps(
            {
                "providers": [
                    {
                        "uid": "tesla",
                        "display_name": "Tesla",
                        "publisher": "Tesla Germany GmbH",
                        "feeds": {
                            "static": {
                                "publication_id": "953828817873125376",
                                "access_mode": "noauth",
                                "data_model": "https://w3id.org/mdp/schema/data_model#DATEX_2_V3",
                                "title": "AFIR-recharging-stat-Tesla",
                            },
                            "dynamic": {
                                "publication_id": "953843379766972416",
                                "access_mode": "noauth",
                                "data_model": "https://w3id.org/mdp/schema/data_model#DATEX_2_V3",
                                "title": "AFIR-recharging-dyn-Tesla",
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
                                "access_mode": "auth",
                                "data_model": "https://w3id.org/mdp/schema/data_model#MODEL_OTHER",
                                "title": "eliso AFIR Static Data (Station & Point)",
                            },
                            "dynamic": {
                                "publication_id": "843502085052710912",
                                "access_mode": "auth",
                                "data_model": "https://w3id.org/mdp/schema/data_model#MODEL_OTHER",
                                "title": "eliso AFIR Dynamic Data (Station & Point)",
                            },
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    offers = load_subscription_offers(
        config_path,
        feed_kinds=("dynamic", "static"),
        data_model=None,
    )

    assert [(offer.provider_uid, offer.feed_kind, offer.access_mode, offer.publication_id) for offer in offers] == [
        ("eliso", "dynamic", "auth", "843502085052710912"),
        ("eliso", "static", "auth", "843477276990078976"),
        ("tesla", "dynamic", "noauth", "953843379766972416"),
        ("tesla", "static", "noauth", "953828817873125376"),
    ]


def test_build_live_subscription_registry_pairs_dyn_and_stat_and_preserves_noauth_behavior():
    offers = [
        SubscriptionOffer(
            provider_uid="eco_movement",
            display_name="eco movement",
            publisher="Eco-Movement",
            publication_id="955166494396665856",
            offer_title="AFIR-recharging-dyn-Eco-Movement-v2 (JSON)",
            feed_kind="dynamic",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="m8mit",
            display_name="m8mit",
            publisher="msu solutions GmbH",
            publication_id="970388804493828096",
            offer_title="AFIR-recharging-dyn-m8mit-v2",
            feed_kind="dynamic",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="m8mit",
            display_name="m8mit",
            publisher="msu solutions GmbH",
            publication_id="970305056590979072",
            offer_title="AFIR-recharging-stat-m8mit",
            feed_kind="static",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="tesla",
            display_name="Tesla",
            publisher="Tesla Germany GmbH",
            publication_id="953843379766972416",
            offer_title="AFIR-recharging-dyn-Tesla",
            feed_kind="dynamic",
            access_mode="noauth",
        ),
        SubscriptionOffer(
            provider_uid="tesla",
            display_name="Tesla",
            publisher="Tesla Germany GmbH",
            publication_id="953828817873125376",
            offer_title="AFIR-recharging-stat-Tesla",
            feed_kind="static",
            access_mode="noauth",
        ),
        SubscriptionOffer(
            provider_uid="pump",
            display_name="PUMP",
            publisher="800 Volt Technologies GmbH",
            publication_id="969322788846231552",
            offer_title="AFIR-recharging-stat-PUMP",
            feed_kind="static",
            access_mode="auth",
        ),
    ]

    registry = build_live_subscription_registry(
        offers,
        [
            {"id": "980986321979551744", "dataOfferId": "955166494396665856", "contractStatus": "ACTIVE"},
            {"id": "980986232691372032", "dataOfferId": "970388804493828096", "contractStatus": "ACTIVE"},
            {"id": "980986244745637888", "dataOfferId": "970305056590979072", "contractStatus": "ACTIVE"},
            {"id": "980986356418981888", "dataOfferId": "953843379766972416", "contractStatus": "ACTIVE"},
            {"id": "980986370583146496", "dataOfferId": "953828817873125376", "contractStatus": "ACTIVE"},
            {"id": "980986256821039104", "dataOfferId": "969322788846231552", "contractStatus": "ACTIVE"},
        ],
    )

    assert registry["eco_movement"]["subscription_id"] == "980986321979551744"
    assert registry["eco_movement"]["fetch_kind"] == "mtls_subscription"
    assert registry["eco_movement"]["enabled"] is True

    assert registry["m8mit"]["subscription_id"] == "980986232691372032"
    assert registry["m8mit"]["static_subscription_id"] == "980986244745637888"
    assert registry["m8mit"]["fetch_kind"] == "mtls_subscription"
    assert registry["m8mit"]["enabled"] is True

    assert registry["tesla"]["subscription_id"] == "980986356418981888"
    assert registry["tesla"]["static_subscription_id"] == "980986370583146496"
    assert "fetch_kind" not in registry["tesla"]
    assert "enabled" not in registry["tesla"]

    assert "subscription_id" not in registry["pump"]
    assert registry["pump"]["static_subscription_id"] == "980986256821039104"
    assert registry["mobidata_bw_datex"]["fetch_kind"] == "direct_url"
    assert registry["mobidata_bw_datex"]["fetch_url"] == "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/realtime"
    assert (
        registry["mobidata_bw_datex"]["static_fetch_url"]
        == "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/static"
    )


def test_resolve_credentials_reads_secret_files(tmp_path: Path, monkeypatch):
    user_path = tmp_path / "mobilithek_user.txt"
    pwd_path = tmp_path / "mobilithek_pwd.txt"
    user_path.write_text("user@example.com\n", encoding="utf-8")
    pwd_path.write_text("secret\n", encoding="utf-8")

    monkeypatch.delenv("MOBILITHEK_USERNAME", raising=False)
    monkeypatch.delenv("MOBILITHEK_PASSWORD", raising=False)

    username, password, source = sync_module.resolve_credentials(str(user_path), str(pwd_path))
    assert username == "user@example.com"
    assert password == "secret"
    assert source == "file"
