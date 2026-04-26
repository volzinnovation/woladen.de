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
            provider_uid="edri",
            display_name="edri",
            publisher="Аmpeco Ltd.",
            publication_id="972842599324557312",
            offer_title="AFIR-recharging-dyn-EDRI",
            feed_kind="dynamic",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="edri",
            display_name="edri",
            publisher="Аmpeco Ltd.",
            publication_id="972837891969273856",
            offer_title="AFIR-recharging-stat-EDRI",
            feed_kind="static",
            access_mode="auth",
        ),
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
            provider_uid="eliso",
            display_name="eliso",
            publisher="eliso GmbH",
            publication_id="843502085052710912",
            offer_title="eliso AFIR Dynamic Data (Station & Point)",
            feed_kind="dynamic",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="eliso",
            display_name="eliso",
            publisher="eliso GmbH",
            publication_id="843477276990078976",
            offer_title="eliso AFIR Static Data (Station & Point)",
            feed_kind="static",
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
            provider_uid="volkswagencharginggroup",
            display_name="Volkswagen Group Charging",
            publisher="Volkswagen Group Charging",
            publication_id="976223649023320064",
            offer_title="AFIR-recharging-dyn-VolkswagenChargingGroup",
            feed_kind="dynamic",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="volkswagencharginggroup",
            display_name="Volkswagen Group Charging",
            publisher="Volkswagen Group Charging",
            publication_id="976221024898781184",
            offer_title="AFIR-recharging-stat-VolkswagenChargingGroup",
            feed_kind="static",
            access_mode="auth",
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
            {"id": "980986189821227008", "dataOfferId": "972842599324557312", "contractStatus": "ACTIVE"},
            {"id": "980986204027498496", "dataOfferId": "972837891969273856", "contractStatus": "ACTIVE"},
            {"id": "980986321979551744", "dataOfferId": "955166494396665856", "contractStatus": "ACTIVE"},
            {"id": "980986474933399552", "dataOfferId": "843502085052710912", "contractStatus": "ACTIVE"},
            {"id": "980986489051262976", "dataOfferId": "843477276990078976", "contractStatus": "ACTIVE"},
            {"id": "980986232691372032", "dataOfferId": "970388804493828096", "contractStatus": "ACTIVE"},
            {"id": "980986244745637888", "dataOfferId": "970305056590979072", "contractStatus": "ACTIVE"},
            {"id": "980986356418981888", "dataOfferId": "953843379766972416", "contractStatus": "ACTIVE"},
            {"id": "980986370583146496", "dataOfferId": "953828817873125376", "contractStatus": "ACTIVE"},
            {"id": "980986122297135104", "dataOfferId": "976223649023320064", "contractStatus": "ACTIVE"},
            {"id": "980986109064261632", "dataOfferId": "976221024898781184", "contractStatus": "ACTIVE"},
            {"id": "980986256821039104", "dataOfferId": "969322788846231552", "contractStatus": "ACTIVE"},
        ],
    )

    assert registry["edri"]["subscription_id"] == "980986189821227008"
    assert registry["edri"]["static_subscription_id"] == "980986204027498496"
    assert registry["edri"]["fetch_kind"] == "mtls_subscription"
    assert registry["edri"]["enabled"] is True
    assert registry["edri"]["delivery_mode"] == "push_with_poll_fallback"
    assert registry["edri"]["push_fallback_after_seconds"] == 300

    assert registry["eco_movement"]["subscription_id"] == "980986321979551744"
    assert registry["eco_movement"]["fetch_kind"] == "mtls_subscription"
    assert registry["eco_movement"]["enabled"] is True
    assert registry["eco_movement"]["delivery_mode"] == "push_with_poll_fallback"
    assert registry["eco_movement"]["push_fallback_after_seconds"] == 300

    assert registry["eliso"]["subscription_id"] == "980986474933399552"
    assert registry["eliso"]["static_subscription_id"] == "980986489051262976"
    assert registry["eliso"]["fetch_kind"] == "mtls_subscription"
    assert registry["eliso"]["enabled"] is True
    assert registry["eliso"]["delivery_mode"] == "push_with_poll_fallback"
    assert registry["eliso"]["push_fallback_after_seconds"] == 300

    assert registry["m8mit"]["subscription_id"] == "980986232691372032"
    assert registry["m8mit"]["static_subscription_id"] == "980986244745637888"
    assert registry["m8mit"]["fetch_kind"] == "mtls_subscription"
    assert registry["m8mit"]["enabled"] is True
    assert registry["m8mit"]["delivery_mode"] == "push_with_poll_fallback"
    assert registry["m8mit"]["push_fallback_after_seconds"] == 300

    assert registry["tesla"]["subscription_id"] == "980986356418981888"
    assert registry["tesla"]["static_subscription_id"] == "980986370583146496"
    assert "fetch_kind" not in registry["tesla"]
    assert "enabled" not in registry["tesla"]
    assert registry["tesla"]["delivery_mode"] == "push_with_poll_fallback"
    assert registry["tesla"]["push_fallback_after_seconds"] == 300

    assert registry["volkswagencharginggroup"]["subscription_id"] == "980986122297135104"
    assert registry["volkswagencharginggroup"]["static_subscription_id"] == "980986109064261632"
    assert registry["volkswagencharginggroup"]["fetch_kind"] == "mtls_subscription"
    assert registry["volkswagencharginggroup"]["enabled"] is True
    assert registry["volkswagencharginggroup"]["delivery_mode"] == "push_with_poll_fallback"
    assert registry["volkswagencharginggroup"]["push_fallback_after_seconds"] == 300

    assert "subscription_id" not in registry["pump"]
    assert registry["pump"]["static_subscription_id"] == "980986256821039104"
    assert registry["mobidata_bw_datex"]["fetch_kind"] == "direct_url"
    assert registry["mobidata_bw_datex"]["fetch_url"] == "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/realtime"
    assert registry["mobidata_bw_datex"]["enabled"] is False
    assert (
        registry["mobidata_bw_datex"]["static_fetch_url"]
        == "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/static"
    )
    assert registry["deprecated_chargecloud"]["enabled"] is False
    assert registry["deprecated_chargecloud"]["fetch_kind"] == "disabled"


def test_build_live_subscription_registry_applies_push_overrides_for_qwello_and_ladenetz():
    offers = [
        SubscriptionOffer(
            provider_uid="qwello",
            display_name="Qwello",
            publisher="Qwello Deutschland GmbH",
            publication_id="972966368902897664",
            offer_title="AFIR-recharging-dyn-Qwello",
            feed_kind="dynamic",
            access_mode="noauth",
        ),
        SubscriptionOffer(
            provider_uid="ladenetz_de_ladestationsdaten",
            display_name="ladenetz.de Ladestationsdaten",
            publisher="smartlab Innovationsgesellschaft mbH",
            publication_id="903240716507836416",
            offer_title="AFIR-recharging-dyn-ladenetz.de Ladestationsdaten",
            feed_kind="dynamic",
            access_mode="auth",
        ),
    ]

    registry = build_live_subscription_registry(
        offers,
        [
            {"id": "980986163111899136", "dataOfferId": "972966368902897664", "contractStatus": "ACTIVE"},
            {"id": "980986407799205888", "dataOfferId": "903240716507836416", "contractStatus": "ACTIVE"},
        ],
    )

    assert registry["qwello"]["delivery_mode"] == "push_with_poll_fallback"
    assert registry["qwello"]["push_fallback_after_seconds"] == 300
    assert registry["ladenetz_de_ladestationsdaten"]["delivery_mode"] == "push_with_poll_fallback"
    assert registry["ladenetz_de_ladestationsdaten"]["push_fallback_after_seconds"] == 300


def test_build_live_subscription_registry_applies_push_overrides_for_remaining_push_providers():
    offers = [
        SubscriptionOffer(
            provider_uid="chargecloud",
            display_name="chargecloud",
            publisher="chargecloud GmbH",
            publication_id="980862594474274816",
            offer_title="AFIR-recharging-dyn-chargecloud-json",
            feed_kind="dynamic",
            access_mode="noauth",
        ),
        SubscriptionOffer(
            provider_uid="enbwmobility",
            display_name="enbwmobility",
            publisher="EnBW AG",
            publication_id="907575401287241728",
            offer_title="AFIR-recharging-dyn-EnBWmobility+",
            feed_kind="dynamic",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="gls_mobility",
            display_name="gls mobility",
            publisher="GLS Mobility GmbH",
            publication_id="980563757096464384",
            offer_title="AFIR-recharging-dyn-gls-mobility",
            feed_kind="dynamic",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="gls_mobility",
            display_name="gls mobility",
            publisher="GLS Mobility GmbH",
            publication_id="980559859451379712",
            offer_title="AFIR-recharging-stat-gls-mobility",
            feed_kind="static",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="enio",
            display_name="enio",
            publisher="ENIO GmbH",
            publication_id="968541134128902144",
            offer_title="AFIR-recharging-dyn-enio",
            feed_kind="dynamic",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="enio",
            display_name="enio",
            publisher="ENIO GmbH",
            publication_id="963766220171735040",
            offer_title="AFIR-recharging-stat-enio",
            feed_kind="static",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="monta",
            display_name="monta",
            publisher="Monta ApS",
            publication_id="963870983660167168",
            offer_title="AFIR-recharging-dyn-MONTA",
            feed_kind="dynamic",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="smatrics",
            display_name="smatrics",
            publisher="SMATRICS GmbH & Co KG",
            publication_id="961319990963605504",
            offer_title="AFIR-recharging-dyn-SMATRICS",
            feed_kind="dynamic",
            access_mode="noauth",
        ),
        SubscriptionOffer(
            provider_uid="vaylens",
            display_name="vaylens",
            publisher="vaylens GmbH",
            publication_id="979364650281549824",
            offer_title="AFIR-recharging-dyn-vaylens GmbH",
            feed_kind="dynamic",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="wirelane",
            display_name="wirelane",
            publisher="Wirelane GmbH",
            publication_id="876587237907525632",
            offer_title="AFIR-recharging-dyn-Wirelane",
            feed_kind="dynamic",
            access_mode="auth",
        ),
    ]

    registry = build_live_subscription_registry(
        offers,
        [
            {"id": "980986017846382592", "dataOfferId": "980862594474274816", "contractStatus": "ACTIVE"},
            {"id": "983491435542016000", "dataOfferId": "968541134128902144", "contractStatus": "ACTIVE"},
            {"id": "983491437521539072", "dataOfferId": "963766220171735040", "contractStatus": "ACTIVE"},
            {"id": "980985483907280896", "dataOfferId": "907575401287241728", "contractStatus": "ACTIVE"},
            {"id": "981536505742471168", "dataOfferId": "980563757096464384", "contractStatus": "ACTIVE"},
            {"id": "981605287991169024", "dataOfferId": "980559859451379712", "contractStatus": "ACTIVE"},
            {"id": "982024950290042880", "dataOfferId": "963870983660167168", "contractStatus": "ACTIVE"},
            {"id": "980986307605835776", "dataOfferId": "961319990963605504", "contractStatus": "ACTIVE"},
            {"id": "980986055062597632", "dataOfferId": "979364650281549824", "contractStatus": "ACTIVE"},
            {"id": "980986434407878656", "dataOfferId": "876587237907525632", "contractStatus": "ACTIVE"},
        ],
    )

    for provider_uid in ("chargecloud", "enbwmobility", "enio", "gls_mobility", "monta", "smatrics", "vaylens", "wirelane"):
        assert registry[provider_uid]["delivery_mode"] == "push_with_poll_fallback"
        assert registry[provider_uid]["push_fallback_after_seconds"] == 300

    assert registry["enio"]["subscription_id"] == "983491435542016000"
    assert registry["enio"]["static_subscription_id"] == "983491437521539072"
    assert registry["enio"]["fetch_kind"] == "mtls_subscription"
    assert registry["enio"]["enabled"] is True

    assert registry["gls_mobility"]["subscription_id"] == "981536505742471168"
    assert registry["gls_mobility"]["static_subscription_id"] == "981605287991169024"
    assert registry["gls_mobility"]["fetch_kind"] == "mtls_subscription"
    assert registry["gls_mobility"]["enabled"] is True


def test_build_live_subscription_registry_enables_new_active_afir_providers():
    offers = [
        SubscriptionOffer(
            provider_uid="grid",
            display_name="grid",
            publisher="Grid & Co. GmbH",
            publication_id="984103903968534528",
            offer_title="AFIR-recharging-dyn-Grid&Co",
            feed_kind="dynamic",
            access_mode="noauth",
        ),
        SubscriptionOffer(
            provider_uid="grid",
            display_name="grid",
            publisher="Grid & Co. GmbH",
            publication_id="984104561811357696",
            offer_title="AFIR-recharging-stat-Grid&Co",
            feed_kind="static",
            access_mode="noauth",
        ),
        SubscriptionOffer(
            provider_uid="ladebusiness_ladestationsdaten",
            display_name="ladebusiness ladestationsdaten",
            publisher="Smartlab Innovationsgesellschaft mbH",
            publication_id="903321397006716928",
            offer_title="ladebusiness Ladestationsdaten - dynamisch",
            feed_kind="dynamic",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="ladebusiness_ladestationsdaten",
            display_name="ladebusiness ladestationsdaten",
            publisher="Smartlab Innovationsgesellschaft mbH",
            publication_id="903241622921695232",
            offer_title="ladebusiness Ladestationsdaten - statisch",
            feed_kind="static",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="stadtwerke_erft",
            display_name="stadtwerke erft",
            publisher="Stadtwerke Erft GmbH",
            publication_id="982973289122725888",
            offer_title="AFIR-recharging-dyn-Stadtwerke Erft GmbH",
            feed_kind="dynamic",
            access_mode="auth",
        ),
        SubscriptionOffer(
            provider_uid="stadtwerke_erft",
            display_name="stadtwerke erft",
            publisher="Stadtwerke Erft GmbH",
            publication_id="982969565784539136",
            offer_title="AFIR-recharging-stat-Stadtwerke Erft GmbH",
            feed_kind="static",
            access_mode="auth",
        ),
    ]

    registry = build_live_subscription_registry(
        offers,
        [
            {"id": "984414112490385408", "dataOfferId": "984103903968534528", "contractStatus": "ACTIVE"},
            {"id": "984414030168719360", "dataOfferId": "984104561811357696", "contractStatus": "ACTIVE"},
            {"id": "985185069718962176", "dataOfferId": "903321397006716928", "contractStatus": "ACTIVE"},
            {"id": "985185057874120704", "dataOfferId": "903241622921695232", "contractStatus": "ACTIVE"},
            {"id": "983032226584989696", "dataOfferId": "982973289122725888", "contractStatus": "ACTIVE"},
            {"id": "983032272248377344", "dataOfferId": "982969565784539136", "contractStatus": "ACTIVE"},
        ],
    )

    assert registry["grid"]["subscription_id"] == "984414112490385408"
    assert registry["grid"]["static_subscription_id"] == "984414030168719360"
    assert "fetch_kind" not in registry["grid"]
    assert "enabled" not in registry["grid"]
    assert registry["grid"]["delivery_mode"] == "push_with_poll_fallback"

    assert registry["ladebusiness_ladestationsdaten"]["subscription_id"] == "985185069718962176"
    assert registry["ladebusiness_ladestationsdaten"]["static_subscription_id"] == "985185057874120704"
    assert registry["ladebusiness_ladestationsdaten"]["fetch_kind"] == "mtls_subscription"
    assert registry["ladebusiness_ladestationsdaten"]["enabled"] is True
    assert registry["ladebusiness_ladestationsdaten"]["delivery_mode"] == "push_with_poll_fallback"

    assert registry["stadtwerke_erft"]["subscription_id"] == "983032226584989696"
    assert registry["stadtwerke_erft"]["static_subscription_id"] == "983032272248377344"
    assert registry["stadtwerke_erft"]["fetch_kind"] == "mtls_subscription"
    assert registry["stadtwerke_erft"]["enabled"] is True
    assert registry["stadtwerke_erft"]["delivery_mode"] == "push_with_poll_fallback"


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
