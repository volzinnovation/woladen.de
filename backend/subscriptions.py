from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

DATEX_V3_DATA_MODEL = "https://w3id.org/mdp/schema/data_model#DATEX_2_V3"
MOBIDATA_BW_DATEX_STATIC_URL = "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/static"
MOBIDATA_BW_DATEX_DYNAMIC_URL = "https://api.mobidata-bw.de/ocpdb/api/public/datex/v3.5/json/realtime"
ACTIVE_SUBSCRIPTION_STATUSES = frozenset({"ACTIVE"})
ACTIVE_DYN_DATEX_SUBSCRIPTION_PROVIDER_UIDS = (
    "edri",
    "eco_movement",
    "stadtwerke_erft",
    "gls_mobility",
    "ladebusiness_ladestationsdaten",
    "elu_mobility",
    "enio",
    "enbwmobility",
    "eround",
    "lichtblick_emobility",
    "ladenetz_de_ladestationsdaten",
    "m8mit",
    "vaylens",
    "volkswagencharginggroup",
    "wirelane",
)
# eliso uses the same authenticated Mobilithek subscription flow, but publishes a
# generic JSON model instead of DATEX V3.
ACTIVE_DYNAMIC_SUBSCRIPTION_PROVIDER_UIDS = ACTIVE_DYN_DATEX_SUBSCRIPTION_PROVIDER_UIDS + ("eliso",)
LIVE_PUSH_FALLBACK_PROVIDER_UIDS = frozenset(
    {
        "chargecloud",
        "eco_movement",
        "edri",
        "eliso",
        "elu_mobility",
        "enio",
        "enbwmobility",
        "eround",
        "grid",
        "gls_mobility",
        "ladebusiness_ladestationsdaten",
        "ladenetz_de_ladestationsdaten",
        "lichtblick_emobility",
        "m8mit",
        "monta",
        "qwello",
        "smatrics",
        "stadtwerke_erft",
        "tesla",
        "vaylens",
        "volkswagencharginggroup",
        "wirelane",
    }
)
LIVE_REGISTRY_PROVIDER_OVERRIDES: dict[str, dict[str, Any]] = {
    provider_uid: {
        "delivery_mode": "push_with_poll_fallback",
        "push_fallback_after_seconds": 300,
    }
    for provider_uid in LIVE_PUSH_FALLBACK_PROVIDER_UIDS
}
LIVE_REGISTRY_PROVIDER_OVERRIDES["monta"].update(
    {
        "enabled": True,
        "fetch_kind": "mtls_subscription",
    }
)
LIVE_REGISTRY_PROVIDER_OVERRIDES["mobidata_bw_datex"] = {"enabled": False}
LIVE_REGISTRY_DISABLED_PROVIDER_ENTRIES: dict[str, dict[str, Any]] = {
    "deprecated_chargecloud": {
        "provider_uid": "deprecated_chargecloud",
        "display_name": "deprecated chargecloud",
        "publisher": "chargecloud GmbH",
        "enabled": False,
        "fetch_kind": "disabled",
        "fetch_url": "",
        "publication_id": "deprecated_chargecloud",
        "access_mode": "",
        "note": "Explicitly disabled after migration away from the deprecated chargecloud target.",
    }
}


@dataclass(frozen=True)
class SubscriptionOffer:
    provider_uid: str
    display_name: str
    publisher: str
    publication_id: str
    offer_title: str
    feed_kind: str = "dynamic"
    access_mode: str = ""
    data_model: str = ""


@dataclass(frozen=True)
class ExternalDatexSource:
    provider_uid: str
    display_name: str
    publisher: str
    dynamic_url: str
    static_url: str
    dynamic_title: str
    static_title: str


EXTERNAL_DIRECT_DATEX_SOURCES = (
    ExternalDatexSource(
        provider_uid="mobidata_bw_datex",
        display_name="MobiData BW DATEX II",
        publisher="MobiData BW",
        dynamic_url=MOBIDATA_BW_DATEX_DYNAMIC_URL,
        static_url=MOBIDATA_BW_DATEX_STATIC_URL,
        dynamic_title="MobiData BW DATEX II realtime",
        static_title="MobiData BW DATEX II static",
    ),
)


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _nested_text(mapping: Mapping[str, Any], *paths: tuple[str, ...]) -> str:
    for path in paths:
        current: Any = mapping
        missing = False
        for part in path:
            if not isinstance(current, Mapping) or part not in current:
                missing = True
                break
            current = current[part]
        if missing:
            continue
        value = _text(current)
        if value:
            return value
    return ""


def load_subscription_offers(
    config_path: Path,
    *,
    feed_kinds: Sequence[str] = ("dynamic",),
    allowed_provider_uids: Sequence[str] | None = None,
    access_modes: Sequence[str] | None = None,
    data_model: str | None = DATEX_V3_DATA_MODEL,
) -> list[SubscriptionOffer]:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    providers = payload.get("providers") or []
    offers: list[SubscriptionOffer] = []
    allowed = set(allowed_provider_uids) if allowed_provider_uids is not None else None
    allowed_feed_kinds = tuple(str(kind).strip() for kind in feed_kinds if str(kind).strip())
    allowed_access_modes = set(access_modes) if access_modes is not None else None

    for provider in providers:
        if not isinstance(provider, Mapping):
            continue
        provider_uid = _text(provider.get("uid"))
        if not provider_uid:
            continue
        if allowed is not None and provider_uid not in allowed:
            continue

        for feed_kind in allowed_feed_kinds:
            feed = ((provider.get("feeds") or {}).get(feed_kind)) or {}
            publication_id = _text(feed.get("publication_id"))
            access_mode = _text(feed.get("access_mode"))
            offer_data_model = _text(feed.get("data_model"))
            if not publication_id:
                continue
            if allowed_access_modes is not None and access_mode not in allowed_access_modes:
                continue
            if data_model is not None and offer_data_model != data_model:
                continue

            offers.append(
                SubscriptionOffer(
                    provider_uid=provider_uid,
                    display_name=_text(provider.get("display_name")),
                    publisher=_text(provider.get("publisher")),
                    publication_id=publication_id,
                    offer_title=_text(feed.get("title")),
                    feed_kind=feed_kind,
                    access_mode=access_mode,
                    data_model=offer_data_model,
                )
            )

    return sorted(offers, key=lambda offer: (offer.provider_uid, offer.feed_kind, offer.publication_id))


def load_active_dyn_datex_subscription_offers(config_path: Path) -> list[SubscriptionOffer]:
    return load_subscription_offers(
        config_path,
        feed_kinds=("dynamic",),
        allowed_provider_uids=ACTIVE_DYN_DATEX_SUBSCRIPTION_PROVIDER_UIDS,
        access_modes=("auth",),
        data_model=DATEX_V3_DATA_MODEL,
    )


def normalize_subscription_contract(contract: Mapping[str, Any]) -> dict[str, str]:
    contract_id = _text(contract.get("id")) or _text(contract.get("contractId")) or _text(
        contract.get("subscriptionId")
    )
    publication_id = _text(contract.get("dataOfferId")) or _text(contract.get("publicationId")) or _nested_text(
        contract,
        ("dataOffer", "publicationId"),
        ("dataOffer", "id"),
        ("offer", "publicationId"),
        ("offer", "id"),
    )
    status = _text(contract.get("contractStatus")) or _text(contract.get("subscriptionStatus")) or _text(
        contract.get("status")
    ) or _nested_text(
        contract,
        ("subscriptionStatus", "value"),
        ("status", "value"),
    )
    offer_title = _text(contract.get("dataOfferTitle")) or _text(contract.get("title")) or _nested_text(
        contract,
        ("dataOffer", "title"),
        ("offer", "title"),
    )
    provider_name = _text(contract.get("providerName")) or _nested_text(
        contract,
        ("dataOffer", "providerName"),
        ("dataOffer", "publisher", "name"),
        ("offer", "providerName"),
    )
    active_since = _text(contract.get("activeSince")) or _text(contract.get("createdAt")) or _text(
        contract.get("updatedAt")
    )
    return {
        "contract_id": contract_id,
        "publication_id": publication_id,
        "status": status.upper(),
        "offer_title": offer_title,
        "provider_name": provider_name,
        "active_since": active_since,
    }


def _contract_sort_key(contract: Mapping[str, str]) -> tuple[int, str, str]:
    contract_id = contract.get("contract_id") or ""
    numeric = contract_id if contract_id.isdigit() else ""
    return (1 if contract.get("status") in ACTIVE_SUBSCRIPTION_STATUSES else 0, contract.get("active_since") or "", numeric)


def select_active_subscription_contracts(
    offers: Iterable[SubscriptionOffer],
    contracts: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, str]]:
    offers_by_publication = {offer.publication_id: offer for offer in offers}
    best_by_publication: dict[str, dict[str, str]] = {}

    for raw_contract in contracts:
        normalized = normalize_subscription_contract(raw_contract)
        publication_id = normalized.get("publication_id") or ""
        contract_id = normalized.get("contract_id") or ""
        if not publication_id or not contract_id or normalized.get("status") not in ACTIVE_SUBSCRIPTION_STATUSES:
            continue
        if publication_id not in offers_by_publication:
            continue
        current = best_by_publication.get(publication_id)
        if current is None or _contract_sort_key(normalized) > _contract_sort_key(current):
            best_by_publication[publication_id] = normalized

    return {
        offer.provider_uid: best_by_publication[offer.publication_id]
        for offer in offers
        if offer.publication_id in best_by_publication
    }


def select_active_subscription_contracts_by_publication(
    offers: Iterable[SubscriptionOffer],
    contracts: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, str]]:
    offers_by_publication = {offer.publication_id: offer for offer in offers}
    best_by_publication: dict[str, dict[str, str]] = {}

    for raw_contract in contracts:
        normalized = normalize_subscription_contract(raw_contract)
        publication_id = normalized.get("publication_id") or ""
        contract_id = normalized.get("contract_id") or ""
        if not publication_id or not contract_id or normalized.get("status") not in ACTIVE_SUBSCRIPTION_STATUSES:
            continue
        if publication_id not in offers_by_publication:
            continue
        current = best_by_publication.get(publication_id)
        if current is None or _contract_sort_key(normalized) > _contract_sort_key(current):
            best_by_publication[publication_id] = normalized

    return best_by_publication


def build_subscription_registry(
    offers: Iterable[SubscriptionOffer],
    contracts: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    offer_list = list(offers)
    active_contracts = select_active_subscription_contracts(offer_list, contracts)
    registry: dict[str, dict[str, Any]] = {}

    for offer in offer_list:
        contract = active_contracts.get(offer.provider_uid)
        note = "Active dynamic DATEX subscription from docs; subscription_id still needs to be resolved from Mobilithek."
        entry: dict[str, Any] = {
            "enabled": False,
            "fetch_kind": "mtls_subscription",
            "subscription_id": "",
            "publication_id": offer.publication_id,
            "offer_title": offer.offer_title,
            "note": note,
        }
        if contract:
            entry["enabled"] = True
            entry["subscription_id"] = contract["contract_id"]
            entry["note"] = (
                "Active dynamic DATEX subscription synced from Mobilithek account "
                f"with status {contract['status']}."
            )
        registry[offer.provider_uid] = entry

    merged_registry = {provider_uid: registry[provider_uid] for provider_uid in sorted(registry)}
    for source in EXTERNAL_DIRECT_DATEX_SOURCES:
        merged_registry[source.provider_uid] = {
            "provider_uid": source.provider_uid,
            "display_name": source.display_name,
            "publisher": source.publisher,
            "enabled": True,
            "fetch_kind": "direct_url",
            "fetch_url": source.dynamic_url,
            "publication_id": source.dynamic_url,
            "offer_title": source.dynamic_title,
            "access_mode": "noauth",
            "static_fetch_kind": "direct_url",
            "static_fetch_url": source.static_url,
            "static_publication_id": source.static_url,
            "static_offer_title": source.static_title,
            "static_access_mode": "noauth",
            "note": "Public DATEX II endpoints outside Mobilithek (since April 2026).",
        }

    return {provider_uid: merged_registry[provider_uid] for provider_uid in sorted(merged_registry)}


def build_live_subscription_registry(
    offers: Iterable[SubscriptionOffer],
    contracts: Iterable[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    offer_list = list(offers)
    active_contracts = select_active_subscription_contracts_by_publication(offer_list, contracts)
    supported_dynamic_uids = set(ACTIVE_DYNAMIC_SUBSCRIPTION_PROVIDER_UIDS)
    registry: dict[str, dict[str, Any]] = {}

    for offer in offer_list:
        contract = active_contracts.get(offer.publication_id)
        if contract is None:
            continue

        entry = registry.setdefault(
            offer.provider_uid,
            {
                "provider_uid": offer.provider_uid,
                "display_name": offer.display_name,
                "publisher": offer.publisher,
            },
        )

        if offer.feed_kind == "dynamic":
            entry["subscription_id"] = contract["contract_id"]
            entry["publication_id"] = offer.publication_id
            entry["offer_title"] = offer.offer_title
            entry["access_mode"] = offer.access_mode
            if offer.access_mode == "auth" and offer.provider_uid in supported_dynamic_uids:
                entry["enabled"] = True
                entry["fetch_kind"] = "mtls_subscription"
        elif offer.feed_kind == "static":
            entry["static_subscription_id"] = contract["contract_id"]
            entry["static_publication_id"] = offer.publication_id
            entry["static_offer_title"] = offer.offer_title
            entry["static_access_mode"] = offer.access_mode

    merged_registry = {provider_uid: registry[provider_uid] for provider_uid in sorted(registry)}
    for source in EXTERNAL_DIRECT_DATEX_SOURCES:
        merged_registry[source.provider_uid] = {
            "provider_uid": source.provider_uid,
            "display_name": source.display_name,
            "publisher": source.publisher,
            "enabled": True,
            "fetch_kind": "direct_url",
            "fetch_url": source.dynamic_url,
            "publication_id": source.dynamic_url,
            "offer_title": source.dynamic_title,
            "access_mode": "noauth",
            "static_fetch_kind": "direct_url",
            "static_fetch_url": source.static_url,
            "static_publication_id": source.static_url,
            "static_offer_title": source.static_title,
            "static_access_mode": "noauth",
            "note": "Public DATEX II endpoints outside Mobilithek (since April 2026).",
        }

    for provider_uid, entry in LIVE_REGISTRY_DISABLED_PROVIDER_ENTRIES.items():
        merged_registry[provider_uid] = dict(entry)

    for provider_uid, overrides in LIVE_REGISTRY_PROVIDER_OVERRIDES.items():
        if provider_uid not in merged_registry:
            continue
        merged_registry[provider_uid].update(overrides)

    return {provider_uid: merged_registry[provider_uid] for provider_uid in sorted(merged_registry)}
