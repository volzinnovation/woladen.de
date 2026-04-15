#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig
from backend.datex import decode_json_payload, extract_dynamic_facts
from backend.fetcher import CurlFetcher
from backend.loaders import load_provider_targets
from backend.store import LiveStore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Mobilithek mTLS subscription registry entries")
    parser.add_argument("--provider", default="", help="Validate only one provider UID")
    parser.add_argument("--probe-certificate", action="store_true", help="Run the mTLS handshake probe with subscriptionID=0")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = AppConfig()
    fetcher = CurlFetcher(config)
    store = LiveStore(config)
    store.initialize()

    result: dict[str, object] = {
        "subscription_registry_path": str(config.subscription_registry_path),
        "machine_cert_p12": str(config.machine_cert_p12),
        "providers": [],
    }

    if args.probe_certificate:
        try:
            probe = fetcher.probe_certificate()
            result["certificate_probe"] = {
                "status": "ok",
                "http_status": probe.http_status,
                "content_type": probe.content_type,
                "body_length": len(probe.body),
            }
        except Exception as exc:
            result["certificate_probe"] = {"status": "error", "error": str(exc)}

    providers = load_provider_targets(
        config.provider_config_path,
        config.provider_override_path,
        config.subscription_registry_path,
    )
    if args.provider:
        providers = [provider for provider in providers if provider.provider_uid == args.provider]

    site_maps = {provider.provider_uid: store.get_site_station_map(provider.provider_uid) for provider in providers}
    for provider in providers:
        if provider.fetch_kind != "mtls_subscription" or not provider.enabled:
            continue
        provider_result = {
            "provider_uid": provider.provider_uid,
            "fetch_url": provider.fetch_url,
            "subscription_id": provider.subscription_id,
        }
        try:
            response = fetcher.fetch(provider)
            provider_result["http_status"] = response.http_status
            provider_result["content_type"] = response.content_type
            if response.http_status in (204, 304):
                provider_result["status"] = "ok"
                provider_result["result"] = "no_data" if response.http_status == 204 else "not_modified"
            else:
                payload = decode_json_payload(response.body)
                facts = extract_dynamic_facts(payload, provider.provider_uid, site_maps.get(provider.provider_uid, {}))
                provider_result["status"] = "ok"
                provider_result["observation_count"] = len(facts)
                provider_result["matched_station_count"] = len({fact.station_id for fact in facts if fact.station_id})
        except Exception as exc:
            provider_result["status"] = "error"
            provider_result["error"] = str(exc)
        result["providers"].append(provider_result)

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
