#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests
import urllib3

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig
from backend.subscriptions import build_live_subscription_registry, load_subscription_offers

CONTRACT_SEARCH_URL = "https://mobilithek.info/mdp-api/mdp-msa-contracts/v1/contracts/search"
MOBILITHEK_TOKEN_URL = "https://mobilithek.info/auth/realms/MDP/protocol/openid-connect/token"
MOBILITHEK_USERNAME_ENV = "MOBILITHEK_USERNAME"
MOBILITHEK_PASSWORD_ENV = "MOBILITHEK_PASSWORD"
DEFAULT_PAGE_SIZE = 100
DEFAULT_USERNAME_FILE = REPO_ROOT / "secret" / "mobilithek_user.txt"
DEFAULT_PASSWORD_FILE = REPO_ROOT / "secret" / "mobilithek_pwd.txt"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Sync active Mobilithek static and dynamic subscriptions into "
            "secret/mobilithek_subscriptions.json and retain configured external DATEX direct URLs"
        )
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the computed registry without writing it")
    parser.add_argument("--access-token", default="", help="Use an explicit Mobilithek bearer token")
    parser.add_argument("--username-file", default=str(DEFAULT_USERNAME_FILE), help="Path to Mobilithek username file")
    parser.add_argument("--password-file", default=str(DEFAULT_PASSWORD_FILE), help="Path to Mobilithek password file")
    return parser.parse_args()


def _read_optional_text(path_text: str) -> str:
    path_value = str(path_text or "").strip()
    if not path_value:
        return ""
    path = Path(path_value).expanduser()
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def resolve_credentials(username_file: str, password_file: str) -> tuple[str, str, str]:
    username = os.environ.get(MOBILITHEK_USERNAME_ENV, "").strip()
    password = os.environ.get(MOBILITHEK_PASSWORD_ENV, "").strip()
    if username and password:
        return username, password, "env"

    file_username = _read_optional_text(username_file)
    file_password = _read_optional_text(password_file)
    if file_username and file_password:
        return file_username, file_password, "file"

    return "", "", "missing"


def fetch_mobilithek_access_token(session: requests.Session, *, username: str, password: str) -> str | None:
    if not username or not password:
        return None

    response = session.post(
        MOBILITHEK_TOKEN_URL,
        data={
            "grant_type": "password",
            "client_id": "Platform",
            "username": username,
            "password": password,
        },
        timeout=30,
        verify=False,
    )
    response.raise_for_status()
    payload = response.json()
    token = str(payload.get("access_token") or "").strip()
    return token or None


def resolve_access_token(explicit_token: str, *, username: str, password: str, credential_source: str) -> tuple[str | None, str]:
    token = explicit_token.strip()
    if token:
        return token, "arg"

    env_token = os.environ.get("MOBILITHEK_ACCESS_TOKEN", "").strip()
    if env_token:
        return env_token, "env"

    session = requests.Session()
    token = fetch_mobilithek_access_token(session, username=username, password=password)
    return token, credential_source if token else "missing"


def fetch_account_subscriptions(access_token: str) -> dict[str, Any]:
    session = requests.Session()
    payloads: list[dict[str, Any]] = []
    page = 0

    while True:
        response = session.post(
            CONTRACT_SEARCH_URL,
            params={"page": page, "size": DEFAULT_PAGE_SIZE, "sort": "contractStatus,asc"},
            headers={"Authorization": f"Bearer {access_token}"},
            json={"searchString": ""},
            timeout=30,
            verify=False,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError("unexpected_contracts_payload")
        payloads.append(payload)
        if payload.get("last", True):
            break
        page += 1

    merged_content: list[dict[str, Any]] = []
    for payload in payloads:
        merged_content.extend(list(payload.get("content") or []))
    return {
        "content": merged_content,
        "totalElements": payloads[0].get("totalElements", len(merged_content)) if payloads else len(merged_content),
        "totalPages": payloads[0].get("totalPages", len(payloads)) if payloads else 0,
    }


def main() -> None:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    args = parse_args()
    config = AppConfig()
    offers = load_subscription_offers(
        config.provider_config_path,
        feed_kinds=("dynamic", "static"),
        data_model=None,
    )
    username, password, credential_source = resolve_credentials(args.username_file, args.password_file)
    access_token, token_source = resolve_access_token(
        args.access_token,
        username=username,
        password=password,
        credential_source=credential_source,
    )

    contracts_payload: dict[str, Any] = {}
    contracts: list[dict[str, Any]] = []
    fetch_error = ""
    if access_token:
        try:
            contracts_payload = fetch_account_subscriptions(access_token)
            contracts = list(contracts_payload.get("content") or [])
        except Exception as exc:
            fetch_error = str(exc)
    registry = build_live_subscription_registry(offers, contracts)

    if not args.dry_run:
        config.subscription_registry_path.parent.mkdir(parents=True, exist_ok=True)
        config.subscription_registry_path.write_text(
            json.dumps(registry, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    result = {
        "registry_path": str(config.subscription_registry_path),
        "offer_count": len(offers),
        "token_source": token_source,
        "contracts_total_elements": contracts_payload.get("totalElements", len(contracts)) if contracts_payload else 0,
        "dynamic_entry_count": sum(1 for entry in registry.values() if str(entry.get("subscription_id") or "").strip()),
        "static_entry_count": sum(1 for entry in registry.values() if str(entry.get("static_subscription_id") or "").strip()),
        "external_direct_entry_count": sum(
            1
            for entry in registry.values()
            if str(entry.get("fetch_kind") or "").strip() == "direct_url"
            and str(entry.get("fetch_url") or "").strip()
        ),
        "paired_provider_count": sum(
            1
            for entry in registry.values()
            if str(entry.get("subscription_id") or "").strip() and str(entry.get("static_subscription_id") or "").strip()
        ),
        "fetch_error": fetch_error,
        "providers": registry,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
