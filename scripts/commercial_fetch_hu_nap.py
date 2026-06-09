#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from commercial_backend.config import AppConfig  # noqa: E402
from commercial_backend.hu_nap import (  # noqa: E402
    COUNTRY_CODE,
    HU_NAP_AMPECO_TEST_DYNAMIC_PROFILE_ID,
    HU_NAP_AMPECO_TEST_DYNAMIC_SOURCE_UID,
    HU_NAP_AMPECO_TEST_STATIC_PROFILE_ID,
    HU_NAP_AMPECO_TEST_STATIC_SOURCE_UID,
    HU_NAP_CONTRACTED_PROFILE_SUMMARIES_URL,
    HU_NAP_ECO_MOVEMENT_STATIC_PROFILE_ID,
    HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID,
    HU_NAP_MAGYAR_KOZUT_PARKING_CHARGING_STATIC_PROFILE_ID,
    HU_NAP_MAGYAR_KOZUT_PARKING_CHARGING_STATIC_SOURCE_UID,
    HU_NAP_MOBILITI_STATIC_PROFILE_ID,
    HU_NAP_MOBILITI_STATIC_SOURCE_UID,
    HU_NAP_TOKEN_URL,
    count_xml_records_from_binary_stream,
    metadata_summary_from_subscription,
)
from commercial_backend.store import create_ingest_store  # noqa: E402

USER_AGENT = "woladen.de public HU NAP ingester"
DEFAULT_HU_NAP_EMAIL = "raphael.volz@hs-pforzheim.de"
DEFAULT_HU_NAP_EMAIL_PATH = REPO_ROOT / "secret" / "hu_nap_email.txt"
DEFAULT_HU_NAP_PASSWORD_PATH = REPO_ROOT / "secret" / "hu_nap_password.txt"
DEFAULT_HU_NAP_CLIENT_AUTHORIZATION = (
    "Basic "
    "V2ViIGRldjpTMkZwTnpaSFVqaFlRMEpCYkdKYVZHbFhaM0JEVVRCalpXSnRXRWQ0V2tsV1dV"
    "NW9VMWxXV2tNdlp6MD0="
)
URL_OPEN_MAX_ATTEMPTS = 3
URL_OPEN_RETRY_SLEEP_SECONDS = (2.0, 5.0)


@dataclass(frozen=True)
class SourceSpec:
    key: str
    profile_id: int
    source_uid: str
    display_name: str
    suffix: str
    source_kind: str
    task_kind: str
    url: str = ""


SOURCES = {
    "eco-movement-static": SourceSpec(
        key="eco-movement-static",
        profile_id=HU_NAP_ECO_MOVEMENT_STATIC_PROFILE_ID,
        source_uid=HU_NAP_ECO_MOVEMENT_STATIC_SOURCE_UID,
        display_name="HU NAP Eco-Movement DATEX II v3.3 charging infrastructure table",
        suffix=".xml",
        source_kind="hu_nap_datex_3_3_static_table",
        task_kind="parse_static_payload",
    ),
    "mobiliti-static": SourceSpec(
        key="mobiliti-static",
        profile_id=HU_NAP_MOBILITI_STATIC_PROFILE_ID,
        source_uid=HU_NAP_MOBILITI_STATIC_SOURCE_UID,
        display_name="HU NAP MVM Mobiliti DATEX II v3.3 charging infrastructure table",
        suffix=".xml",
        source_kind="hu_nap_datex_3_3_static_table",
        task_kind="parse_static_payload",
    ),
    "ampeco-test-static": SourceSpec(
        key="ampeco-test-static",
        profile_id=HU_NAP_AMPECO_TEST_STATIC_PROFILE_ID,
        source_uid=HU_NAP_AMPECO_TEST_STATIC_SOURCE_UID,
        display_name="HU NAP AMPECO test DATEX II v3.3 static subscription",
        suffix=".xml",
        source_kind="hu_nap_datex_3_3_static_test_ack",
        task_kind="parse_static_payload",
    ),
    "ampeco-test-dynamic": SourceSpec(
        key="ampeco-test-dynamic",
        profile_id=HU_NAP_AMPECO_TEST_DYNAMIC_PROFILE_ID,
        source_uid=HU_NAP_AMPECO_TEST_DYNAMIC_SOURCE_UID,
        display_name="HU NAP AMPECO test DATEX II v3.3 dynamic subscription",
        suffix=".xml",
        source_kind="hu_nap_datex_3_3_dynamic_test_ack",
        task_kind="parse_dynamic_payload",
    ),
    "magyar-kozut-static": SourceSpec(
        key="magyar-kozut-static",
        profile_id=HU_NAP_MAGYAR_KOZUT_PARKING_CHARGING_STATIC_PROFILE_ID,
        source_uid=HU_NAP_MAGYAR_KOZUT_PARKING_CHARGING_STATIC_SOURCE_UID,
        display_name="HU NAP Magyar Kozut parking/charging static subscription",
        suffix=".xml",
        source_kind="hu_nap_datex_static_parking_charging_ack",
        task_kind="parse_static_payload",
    ),
}

SOURCE_GROUPS = {
    "static-real": ("eco-movement-static", "mobiliti-static"),
    "all-active": tuple(SOURCES),
}


def _read_secret_file(path_text: str) -> str:
    if not path_text:
        return ""
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def _credential_value(
    *,
    env: dict[str, str],
    value_name: str,
    file_name: str,
    default_path: Path,
    default_value: str = "",
) -> str:
    value = str(env.get(value_name, "")).strip()
    if value:
        return value
    value = _read_secret_file(str(env.get(file_name, "")).strip())
    if value:
        return value
    value = _read_secret_file(str(default_path))
    if value:
        return value
    return default_value


def _credentials_from_env(env: dict[str, str] | None = None) -> tuple[str, str]:
    effective_env = os.environ if env is None else env
    email = _credential_value(
        env=effective_env,
        value_name="HU_NAP_EMAIL",
        file_name="HU_NAP_EMAIL_FILE",
        default_path=DEFAULT_HU_NAP_EMAIL_PATH,
        default_value=DEFAULT_HU_NAP_EMAIL,
    )
    password = _credential_value(
        env=effective_env,
        value_name="HU_NAP_PASSWORD",
        file_name="HU_NAP_PASSWORD_FILE",
        default_path=DEFAULT_HU_NAP_PASSWORD_PATH,
    )
    if not email:
        raise RuntimeError("HU NAP email missing. Set HU_NAP_EMAIL or HU_NAP_EMAIL_FILE.")
    if not password:
        raise RuntimeError(
            "HU NAP password missing. Set HU_NAP_PASSWORD, HU_NAP_PASSWORD_FILE, or secret/hu_nap_password.txt."
        )
    return email, password


def _client_authorization(env: dict[str, str] | None = None) -> str:
    effective_env = os.environ if env is None else env
    value = str(effective_env.get("HU_NAP_CLIENT_AUTHORIZATION", "")).strip()
    return value or DEFAULT_HU_NAP_CLIENT_AUTHORIZATION


def _open_url_once(request: urllib.request.Request, *, timeout_seconds: int):
    try:
        return urllib.request.urlopen(request, timeout=timeout_seconds)
    except urllib.error.HTTPError:
        raise
    except (ssl.SSLError, urllib.error.URLError) as first_error:
        context = ssl._create_unverified_context()
        try:
            return urllib.request.urlopen(request, timeout=timeout_seconds, context=context)
        except urllib.error.HTTPError:
            raise
        except (ssl.SSLError, urllib.error.URLError) as fallback_error:
            raise fallback_error from first_error


def _open_url(request: urllib.request.Request, *, timeout_seconds: int):
    for attempt_index in range(URL_OPEN_MAX_ATTEMPTS):
        try:
            return _open_url_once(request, timeout_seconds=timeout_seconds)
        except (ssl.SSLError, urllib.error.URLError):
            if attempt_index == URL_OPEN_MAX_ATTEMPTS - 1:
                raise
            sleep_seconds = URL_OPEN_RETRY_SLEEP_SECONDS[
                min(attempt_index, len(URL_OPEN_RETRY_SLEEP_SECONDS) - 1)
            ]
            time.sleep(sleep_seconds)
    raise RuntimeError("unreachable")


def _request_token(*, username: str, password: str, timeout_seconds: int) -> dict[str, Any]:
    body = urllib.parse.urlencode(
        {
            "grant_type": "password",
            "username": username,
            "password": password,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        HU_NAP_TOKEN_URL,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "Authorization": _client_authorization(),
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    with _open_url(request, timeout_seconds=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _request_json(url: str, *, access_token: str, timeout_seconds: int) -> Any:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"bearer {access_token}",
            "User-Agent": USER_AGENT,
        },
    )
    with _open_url(request, timeout_seconds=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _contracted_profiles_from_payload(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "items", "result", "profiles"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _profile_id(profile: dict[str, Any]) -> int:
    for key in ("serviceProviderUserProfileId", "profileId", "id"):
        value = profile.get(key)
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def _profile_data_url(profile: dict[str, Any]) -> str:
    data_accesses = profile.get("dataAccesses")
    if isinstance(data_accesses, list):
        for access in data_accesses:
            if isinstance(access, dict):
                url = str(access.get("url") or access.get("hubWebUrl") or "").strip()
                if url:
                    return url
    for key in ("url", "hubWebUrl", "dataUrl", "serviceUrl"):
        url = str(profile.get(key) or "").strip()
        if url:
            return url
    return ""


def _resolve_source_urls(specs: Iterable[SourceSpec], profiles: Iterable[dict[str, Any]]) -> list[SourceSpec]:
    url_by_profile_id = {_profile_id(profile): _profile_data_url(profile) for profile in profiles}
    resolved: list[SourceSpec] = []
    missing: list[int] = []
    for spec in specs:
        url = url_by_profile_id.get(spec.profile_id, "")
        if not url:
            missing.append(spec.profile_id)
            continue
        resolved.append(replace(spec, url=url))
    if missing:
        raise RuntimeError(f"HU NAP active subscription URL missing for profile ids: {', '.join(map(str, missing))}")
    return resolved


def _selected_sources(value: str) -> list[SourceSpec]:
    if value in SOURCE_GROUPS:
        return [SOURCES[key] for key in SOURCE_GROUPS[value]]
    return [SOURCES[value]]


def _temp_payload_path(config: AppConfig, spec: SourceSpec) -> Path:
    incoming_dir = config.raw_payload_dir / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=incoming_dir,
        prefix=f"hu-nap-{spec.key}-",
        suffix=spec.suffix,
        delete=False,
    )
    handle.close()
    return Path(handle.name)


def _fetch_to_file(
    spec: SourceSpec,
    *,
    config: AppConfig,
    timeout_seconds: int,
) -> tuple[Path, str, int, dict[str, str]]:
    request = urllib.request.Request(
        spec.url,
        headers={
            "Accept": "application/xml,text/xml,*/*",
            "User-Agent": USER_AGENT,
        },
    )

    def write_response(response) -> tuple[Path, str, int, dict[str, str]]:
        payload_path = _temp_payload_path(config, spec)
        payload_sha256 = hashlib.sha256()
        byte_length = 0
        with payload_path.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
                payload_sha256.update(chunk)
                byte_length += len(chunk)
        return (
            payload_path,
            payload_sha256.hexdigest(),
            byte_length,
            {key.lower(): value for key, value in response.headers.items()},
        )

    with _open_url(request, timeout_seconds=timeout_seconds) as response:
        return write_response(response)


def _count_archived_xml_records(store: Any, storage_uri: str, content_encoding: str) -> dict[str, Any]:
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        return count_xml_records_from_binary_stream(payload_stream, content_encoding=content_encoding)


def _archive_source(
    *,
    spec: SourceSpec,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
) -> dict[str, Any]:
    if not spec.url:
        raise RuntimeError(f"HU NAP source {spec.key} has no resolved subscription URL")
    temp_path, payload_sha256, byte_length, headers = _fetch_to_file(
        spec,
        config=config,
        timeout_seconds=timeout_seconds,
    )
    receipt = store.record_pull_payload_file(
        country_code=COUNTRY_CODE,
        source_uid=spec.source_uid,
        payload_path=temp_path,
        payload_sha256=payload_sha256,
        byte_length=byte_length,
        content_type=headers.get("content-type", ""),
        content_encoding=headers.get("content-encoding", ""),
        request_path=HU_NAP_CONTRACTED_PROFILE_SUMMARIES_URL,
        request_query=f"profile_id={spec.profile_id}",
        request_headers={
            "accept": "application/xml,text/xml,*/*",
            "authorization": "not sent to subscription snapshot URL",
            "user-agent": USER_AGENT,
            "source-url": "discovered_from_hu_nap_portal_subscription",
        },
        source_kind=spec.source_kind,
        display_name=spec.display_name,
        task_kind=spec.task_kind,
        remote_addr="",
    )
    summary: dict[str, Any] = {
        "source": spec.key,
        "profile_id": spec.profile_id,
        "source_uid": spec.source_uid,
        "task_kind": spec.task_kind,
        "no_auth_data_url": True,
        "payload_sha256": payload_sha256,
        "byte_length": byte_length,
        "duplicate_payload": receipt.duplicate_payload,
        "queued": receipt.queued,
        "storage_uri": receipt.storage_uri,
    }
    if parse_summary:
        summary["datex_summary"] = _count_archived_xml_records(
            store,
            receipt.storage_uri,
            headers.get("content-encoding", ""),
        )
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and archive HU NAP subscription DATEX payloads.")
    parser.add_argument(
        "--source",
        choices=tuple(SOURCE_GROUPS) + tuple(SOURCES),
        default="static-real",
        help="HU NAP subscription payload or group to fetch.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=300)
    parser.add_argument(
        "--no-parse-summary",
        action="store_true",
        help="Only archive raw bytes; skip lightweight DATEX record counts.",
    )
    args = parser.parse_args(argv)

    username, password = _credentials_from_env()
    token_payload = _request_token(username=username, password=password, timeout_seconds=args.timeout_seconds)
    access_token = str(token_payload.get("access_token") or "").strip()
    if not access_token:
        raise RuntimeError("HU NAP OAuth response did not include access_token")

    profiles_payload = _request_json(
        HU_NAP_CONTRACTED_PROFILE_SUMMARIES_URL,
        access_token=access_token,
        timeout_seconds=args.timeout_seconds,
    )
    profiles = _contracted_profiles_from_payload(profiles_payload)
    specs = _resolve_source_urls(_selected_sources(args.source), profiles)

    config = AppConfig()
    store = create_ingest_store(config)
    store.initialize()
    summaries = [
        _archive_source(
            spec=spec,
            config=config,
            store=store,
            timeout_seconds=args.timeout_seconds,
            parse_summary=not args.no_parse_summary,
        )
        for spec in specs
    ]
    print(
        json.dumps(
            {
                "ok": True,
                "country_code": COUNTRY_CODE,
                "token_type": token_payload.get("token_type", ""),
                "expires_in": token_payload.get("expires_in", ""),
                "active_subscriptions": [metadata_summary_from_subscription(profile) for profile in profiles],
                "sources": summaries,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
