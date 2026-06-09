#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shutil
import ssl
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from commercial_backend.at_econtrol import (
    COUNTRY_CODE,
    ECONTROL_PUBLIC_API_DATEX_STATUS_SOURCE_UID,
    ECONTROL_PUBLIC_API_DATEX_STATUS_URL,
    ECONTROL_PUBLIC_API_DATEX_TABLE_SOURCE_UID,
    ECONTROL_PUBLIC_API_DATEX_TABLE_URL,
    ECONTROL_PUBLIC_API_DOCS_SOURCE_UID,
    ECONTROL_PUBLIC_API_DOCS_URL,
    ECONTROL_PUBLIC_API_SEARCH_SOURCE_UID,
    ECONTROL_PUBLIC_API_SEARCH_URL,
    ECONTROL_TECHNICAL_INFO_SOURCE_UID,
    ECONTROL_TECHNICAL_INFO_URL,
    MOBILITYDATA_DATASET_SOURCE_UID,
    MOBILITYDATA_DATASET_URL,
)
from commercial_backend.config import AppConfig
from commercial_backend.store import create_ingest_store

USER_AGENT = "woladen.de public AT E-Control ingester"
DEFAULT_AT_SECRET_DESCRIPTOR_PATH = REPO_ROOT / "secret" / "at_nap_credentials.json"
DEFAULT_AT_ECONTROL_API_KEY_PATH = REPO_ROOT / "secret" / "at_econtrol_api_key.txt"
DEFAULT_AT_ECONTROL_REFERER_PATH = REPO_ROOT / "secret" / "at_econtrol_referer.txt"
DEFAULT_AT_ECONTROL_REFERER = "https://woladen.de"


@dataclass(frozen=True)
class SourceSpec:
    key: str
    source_uid: str
    display_name: str
    url: str
    suffix: str
    accept: str
    source_kind: str
    task_kind: str
    requires_api_key: bool = False
    requires_basic_auth: bool = False


SOURCES = {
    "metadata": SourceSpec(
        key="metadata",
        source_uid=MOBILITYDATA_DATASET_SOURCE_UID,
        display_name="AT Mobilitydata E-Control charging-register dataset page",
        url=MOBILITYDATA_DATASET_URL,
        suffix=".html",
        accept="text/html,application/xhtml+xml",
        source_kind="national_register_metadata_page",
        task_kind="archive_only_payload",
    ),
    "technical-info": SourceSpec(
        key="technical-info",
        source_uid=ECONTROL_TECHNICAL_INFO_SOURCE_UID,
        display_name="AT E-Control charging-register technical information page",
        url=ECONTROL_TECHNICAL_INFO_URL,
        suffix=".html",
        accept="text/html,application/xhtml+xml",
        source_kind="national_register_technical_metadata_page",
        task_kind="archive_only_payload",
    ),
    "api-search": SourceSpec(
        key="api-search",
        source_uid=ECONTROL_PUBLIC_API_SEARCH_SOURCE_UID,
        display_name="AT E-Control public charging-register API search probe",
        url=ECONTROL_PUBLIC_API_SEARCH_URL,
        suffix=".json",
        accept="application/json,application/xml,text/xml",
        source_kind="national_register_public_api_probe",
        task_kind="parse_dynamic_payload",
        requires_api_key=True,
        requires_basic_auth=True,
    ),
    "api-docs": SourceSpec(
        key="api-docs",
        source_uid=ECONTROL_PUBLIC_API_DOCS_SOURCE_UID,
        display_name="AT E-Control public charging-register API Swagger definition",
        url=ECONTROL_PUBLIC_API_DOCS_URL,
        suffix=".json",
        accept="application/json",
        source_kind="national_register_public_api_documentation",
        task_kind="archive_only_payload",
        requires_api_key=True,
        requires_basic_auth=True,
    ),
    "datex-table": SourceSpec(
        key="datex-table",
        source_uid=ECONTROL_PUBLIC_API_DATEX_TABLE_SOURCE_UID,
        display_name="AT E-Control DATEX energy infrastructure table publication",
        url=ECONTROL_PUBLIC_API_DATEX_TABLE_URL,
        suffix=".json",
        accept="application/json",
        source_kind="national_register_datex_static",
        task_kind="parse_static_payload",
        requires_api_key=True,
        requires_basic_auth=True,
    ),
    "datex-status": SourceSpec(
        key="datex-status",
        source_uid=ECONTROL_PUBLIC_API_DATEX_STATUS_SOURCE_UID,
        display_name="AT E-Control DATEX energy infrastructure status publication",
        url=ECONTROL_PUBLIC_API_DATEX_STATUS_URL,
        suffix=".json",
        accept="application/json",
        source_kind="national_register_datex_status",
        task_kind="parse_dynamic_payload",
        requires_api_key=True,
        requires_basic_auth=True,
    ),
}


def _temp_payload_path(config: AppConfig, spec: SourceSpec) -> Path:
    incoming_dir = config.raw_payload_dir / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=incoming_dir,
        prefix=f"at-econtrol-{spec.key}-",
        suffix=spec.suffix,
        delete=False,
    )
    handle.close()
    return Path(handle.name)


def _read_secret_file(path_text: str) -> str:
    if not path_text:
        return ""
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8").strip()


def _secret_descriptor(env: dict[str, str] | None = None) -> dict[str, Any]:
    effective_env = os.environ if env is None else env
    descriptor_path = str(effective_env.get("AT_ECONTROL_SECRET_FILE", "")).strip()
    if not descriptor_path and env is None and DEFAULT_AT_SECRET_DESCRIPTOR_PATH.exists():
        descriptor_path = str(DEFAULT_AT_SECRET_DESCRIPTOR_PATH)
    if not descriptor_path:
        return {}
    descriptor_text = _read_secret_file(descriptor_path)
    if not descriptor_text:
        return {}
    payload = json.loads(descriptor_text)
    if not isinstance(payload, dict):
        raise RuntimeError(f"AT E-Control secret descriptor must be a JSON object: {descriptor_path}")
    return payload


def _api_key_from_env(env: dict[str, str] | None = None) -> str:
    effective_env = os.environ if env is None else env
    value = str(effective_env.get("AT_ECONTROL_API_KEY", "")).strip()
    if value:
        return value
    key_file = str(effective_env.get("AT_ECONTROL_API_KEY_FILE", "")).strip()
    value = _read_secret_file(key_file)
    if value:
        return value
    descriptor = _secret_descriptor(env)
    value = _read_secret_file(str(descriptor.get("api_key_file", "")).strip())
    if value:
        return value
    if env is None:
        value = _read_secret_file(str(DEFAULT_AT_ECONTROL_API_KEY_PATH))
        if value:
            return value
    return ""


def _referer_from_env(env: dict[str, str] | None = None) -> str:
    effective_env = os.environ if env is None else env
    value = str(effective_env.get("AT_ECONTROL_REFERER", "")).strip()
    if value:
        return value
    value = _read_secret_file(str(effective_env.get("AT_ECONTROL_REFERER_FILE", "")).strip())
    if value:
        return value
    descriptor = _secret_descriptor(env)
    value = _read_secret_file(str(descriptor.get("referer_file", "")).strip())
    if value:
        return value
    if env is None:
        value = _read_secret_file(str(DEFAULT_AT_ECONTROL_REFERER_PATH))
        if value:
            return value
    return DEFAULT_AT_ECONTROL_REFERER


def _account_credentials_from_env(env: dict[str, str] | None = None) -> tuple[str, str]:
    effective_env = os.environ if env is None else env
    user = str(effective_env.get("AT_LADESTELLEN_USER", "")).strip()
    password = str(effective_env.get("AT_LADESTELLEN_PASSWORD", "")).strip()
    if user and password:
        return user, password
    user = user or _read_secret_file(str(effective_env.get("AT_LADESTELLEN_USER_FILE", "")).strip())
    password = password or _read_secret_file(
        str(effective_env.get("AT_LADESTELLEN_PASSWORD_FILE", "")).strip()
    )
    if user and password:
        return user, password
    descriptor = _secret_descriptor(env)
    user = user or _read_secret_file(str(descriptor.get("user_file", "")).strip())
    password = password or _read_secret_file(str(descriptor.get("password_file", "")).strip())
    return user, password


def _basic_auth_header_from_env(env: dict[str, str] | None = None) -> str:
    user, password = _account_credentials_from_env(env)
    if not user or not password:
        return ""
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return f"Basic {token}"


def _request_headers(spec: SourceSpec, env: dict[str, str] | None = None) -> dict[str, str]:
    headers = {
        "Accept": spec.accept,
        "User-Agent": USER_AGENT,
    }
    if spec.requires_api_key:
        api_key = _api_key_from_env(env)
        if not api_key:
            raise RuntimeError(
                "Credentialed AT E-Control sources require an API key. Set "
                "AT_ECONTROL_API_KEY, AT_ECONTROL_API_KEY_FILE, "
                "AT_ECONTROL_SECRET_FILE, or store it in secret/at_nap_credentials.json "
                "or secret/at_econtrol_api_key.txt."
            )
        headers["Apikey"] = api_key
        headers["Referer"] = _referer_from_env(env)
    if spec.requires_basic_auth:
        basic_auth_header = _basic_auth_header_from_env(env)
        if not basic_auth_header:
            raise RuntimeError(
                "AT_LADESTELLEN_USER/PASSWORD or AT_LADESTELLEN_USER_FILE/"
                "AT_LADESTELLEN_PASSWORD_FILE is required for credentialed AT API reads"
            )
        headers["Authorization"] = basic_auth_header
    return headers


def _fetch_to_file(
    spec: SourceSpec,
    *,
    config: AppConfig,
    timeout_seconds: int,
) -> tuple[Path, str, int, dict[str, str]]:
    request = urllib.request.Request(spec.url, headers=_request_headers(spec))

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

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return write_response(response)
    except (ssl.SSLError, urllib.error.URLError):
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
            return write_response(response)


def _quarantine_invalid_payload(
    *,
    config: AppConfig,
    spec: SourceSpec,
    temp_path: Path,
    payload_sha256: str,
    byte_length: int,
    reason: str,
) -> Path:
    now = datetime.now(timezone.utc)
    target_dir = (
        config.raw_payload_dir
        / "_invalid"
        / COUNTRY_CODE
        / spec.source_uid
        / f"{now:%Y}"
        / f"{now:%m}"
        / f"{now:%d}"
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{now:%Y%m%dT%H%M%SZ}-{payload_sha256[:16]}{spec.suffix}"
    shutil.move(str(temp_path), target_path)
    manifest_row = {
        "received_at": now.isoformat(),
        "country_code": COUNTRY_CODE,
        "source": spec.key,
        "source_uid": spec.source_uid,
        "url": spec.url,
        "payload_sha256": payload_sha256,
        "byte_length": byte_length,
        "reason": reason,
        "path": str(target_path),
    }
    with (target_dir / "invalid_payloads.ndjson").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(manifest_row, sort_keys=True) + "\n")
    return target_path


def _json_container_shape_error(path: Path) -> str:
    with path.open("rb") as handle:
        start = handle.read(4096)
        if not start:
            return "empty_json_payload"
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        handle.seek(max(size - 4096, 0))
        end = handle.read()
    first = next((byte for byte in start if chr(byte) not in " \t\r\n"), 0)
    last = next((byte for byte in reversed(end) if chr(byte) not in " \t\r\n"), 0)
    pairs = {ord("{"): ord("}"), ord("["): ord("]")}
    if first not in pairs:
        return f"json_payload_unexpected_start:{chr(first) if first else 'missing'}"
    if last != pairs[first]:
        return f"json_payload_truncated_or_unexpected_end:{chr(last) if last else 'missing'}"
    return ""


def _validate_json_container_shape(
    *,
    config: AppConfig,
    spec: SourceSpec,
    temp_path: Path,
    payload_sha256: str,
    byte_length: int,
) -> None:
    if spec.suffix != ".json":
        return
    reason = _json_container_shape_error(temp_path)
    if not reason:
        return
    quarantine_path = _quarantine_invalid_payload(
        config=config,
        spec=spec,
        temp_path=temp_path,
        payload_sha256=payload_sha256,
        byte_length=byte_length,
        reason=reason,
    )
    raise RuntimeError(f"invalid_at_econtrol_payload:{spec.key}:{reason}; quarantined={quarantine_path}")


def _selected_sources(value: str) -> list[SourceSpec]:
    if value == "all-open":
        return [SOURCES["metadata"], SOURCES["technical-info"]]
    if value == "all-credentialed":
        return [SOURCES["api-docs"], SOURCES["datex-table"], SOURCES["datex-status"]]
    return [SOURCES[value]]


def _archive_source(
    *,
    spec: SourceSpec,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
) -> dict[str, Any]:
    temp_path, payload_sha256, byte_length, headers = _fetch_to_file(
        spec,
        config=config,
        timeout_seconds=timeout_seconds,
    )
    _validate_json_container_shape(
        config=config,
        spec=spec,
        temp_path=temp_path,
        payload_sha256=payload_sha256,
        byte_length=byte_length,
    )
    stored_headers = {"user-agent": USER_AGENT, "source-url": spec.url}
    if spec.requires_api_key:
        stored_headers["referer"] = _referer_from_env()
        stored_headers["auth"] = "apikey_header_redacted"
    if spec.requires_basic_auth:
        stored_headers["account"] = "basic_auth_redacted"
    receipt = store.record_pull_payload_file(
        country_code=COUNTRY_CODE,
        source_uid=spec.source_uid,
        payload_path=temp_path,
        payload_sha256=payload_sha256,
        byte_length=byte_length,
        content_type=headers.get("content-type", ""),
        content_encoding=headers.get("content-encoding", ""),
        request_path=spec.url,
        request_query="",
        request_headers=stored_headers,
        source_kind=spec.source_kind,
        display_name=spec.display_name,
        task_kind=spec.task_kind,
        remote_addr="",
    )
    return {
        "source": spec.key,
        "source_uid": spec.source_uid,
        "url": spec.url,
        "task_kind": spec.task_kind,
        "payload_sha256": payload_sha256,
        "byte_length": byte_length,
        "duplicate_payload": receipt.duplicate_payload,
        "queued": receipt.queued,
        "storage_uri": receipt.storage_uri,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and archive AT E-Control public source metadata/API payloads.")
    parser.add_argument(
        "--source",
        choices=(
            "all-open",
            "all-credentialed",
            "metadata",
            "technical-info",
            "api-docs",
            "api-search",
            "datex-table",
            "datex-status",
        ),
        default="all-open",
        help=(
            "AT source payload to fetch. api-docs and api-search use the ignored "
            "ladestellen.at/E-Control secret bundle by default."
        ),
    )
    parser.add_argument("--timeout-seconds", type=int, default=60)
    args = parser.parse_args(argv)

    config = AppConfig()
    store = create_ingest_store(config)
    store.initialize()

    summaries = []
    failures = []
    for spec in _selected_sources(args.source):
        try:
            summaries.append(
                _archive_source(
                    spec=spec,
                    config=config,
                    store=store,
                    timeout_seconds=args.timeout_seconds,
                )
            )
        except urllib.error.HTTPError as exc:
            failures.append(
                {
                    "source": spec.key,
                    "source_uid": spec.source_uid,
                    "url": spec.url,
                    "http_status": exc.code,
                    "reason": exc.reason,
                }
            )
        except Exception as exc:
            failures.append(
                {
                    "source": spec.key,
                    "source_uid": spec.source_uid,
                    "url": spec.url,
                    "error": str(exc),
                }
            )

    print(
        json.dumps(
            {
                "ok": not failures,
                "country_code": COUNTRY_CODE,
                "sources": summaries,
                "failures": failures,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 2 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
