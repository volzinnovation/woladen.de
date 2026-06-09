#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import ssl
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any, Callable, Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from commercial_backend.be_transportdata import (
    COUNTRY_CODE,
    ECO_MOVEMENT_STATIC_SOURCE_UID,
    ECO_MOVEMENT_STATIC_URL,
    ENERGYVISION_LOCATIONS_SOURCE_UID,
    ENERGYVISION_LOCATIONS_URL,
    ENERGYVISION_PRODUCTION_OCPI_VERSION,
    ENERGYVISION_STAGING_OCPI_VERSION,
    ENERGYVISION_TARIFFS_SOURCE_UID,
    ENERGYVISION_TARIFFS_URL,
    GROUP_INDIGO_STATIC_SOURCE_UID,
    GROUP_INDIGO_STATIC_URL,
    MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
    MONTA_AFIR_CHARGE_POINTS_URL,
    MONTA_AUTH_TOKEN_URL,
    ROAD_OCPI_LOCATIONS_SOURCE_UID,
    ROAD_OCPI_LOCATIONS_URL,
    SOURCE_REGISTRY_BY_UID,
    iter_eco_movement_static_rows_from_binary_stream,
    iter_energyvision_location_rows_from_binary_stream,
    iter_energyvision_tariff_rows_from_binary_stream,
    iter_group_indigo_static_rows_from_binary_stream,
    iter_monta_charge_point_rows_from_binary_stream,
    iter_road_location_rows_from_binary_stream,
    energyvision_module_url,
    load_secret,
    secret_hint,
)
from commercial_backend.config import AppConfig
from commercial_backend.store import create_ingest_store

USER_AGENT = "woladen.de public BE ingester"


@dataclass(frozen=True)
class SourceSpec:
    key: str
    source_uid: str
    display_name: str
    url: str
    source_kind: str
    task_kind: str
    auth_kind: str
    stream_parser: Callable[..., Iterable[dict[str, Any]]]
    suffix: str = ".json"


def _env_text(name: str, default: str = "") -> str:
    return str(os.environ.get(name, default)).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    value = _env_text(name).casefold()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def _energyvision_version_for_base_url(base_url: str) -> str:
    configured = _env_text("TRANSPORTDATA_BE_ENERGYVISION_OCPI_VERSION")
    if configured:
        return configured
    parsed = urllib.parse.urlparse(base_url)
    if parsed.hostname and parsed.hostname.casefold().endswith("myev-dev.be"):
        return ENERGYVISION_STAGING_OCPI_VERSION
    return ENERGYVISION_PRODUCTION_OCPI_VERSION


def _energyvision_source_url(*, module: str, default_url: str) -> str:
    specific_env_name = f"TRANSPORTDATA_BE_ENERGYVISION_{module.upper()}_URL"
    specific_url = _env_text(specific_env_name)
    if specific_url:
        return specific_url
    base_url = _env_text("TRANSPORTDATA_BE_ENERGYVISION_BASE_URL")
    if base_url:
        return energyvision_module_url(
            base_url=base_url,
            ocpi_version=_energyvision_version_for_base_url(base_url),
            module=module,
        )
    return default_url


def _is_energyvision_staging_url(url: str) -> bool:
    if _env_bool("TRANSPORTDATA_BE_ENERGYVISION_USE_STAGING_TOKEN"):
        return True
    parsed = urllib.parse.urlparse(url)
    return bool(parsed.hostname and parsed.hostname.casefold().endswith("myev-dev.be"))


def _energyvision_environment(spec: SourceSpec) -> str:
    return "staging" if _is_energyvision_staging_url(spec.url) else "production"


SOURCES = {
    "energyvision-locations": SourceSpec(
        key="energyvision-locations",
        source_uid=ENERGYVISION_LOCATIONS_SOURCE_UID,
        display_name="BE EnergyVision OCPI locations and EVSE status",
        url=_energyvision_source_url(module="locations", default_url=ENERGYVISION_LOCATIONS_URL),
        source_kind="afir_ocpi_locations_with_status",
        task_kind="parse_dynamic_payload",
        auth_kind="energyvision",
        stream_parser=iter_energyvision_location_rows_from_binary_stream,
    ),
    "energyvision-tariffs": SourceSpec(
        key="energyvision-tariffs",
        source_uid=ENERGYVISION_TARIFFS_SOURCE_UID,
        display_name="BE EnergyVision OCPI tariffs",
        url=_energyvision_source_url(module="tariffs", default_url=ENERGYVISION_TARIFFS_URL),
        source_kind="afir_ocpi_tariffs",
        task_kind="parse_metadata_payload",
        auth_kind="energyvision",
        stream_parser=iter_energyvision_tariff_rows_from_binary_stream,
    ),
    "eco-static": SourceSpec(
        key="eco-static",
        source_uid=ECO_MOVEMENT_STATIC_SOURCE_UID,
        display_name="BE Eco-Movement static DATEX II selected CPOs",
        url=ECO_MOVEMENT_STATIC_URL,
        source_kind="national_register_static",
        task_kind="parse_static_payload",
        auth_kind="eco-movement",
        stream_parser=iter_eco_movement_static_rows_from_binary_stream,
    ),
    "monta-charge-points": SourceSpec(
        key="monta-charge-points",
        source_uid=MONTA_AFIR_CHARGE_POINTS_SOURCE_UID,
        display_name="BE Monta AFIR charge points",
        url=MONTA_AFIR_CHARGE_POINTS_URL,
        source_kind="afir_static_dynamic_charge_points",
        task_kind="parse_dynamic_payload",
        auth_kind="monta",
        stream_parser=iter_monta_charge_point_rows_from_binary_stream,
    ),
    "road-locations": SourceSpec(
        key="road-locations",
        source_uid=ROAD_OCPI_LOCATIONS_SOURCE_UID,
        display_name="BE Road OCPI locations and EVSE status",
        url=ROAD_OCPI_LOCATIONS_URL,
        source_kind="afir_ocpi_locations_with_status",
        task_kind="parse_dynamic_payload",
        auth_kind="no-auth",
        stream_parser=iter_road_location_rows_from_binary_stream,
    ),
    "indigo-static": SourceSpec(
        key="indigo-static",
        source_uid=GROUP_INDIGO_STATIC_SOURCE_UID,
        display_name="BE Group INDIGO static DATEX II charging infrastructure",
        url=GROUP_INDIGO_STATIC_URL,
        source_kind="national_register_static",
        task_kind="parse_static_payload",
        auth_kind="no-auth",
        stream_parser=iter_group_indigo_static_rows_from_binary_stream,
        suffix=".xml",
    ),
}


def _temp_payload_path(config: AppConfig, source_key: str, *, suffix: str = ".json") -> Path:
    incoming_dir = config.raw_payload_dir / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=incoming_dir,
        prefix=f"be-{source_key}-",
        suffix=suffix,
        delete=False,
    )
    handle.close()
    return Path(handle.name)


def _energyvision_secret_hint(spec: SourceSpec) -> str:
    if _is_energyvision_staging_url(spec.url):
        return (
            f"missing credentials for {spec.source_uid}; env: "
            "TRANSPORTDATA_BE_ENERGYVISION_STAGING_TOKEN or "
            "TRANSPORTDATA_BE_ENERGYVISION_STAGING_TOKEN_C; files: "
            "secret/transportdata_be_energyvision_staging_token.txt or "
            "secret/transportdata_be_energyvision_staging_token_c.txt"
        )
    return (
        f"missing credentials for {spec.source_uid}; env: "
        "TRANSPORTDATA_BE_ENERGYVISION_TOKEN_A, TRANSPORTDATA_BE_ENERGYVISION_PROD_TOKEN, "
        "TRANSPORTDATA_BE_ENERGYVISION_TOKEN_C, TRANSPORTDATA_BE_ENERGYVISION_PROD_TOKEN_C, "
        "TRANSPORTDATA_BE_ENERGYVISION_API_KEY, WOLADEN_TRANSPORTDATA_BE_API_KEY; files: "
        "secret/transportdata_be_energyvision_prod_token.txt, "
        "secret/transportdata_be_energyvision_prod_token_c.txt, "
        "secret/transportdata_be_energyvision_api_key.txt"
    )


def _energyvision_token_a(spec: SourceSpec) -> str:
    env_token = _energyvision_token_a_env(spec)
    if env_token:
        return env_token
    return _energyvision_token_a_file(spec)


def _energyvision_token_a_env(spec: SourceSpec) -> str:
    if _is_energyvision_staging_url(spec.url):
        staging_token = load_secret(
            env_names=("TRANSPORTDATA_BE_ENERGYVISION_STAGING_TOKEN", "TRANSPORTDATA_BE_ENERGYVISION_TOKEN_A"),
            filenames=(),
        )
        if staging_token:
            return staging_token
    return load_secret(
        env_names=(
            "TRANSPORTDATA_BE_ENERGYVISION_TOKEN_A",
            "TRANSPORTDATA_BE_ENERGYVISION_PROD_TOKEN",
            "TRANSPORTDATA_BE_ENERGYVISION_API_KEY",
            "WOLADEN_TRANSPORTDATA_BE_API_KEY",
        ),
        filenames=(),
    )


def _energyvision_token_a_file(spec: SourceSpec) -> str:
    if _is_energyvision_staging_url(spec.url):
        staging_token = load_secret(
            env_names=(),
            filenames=("transportdata_be_energyvision_staging_token.txt",),
        )
        if staging_token:
            return staging_token
    return load_secret(
        env_names=(),
        filenames=(
            "transportdata_be_energyvision_prod_token.txt",
            "transportdata_be_energyvision_api_key.txt",
            "transportdata_be_api_key",
        ),
    )


def _energyvision_cached_token_c(spec: SourceSpec) -> str:
    env_token = _energyvision_cached_token_c_env(spec)
    if env_token:
        return env_token
    return _energyvision_cached_token_c_file(spec)


def _energyvision_cached_token_c_env(spec: SourceSpec) -> str:
    if _is_energyvision_staging_url(spec.url):
        token = load_secret(
            env_names=("TRANSPORTDATA_BE_ENERGYVISION_STAGING_TOKEN_C", "TRANSPORTDATA_BE_ENERGYVISION_TOKEN_C"),
            filenames=(),
        )
        if token:
            return token
    return load_secret(
        env_names=("TRANSPORTDATA_BE_ENERGYVISION_PROD_TOKEN_C", "TRANSPORTDATA_BE_ENERGYVISION_TOKEN_C"),
        filenames=(),
    )


def _energyvision_cached_token_c_file(spec: SourceSpec) -> str:
    configured_token = _energyvision_cached_token_c_configured_file(spec)
    if configured_token:
        return configured_token
    return _energyvision_cached_token_c_secret_file(spec)


def _energyvision_cached_token_c_configured_file(spec: SourceSpec) -> str:
    configured_path = _env_text(
        "TRANSPORTDATA_BE_ENERGYVISION_STAGING_TOKEN_C_FILE"
        if _is_energyvision_staging_url(spec.url)
        else "TRANSPORTDATA_BE_ENERGYVISION_PROD_TOKEN_C_FILE"
    ) or _env_text("TRANSPORTDATA_BE_ENERGYVISION_TOKEN_C_FILE")
    if configured_path:
        path = Path(configured_path).expanduser()
        if path.exists():
            token = path.read_text(encoding="utf-8").strip()
            if token:
                return token
    return ""


def _energyvision_cached_token_c_secret_file(spec: SourceSpec) -> str:
    if _is_energyvision_staging_url(spec.url):
        token = load_secret(
            env_names=(),
            filenames=("transportdata_be_energyvision_staging_token_c.txt",),
        )
        if token:
            return token
    return load_secret(
        env_names=(),
        filenames=("transportdata_be_energyvision_prod_token_c.txt",),
    )


def _write_energyvision_token_c_cache(spec: SourceSpec, token_c: str) -> None:
    configured_path = _env_text(
        "TRANSPORTDATA_BE_ENERGYVISION_STAGING_TOKEN_C_FILE"
        if _is_energyvision_staging_url(spec.url)
        else "TRANSPORTDATA_BE_ENERGYVISION_PROD_TOKEN_C_FILE"
    ) or _env_text("TRANSPORTDATA_BE_ENERGYVISION_TOKEN_C_FILE")
    if not configured_path:
        return
    path = Path(configured_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(token_c.strip() + "\n", encoding="utf-8")
    try:
        path.chmod(0o600)
    except OSError:
        pass


def _energyvision_emsp_token_b() -> str:
    return load_secret(
        env_names=("TRANSPORTDATA_BE_ENERGYVISION_EMSP_TOKEN_B",),
        filenames=("transportdata_be_energyvision_emsp_token_b.txt",),
    )


def _energyvision_emsp_versions_url() -> str:
    return _env_text("TRANSPORTDATA_BE_ENERGYVISION_EMSP_VERSIONS_URL")


def _energyvision_auth_header(token: str) -> str:
    first_word = token.split(None, 1)[0].casefold() if token.split(None, 1) else ""
    scheme = str(os.environ.get("TRANSPORTDATA_BE_ENERGYVISION_AUTH_SCHEME", "Token")).strip() or "Token"
    return token if first_word in {"token", "bearer", "basic"} else f"{scheme} {token}"


def _energyvision_credentials_url(module_url: str) -> str:
    parsed = urllib.parse.urlparse(module_url)
    path_parts = [part for part in parsed.path.split("/") if part]
    if len(path_parts) < 2:
        raise RuntimeError(f"energyvision_module_url_missing_ocpi_version:{module_url}")
    base_parts = path_parts[:-1]
    path = "/" + "/".join([*base_parts, "credentials"]) + "/"
    return urllib.parse.urlunparse(parsed._replace(path=path, params="", query="", fragment=""))


def _energyvision_ocpi_credentials_payload(spec: SourceSpec) -> dict[str, Any]:
    token_b = _energyvision_emsp_token_b()
    if not token_b:
        raise RuntimeError(
            "missing EnergyVision eMSP Token B; set "
            "TRANSPORTDATA_BE_ENERGYVISION_EMSP_TOKEN_B or "
            "secret/transportdata_be_energyvision_emsp_token_b.txt"
        )
    emsp_versions_url = _energyvision_emsp_versions_url()
    if not emsp_versions_url:
        raise RuntimeError(
            "missing EnergyVision eMSP versions callback URL; set "
            "TRANSPORTDATA_BE_ENERGYVISION_EMSP_VERSIONS_URL"
        )
    return {
        "token": token_b,
        "url": emsp_versions_url,
        "roles": [
            {
                "role": "EMSP",
                "business_details": {
                    "name": _env_text("TRANSPORTDATA_BE_ENERGYVISION_EMSP_NAME", "Woladen"),
                    "website": _env_text("TRANSPORTDATA_BE_ENERGYVISION_EMSP_WEBSITE", "https://woladen.de"),
                },
                "party_id": _env_text("TRANSPORTDATA_BE_ENERGYVISION_EMSP_PARTY_ID", "WLA"),
                "country_code": _env_text("TRANSPORTDATA_BE_ENERGYVISION_EMSP_COUNTRY_CODE", "DE"),
            }
        ],
    }


def _exchange_energyvision_credentials(spec: SourceSpec, *, timeout_seconds: int) -> str:
    token_a = _energyvision_token_a(spec)
    if not token_a:
        raise RuntimeError(_energyvision_secret_hint(spec))
    url = _energyvision_credentials_url(spec.url)
    body = json.dumps(_energyvision_ocpi_credentials_payload(spec), separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Accept": "application/json",
            "Authorization": _energyvision_auth_header(token_a),
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (ssl.SSLError, urllib.error.URLError):
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
            payload = json.loads(response.read().decode("utf-8"))
    data = payload.get("data") if isinstance(payload, dict) else {}
    if not isinstance(data, dict):
        data = {}
    token_c = str(data.get("token") or payload.get("token") or "").strip() if isinstance(payload, dict) else ""
    if not token_c:
        status_code = payload.get("status_code") if isinstance(payload, dict) else ""
        status_message = payload.get("status_message") if isinstance(payload, dict) else ""
        raise RuntimeError(f"energyvision_credentials_response_missing_token_c:{status_code}:{status_message}")
    _write_energyvision_token_c_cache(spec, token_c)
    return token_c


def _energyvision_access_token(spec: SourceSpec, *, timeout_seconds: int) -> str:
    if _env_bool("TRANSPORTDATA_BE_ENERGYVISION_FORCE_CREDENTIALS_EXCHANGE"):
        return _exchange_energyvision_credentials(spec, timeout_seconds=timeout_seconds)
    for token in (
        _energyvision_cached_token_c_env(spec),
        _energyvision_cached_token_c_configured_file(spec),
    ):
        if token:
            return token
    if _energyvision_emsp_versions_url():
        return _exchange_energyvision_credentials(spec, timeout_seconds=timeout_seconds)
    for token in (
        _energyvision_token_a_env(spec),
        _energyvision_cached_token_c_secret_file(spec),
        _energyvision_token_a_file(spec),
    ):
        if token:
            return token
    return ""


def _eco_movement_token() -> str:
    return load_secret(
        env_names=("TRANSPORTDATA_BE_ECO_MOVEMENT_TOKEN",),
        filenames=("transportdata_be_eco_movement_token.txt",),
    )


def _monta_client_id() -> str:
    return load_secret(
        env_names=("TRANSPORTDATA_BE_MONTA_CLIENT_ID",),
        filenames=("transportdata_be_monta_client_id.txt",),
    )


def _monta_client_secret() -> str:
    return load_secret(
        env_names=("TRANSPORTDATA_BE_MONTA_CLIENT_SECRET",),
        filenames=("transportdata_be_monta_client_secret.txt",),
    )


def _monta_bearer_token() -> str:
    return load_secret(
        env_names=("TRANSPORTDATA_BE_MONTA_BEARER_TOKEN",),
        filenames=("transportdata_be_monta_bearer_token.txt",),
    )


def _has_credentials(spec: SourceSpec) -> bool:
    if spec.auth_kind == "energyvision":
        return bool(_energyvision_cached_token_c(spec) or _energyvision_token_a(spec))
    if spec.auth_kind == "eco-movement":
        return bool(_eco_movement_token())
    if spec.auth_kind == "monta":
        return bool(_monta_bearer_token() or (_monta_client_id() and _monta_client_secret()))
    return True


def _source_secret_hint(spec: SourceSpec) -> str:
    source = SOURCE_REGISTRY_BY_UID[spec.source_uid]
    return secret_hint(source)


def _energyvision_headers(spec: SourceSpec, *, timeout_seconds: int = 120) -> dict[str, str]:
    access_token = _energyvision_access_token(spec, timeout_seconds=timeout_seconds)
    if not access_token:
        raise RuntimeError(_energyvision_secret_hint(spec))
    return {
        "Accept": "application/json",
        "Authorization": _energyvision_auth_header(access_token),
        "User-Agent": USER_AGENT,
    }


def _eco_movement_url(spec: SourceSpec) -> str:
    token = _eco_movement_token()
    if not token:
        raise RuntimeError(_source_secret_hint(spec))
    return f"{spec.url}?{urllib.parse.urlencode({'token': token})}"


def _request_monta_access_token(timeout_seconds: int) -> str:
    bearer_token = _monta_bearer_token()
    if bearer_token:
        return bearer_token

    client_id = _monta_client_id()
    client_secret = _monta_client_secret()
    if not client_id or not client_secret:
        raise RuntimeError(_source_secret_hint(SOURCES["monta-charge-points"]))

    body = json.dumps({"clientId": client_id, "clientSecret": client_secret}).encode("utf-8")
    request = urllib.request.Request(
        MONTA_AUTH_TOKEN_URL,
        data=body,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
    access_token = str(payload.get("accessToken") or "").strip()
    if not access_token:
        raise RuntimeError("monta_auth_response_missing_accessToken")
    return access_token


def _fetch_to_file(
    url: str,
    *,
    config: AppConfig,
    source_key: str,
    headers: dict[str, str],
    timeout_seconds: int,
    max_rate_limit_retries: int,
    source_suffix: str = ".json",
) -> tuple[Path, str, int, dict[str, str]]:
    request = urllib.request.Request(url, headers=headers)

    def write_response(response) -> tuple[Path, str, int, dict[str, str]]:
        payload_path = _temp_payload_path(config, source_key, suffix=source_suffix)
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

    attempt = 0
    while True:
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                return write_response(response)
        except urllib.error.HTTPError as exc:
            if exc.code != 429 or attempt >= max_rate_limit_retries:
                raise
            retry_after = str(exc.headers.get("Retry-After") or "60").strip()
            try:
                sleep_seconds = max(float(retry_after), 1.0)
            except ValueError:
                sleep_seconds = 60.0
            time.sleep(sleep_seconds)
            attempt += 1
        except (ssl.SSLError, urllib.error.URLError):
            context = ssl._create_unverified_context()
            with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
                return write_response(response)


def _int_header(headers: dict[str, str], name: str) -> int:
    text = str(headers.get(name.lower()) or "").strip()
    try:
        return int(text)
    except ValueError:
        return 0


def _url_with_query_params(url: str, params: dict[str, str]) -> tuple[str, str]:
    parsed = urllib.parse.urlparse(url)
    query_pairs = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query = {key: value for key, value in query_pairs}
    query.update({key: value for key, value in params.items() if value != ""})
    query_text = urllib.parse.urlencode(query)
    return urllib.parse.urlunparse(parsed._replace(query=query_text)), query_text


def _safe_query_with_batch(query_text: str, batch_id: str) -> str:
    pairs = urllib.parse.parse_qsl(query_text, keep_blank_values=True)
    pairs.append(("batch_id", batch_id))
    return urllib.parse.urlencode(pairs)


def _count_archived_rows(store: Any, spec: SourceSpec, storage_uri: str, content_encoding: str) -> int:
    row_count = 0
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        for _row in spec.stream_parser(payload_stream, content_encoding=content_encoding):
            row_count += 1
    return row_count


def _archive_payload_file(
    *,
    store: Any,
    spec: SourceSpec,
    temp_path: Path,
    payload_sha256: str,
    byte_length: int,
    headers: dict[str, str],
    safe_request_path: str,
    safe_request_query: str,
    parse_summary: bool,
) -> dict[str, Any]:
    receipt = store.record_pull_payload_file(
        country_code=COUNTRY_CODE,
        source_uid=spec.source_uid,
        payload_path=temp_path,
        payload_sha256=payload_sha256,
        byte_length=byte_length,
        content_type=headers.get("content-type", ""),
        content_encoding=headers.get("content-encoding", ""),
        request_path=safe_request_path,
        request_query=safe_request_query,
        request_headers={"user-agent": USER_AGENT, "source-url": safe_request_path},
        source_kind=spec.source_kind,
        display_name=spec.display_name,
        task_kind=spec.task_kind,
        remote_addr="",
    )

    summary: dict[str, Any] = {
        "source": spec.key,
        "source_uid": spec.source_uid,
        "payload_sha256": payload_sha256,
        "byte_length": byte_length,
        "duplicate_payload": receipt.duplicate_payload,
        "queued": receipt.queued,
        "storage_uri": receipt.storage_uri,
    }
    if parse_summary:
        summary["parsed_row_count"] = _count_archived_rows(
            store,
            spec,
            receipt.storage_uri,
            headers.get("content-encoding", ""),
        )
    return summary


def _archive_single_source(
    *,
    spec: SourceSpec,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
    max_rate_limit_retries: int,
) -> dict[str, Any]:
    if spec.auth_kind == "energyvision":
        url = spec.url
        headers = _energyvision_headers(spec, timeout_seconds=timeout_seconds)
        safe_request_path = spec.url
        safe_request_query = ""
    elif spec.auth_kind == "eco-movement":
        url = _eco_movement_url(spec)
        headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        safe_request_path = spec.url
        safe_request_query = "token=<redacted>"
    elif spec.auth_kind == "no-auth":
        url = spec.url
        headers = {
            "Accept": "application/json,application/xml,text/xml,*/*",
            "User-Agent": USER_AGENT,
        }
        parsed = urllib.parse.urlparse(spec.url)
        safe_request_path = urllib.parse.urlunparse(parsed._replace(query="", fragment=""))
        safe_request_query = parsed.query
    else:
        raise RuntimeError(f"unsupported_single_source_auth_kind:{spec.auth_kind}")

    temp_path, payload_sha256, byte_length, response_headers = _fetch_to_file(
        url,
        config=config,
        source_key=spec.key,
        source_suffix=spec.suffix,
        headers=headers,
        timeout_seconds=timeout_seconds,
        max_rate_limit_retries=max_rate_limit_retries,
    )
    return _archive_payload_file(
        store=store,
        spec=spec,
        temp_path=temp_path,
        payload_sha256=payload_sha256,
        byte_length=byte_length,
        headers=response_headers,
        safe_request_path=safe_request_path,
        safe_request_query=safe_request_query,
        parse_summary=parse_summary,
    )


def _archive_energyvision_source(
    *,
    spec: SourceSpec,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
    max_rate_limit_retries: int,
    page_limit: int,
    max_pages: int,
) -> dict[str, Any]:
    headers = _energyvision_headers(spec, timeout_seconds=timeout_seconds)
    summaries: list[dict[str, Any]] = []
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    offset = 0
    page = 0
    while page < max_pages:
        page_params: dict[str, str] = {}
        if page_limit > 0:
            page_params["limit"] = str(page_limit)
            page_params["offset"] = str(offset)
        url, query_text = _url_with_query_params(spec.url, page_params) if page_params else (spec.url, "")
        temp_path, payload_sha256, byte_length, response_headers = _fetch_to_file(
            url,
            config=config,
            source_key=f"{spec.key}-page-{page + 1}",
            source_suffix=spec.suffix,
            headers=headers,
            timeout_seconds=timeout_seconds,
            max_rate_limit_retries=max_rate_limit_retries,
        )
        summary = _archive_payload_file(
            store=store,
            spec=spec,
            temp_path=temp_path,
            payload_sha256=payload_sha256,
            byte_length=byte_length,
            headers=response_headers,
            safe_request_path=spec.url.split("?", 1)[0],
            safe_request_query=_safe_query_with_batch(query_text, batch_id),
            parse_summary=parse_summary,
        )
        summaries.append(
            {
                **summary,
                "offset": offset if page_limit > 0 else None,
                "limit": page_limit if page_limit > 0 else None,
                "x_total_count": _int_header(response_headers, "x-total-count"),
                "x_limit": _int_header(response_headers, "x-limit"),
            }
        )

        total_count = _int_header(response_headers, "x-total-count")
        effective_limit = _int_header(response_headers, "x-limit") or page_limit
        if page_limit <= 0 or effective_limit <= 0 or total_count <= 0:
            break
        offset += effective_limit
        page += 1
        if offset >= total_count:
            break

    parsed_row_count = sum(int(page.get("parsed_row_count") or 0) for page in summaries)
    return {
        "source": spec.key,
        "source_uid": spec.source_uid,
        "batch_id": batch_id,
        "page_count": len(summaries),
        "parsed_row_count": parsed_row_count,
        "pages": summaries,
    }


def _read_archived_json_page(store: Any, storage_uri: str, content_encoding: str) -> Any:
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        start = payload_stream.peek(2)[:2]
        compressed = content_encoding.casefold() == "gzip" or start == b"\x1f\x8b"
        binary_stream = gzip.GzipFile(fileobj=payload_stream) if compressed else payload_stream
        text_stream = io.TextIOWrapper(binary_stream, encoding="utf-8")
        try:
            return json.load(text_stream)
        finally:
            text_stream.detach()


def _next_monta_cursor(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    for key in ("nextCursor", "next_cursor", "cursor", "next"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    for parent_key in ("meta", "pagination"):
        parent = payload.get(parent_key)
        if not isinstance(parent, dict):
            continue
        for key in ("nextCursor", "next_cursor", "cursor", "next"):
            value = str(parent.get(key) or "").strip()
            if value:
                return value
    links = payload.get("links")
    if isinstance(links, dict):
        next_value = links.get("next")
        if isinstance(next_value, dict):
            next_value = next_value.get("href")
        next_text = str(next_value or "").strip()
        if next_text:
            parsed = urllib.parse.urlparse(next_text)
            query = urllib.parse.parse_qs(parsed.query)
            for key in ("cursor", "nextCursor", "next_cursor"):
                value = query.get(key, [""])[0]
                if value:
                    return value
    return ""


def _monta_url(*, country_id: str, limit: int, cursor: str) -> tuple[str, str]:
    query: dict[str, str] = {}
    if country_id:
        query["countryId"] = country_id
    if limit > 0:
        query["limit"] = str(limit)
    if cursor:
        query["cursor"] = cursor
    query_text = urllib.parse.urlencode(query)
    url = f"{MONTA_AFIR_CHARGE_POINTS_URL}?{query_text}" if query_text else MONTA_AFIR_CHARGE_POINTS_URL
    return url, query_text


def _archive_monta_source(
    *,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
    max_rate_limit_retries: int,
    monta_country_id: str,
    monta_page_limit: int,
    max_pages: int,
) -> dict[str, Any]:
    spec = SOURCES["monta-charge-points"]
    access_token = _request_monta_access_token(timeout_seconds)
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
        "User-Agent": USER_AGENT,
    }
    summaries: list[dict[str, Any]] = []
    cursor = ""
    page = 0
    while page < max_pages:
        url, safe_query = _monta_url(country_id=monta_country_id, limit=monta_page_limit, cursor=cursor)
        temp_path, payload_sha256, byte_length, response_headers = _fetch_to_file(
            url,
            config=config,
            source_key=f"{spec.key}-page-{page + 1}",
            source_suffix=spec.suffix,
            headers=headers,
            timeout_seconds=timeout_seconds,
            max_rate_limit_retries=max_rate_limit_retries,
        )
        summary = _archive_payload_file(
            store=store,
            spec=spec,
            temp_path=temp_path,
            payload_sha256=payload_sha256,
            byte_length=byte_length,
            headers=response_headers,
            safe_request_path=MONTA_AFIR_CHARGE_POINTS_URL,
            safe_request_query=safe_query,
            parse_summary=parse_summary,
        )
        summaries.append(summary)
        payload = _read_archived_json_page(store, summary["storage_uri"], response_headers.get("content-encoding", ""))
        next_cursor = _next_monta_cursor(payload)
        if not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
        page += 1

    return {
        "source": spec.key,
        "source_uid": spec.source_uid,
        "pages": summaries,
        "page_count": len(summaries),
    }


def _selected_sources(value: str) -> tuple[list[SourceSpec], list[dict[str, str]]]:
    if value == "all":
        return list(SOURCES.values()), []
    if value in {"all-configured", "dynamic-configured"}:
        selected = []
        skipped = []
        for spec in SOURCES.values():
            if value == "dynamic-configured" and spec.task_kind == "parse_static_payload":
                skipped.append(
                    {
                        "source": spec.key,
                        "source_uid": spec.source_uid,
                        "reason": "static_source_excluded_from_dynamic_cycle",
                        "hint": "Run this source from a daily/weekly static refresh job instead of every dynamic cycle.",
                    }
                )
                continue
            if _has_credentials(spec):
                selected.append(spec)
            else:
                skipped.append(
                    {
                        "source": spec.key,
                        "source_uid": spec.source_uid,
                        "reason": "missing_credentials",
                        "hint": _source_secret_hint(spec),
                    }
                )
        return selected, skipped
    return [SOURCES[value]], []


def _archive_selected_sources(
    *,
    selected_sources: Sequence[SourceSpec],
    skipped_sources: Sequence[dict[str, str]],
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
    max_rate_limit_retries: int,
    energyvision_page_limit: int,
    monta_country_id: str,
    monta_page_limit: int,
    max_pages: int,
) -> dict[str, Any]:
    summaries: list[dict[str, Any]] = []
    for spec in selected_sources:
        if spec.auth_kind == "monta":
            summaries.append(
                _archive_monta_source(
                    config=config,
                    store=store,
                    timeout_seconds=timeout_seconds,
                    parse_summary=parse_summary,
                    max_rate_limit_retries=max_rate_limit_retries,
                    monta_country_id=str(monta_country_id or "").strip(),
                    monta_page_limit=monta_page_limit,
                    max_pages=max_pages,
                )
            )
        elif spec.auth_kind == "energyvision":
            summaries.append(
                _archive_energyvision_source(
                    spec=spec,
                    config=config,
                    store=store,
                    timeout_seconds=timeout_seconds,
                    parse_summary=parse_summary,
                    max_rate_limit_retries=max_rate_limit_retries,
                    page_limit=energyvision_page_limit,
                    max_pages=max_pages,
                )
            )
        else:
            summaries.append(
                _archive_single_source(
                    spec=spec,
                    config=config,
                    store=store,
                    timeout_seconds=timeout_seconds,
                    parse_summary=parse_summary,
                    max_rate_limit_retries=max_rate_limit_retries,
                )
            )
    return {
        "ok": True,
        "country_code": COUNTRY_CODE,
        "sources": summaries,
        "skipped_sources": list(skipped_sources),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and archive Belgium Transportdata AFIR payloads.")
    parser.add_argument(
        "--source",
        choices=("all-configured", "dynamic-configured", "all", *SOURCES.keys()),
        default="all-configured",
        help=(
            "BE source payload to fetch. all-configured skips providers without local secrets; "
            "dynamic-configured also excludes static-only sources from high-frequency cycles."
        ),
    )
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument(
        "--energyvision-page-limit",
        type=int,
        default=int(os.environ.get("TRANSPORTDATA_BE_ENERGYVISION_PAGE_LIMIT", "2000") or "2000"),
        help="OCPI page limit for EnergyVision locations/tariffs. Set to 0 to disable pagination parameters.",
    )
    parser.add_argument("--monta-country-id", default=os.environ.get("TRANSPORTDATA_BE_MONTA_COUNTRY_ID", "BE"))
    parser.add_argument("--monta-page-limit", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--max-rate-limit-retries", type=int, default=2)
    parser.add_argument(
        "--no-parse-summary",
        action="store_true",
        help="Only archive raw bytes; skip parse-count summaries.",
    )
    parser.add_argument("--loop", action="store_true", help="Continuously fetch selected sources.")
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=float(os.environ.get("TRANSPORTDATA_BE_FETCH_SLEEP_SECONDS", "60") or "60"),
        help="Sleep between loop iterations.",
    )
    args = parser.parse_args(argv)

    config = AppConfig()
    store = create_ingest_store(config)
    store.initialize()

    selected_sources, skipped_sources = _selected_sources(args.source)

    while True:
        try:
            result = _archive_selected_sources(
                selected_sources=selected_sources,
                skipped_sources=skipped_sources,
                config=config,
                store=store,
                timeout_seconds=args.timeout_seconds,
                parse_summary=not args.no_parse_summary,
                max_rate_limit_retries=args.max_rate_limit_retries,
                energyvision_page_limit=args.energyvision_page_limit,
                monta_country_id=args.monta_country_id,
                monta_page_limit=args.monta_page_limit,
                max_pages=args.max_pages,
            )
            print(json.dumps(result, indent=2, sort_keys=True), flush=True)
        except Exception as exc:
            if not args.loop:
                raise
            print(
                json.dumps(
                    {
                        "ok": False,
                        "country_code": COUNTRY_CODE,
                        "error_type": exc.__class__.__name__,
                        "error": str(exc),
                    },
                    indent=2,
                    sort_keys=True,
                ),
                flush=True,
            )
        if not args.loop:
            break
        time.sleep(max(float(args.sleep_seconds), 1.0))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
