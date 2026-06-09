#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import ssl
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any, Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from commercial_backend.config import AppConfig  # noqa: E402
from commercial_backend.dk_monta import (  # noqa: E402
    COUNTRY_CODE,
    MONTA_AFIR_CHARGE_POINTS_URL,
    MONTA_AFIR_EVSE_STATUS_URL_TEMPLATE,
    MONTA_PUBLIC_AUTH_TOKEN_URL,
    count_table_payload,
    extract_evse_ids_from_table_payload,
    iter_static_rows_from_binary_stream,
    iter_status_rows_from_binary_stream,
    monta_country_config,
)
from commercial_backend.http_fetch import stream_request_to_file  # noqa: E402
from commercial_backend.store import create_ingest_store  # noqa: E402

USER_AGENT = "woladen.de public Monta Public AFIR ingester"
SECRET_DIR = REPO_ROOT / "secret"


@dataclass(frozen=True)
class SourceSpec:
    key: str
    country_code: str
    source_uid: str
    provider_uid: str
    display_name: str
    url: str
    source_kind: str
    task_kind: str
    suffix: str


def table_source_for_country(country: str) -> SourceSpec:
    config = monta_country_config(country)
    return SourceSpec(
        key="table",
        country_code=config.country_code,
        source_uid=config.table_source_uid,
        provider_uid=config.provider_uid,
        display_name=f"{config.country_code} Monta AFIR charge-point DATEX II table JSON",
        url=MONTA_AFIR_CHARGE_POINTS_URL,
        source_kind=f"{config.country_code.lower()}_monta_afir_static_table",
        task_kind="parse_static_payload",
        suffix=".json",
    )


def status_source_for_country(country: str) -> SourceSpec:
    config = monta_country_config(country)
    return SourceSpec(
        key="status",
        country_code=config.country_code,
        source_uid=config.status_source_uid,
        provider_uid=config.provider_uid,
        display_name=f"{config.country_code} Monta AFIR EVSE status and ad-hoc price JSON",
        url=MONTA_AFIR_EVSE_STATUS_URL_TEMPLATE,
        source_kind=f"{config.country_code.lower()}_monta_afir_dynamic_status_price",
        task_kind="parse_dynamic_payload",
        suffix=".json",
    )


TABLE_SOURCE = table_source_for_country(COUNTRY_CODE)
STATUS_SOURCE = status_source_for_country(COUNTRY_CODE)


def _env_text(name: str) -> str:
    return str(os.environ.get(name, "")).strip()


def load_secret(*, env_names: Iterable[str], filenames: Iterable[str]) -> str:
    for env_name in env_names:
        value = _env_text(env_name)
        if value:
            return value
    for filename in filenames:
        path = SECRET_DIR / filename
        if path.exists():
            value = path.read_text(encoding="utf-8").strip()
            if value:
                return value
    return ""


def _monta_client_id() -> str:
    return load_secret(
        env_names=("MONTA_PUBLIC_CLIENT_ID", "DK_MONTA_CLIENT_ID", "MONTA_DK_CLIENT_ID"),
        filenames=("dk_monta_client_ID.txt", "dk_monta_client_id.txt"),
    )


def _monta_client_secret() -> str:
    return load_secret(
        env_names=(
            "MONTA_PUBLIC_CLIENT_SECRET",
            "DK_MONTA_CLIENT_SECRET",
            "DK_MONTA_PASSWORD",
            "DK_MONTA_PWD",
            "MONTA_DK_CLIENT_SECRET",
        ),
        filenames=("dk_monta_pwd.txt", "dk_monta_client_secret.txt"),
    )


def _monta_bearer_token() -> str:
    return load_secret(
        env_names=("MONTA_PUBLIC_BEARER_TOKEN", "DK_MONTA_BEARER_TOKEN", "MONTA_DK_BEARER_TOKEN"),
        filenames=("dk_monta_bearer_token.txt",),
    )


def secret_hint() -> str:
    return (
        "missing Monta Public API credentials; env: MONTA_PUBLIC_CLIENT_ID plus "
        "MONTA_PUBLIC_CLIENT_SECRET, DK_MONTA_CLIENT_ID plus "
        "DK_MONTA_CLIENT_SECRET/DK_MONTA_PWD, or MONTA_PUBLIC_BEARER_TOKEN/DK_MONTA_BEARER_TOKEN; files: "
        "secret/dk_monta_client_ID.txt plus secret/dk_monta_pwd.txt or "
        "secret/dk_monta_bearer_token.txt"
    )


def _request_json(
    request: urllib.request.Request,
    *,
    timeout_seconds: int,
) -> Any:
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except (ssl.SSLError, urllib.error.URLError):
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
            return json.loads(response.read().decode("utf-8"))


def request_monta_access_token(timeout_seconds: int) -> str:
    bearer_token = _monta_bearer_token()
    if bearer_token:
        return bearer_token

    client_id = _monta_client_id()
    client_secret = _monta_client_secret()
    if not client_id or not client_secret:
        raise RuntimeError(secret_hint())

    body = json.dumps({"clientId": client_id, "clientSecret": client_secret}).encode("utf-8")
    request = urllib.request.Request(
        MONTA_PUBLIC_AUTH_TOKEN_URL,
        data=body,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    payload = _request_json(request, timeout_seconds=timeout_seconds)
    access_token = str(payload.get("accessToken") or "").strip() if isinstance(payload, dict) else ""
    if not access_token:
        raise RuntimeError("monta_auth_response_missing_accessToken")
    return access_token


def _temp_payload_path(config: AppConfig, source_key: str, *, suffix: str = ".json") -> Path:
    incoming_dir = config.raw_payload_dir / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=incoming_dir,
        prefix=f"monta-{source_key}-",
        suffix=suffix,
        delete=False,
    )
    handle.close()
    return Path(handle.name)


def afir_charge_points_url(*, country: str, page: int, per_page: int) -> tuple[str, str]:
    country_code = monta_country_config(country).country_code
    query = urllib.parse.urlencode(
        {
            "country": country_code,
            "page": str(max(int(page), 1)),
            "perPage": str(max(min(int(per_page), 1000), 1)),
        }
    )
    return f"{MONTA_AFIR_CHARGE_POINTS_URL}?{query}", query


def afir_status_url(evse_id: str) -> str:
    return MONTA_AFIR_EVSE_STATUS_URL_TEMPLATE.format(
        evse_id=urllib.parse.quote(str(evse_id), safe="")
    )


def _authorization_headers(access_token: str) -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}",
        "User-Agent": USER_AGENT,
    }


def _fetch_to_file(
    url: str,
    *,
    config: AppConfig,
    source_key: str,
    headers: dict[str, str],
    timeout_seconds: int,
    max_attempts: int,
    suffix: str = ".json",
) -> tuple[Path, str, int, dict[str, str]]:
    request = urllib.request.Request(url, headers=headers)
    result = stream_request_to_file(
        request,
        _temp_payload_path(config, source_key, suffix=suffix),
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
        allow_insecure_tls_retry=True,
    )
    return result.payload_path, result.payload_sha256, result.byte_length, result.headers


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
    safe_headers: dict[str, str],
    parse_summary: bool,
) -> dict[str, Any]:
    receipt = store.record_pull_payload_file(
        country_code=spec.country_code,
        source_uid=spec.source_uid,
        payload_path=temp_path,
        payload_sha256=payload_sha256,
        byte_length=byte_length,
        content_type=headers.get("content-type", ""),
        content_encoding=headers.get("content-encoding", ""),
        request_path=safe_request_path,
        request_query=safe_request_query,
        request_headers=safe_headers,
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


def _count_archived_rows(store: Any, spec: SourceSpec, storage_uri: str, content_encoding: str) -> int:
    row_count = 0
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        if spec.key == "status":
            iterator = iter_status_rows_from_binary_stream(
                payload_stream,
                content_encoding=content_encoding,
                country_code=spec.country_code,
            )
        else:
            iterator = iter_static_rows_from_binary_stream(
                payload_stream,
                content_encoding=content_encoding,
                country_code=spec.country_code,
            )
        for _row in iterator:
            row_count += 1
    return row_count


def _read_archived_json(store: Any, storage_uri: str, content_encoding: str) -> Any:
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        start = payload_stream.peek(2)[:2] if isinstance(payload_stream, io.BufferedReader) else b""
        compressed = content_encoding.casefold() == "gzip" or start == b"\x1f\x8b"
        binary_stream = gzip.GzipFile(fileobj=payload_stream) if compressed else payload_stream
        text_stream = io.TextIOWrapper(binary_stream, encoding="utf-8")
        try:
            return json.load(text_stream)
        finally:
            text_stream.detach()


def next_page_from_table_payload(payload: Any, current_page: int) -> int | None:
    meta = payload.get("meta") if isinstance(payload, dict) and isinstance(payload.get("meta"), dict) else {}
    page = int(meta.get("page") or current_page)
    per_page = int(meta.get("perPage") or 0)
    total = int(meta.get("total") or 0)
    if per_page <= 0 or total <= 0:
        return None
    return page + 1 if page * per_page < total else None


def _archive_table_pages(
    *,
    config: AppConfig,
    store: Any,
    access_token: str,
    timeout_seconds: int,
    parse_summary: bool,
    max_attempts: int,
    spec: SourceSpec,
    country: str,
    per_page: int,
    max_pages: int,
) -> tuple[dict[str, Any], list[str]]:
    page = 1
    page_count = 0
    pages: list[dict[str, Any]] = []
    evse_ids: list[str] = []
    seen_evse_ids: set[str] = set()
    while page_count < max_pages:
        url, safe_query = afir_charge_points_url(country=country, page=page, per_page=per_page)
        temp_path, payload_sha256, byte_length, response_headers = _fetch_to_file(
            url,
            config=config,
            source_key=f"{spec.country_code.lower()}-table-page-{page}",
            suffix=spec.suffix,
            headers=_authorization_headers(access_token),
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
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
            safe_headers={"accept": "application/json", "user-agent": USER_AGENT},
            parse_summary=parse_summary,
        )
        payload = _read_archived_json(store, summary["storage_uri"], response_headers.get("content-encoding", ""))
        summary["datex_summary"] = count_table_payload(payload)
        pages.append(summary)
        for evse_id in extract_evse_ids_from_table_payload(payload, country_code=spec.country_code):
            if evse_id not in seen_evse_ids:
                seen_evse_ids.add(evse_id)
                evse_ids.append(evse_id)
        next_page = next_page_from_table_payload(payload, page)
        if next_page is None:
            break
        page = next_page
        page_count += 1
    return (
        {
            "source": spec.key,
            "source_uid": spec.source_uid,
            "page_count": len(pages),
            "evse_id_count": len(evse_ids),
            "pages": pages,
        },
        evse_ids,
    )


def _archive_statuses(
    *,
    config: AppConfig,
    store: Any,
    access_token: str,
    timeout_seconds: int,
    parse_summary: bool,
    max_attempts: int,
    spec: SourceSpec,
    evse_ids: Sequence[str],
    max_status_evses: int,
) -> dict[str, Any]:
    selected_evse_ids = list(evse_ids)
    if max_status_evses > 0:
        selected_evse_ids = selected_evse_ids[:max_status_evses]
    pages: list[dict[str, Any]] = []
    for index, evse_id in enumerate(selected_evse_ids, start=1):
        url = afir_status_url(evse_id)
        temp_path, payload_sha256, byte_length, response_headers = _fetch_to_file(
            url,
            config=config,
            source_key=f"{spec.country_code.lower()}-status-{index}",
            suffix=spec.suffix,
            headers=_authorization_headers(access_token),
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
        )
        pages.append(
            _archive_payload_file(
                store=store,
                spec=spec,
                temp_path=temp_path,
                payload_sha256=payload_sha256,
                byte_length=byte_length,
                headers=response_headers,
                safe_request_path=url,
                safe_request_query="",
                safe_headers={"accept": "application/json", "user-agent": USER_AGENT},
                parse_summary=parse_summary,
            )
        )
    return {
        "source": spec.key,
        "source_uid": spec.source_uid,
        "requested_evse_count": len(evse_ids),
        "archived_status_count": len(pages),
        "pages": pages,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and archive Monta Public API AFIR payloads.")
    parser.add_argument("--country", default=COUNTRY_CODE, help="AFIR country query, defaults to DK. Supported: DK, BE.")
    parser.add_argument("--per-page", type=int, default=1000, help="Monta AFIR page size, max 1000.")
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument(
        "--include-status",
        action="store_true",
        help="Also fetch per-EVSE private dynamic status/price documents for EVSEs found in table pages.",
    )
    parser.add_argument(
        "--max-status-evses",
        type=int,
        default=0,
        help="Maximum EVSE statuses to fetch when --include-status is set. 0 means all EVSEs from fetched pages.",
    )
    parser.add_argument(
        "--no-parse-summary",
        action="store_true",
        help="Only archive raw bytes; skip parse-count summaries.",
    )
    args = parser.parse_args(argv)

    country = monta_country_config(args.country).country_code
    table_source = table_source_for_country(country)
    status_source = status_source_for_country(country)
    config = AppConfig()
    store = create_ingest_store(config)
    store.initialize()
    access_token = request_monta_access_token(args.timeout_seconds)

    table_summary, evse_ids = _archive_table_pages(
        config=config,
        store=store,
        access_token=access_token,
        timeout_seconds=args.timeout_seconds,
        parse_summary=not args.no_parse_summary,
        max_attempts=args.max_attempts,
        spec=table_source,
        country=country,
        per_page=args.per_page,
        max_pages=args.max_pages,
    )
    sources: list[dict[str, Any]] = [table_summary]
    if args.include_status:
        sources.append(
            _archive_statuses(
                config=config,
                store=store,
                access_token=access_token,
                timeout_seconds=args.timeout_seconds,
                parse_summary=not args.no_parse_summary,
                max_attempts=args.max_attempts,
                spec=status_source,
                evse_ids=evse_ids,
                max_status_evses=args.max_status_evses,
            )
        )
    print(json.dumps({"ok": True, "country_code": country, "sources": sources}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
