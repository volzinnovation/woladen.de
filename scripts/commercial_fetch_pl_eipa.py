#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import ssl
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from commercial_backend.config import AppConfig
from commercial_backend.pl_eipa import (
    COUNTRY_CODE,
    DANE_GOV_AFIR_SEARCH_SOURCE_UID,
    DANE_GOV_AFIR_SEARCH_URL,
    DANE_GOV_API_DOC_SOURCE_UID,
    DANE_GOV_API_DOC_URL,
    DANE_GOV_API_SPEC_SOURCE_UID,
    DANE_GOV_API_SPEC_URL,
    DANE_GOV_CHARGING_SEARCH_SOURCE_UID,
    DANE_GOV_CHARGING_SEARCH_URL,
    EIPA_BROWSER_INDEX_SOURCE_UID,
    EIPA_BROWSER_PROVINCE_SOURCE_UID,
    EIPA_BROWSER_URL,
    EIPA_READER_FILE_KEYS,
    EIPA_READER_FILE_SOURCE_UIDS,
    EIPA_READER_STATIC_FILE_KEYS,
    EIPA_HOME_SOURCE_UID,
    EIPA_HOME_URL,
    EIPA_READER_DOCS_SOURCE_UID,
    EIPA_READER_DOCS_URL,
    EIPA_STATS_SOURCE_UID,
    EIPA_STATS_URL,
    build_eipa_reader_export_url,
    build_eipa_browser_province_url,
    count_eipa_browser_rows,
    extract_eipa_provinces,
    max_eipa_browser_page,
    redacted_eipa_reader_export_url,
    summarize_dane_dataset_search_payload,
)
from commercial_backend.store import create_ingest_store

USER_AGENT = "woladen.de public PL EIPA ingester"
EIPA_DEFAULT_SOURCE_KEYS = [
    "eipa-home",
    "eipa-docs",
    "eipa-stats",
    "eipa-browser-provinces",
]
EIPA_READER_STATIC_SOURCE_KEYS = [f"eipa-reader-{file_key}" for file_key in EIPA_READER_STATIC_FILE_KEYS]
EIPA_READER_SOURCE_KEYS = [f"eipa-reader-{file_key}" for file_key in EIPA_READER_FILE_KEYS]
DANE_GOV_DISCOVERY_SOURCE_KEYS = [
    "dane-doc",
    "dane-spec",
    "dane-afir-search",
    "dane-charging-search",
]


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
    parse_summary: str = ""
    reader_file_key: str = ""


SOURCES = {
    "dane-doc": SourceSpec(
        key="dane-doc",
        source_uid=DANE_GOV_API_DOC_SOURCE_UID,
        display_name="PL dane.gov.pl API Swagger UI",
        url=DANE_GOV_API_DOC_URL,
        suffix=".html",
        accept="text/html,application/xhtml+xml",
        source_kind="national_open_data_api_doc",
        task_kind="archive_only_payload",
    ),
    "dane-spec": SourceSpec(
        key="dane-spec",
        source_uid=DANE_GOV_API_SPEC_SOURCE_UID,
        display_name="PL dane.gov.pl OpenAPI specification",
        url=DANE_GOV_API_SPEC_URL,
        suffix=".json",
        accept="application/json",
        source_kind="national_open_data_api_spec",
        task_kind="archive_only_payload",
    ),
    "dane-afir-search": SourceSpec(
        key="dane-afir-search",
        source_uid=DANE_GOV_AFIR_SEARCH_SOURCE_UID,
        display_name="PL dane.gov.pl AFIR dataset search",
        url=DANE_GOV_AFIR_SEARCH_URL,
        suffix=".json",
        accept="application/json",
        source_kind="national_open_data_search_metadata",
        task_kind="archive_only_payload",
        parse_summary="dane_search",
    ),
    "dane-charging-search": SourceSpec(
        key="dane-charging-search",
        source_uid=DANE_GOV_CHARGING_SEARCH_SOURCE_UID,
        display_name="PL dane.gov.pl charging dataset search",
        url=DANE_GOV_CHARGING_SEARCH_URL,
        suffix=".json",
        accept="application/json",
        source_kind="national_open_data_search_metadata",
        task_kind="archive_only_payload",
        parse_summary="dane_search",
    ),
    "eipa-docs": SourceSpec(
        key="eipa-docs",
        source_uid=EIPA_READER_DOCS_SOURCE_UID,
        display_name="PL EIPA reader JSON file documentation",
        url=EIPA_READER_DOCS_URL,
        suffix=".html",
        accept="text/html,application/xhtml+xml",
        source_kind="national_register_reader_schema_docs",
        task_kind="archive_only_payload",
    ),
    "eipa-home": SourceSpec(
        key="eipa-home",
        source_uid=EIPA_HOME_SOURCE_UID,
        display_name="PL EIPA public registry home",
        url=EIPA_HOME_URL,
        suffix=".html",
        accept="text/html,application/xhtml+xml",
        source_kind="national_register_public_home",
        task_kind="archive_only_payload",
    ),
    "eipa-browser-index": SourceSpec(
        key="eipa-browser-index",
        source_uid=EIPA_BROWSER_INDEX_SOURCE_UID,
        display_name="PL EIPA public browser index",
        url=EIPA_BROWSER_URL,
        suffix=".html",
        accept="text/html,application/xhtml+xml",
        source_kind="national_register_public_browser_index",
        task_kind="archive_only_payload",
        parse_summary="eipa_browser_index",
    ),
    "eipa-stats": SourceSpec(
        key="eipa-stats",
        source_uid=EIPA_STATS_SOURCE_UID,
        display_name="PL EIPA public registry statistics",
        url=EIPA_STATS_URL,
        suffix=".html",
        accept="text/html,application/xhtml+xml",
        source_kind="national_register_public_statistics",
        task_kind="archive_only_payload",
        parse_summary="eipa_browser_page",
    ),
}
for _reader_file_key in EIPA_READER_FILE_KEYS:
    _is_dynamic = _reader_file_key == "dynamic"
    SOURCES[f"eipa-reader-{_reader_file_key}"] = SourceSpec(
        key=f"eipa-reader-{_reader_file_key}",
        source_uid=EIPA_READER_FILE_SOURCE_UIDS[_reader_file_key],
        display_name=f"PL EIPA reader {_reader_file_key}.json",
        url=redacted_eipa_reader_export_url(file_key=_reader_file_key),
        suffix=".json",
        accept="application/json",
        source_kind="national_register_reader_json_dynamic"
        if _is_dynamic
        else "national_register_reader_json_static",
        task_kind="archive_private_dynamic_payload" if _is_dynamic else "archive_only_payload",
        parse_summary="eipa_reader_json",
        reader_file_key=_reader_file_key,
    )


def _unquote_env_value(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def _read_token_file(path: Path) -> str:
    if not path.exists() or not path.is_file():
        return ""
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return ""
    env_names = {"PL_EIPA_READER_TOKEN", "EIPA_READER_TOKEN"}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() in env_names:
            return _unquote_env_value(value)
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" not in line:
            return line
    return ""


def _default_reader_token_files() -> tuple[Path, ...]:
    return (
        REPO_ROOT / "secret" / "pl_eipa_reader_token.txt",
        REPO_ROOT / "secret" / "eipa_reader_token.txt",
        Path("/run/secrets/woladen/pl_eipa_reader_token.txt"),
        Path("/run/secrets/woladen/eipa_reader_token.txt"),
        Path("/run/secrets/woladen-local/pl_eipa_reader_token.txt"),
        Path("/run/secrets/woladen-local/eipa_reader_token.txt"),
    )


def _resolve_reader_token(*, token_file: Path | None) -> str:
    for env_name in ("PL_EIPA_READER_TOKEN", "EIPA_READER_TOKEN"):
        token = str(os.environ.get(env_name, "")).strip()
        if token:
            return token
    for env_name in ("PL_EIPA_READER_TOKEN_FILE", "EIPA_READER_TOKEN_FILE"):
        path_text = str(os.environ.get(env_name, "")).strip()
        if path_text:
            token = _read_token_file(Path(path_text).expanduser())
            if token:
                return token
    if token_file is not None:
        token = _read_token_file(token_file.expanduser())
        if token:
            return token
    for path in _default_reader_token_files():
        token = _read_token_file(path)
        if token:
            return token
    return ""


def _temp_payload_path(config: AppConfig, prefix: str, suffix: str) -> Path:
    incoming_dir = config.raw_payload_dir / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=incoming_dir,
        prefix=prefix,
        suffix=suffix,
        delete=False,
    )
    handle.close()
    return Path(handle.name)


def _fetch_to_file(
    *,
    url: str,
    accept: str,
    config: AppConfig,
    timeout_seconds: int,
    prefix: str,
    suffix: str,
) -> tuple[Path, str, int, dict[str, str], str]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": accept,
            "User-Agent": USER_AGENT,
        },
    )

    def write_response(response) -> tuple[Path, str, int, dict[str, str], str]:
        payload_path = _temp_payload_path(config, prefix, suffix)
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
            response.geturl(),
        )

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return write_response(response)
    except (ssl.SSLError, urllib.error.URLError):
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
            return write_response(response)


def _read_text_payload(store: Any, storage_uri: str) -> str:
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        return payload_stream.read().decode("utf-8", errors="replace")


def _build_parse_summary(store: Any, spec: SourceSpec, storage_uri: str) -> dict[str, Any]:
    if spec.parse_summary == "dane_search":
        with store.open_storage_uri_reader(storage_uri) as payload_stream:
            payload = json.loads(payload_stream.read().decode("utf-8"))
        return {"search_summary": summarize_dane_dataset_search_payload(payload)}
    if spec.parse_summary == "eipa_browser_index":
        html_text = _read_text_payload(store, storage_uri)
        return {"province_count": len(extract_eipa_provinces(html_text))}
    if spec.parse_summary == "eipa_browser_page":
        html_text = _read_text_payload(store, storage_uri)
        return {
            "browser_row_count": count_eipa_browser_rows(html_text),
            "max_browser_page": max_eipa_browser_page(html_text),
        }
    if spec.parse_summary == "eipa_reader_json":
        with store.open_storage_uri_reader(storage_uri) as payload_stream:
            payload = json.loads(payload_stream.read().decode("utf-8"))
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            return {
                "reader_file": spec.reader_file_key,
                "generated": str(payload.get("generated") or ""),
                "row_count": len(payload["data"]),
            }
        if isinstance(payload, dict):
            return {
                "reader_file": spec.reader_file_key,
                "dictionary_sections": sorted(payload),
                "dictionary_entry_count": sum(
                    len(value) for value in payload.values() if isinstance(value, list)
                ),
            }
    return {}


def _archive_source(
    *,
    spec: SourceSpec,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
) -> dict[str, Any]:
    temp_path, payload_sha256, byte_length, headers, final_url = _fetch_to_file(
        url=spec.url,
        accept=spec.accept,
        config=config,
        timeout_seconds=timeout_seconds,
        prefix=f"pl-eipa-{spec.key}-",
        suffix=spec.suffix,
    )
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
        request_headers={
            "user-agent": USER_AGENT,
            "source-url": spec.url,
            "final-url": final_url,
        },
        source_kind=spec.source_kind,
        display_name=spec.display_name,
        task_kind=spec.task_kind,
        remote_addr="",
    )
    summary: dict[str, Any] = {
        "source": spec.key,
        "source_uid": spec.source_uid,
        "url": spec.url,
        "final_url": final_url,
        "task_kind": spec.task_kind,
        "payload_sha256": payload_sha256,
        "byte_length": byte_length,
        "duplicate_payload": receipt.duplicate_payload,
        "queued": receipt.queued,
        "storage_uri": receipt.storage_uri,
    }
    if parse_summary:
        summary.update(_build_parse_summary(store, spec, receipt.storage_uri))
    return summary


def _archive_eipa_reader_source(
    *,
    spec: SourceSpec,
    token: str,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
) -> dict[str, Any]:
    if not spec.reader_file_key:
        raise ValueError(f"not_a_reader_source:{spec.key}")
    fetch_url = build_eipa_reader_export_url(file_key=spec.reader_file_key, token=token)
    display_url = redacted_eipa_reader_export_url(file_key=spec.reader_file_key)
    temp_path, payload_sha256, byte_length, headers, final_url = _fetch_to_file(
        url=fetch_url,
        accept=spec.accept,
        config=config,
        timeout_seconds=timeout_seconds,
        prefix=f"pl-eipa-reader-{spec.reader_file_key}-",
        suffix=spec.suffix,
    )
    final_display_url = final_url.replace(token, "<token-redacted>")
    receipt = store.record_pull_payload_file(
        country_code=COUNTRY_CODE,
        source_uid=spec.source_uid,
        payload_path=temp_path,
        payload_sha256=payload_sha256,
        byte_length=byte_length,
        content_type=headers.get("content-type", ""),
        content_encoding=headers.get("content-encoding", ""),
        request_path=display_url,
        request_query="",
        request_headers={
            "user-agent": USER_AGENT,
            "source-url": display_url,
            "final-url": final_display_url,
            "reader-file": spec.reader_file_key,
        },
        source_kind=spec.source_kind,
        display_name=spec.display_name,
        task_kind=spec.task_kind,
        remote_addr="",
    )
    summary: dict[str, Any] = {
        "source": spec.key,
        "source_uid": spec.source_uid,
        "reader_file": spec.reader_file_key,
        "url": display_url,
        "final_url": final_display_url,
        "task_kind": spec.task_kind,
        "payload_sha256": payload_sha256,
        "byte_length": byte_length,
        "duplicate_payload": receipt.duplicate_payload,
        "queued": receipt.queued,
        "storage_uri": receipt.storage_uri,
    }
    if parse_summary:
        summary.update(_build_parse_summary(store, spec, receipt.storage_uri))
    return summary


def _archive_eipa_browser_page(
    *,
    province: str,
    page: int,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
) -> dict[str, Any]:
    url = build_eipa_browser_province_url(province=province, page=page)
    temp_path, payload_sha256, byte_length, headers, final_url = _fetch_to_file(
        url=url,
        accept="text/html,application/xhtml+xml",
        config=config,
        timeout_seconds=timeout_seconds,
        prefix=f"pl-eipa-browser-{province}-{page}-",
        suffix=".html",
    )
    receipt = store.record_pull_payload_file(
        country_code=COUNTRY_CODE,
        source_uid=EIPA_BROWSER_PROVINCE_SOURCE_UID,
        payload_path=temp_path,
        payload_sha256=payload_sha256,
        byte_length=byte_length,
        content_type=headers.get("content-type", ""),
        content_encoding=headers.get("content-encoding", ""),
        request_path=url,
        request_query="",
        request_headers={
            "user-agent": USER_AGENT,
            "source-url": url,
            "final-url": final_url,
            "province": province,
            "page": str(page),
        },
        source_kind="national_register_public_browser_html",
        display_name="PL EIPA public province browser page",
        task_kind="archive_only_payload",
        remote_addr="",
    )
    summary: dict[str, Any] = {
        "source": "eipa-browser-province-page",
        "source_uid": EIPA_BROWSER_PROVINCE_SOURCE_UID,
        "province": province,
        "page": page,
        "url": url,
        "final_url": final_url,
        "task_kind": "archive_only_payload",
        "payload_sha256": payload_sha256,
        "byte_length": byte_length,
        "duplicate_payload": receipt.duplicate_payload,
        "queued": receipt.queued,
        "storage_uri": receipt.storage_uri,
    }
    if parse_summary:
        html_text = _read_text_payload(store, receipt.storage_uri)
        summary["browser_row_count"] = count_eipa_browser_rows(html_text)
        summary["max_browser_page"] = max_eipa_browser_page(html_text)
    return summary


def _selected_source_keys(value: str) -> list[str]:
    if value == "all-open":
        return list(EIPA_DEFAULT_SOURCE_KEYS)
    if value == "all-reader-static":
        return list(EIPA_READER_STATIC_SOURCE_KEYS)
    if value == "all-reader-json":
        return list(EIPA_READER_SOURCE_KEYS)
    if value == "all-discovery":
        return [*EIPA_DEFAULT_SOURCE_KEYS, *DANE_GOV_DISCOVERY_SOURCE_KEYS]
    return [value]


def _archive_eipa_browser_provinces(
    *,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
    selected_provinces: set[str],
    max_pages_per_province: int,
) -> dict[str, Any]:
    index_summary = _archive_source(
        spec=SOURCES["eipa-browser-index"],
        config=config,
        store=store,
        timeout_seconds=timeout_seconds,
        parse_summary=parse_summary,
    )
    html_text = _read_text_payload(store, index_summary["storage_uri"])
    provinces = extract_eipa_provinces(html_text)
    if selected_provinces:
        provinces = [province for province in provinces if province in selected_provinces]

    page_summaries: list[dict[str, Any]] = []
    for province in provinces:
        first_page_summary = _archive_eipa_browser_page(
            province=province,
            page=1,
            config=config,
            store=store,
            timeout_seconds=timeout_seconds,
            parse_summary=True,
        )
        page_summaries.append(first_page_summary)
        page_count = int(first_page_summary.get("max_browser_page") or 1)
        if max_pages_per_province > 0:
            page_count = min(page_count, max_pages_per_province)
        for page in range(2, page_count + 1):
            page_summaries.append(
                _archive_eipa_browser_page(
                    province=province,
                    page=page,
                    config=config,
                    store=store,
                    timeout_seconds=timeout_seconds,
                    parse_summary=parse_summary,
                )
            )

    return {
        "source": "eipa-browser-provinces",
        "source_uid": EIPA_BROWSER_PROVINCE_SOURCE_UID,
        "index": index_summary,
        "province_count": len(provinces),
        "page_count": len(page_summaries),
        "row_count": sum(int(item.get("browser_row_count") or 0) for item in page_summaries),
        "pages": page_summaries,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and archive PL EIPA source payloads.")
    parser.add_argument(
        "--source",
        choices=(
            "all-open",
            "all-reader-static",
            "all-reader-json",
            "all-discovery",
            "dane-doc",
            "dane-spec",
            "dane-afir-search",
            "dane-charging-search",
            "eipa-home",
            "eipa-docs",
            "eipa-browser-index",
            "eipa-browser-provinces",
            "eipa-stats",
            *EIPA_READER_SOURCE_KEYS,
        ),
        default="all-open",
        help="PL source payload to fetch. Use all-reader-json after EIPA reader access is granted.",
    )
    parser.add_argument(
        "--reader-token-file",
        type=Path,
        default=None,
        help="Optional file containing the EIPA reader token. Defaults to ignored secret/ paths or PL_EIPA_READER_TOKEN_FILE.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument(
        "--max-pages-per-province",
        type=int,
        default=0,
        help="Limit EIPA province browser pages per province. 0 means all discovered pages.",
    )
    parser.add_argument(
        "--province",
        action="append",
        default=[],
        help="Restrict EIPA browser province archiving to one province value. May be repeated.",
    )
    parser.add_argument(
        "--no-parse-summary",
        action="store_true",
        help="Only archive raw bytes; skip lightweight search/browser summaries where possible.",
    )
    args = parser.parse_args(argv)

    config = AppConfig()
    store = create_ingest_store(config)
    store.initialize()

    summaries = []
    for source_key in _selected_source_keys(args.source):
        if source_key in EIPA_READER_SOURCE_KEYS:
            token = _resolve_reader_token(token_file=args.reader_token_file)
            if not token:
                raise SystemExit(
                    "EIPA reader token missing; set PL_EIPA_READER_TOKEN_FILE or store it in secret/pl_eipa_reader_token.txt"
                )
            summaries.append(
                _archive_eipa_reader_source(
                    spec=SOURCES[source_key],
                    token=token,
                    config=config,
                    store=store,
                    timeout_seconds=args.timeout_seconds,
                    parse_summary=not args.no_parse_summary,
                )
            )
            continue
        if source_key == "eipa-browser-provinces":
            summaries.append(
                _archive_eipa_browser_provinces(
                    config=config,
                    store=store,
                    timeout_seconds=args.timeout_seconds,
                    parse_summary=not args.no_parse_summary,
                    selected_provinces=set(args.province),
                    max_pages_per_province=args.max_pages_per_province,
                )
            )
            continue
        summaries.append(
            _archive_source(
                spec=SOURCES[source_key],
                config=config,
                store=store,
                timeout_seconds=args.timeout_seconds,
                parse_summary=not args.no_parse_summary,
            )
        )
    print(json.dumps({"ok": True, "country_code": COUNTRY_CODE, "sources": summaries}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
