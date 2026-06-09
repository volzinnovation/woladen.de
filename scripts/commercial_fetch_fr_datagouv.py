#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
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
from commercial_backend.fr_datagouv import (
    AFIR_SEARCH_API_URL,
    AFIR_SEARCH_SOURCE_UID,
    BASE_NATIONALE_STATIC_RESOURCE_URL,
    BASE_NATIONALE_STATIC_SOURCE_UID,
    COUNTRY_CODE,
    ECO_MOVEMENT_DYNAMIC_RESOURCE_URL,
    ECO_MOVEMENT_DYNAMIC_SOURCE_UID,
    ECO_MOVEMENT_STATIC_RESOURCE_URL,
    ECO_MOVEMENT_STATIC_SOURCE_UID,
    summarize_afir_search_payload,
)
from commercial_backend.store import create_ingest_store

USER_AGENT = "woladen.de public FR data.gouv.fr ingester"


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


SOURCES = {
    "search": SourceSpec(
        key="search",
        source_uid=AFIR_SEARCH_SOURCE_UID,
        display_name="FR data.gouv.fr AFIR dataset search results",
        url=AFIR_SEARCH_API_URL,
        suffix=".json",
        accept="application/json",
        source_kind="national_catalog_search_metadata",
        task_kind="archive_only_payload",
        parse_summary="afir_search",
    ),
    "eco-dynamic": SourceSpec(
        key="eco-dynamic",
        source_uid=ECO_MOVEMENT_DYNAMIC_SOURCE_UID,
        display_name="FR Eco-Movement AFIR IRVE dynamic CSV",
        url=ECO_MOVEMENT_DYNAMIC_RESOURCE_URL,
        suffix=".csv",
        accept="text/csv,application/octet-stream",
        source_kind="fr_afir_irve_dynamic_csv",
        task_kind="parse_dynamic_payload",
        parse_summary="csv_rows",
    ),
    "base-static": SourceSpec(
        key="base-static",
        source_uid=BASE_NATIONALE_STATIC_SOURCE_UID,
        display_name="FR Base nationale IRVE static CSV",
        url=BASE_NATIONALE_STATIC_RESOURCE_URL,
        suffix=".csv",
        accept="text/csv,application/octet-stream",
        source_kind="fr_base_nationale_irve_static_csv",
        task_kind="parse_static_payload",
        parse_summary="csv_rows",
    ),
    "eco-static": SourceSpec(
        key="eco-static",
        source_uid=ECO_MOVEMENT_STATIC_SOURCE_UID,
        display_name="FR Eco-Movement AFIR IRVE static CSV",
        url=ECO_MOVEMENT_STATIC_RESOURCE_URL,
        suffix=".csv",
        accept="text/csv,application/octet-stream",
        source_kind="fr_afir_irve_static_csv",
        task_kind="parse_static_payload",
        parse_summary="csv_rows",
    ),
}


def _temp_payload_path(config: AppConfig, spec: SourceSpec) -> Path:
    incoming_dir = config.raw_payload_dir / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=incoming_dir,
        prefix=f"fr-datagouv-{spec.key}-",
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
) -> tuple[Path, str, int, dict[str, str], str]:
    request = urllib.request.Request(
        spec.url,
        headers={
            "Accept": spec.accept,
            "User-Agent": USER_AGENT,
        },
    )

    def write_response(response) -> tuple[Path, str, int, dict[str, str], str]:
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
            response.geturl(),
        )

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return write_response(response)
    except (ssl.SSLError, urllib.error.URLError):
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
            return write_response(response)


def _count_csv_data_rows(store: Any, storage_uri: str) -> int:
    line_count = 0
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        for _line in payload_stream:
            line_count += 1
    return max(line_count - 1, 0)


def _build_parse_summary(store: Any, spec: SourceSpec, storage_uri: str) -> dict[str, Any]:
    if spec.parse_summary == "csv_rows":
        return {"csv_data_row_count": _count_csv_data_rows(store, storage_uri)}
    if spec.parse_summary == "afir_search":
        with store.open_storage_uri_reader(storage_uri) as payload_stream:
            payload = json.loads(payload_stream.read().decode("utf-8"))
        return {"search_summary": summarize_afir_search_payload(payload)}
    return {}


def _selected_sources(value: str) -> list[SourceSpec]:
    if value == "all":
        return [SOURCES["search"], SOURCES["base-static"], SOURCES["eco-dynamic"], SOURCES["eco-static"]]
    return [SOURCES[value]]


def _archive_source(
    *,
    spec: SourceSpec,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
) -> dict[str, Any]:
    temp_path, payload_sha256, byte_length, headers, final_url = _fetch_to_file(
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and archive FR data.gouv.fr AFIR catalogue/resources.")
    parser.add_argument(
        "--source",
        choices=("all", "search", "base-static", "eco-dynamic", "eco-static"),
        default="all",
        help="FR data.gouv.fr source payload to fetch.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument(
        "--no-parse-summary",
        action="store_true",
        help="Only archive raw bytes; skip lightweight search/CSV count summaries.",
    )
    args = parser.parse_args(argv)

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
        for spec in _selected_sources(args.source)
    ]
    print(json.dumps({"ok": True, "country_code": COUNTRY_CODE, "sources": summaries}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
