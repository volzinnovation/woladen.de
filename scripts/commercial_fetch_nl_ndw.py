#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import tempfile
import urllib.request
from dataclasses import dataclass
from pathlib import Path
import sys
from typing import Any, Callable, Iterable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from commercial_backend.config import AppConfig
from commercial_backend.http_fetch import stream_request_to_file
from commercial_backend.nl_ndw import (
    COUNTRY_CODE,
    GEOJSON_LOCATIONS_SOURCE_UID,
    NL_GEOJSON_LOCATIONS_URL,
    NL_OCPI_LOCATIONS_URL,
    NL_OCPI_TARIFFS_URL,
    OCPI_TARIFFS_SOURCE_UID,
    SOURCE_UID,
    iter_location_rows_from_binary_stream,
    iter_tariff_rows_from_binary_stream,
)
from commercial_backend.store import create_ingest_store

USER_AGENT = "woladen.de public NL ingester"


@dataclass(frozen=True)
class SourceSpec:
    key: str
    source_uid: str
    display_name: str
    url: str
    suffix: str
    source_kind: str
    task_kind: str
    stream_parser: Callable[..., Iterable[dict[str, Any]]] | None = None


SOURCES = {
    "locations": SourceSpec(
        key="locations",
        source_uid=SOURCE_UID,
        display_name="NL NDW/DOT-NL OCPI locations and EVSE status",
        url=NL_OCPI_LOCATIONS_URL,
        suffix=".json.gz",
        source_kind="national_register_ocpi_locations_with_status",
        task_kind="parse_dynamic_payload",
        stream_parser=iter_location_rows_from_binary_stream,
    ),
    "geojson": SourceSpec(
        key="geojson",
        source_uid=GEOJSON_LOCATIONS_SOURCE_UID,
        display_name="NL NDW/DOT-NL GeoJSON charging-point locations",
        url=NL_GEOJSON_LOCATIONS_URL,
        suffix=".geojson.gz",
        source_kind="national_register_geojson_locations",
        task_kind="archive_only_payload",
    ),
    "tariffs": SourceSpec(
        key="tariffs",
        source_uid=OCPI_TARIFFS_SOURCE_UID,
        display_name="NL NDW/DOT-NL OCPI tariffs",
        url=NL_OCPI_TARIFFS_URL,
        suffix=".json.gz",
        source_kind="national_register_ocpi_tariffs",
        task_kind="parse_metadata_payload",
        stream_parser=iter_tariff_rows_from_binary_stream,
    ),
}


def _temp_payload_path(config: AppConfig, spec: SourceSpec) -> Path:
    incoming_dir = config.raw_payload_dir / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=incoming_dir,
        prefix=f"nl-ndw-{spec.key}-",
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
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
    )
    result = stream_request_to_file(
        request,
        _temp_payload_path(config, spec),
        timeout_seconds=timeout_seconds,
    )
    return result.payload_path, result.payload_sha256, result.byte_length, result.headers


def _count_archived_rows(store, spec: SourceSpec, storage_uri: str, content_encoding: str) -> int | None:
    if spec.stream_parser is None:
        return None
    row_count = 0
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        for _row in spec.stream_parser(payload_stream, content_encoding=content_encoding):
            row_count += 1
    return row_count


def _selected_sources(value: str) -> list[SourceSpec]:
    if value == "all":
        return [SOURCES["locations"], SOURCES["geojson"], SOURCES["tariffs"]]
    return [SOURCES[value]]


def _archive_source(
    *,
    spec: SourceSpec,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
) -> dict[str, Any]:
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
        request_path=spec.url,
        request_query="",
        request_headers={"user-agent": USER_AGENT, "source-url": spec.url},
        source_kind=spec.source_kind,
        display_name=spec.display_name,
        task_kind=spec.task_kind,
        remote_addr="",
    )

    summary: dict[str, Any] = {
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
    if parse_summary and spec.stream_parser is not None:
        summary["parsed_row_count"] = _count_archived_rows(
            store,
            spec,
            receipt.storage_uri,
            headers.get("content-encoding", ""),
        )
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and archive NL NDW no-auth payloads.")
    parser.add_argument(
        "--source",
        choices=("all", "locations", "geojson", "tariffs"),
        default="all",
        help="NL NDW source payload to fetch.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument(
        "--no-parse-summary",
        action="store_true",
        help="Only archive raw bytes; skip parse-count summaries.",
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
