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
from commercial_backend.fi_digitraffic import (
    COUNTRY_CODE,
    DIGITRAFFIC_DATEX_LOCATIONS_SOURCE_UID,
    DIGITRAFFIC_DATEX_LOCATIONS_URL,
    DIGITRAFFIC_DATEX_STATUS_SOURCE_UID,
    DIGITRAFFIC_DATEX_STATUS_URL,
    count_datex_records,
)
from commercial_backend.store import create_ingest_store

USER_AGENT = "woladen.de public FI Digitraffic ingester"


@dataclass(frozen=True)
class SourceSpec:
    key: str
    source_uid: str
    display_name: str
    url: str
    suffix: str
    source_kind: str
    task_kind: str
    publication_key: str
    record_key: str


SOURCES = {
    "statuses-datex": SourceSpec(
        key="statuses-datex",
        source_uid=DIGITRAFFIC_DATEX_STATUS_SOURCE_UID,
        display_name="FI Digitraffic AFIR DATEX II 3.6 charging-point statuses snapshot",
        url=DIGITRAFFIC_DATEX_STATUS_URL,
        suffix=".json",
        source_kind="afir_datex_3_6_status_snapshot",
        task_kind="parse_dynamic_payload",
        publication_key="egiEnergyInfrastructureStatusPublication",
        record_key="energyInfrastructureSiteStatus",
    ),
    "locations-datex": SourceSpec(
        key="locations-datex",
        source_uid=DIGITRAFFIC_DATEX_LOCATIONS_SOURCE_UID,
        display_name="FI Digitraffic AFIR DATEX II 3.6 charging-point locations snapshot",
        url=DIGITRAFFIC_DATEX_LOCATIONS_URL,
        suffix=".json",
        source_kind="afir_datex_3_6_locations_snapshot",
        task_kind="parse_static_payload",
        publication_key="egiEnergyInfrastructureTablePublication",
        record_key="energyInfrastructureTable",
    ),
}


def _temp_payload_path(config: AppConfig, spec: SourceSpec) -> Path:
    incoming_dir = config.raw_payload_dir / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=incoming_dir,
        prefix=f"fi-digitraffic-{spec.key}-",
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


def _count_archived_datex_records(store: Any, spec: SourceSpec, storage_uri: str) -> int:
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        payload = json.loads(payload_stream.read().decode("utf-8"))
    return count_datex_records(
        payload,
        publication_key=spec.publication_key,
        record_key=spec.record_key,
    )


def _selected_sources(value: str) -> list[SourceSpec]:
    if value == "all-datex":
        return [SOURCES["statuses-datex"], SOURCES["locations-datex"]]
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
    if parse_summary:
        summary["datex_record_count"] = _count_archived_datex_records(
            store,
            spec,
            receipt.storage_uri,
        )
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and archive FI Digitraffic AFIR DATEX payloads.")
    parser.add_argument(
        "--source",
        choices=("statuses-datex", "locations-datex", "all-datex"),
        default="statuses-datex",
        help="FI Digitraffic DATEX source payload to fetch.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument(
        "--no-parse-summary",
        action="store_true",
        help="Only archive raw bytes; skip lightweight DATEX record counts.",
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
