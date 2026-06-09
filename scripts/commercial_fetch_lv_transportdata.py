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

from commercial_backend.config import AppConfig  # noqa: E402
from commercial_backend.lv_transportdata import (  # noqa: E402
    COUNTRY_CODE,
    LV_ECO_MOVEMENT_STATIC_CARD_URL,
    LV_ECO_MOVEMENT_STATIC_DATASET_ID,
    LV_ECO_MOVEMENT_STATIC_SOURCE_UID,
    LV_ECO_MOVEMENT_STATUS_PRICE_CARD_URL,
    LV_ECO_MOVEMENT_STATUS_PRICE_DATASET_ID,
    LV_ECO_MOVEMENT_STATUS_PRICE_SOURCE_UID,
    LV_LVC_EV_CHARGING_STREAM_CARD_URL,
    LV_LVC_EV_CHARGING_STREAM_DATASET_ID,
    LV_LVC_EV_CHARGING_STREAM_SOURCE_UID,
    LV_TRANSPORTDATA_FILE_INFO_URL,
    LV_TRANSPORTDATA_REST_DOWNLOAD_URL,
    count_xml_records_from_binary_stream,
)
from commercial_backend.store import create_ingest_store  # noqa: E402

USER_AGENT = "woladen.de public LV Transportdata ingester"


@dataclass(frozen=True)
class SourceSpec:
    key: str
    source_uid: str
    display_name: str
    card_url: str
    dataset_id: str
    secret_env_names: tuple[str, ...]
    secret_filenames: tuple[str, ...]
    source_kind: str
    task_kind: str
    file_mode: str
    snapshot_file_id: str = ""
    suffix: str = ".xml"


SOURCES = {
    "eco-status-price": SourceSpec(
        key="eco-status-price",
        source_uid=LV_ECO_MOVEMENT_STATUS_PRICE_SOURCE_UID,
        display_name="LV Transportdata Eco-Movement status and price DATEX snapshot",
        card_url=LV_ECO_MOVEMENT_STATUS_PRICE_CARD_URL,
        dataset_id=LV_ECO_MOVEMENT_STATUS_PRICE_DATASET_ID,
        secret_env_names=(
            "TRANSPORTDATA_LV_ECO_MOVEMENT_STATUS_PRICE_API_KEY",
            "TRANSPORTDATA_LV_API_KEY_ECO_STATUS_PRICE",
        ),
        secret_filenames=("transportdata_lv_eco_movement_status_price_api_key.txt",),
        source_kind="lv_transportdata_datex_3_4_status_price",
        task_kind="parse_dynamic_payload",
        file_mode="file_info",
    ),
    "eco-static": SourceSpec(
        key="eco-static",
        source_uid=LV_ECO_MOVEMENT_STATIC_SOURCE_UID,
        display_name="LV Transportdata Eco-Movement static DATEX snapshot",
        card_url=LV_ECO_MOVEMENT_STATIC_CARD_URL,
        dataset_id=LV_ECO_MOVEMENT_STATIC_DATASET_ID,
        secret_env_names=(
            "TRANSPORTDATA_LV_ECO_MOVEMENT_STATIC_API_KEY",
            "TRANSPORTDATA_LV_API_KEY_ECO_STATIC",
        ),
        secret_filenames=("transportdata_lv_eco_movement_static_api_key.txt",),
        source_kind="lv_transportdata_datex_3_4_static_table",
        task_kind="parse_static_payload",
        file_mode="file_info",
    ),
    "lvc-stream": SourceSpec(
        key="lvc-stream",
        source_uid=LV_LVC_EV_CHARGING_STREAM_SOURCE_UID,
        display_name="LV Transportdata LVC electric charging infrastructure stream snapshot",
        card_url=LV_LVC_EV_CHARGING_STREAM_CARD_URL,
        dataset_id=LV_LVC_EV_CHARGING_STREAM_DATASET_ID,
        secret_env_names=(
            "TRANSPORTDATA_LV_LVC_EV_CHARGING_STREAM_API_KEY",
            "TRANSPORTDATA_LV_API_KEY_LVC_STREAM",
        ),
        secret_filenames=("transportdata_lv_lvc_ev_charging_stream_api_key.txt",),
        source_kind="lv_transportdata_datex_3_4_static_stream_snapshot",
        task_kind="parse_static_payload",
        file_mode="stream_snapshot",
        snapshot_file_id="1",
    ),
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


def _load_secret(env: dict[str, str], spec: SourceSpec) -> tuple[str, str]:
    for name in spec.secret_env_names:
        value = str(env.get(name, "")).strip()
        if value:
            return value, f"env:{name}"
    for filename in spec.secret_filenames:
        value = _read_secret_file(str(REPO_ROOT / "secret" / filename))
        if value:
            return value, f"secret/{filename}"
    raise RuntimeError(
        f"missing LV Transportdata API key for {spec.key}; set one of "
        f"{', '.join(spec.secret_env_names)} or secret/{spec.secret_filenames[0]}"
    )


def _request(
    url: str,
    *,
    timeout_seconds: int,
    headers: dict[str, str],
    data: bytes | None = None,
) -> Any:
    request = urllib.request.Request(url, data=data, headers=headers, method="POST" if data else "GET")
    try:
        return urllib.request.urlopen(request, timeout=timeout_seconds)
    except (ssl.SSLError, urllib.error.URLError):
        context = ssl._create_unverified_context()
        return urllib.request.urlopen(request, timeout=timeout_seconds, context=context)


def _file_info(spec: SourceSpec, *, api_key: str, timeout_seconds: int) -> dict[str, Any]:
    with _request(
        LV_TRANSPORTDATA_FILE_INFO_URL,
        timeout_seconds=timeout_seconds,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
            "x-api-key": api_key,
        },
    ) as response:
        payload = json.loads(response.read().decode("utf-8"))
    dataset_id = str(payload.get("dataset", "")).strip()
    if dataset_id and dataset_id != spec.dataset_id:
        raise RuntimeError(f"{spec.key} returned file info for dataset {dataset_id}, expected {spec.dataset_id}")
    files = payload.get("files")
    if not isinstance(files, list) or not files:
        raise RuntimeError(f"{spec.key} file info did not include files")
    first = files[0]
    file_id = str(first.get("file_id", "")).strip()
    if not file_id:
        raise RuntimeError(f"{spec.key} file info did not include a file_id")
    return {
        "file_id": file_id,
        "file_name": str(first.get("file_name", "")).strip(),
        "file_size": int(first.get("file_size") or 0),
    }


def _selected_file(spec: SourceSpec, *, api_key: str, timeout_seconds: int) -> dict[str, Any]:
    if spec.file_mode == "stream_snapshot":
        return {
            "file_id": spec.snapshot_file_id,
            "file_name": f"{spec.key}-snapshot.xml",
            "file_size": 0,
        }
    return _file_info(spec, api_key=api_key, timeout_seconds=timeout_seconds)


def _temp_payload_path(config: AppConfig, spec: SourceSpec) -> Path:
    incoming_dir = config.raw_payload_dir / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=incoming_dir,
        prefix=f"lv-transportdata-{spec.key}-",
        suffix=spec.suffix,
        delete=False,
    )
    handle.close()
    return Path(handle.name)


def _fetch_to_file(
    spec: SourceSpec,
    *,
    api_key: str,
    file_id: str,
    config: AppConfig,
    timeout_seconds: int,
) -> tuple[Path, str, int, dict[str, str]]:
    body = json.dumps({"file_id": str(file_id)}).encode("utf-8")
    with _request(
        LV_TRANSPORTDATA_REST_DOWNLOAD_URL,
        timeout_seconds=timeout_seconds,
        headers={
            "Accept": "application/xml,text/xml,*/*",
            "Content-Type": "application/json",
            "User-Agent": USER_AGENT,
            "x-api-key": api_key,
        },
        data=body,
    ) as response:
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
        if byte_length == 0:
            payload_path.unlink(missing_ok=True)
            raise RuntimeError(f"{spec.key} returned an empty payload for file_id {file_id}")
        return (
            payload_path,
            payload_sha256.hexdigest(),
            byte_length,
            {key.lower(): value for key, value in response.headers.items()},
        )


def _count_archived_xml_records(store: Any, storage_uri: str, content_encoding: str) -> dict[str, Any]:
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        return count_xml_records_from_binary_stream(payload_stream, content_encoding=content_encoding)


def _selected_sources(value: str) -> list[SourceSpec]:
    if value == "all":
        return [SOURCES["eco-static"], SOURCES["eco-status-price"], SOURCES["lvc-stream"]]
    return [SOURCES[value]]


def _archive_source(
    *,
    spec: SourceSpec,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
) -> dict[str, Any]:
    api_key, api_key_source = _load_secret(os.environ, spec)
    selected_file = _selected_file(spec, api_key=api_key, timeout_seconds=timeout_seconds)
    temp_path, payload_sha256, byte_length, headers = _fetch_to_file(
        spec,
        api_key=api_key,
        file_id=selected_file["file_id"],
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
        request_path=LV_TRANSPORTDATA_REST_DOWNLOAD_URL,
        request_query=f"dataset_id={spec.dataset_id};file_id={selected_file['file_id']}",
        request_headers={
            "accept": "application/xml,text/xml,*/*",
            "content-type": "application/json",
            "user-agent": USER_AGENT,
            "x-api-key": "<redacted>",
            "source-card-url": spec.card_url,
        },
        source_kind=spec.source_kind,
        display_name=spec.display_name,
        task_kind=spec.task_kind,
        remote_addr="",
    )
    summary: dict[str, Any] = {
        "source": spec.key,
        "source_uid": spec.source_uid,
        "dataset_id": spec.dataset_id,
        "card_url": spec.card_url,
        "task_kind": spec.task_kind,
        "api_key_source": api_key_source,
        "file_id": selected_file["file_id"],
        "file_name": selected_file["file_name"],
        "reported_file_size": selected_file["file_size"],
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
    parser = argparse.ArgumentParser(description="Fetch and archive LV Transportdata DATEX payloads.")
    parser.add_argument(
        "--source",
        choices=("eco-static", "eco-status-price", "lvc-stream", "all"),
        default="all",
        help="LV Transportdata source payload to fetch.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=300)
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
