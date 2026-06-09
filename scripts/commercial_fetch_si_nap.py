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
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from commercial_backend.config import AppConfig  # noqa: E402
from commercial_backend.si_nap import (  # noqa: E402
    COUNTRY_CODE,
    SI_NAP_STATUS_SOURCE_UID,
    SI_NAP_STATUS_URL,
    SI_NAP_TABLE_SOURCE_UID,
    SI_NAP_TABLE_URL,
    SI_NAP_TOKEN_URL,
    count_xml_records_from_binary_stream,
)
from commercial_backend.store import create_ingest_store  # noqa: E402

USER_AGENT = "woladen.de public SI NAP ingester"
DEFAULT_SI_NAP_EMAIL = "raphael.volz@hs-pforzheim.de"
DEFAULT_SI_NAP_EMAIL_PATH = REPO_ROOT / "secret" / "si_nap_email.txt"
DEFAULT_SI_NAP_PASSWORD_PATH = REPO_ROOT / "secret" / "si_nap_password.txt"


@dataclass(frozen=True)
class SourceSpec:
    key: str
    source_uid: str
    display_name: str
    url: str
    suffix: str
    source_kind: str
    task_kind: str


SOURCES = {
    "table": SourceSpec(
        key="table",
        source_uid=SI_NAP_TABLE_SOURCE_UID,
        display_name="SI NAP Prometej IDACS Energy Infrastructure Table Publication",
        url=SI_NAP_TABLE_URL,
        suffix=".xml",
        source_kind="nap_b2b_datex_3_6_static_table",
        task_kind="parse_static_payload",
    ),
    "status": SourceSpec(
        key="status",
        source_uid=SI_NAP_STATUS_SOURCE_UID,
        display_name="SI NAP Prometej IDACS Energy Infrastructure Status Publication",
        url=SI_NAP_STATUS_URL,
        suffix=".xml",
        source_kind="nap_b2b_datex_3_6_dynamic_status",
        task_kind="parse_dynamic_payload",
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
        value_name="SI_NAP_EMAIL",
        file_name="SI_NAP_EMAIL_FILE",
        default_path=DEFAULT_SI_NAP_EMAIL_PATH,
        default_value=DEFAULT_SI_NAP_EMAIL,
    )
    password = _credential_value(
        env=effective_env,
        value_name="SI_NAP_PASSWORD",
        file_name="SI_NAP_PASSWORD_FILE",
        default_path=DEFAULT_SI_NAP_PASSWORD_PATH,
    )
    if not email:
        raise RuntimeError("SI NAP email missing. Set SI_NAP_EMAIL or SI_NAP_EMAIL_FILE.")
    if not password:
        raise RuntimeError("SI NAP password missing. Set SI_NAP_PASSWORD, SI_NAP_PASSWORD_FILE, or secret/si_nap_password.txt.")
    return email, password


def _request_token(*, username: str, password: str, timeout_seconds: int) -> dict[str, Any]:
    body = urllib.parse.urlencode(
        {
            "grant_type": "password",
            "username": username,
            "password": password,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        SI_NAP_TOKEN_URL,
        data=body,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except (ssl.SSLError, urllib.error.URLError):
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
            return json.loads(response.read().decode("utf-8"))


def _temp_payload_path(config: AppConfig, spec: SourceSpec) -> Path:
    incoming_dir = config.raw_payload_dir / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=incoming_dir,
        prefix=f"si-nap-{spec.key}-",
        suffix=spec.suffix,
        delete=False,
    )
    handle.close()
    return Path(handle.name)


def _fetch_to_file(
    spec: SourceSpec,
    *,
    access_token: str,
    config: AppConfig,
    timeout_seconds: int,
) -> tuple[Path, str, int, dict[str, str]]:
    request = urllib.request.Request(
        spec.url,
        headers={
            "Accept": "application/xml,text/xml,*/*",
            "Authorization": f"bearer {access_token}",
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


def _count_archived_xml_records(store: Any, storage_uri: str, content_encoding: str) -> dict[str, Any]:
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        return count_xml_records_from_binary_stream(payload_stream, content_encoding=content_encoding)


def _selected_sources(value: str) -> list[SourceSpec]:
    if value == "all":
        return [SOURCES["table"], SOURCES["status"]]
    return [SOURCES[value]]


def _archive_source(
    *,
    spec: SourceSpec,
    access_token: str,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
) -> dict[str, Any]:
    temp_path, payload_sha256, byte_length, headers = _fetch_to_file(
        spec,
        access_token=access_token,
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
            "accept": "application/xml,text/xml,*/*",
            "authorization": "bearer <redacted>",
            "user-agent": USER_AGENT,
            "source-url": spec.url,
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
        "task_kind": spec.task_kind,
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
    parser = argparse.ArgumentParser(description="Fetch and archive SI NAP Prometej B2B DATEX payloads.")
    parser.add_argument(
        "--source",
        choices=("table", "status", "all"),
        default="status",
        help="SI NAP DATEX source payload to fetch.",
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
        raise RuntimeError("SI NAP OAuth response did not include access_token")

    config = AppConfig()
    store = create_ingest_store(config)
    store.initialize()
    summaries = [
        _archive_source(
            spec=spec,
            access_token=access_token,
            config=config,
            store=store,
            timeout_seconds=args.timeout_seconds,
            parse_summary=not args.no_parse_summary,
        )
        for spec in _selected_sources(args.source)
    ]
    print(
        json.dumps(
            {
                "ok": True,
                "country_code": COUNTRY_CODE,
                "token_type": token_payload.get("token_type", ""),
                "expires_in": token_payload.get("expires_in", ""),
                "sources": summaries,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
