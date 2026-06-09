#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import ssl
import sys
import tempfile
import time
import urllib.parse
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from commercial_backend.config import AppConfig
from commercial_backend.eu_public_static import (
    LT_LEGACY_EV_LOCATIONS_URL,
    NOBIL_DATADUMP_URL,
    PUBLIC_STATIC_SOURCES,
    PublicStaticSourceSpec,
    iter_cy_rows_from_binary_stream,
    iter_cz_rows_from_binary_stream,
    iter_es_rows_from_binary_stream,
    iter_gr_dynamic_rows_from_binary_stream,
    iter_gr_rows_from_binary_stream,
    iter_lt_dynamic_rows_from_binary_stream,
    iter_lt_rows_from_binary_stream,
    iter_lu_rows_from_binary_stream,
    iter_mt_rows_from_binary_stream,
    iter_no_nobil_rows_from_binary_stream,
    iter_se_nobil_rows_from_binary_stream,
)
from commercial_backend.store import create_ingest_store

USER_AGENT = "woladen.de public EU public static ingester"
LT_VIA_LIETUVA_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
LT_VIA_LIETUVA_STATIC_FALLBACK_ENV = "LT_VIA_LIETUVA_STATIC_FALLBACK_PATH"
LT_VIA_LIETUVA_STATIC_FALLBACK_PATHS = (
    REPO_ROOT / "data" / "lt-EnergyInfrastructureTablePublication.xml",
)


PARSERS: dict[str, Callable[..., Any]] = {
    "cy": iter_cy_rows_from_binary_stream,
    "cz": iter_cz_rows_from_binary_stream,
    "es": iter_es_rows_from_binary_stream,
    "gr": iter_gr_rows_from_binary_stream,
    "gr-dynamic": iter_gr_dynamic_rows_from_binary_stream,
    "lt": iter_lt_rows_from_binary_stream,
    "lt-dynamic": iter_lt_dynamic_rows_from_binary_stream,
    "lu": iter_lu_rows_from_binary_stream,
    "mt": iter_mt_rows_from_binary_stream,
    "no-nobil": iter_no_nobil_rows_from_binary_stream,
    "se-nobil": iter_se_nobil_rows_from_binary_stream,
}

NOBIL_COUNTRYCODE_BY_SOURCE_KEY = {
    "no-nobil": "NOR",
    "se-nobil": "SWE",
}


@dataclass(frozen=True)
class FetchResult:
    payload_path: Path
    payload_sha256: str
    byte_length: int
    content_type: str
    content_encoding: str
    final_url: str


class CloudflareChallengeError(RuntimeError):
    """Raised when a public source returns a Cloudflare challenge page."""


def _temp_payload_path(config: AppConfig, spec: PublicStaticSourceSpec) -> Path:
    incoming_dir = config.raw_payload_dir / "_incoming"
    incoming_dir.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        dir=incoming_dir,
        prefix=f"eu-public-static-{spec.key}-",
        suffix=spec.suffix,
        delete=False,
    )
    handle.close()
    return Path(handle.name)


def _urlopen(request: urllib.request.Request, *, timeout_seconds: int):
    try:
        return urllib.request.urlopen(request, timeout=timeout_seconds)
    except urllib.error.HTTPError:
        raise
    except (ssl.SSLError, urllib.error.URLError):
        context = ssl._create_unverified_context()
        return urllib.request.urlopen(request, timeout=timeout_seconds, context=context)


def _read_first_existing_secret(paths: Sequence[Path]) -> str:
    for path in paths:
        try:
            value = path.expanduser().read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            continue
        if value:
            return value
    return ""


def _is_lt_via_lietuva_source(spec: PublicStaticSourceSpec) -> bool:
    return urllib.parse.urlparse(spec.url).netloc.casefold() == "ev.vialietuva.lt"


def _lt_via_lietuva_user_agent() -> str:
    return os.environ.get("LT_VIA_LIETUVA_USER_AGENT", "").strip() or LT_VIA_LIETUVA_BROWSER_USER_AGENT


def _lt_via_lietuva_cookie_header() -> str:
    env_value = os.environ.get("LT_VIA_LIETUVA_COOKIE", "").strip()
    if env_value:
        return env_value if "=" in env_value else f"cf_clearance={env_value}"
    cookie_file = os.environ.get("LT_VIA_LIETUVA_COOKIE_FILE", "").strip()
    secret_paths = [Path(cookie_file)] if cookie_file else []
    secret_paths.extend(
        [
            REPO_ROOT / "secret" / "lt_vialietuva_cookie.txt",
            Path("/run/secrets/woladen-local/lt_vialietuva_cookie.txt"),
        ]
    )
    file_value = _read_first_existing_secret(secret_paths)
    if not file_value:
        return ""
    return file_value if "=" in file_value else f"cf_clearance={file_value}"


def _source_request_headers(spec: PublicStaticSourceSpec) -> dict[str, str]:
    headers = {
        "Accept": spec.accept,
        "User-Agent": USER_AGENT,
    }
    if _is_lt_via_lietuva_source(spec):
        headers.update(
            {
                "User-Agent": _lt_via_lietuva_user_agent(),
                "Accept-Language": "en-US,en;q=0.9,lt;q=0.8",
                "Referer": "https://ev.vialietuva.lt/en/data-provision",
            }
        )
        cookie_header = _lt_via_lietuva_cookie_header()
        if cookie_header:
            headers["Cookie"] = cookie_header
    return headers


def _cloudflare_challenge_message(
    *,
    source_key: str,
    url: str,
    status_code: int,
    clearance_configured: bool,
) -> str:
    state = "clearance_cookie_configured" if clearance_configured else "clearance_cookie_missing"
    return (
        f"cloudflare_challenge:{source_key}:{status_code}:{state}: "
        f"{url}; browser access works after Cloudflare clearance, but unattended "
        "backend fetches need Via Lietuva/provider allowlisting or an official "
        "non-challenged backend distribution route. Standard Playwright "
        "automation did not generate a usable clearance locally. If Via Lietuva "
        "provides an approved clearance mechanism, set LT_VIA_LIETUVA_COOKIE_FILE "
        "to an ignored secret file, or LT_VIA_LIETUVA_COOKIE in the runtime "
        "environment; set LT_VIA_LIETUVA_USER_AGENT too if the clearance is "
        "user-agent-bound."
    )


def _is_cloudflare_challenge(headers: Any, body: bytes) -> bool:
    if str(headers.get("cf-mitigated", "")).casefold() == "challenge":
        return True
    body_sample = body[:16_384].decode("utf-8", errors="replace").casefold()
    return (
        "challenges.cloudflare.com" in body_sample
        or "just a moment..." in body_sample
        or "cf-chl" in body_sample
    )


def _raise_cloudflare_challenge_if_present(
    *,
    source_key: str,
    url: str,
    request_headers: dict[str, str],
    error: urllib.error.HTTPError,
) -> None:
    body = error.read(16_384)
    if not _is_cloudflare_challenge(error.headers, body):
        return
    raise CloudflareChallengeError(
        _cloudflare_challenge_message(
            source_key=source_key,
            url=url,
            status_code=error.code,
            clearance_configured=bool(request_headers.get("Cookie")),
        )
    ) from error


def _nobil_api_key() -> str:
    for env_name in ("NOBIL_API_KEY", "NO_SE_NOBIL_KEY", "NO_SE_NOBIL_ENOVA_KEY"):
        value = os.environ.get(env_name, "").strip()
        if value:
            return value
    secret_paths: list[Path] = []
    for env_name in ("NOBIL_API_KEY_FILE", "NO_SE_NOBIL_KEY_FILE", "NO_SE_NOBIL_ENOVA_KEY_FILE"):
        value = os.environ.get(env_name, "").strip()
        if value:
            secret_paths.append(Path(value))
    secret_paths.extend(
        [
            REPO_ROOT / "secret" / "no_se_nobil_enova_key.txt",
            REPO_ROOT / "no_se_nobil_enova_key.txt",
            Path("/run/secrets/woladen-local/no_se_nobil_enova_key.txt"),
            Path("/run/secrets/no_se_nobil_enova_key.txt"),
        ]
    )
    key = _read_first_existing_secret(secret_paths)
    if not key:
        raise RuntimeError(
            "missing_nobil_api_key:set NOBIL_API_KEY, NO_SE_NOBIL_ENOVA_KEY, "
            "NO_SE_NOBIL_KEY, NOBIL_API_KEY_FILE, or provide secret/no_se_nobil_enova_key.txt"
        )
    return key


def _nobil_query(country_code: str, *, redacted: bool = True) -> str:
    params = {
        "countrycode": country_code,
        "format": "json",
        "apiversion": "3",
        "apikey": "<redacted>" if redacted else _nobil_api_key(),
    }
    return urllib.parse.urlencode(params)


def _write_bytes_payload(
    *,
    config: AppConfig,
    spec: PublicStaticSourceSpec,
    payload_bytes: bytes,
    content_type: str,
    content_encoding: str = "",
    final_url: str = "",
) -> FetchResult:
    payload_path = _temp_payload_path(config, spec)
    payload_path.write_bytes(payload_bytes)
    return FetchResult(
        payload_path=payload_path,
        payload_sha256=hashlib.sha256(payload_bytes).hexdigest(),
        byte_length=len(payload_bytes),
        content_type=content_type,
        content_encoding=content_encoding,
        final_url=final_url or spec.url,
    )


def _write_payload_file_from_path(
    *,
    config: AppConfig,
    spec: PublicStaticSourceSpec,
    source_path: Path,
    content_type: str,
    content_encoding: str = "",
    final_url: str = "",
) -> FetchResult:
    payload_sha256 = hashlib.sha256()
    byte_length = 0
    payload_path = _temp_payload_path(config, spec)
    with source_path.open("rb") as source, payload_path.open("wb") as output:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                break
            output.write(chunk)
            payload_sha256.update(chunk)
            byte_length += len(chunk)
    return FetchResult(
        payload_path=payload_path,
        payload_sha256=payload_sha256.hexdigest(),
        byte_length=byte_length,
        content_type=content_type,
        content_encoding=content_encoding,
        final_url=final_url or source_path.resolve().as_uri(),
    )


def _lt_via_lietuva_static_fallback_path() -> Path | None:
    configured = os.environ.get(LT_VIA_LIETUVA_STATIC_FALLBACK_ENV, "").strip()
    candidates = [Path(configured)] if configured else []
    candidates.extend(LT_VIA_LIETUVA_STATIC_FALLBACK_PATHS)
    for path in candidates:
        if path.expanduser().is_file():
            return path.expanduser()
    return None


def _fetch_lt_datex_static_with_fallback(
    spec: PublicStaticSourceSpec,
    *,
    config: AppConfig,
    timeout_seconds: int,
) -> FetchResult:
    try:
        return _fetch_to_file(spec, config=config, timeout_seconds=timeout_seconds)
    except CloudflareChallengeError as exc:
        fallback_path = _lt_via_lietuva_static_fallback_path()
        if fallback_path is None:
            raise
        print(
            "Via Lietuva LT DATEX static fetch hit the known Cloudflare challenge; "
            f"using tracked static fallback {fallback_path}. {exc}",
            file=sys.stderr,
        )
        return _write_payload_file_from_path(
            config=config,
            spec=spec,
            source_path=fallback_path,
            content_type=spec.content_type,
        )


def _fetch_nobil_datadump(
    spec: PublicStaticSourceSpec,
    *,
    config: AppConfig,
    timeout_seconds: int,
) -> FetchResult:
    country_code = NOBIL_COUNTRYCODE_BY_SOURCE_KEY[spec.key]
    request_url = f"{NOBIL_DATADUMP_URL}?{_nobil_query(country_code, redacted=False)}"
    redacted_url = f"{NOBIL_DATADUMP_URL}?{_nobil_query(country_code, redacted=True)}"
    request = urllib.request.Request(request_url, headers=_source_request_headers(spec))
    with _urlopen(request, timeout_seconds=timeout_seconds) as response:
        payload_sha256 = hashlib.sha256()
        byte_length = 0
        payload_path = _temp_payload_path(config, spec)
        with payload_path.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
                payload_sha256.update(chunk)
                byte_length += len(chunk)
        return FetchResult(
            payload_path=payload_path,
            payload_sha256=payload_sha256.hexdigest(),
            byte_length=byte_length,
            content_type=response.headers.get("content-type", spec.content_type),
            content_encoding=response.headers.get("content-encoding", ""),
            final_url=redacted_url,
        )


def _fetch_to_file(
    spec: PublicStaticSourceSpec,
    *,
    config: AppConfig,
    timeout_seconds: int,
) -> FetchResult:
    request_headers = _source_request_headers(spec)
    request = urllib.request.Request(spec.url, headers=request_headers)
    try:
        response_handle = _urlopen(request, timeout_seconds=timeout_seconds)
    except urllib.error.HTTPError as exc:
        _raise_cloudflare_challenge_if_present(
            source_key=spec.key,
            url=spec.url,
            request_headers=request_headers,
            error=exc,
        )
        raise
    with response_handle as response:
        payload_sha256 = hashlib.sha256()
        byte_length = 0
        payload_path = _temp_payload_path(config, spec)
        with payload_path.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
                payload_sha256.update(chunk)
                byte_length += len(chunk)
        return FetchResult(
            payload_path=payload_path,
            payload_sha256=payload_sha256.hexdigest(),
            byte_length=byte_length,
            content_type=response.headers.get("content-type", spec.content_type),
            content_encoding=response.headers.get("content-encoding", ""),
            final_url=response.geturl(),
        )


def _json_request(
    url: str,
    *,
    timeout_seconds: int,
    data: dict[str, Any] | None = None,
    referer: str = "",
    max_retries: int = 5,
) -> tuple[dict[str, Any], str]:
    payload = None if data is None else json.dumps(data, ensure_ascii=False).encode("utf-8")
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json; charset=UTF-8",
        "User-Agent": USER_AGENT,
    }
    if referer:
        headers["Referer"] = referer
        headers["X-Requested-With"] = "XMLHttpRequest"
    for attempt in range(max_retries + 1):
        request = urllib.request.Request(url, data=payload, headers=headers)
        try:
            with _urlopen(request, timeout_seconds=timeout_seconds) as response:
                raw = response.read().decode("utf-8", errors="replace")
                first_json = raw.find("{")
                if first_json > 0:
                    raw = raw[first_json:]
                return json.loads(raw), response.geturl()
        except urllib.error.HTTPError as exc:
            _raise_cloudflare_challenge_if_present(
                source_key="json",
                url=url,
                request_headers=headers,
                error=exc,
            )
            if exc.code != 429 or attempt >= max_retries:
                raise
            retry_after = str(exc.headers.get("Retry-After") or "").strip()
            sleep_seconds = float(retry_after) if retry_after.isdigit() else min(60.0, 5.0 * (attempt + 1))
            time.sleep(sleep_seconds)
    raise RuntimeError("json_request_unreachable")


def _fetch_lt_all_pages(
    *,
    config: AppConfig,
    spec: PublicStaticSourceSpec,
    timeout_seconds: int,
) -> FetchResult:
    pages: list[dict[str, Any]] = []
    offset = 0
    while True:
        page_url = f"{LT_LEGACY_EV_LOCATIONS_URL}?offset={offset}"
        response, _final_url = _json_request(page_url, timeout_seconds=timeout_seconds)
        rows = response.get("rows") if isinstance(response, dict) else []
        if not isinstance(rows, list):
            rows = []
        pages.append({"offset": offset, "url": page_url, "response": response})
        if len(rows) < 100:
            break
        offset += 100
    payload_bytes = json.dumps(
        {"source_url": LT_LEGACY_EV_LOCATIONS_URL, "page_count": len(pages), "pages": pages},
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")
    return _write_bytes_payload(
        config=config,
        spec=spec,
        payload_bytes=payload_bytes,
        content_type="application/json",
        final_url=LT_LEGACY_EV_LOCATIONS_URL,
    )


def _fetch_source(
    *,
    config: AppConfig,
    spec: PublicStaticSourceSpec,
    timeout_seconds: int,
) -> FetchResult:
    if spec.key == "lt":
        return _fetch_lt_datex_static_with_fallback(spec, config=config, timeout_seconds=timeout_seconds)
    if spec.url == LT_LEGACY_EV_LOCATIONS_URL:
        return _fetch_lt_all_pages(config=config, spec=spec, timeout_seconds=timeout_seconds)
    if spec.key in NOBIL_COUNTRYCODE_BY_SOURCE_KEY:
        return _fetch_nobil_datadump(spec, config=config, timeout_seconds=timeout_seconds)
    return _fetch_to_file(spec, config=config, timeout_seconds=timeout_seconds)


def _parse_summary(store: Any, spec: PublicStaticSourceSpec, storage_uri: str) -> dict[str, Any]:
    parser = PARSERS.get(spec.key)
    if parser is None:
        return {}
    with store.open_storage_uri_reader(storage_uri) as payload_stream:
        rows = list(parser(payload_stream, content_encoding=""))
    row_count_key = "parsed_dynamic_row_count" if spec.task_kind == "parse_dynamic_payload" else "parsed_static_row_count"
    return {
        row_count_key: len(rows),
        "parsed_station_count": len({str(row.get("station_id")) for row in rows if row.get("station_id")}),
        "parsed_charger_count": len({str(row.get("charger_id")) for row in rows if row.get("charger_id")}),
    }


def _quarantine_invalid_payload(
    *,
    config: AppConfig,
    spec: PublicStaticSourceSpec,
    fetched: FetchResult,
    reason: str,
) -> Path:
    now = datetime.now(timezone.utc)
    target_dir = (
        config.raw_payload_dir
        / "_invalid"
        / spec.country_code
        / spec.source_uid
        / f"{now:%Y}"
        / f"{now:%m}"
        / f"{now:%d}"
    )
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{now:%Y%m%dT%H%M%SZ}-{fetched.payload_sha256[:16]}{spec.suffix}"
    shutil.move(str(fetched.payload_path), target_path)
    manifest_row = {
        "received_at": now.isoformat(),
        "country_code": spec.country_code,
        "source": spec.key,
        "source_uid": spec.source_uid,
        "url": spec.url,
        "final_url": fetched.final_url,
        "payload_sha256": fetched.payload_sha256,
        "byte_length": fetched.byte_length,
        "reason": reason,
        "path": str(target_path),
    }
    with (target_dir / "invalid_payloads.ndjson").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(manifest_row, sort_keys=True) + "\n")
    return target_path


def _validate_gr_payload(
    *,
    config: AppConfig,
    spec: PublicStaticSourceSpec,
    fetched: FetchResult,
) -> dict[str, Any]:
    if spec.key not in {"gr", "gr-dynamic"}:
        return {}
    parser = PARSERS[spec.key]
    try:
        with fetched.payload_path.open("rb") as handle:
            rows = list(parser(handle, content_encoding=fetched.content_encoding))
    except Exception as exc:
        quarantine_path = _quarantine_invalid_payload(
            config=config,
            spec=spec,
            fetched=fetched,
            reason=str(exc),
        )
        raise RuntimeError(
            f"invalid_eu_public_payload:{spec.key}:{exc}; quarantined={quarantine_path}"
        ) from exc
    row_count_key = "parsed_dynamic_row_count" if spec.task_kind == "parse_dynamic_payload" else "parsed_static_row_count"
    return {
        row_count_key: len(rows),
        "parsed_station_count": len({str(row.get("station_id")) for row in rows if row.get("station_id")}),
        "parsed_charger_count": len({str(row.get("charger_id")) for row in rows if row.get("charger_id")}),
    }


def _archive_source(
    *,
    spec: PublicStaticSourceSpec,
    config: AppConfig,
    store: Any,
    timeout_seconds: int,
    parse_summary: bool,
) -> dict[str, Any]:
    fetched = _fetch_source(
        config=config,
        spec=spec,
        timeout_seconds=timeout_seconds,
    )
    validation_summary = _validate_gr_payload(config=config, spec=spec, fetched=fetched)
    receipt = store.record_pull_payload_file(
        country_code=spec.country_code,
        source_uid=spec.source_uid,
        payload_path=fetched.payload_path,
        payload_sha256=fetched.payload_sha256,
        byte_length=fetched.byte_length,
        content_type=fetched.content_type,
        content_encoding=fetched.content_encoding,
        request_path=spec.url,
        request_query=_nobil_query(NOBIL_COUNTRYCODE_BY_SOURCE_KEY[spec.key], redacted=True)
        if spec.key in NOBIL_COUNTRYCODE_BY_SOURCE_KEY
        else "",
        request_headers={
            "user-agent": USER_AGENT,
            "source-url": spec.url,
            "final-url": fetched.final_url,
        },
        source_kind=spec.source_kind,
        display_name=spec.display_name,
        task_kind=spec.task_kind,
        remote_addr="",
    )
    summary: dict[str, Any] = {
        "country_code": spec.country_code,
        "source": spec.key,
        "source_uid": spec.source_uid,
        "url": spec.url,
        "final_url": fetched.final_url,
        "payload_sha256": fetched.payload_sha256,
        "byte_length": fetched.byte_length,
        "duplicate_payload": receipt.duplicate_payload,
        "queued": receipt.queued,
        "storage_uri": receipt.storage_uri,
    }
    if parse_summary:
        summary.update(validation_summary or _parse_summary(store, spec, receipt.storage_uri))
    return summary


def _selected_sources(value: str) -> list[PublicStaticSourceSpec]:
    if value == "all-new-direct":
        keys = ("cy", "cz", "es", "gr", "lt", "lu", "mt")
    elif value == "all-nobil":
        keys = ("no-nobil", "se-nobil")
    else:
        keys = tuple(part.strip().lower() for part in value.split(",") if part.strip())
    specs: list[PublicStaticSourceSpec] = []
    for key in keys:
        if key not in PUBLIC_STATIC_SOURCES:
            raise KeyError(f"unknown_source:{key}")
        specs.append(PUBLIC_STATIC_SOURCES[key])
    return specs


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch and archive EU/NO/SE public static charging sources.")
    parser.add_argument(
        "--source",
        default="all-new-direct",
        help="Source key list (`cy,cz,es,no-nobil,se-nobil,...`), `all-new-direct`, or `all-nobil`.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument(
        "--no-parse-summary",
        action="store_true",
        help="Only archive raw bytes; skip parser row-count summaries.",
    )
    args = parser.parse_args(argv)

    config = AppConfig()
    store = create_ingest_store(config)
    store.initialize()
    try:
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
    except CloudflareChallengeError as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error": "cloudflare_challenge",
                    "detail": str(exc),
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps({"ok": True, "sources": summaries}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
