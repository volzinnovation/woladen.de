#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
import ssl
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig
from backend.loaders import load_provider_targets
from backend.models import ProviderTarget

DEFAULT_STATUS_URL = "https://live.woladen.de/v1/status"
PROBLEM_LABELS = {
    "disabled_in_live_status": "disabled in live status",
    "no_mapped_live_data": "no mapped live data",
    "latest_update_error": "latest update error",
    "recent_observations_unmapped": "recent observations unmapped",
    "stale_last_received_update": "stale last received update",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_iso(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_minutes(value: Any, *, now: datetime) -> float | None:
    parsed = _parse_iso(value)
    if parsed is None:
        return None
    return round((now - parsed).total_seconds() / 60.0, 1)


def _first_recent_update(provider: dict[str, Any]) -> dict[str, Any]:
    recent_updates = provider.get("recent_updates")
    if not isinstance(recent_updates, list):
        return {}
    for item in recent_updates:
        if isinstance(item, dict):
            return item
    return {}


def _require_status_payload(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("expected_status_json_object")
    providers = payload.get("providers")
    if not isinstance(providers, list):
        raise ValueError("expected_status_provider_list")
    return payload


def _fetch_with_urllib(url: str, *, timeout_seconds: int, insecure: bool) -> dict[str, Any]:
    context = ssl._create_unverified_context() if insecure else ssl.create_default_context()
    request = urllib.request.Request(url, headers={"User-Agent": "woladen-live-status-analyzer/1.0"})
    with urllib.request.urlopen(request, timeout=timeout_seconds, context=context) as response:
        body = response.read().decode("utf-8")
    return _require_status_payload(json.loads(body))


def _fetch_with_curl(url: str, *, timeout_seconds: int, insecure: bool) -> dict[str, Any]:
    curl_path = shutil.which("curl")
    if not curl_path:
        raise RuntimeError("curl_not_found")
    command = [curl_path, "-fsSL", "--max-time", str(timeout_seconds)]
    if insecure:
        command.append("-k")
    command.append(url)
    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return _require_status_payload(json.loads(result.stdout))


def fetch_status_payload(url: str, *, timeout_seconds: int, insecure: bool) -> dict[str, Any]:
    try:
        return _fetch_with_urllib(url, timeout_seconds=timeout_seconds, insecure=insecure)
    except Exception as urllib_exc:
        try:
            return _fetch_with_curl(url, timeout_seconds=timeout_seconds, insecure=insecure)
        except Exception as curl_exc:
            raise RuntimeError(f"failed_to_fetch_status: urllib={urllib_exc}; curl={curl_exc}") from curl_exc


def load_status_payload(path: Path | None, *, url: str, timeout_seconds: int, insecure: bool) -> dict[str, Any]:
    if path is None:
        return fetch_status_payload(url, timeout_seconds=timeout_seconds, insecure=insecure)
    if str(path) == "-":
        return _require_status_payload(json.load(sys.stdin))
    return _require_status_payload(json.loads(path.read_text(encoding="utf-8")))


def load_expected_enabled_providers(config: AppConfig | None = None) -> list[ProviderTarget]:
    config = config or AppConfig()
    try:
        providers = load_provider_targets(
            config.provider_config_path,
            config.provider_override_path,
            config.subscription_registry_path,
        )
    except FileNotFoundError:
        return []
    return [provider for provider in providers if provider.enabled]


def analyze_provider(
    provider: dict[str, Any],
    *,
    expected_enabled_uids: set[str] | None,
    stale_after_minutes: int,
    now: datetime,
) -> dict[str, Any]:
    provider_uid = _text(provider.get("provider_uid"))
    recent_update = _first_recent_update(provider)
    expected_enabled = None if expected_enabled_uids is None else provider_uid in expected_enabled_uids
    live_enabled = bool(provider.get("enabled"))
    stations_with_live = _to_int(provider.get("stations_with_any_live_observation"))
    observation_rows = _to_int(provider.get("observation_rows"))
    last_received_update_at = _text(provider.get("last_received_update_at")) or None
    last_received_age_minutes = _age_minutes(last_received_update_at, now=now)
    latest_http_status = _to_optional_int(recent_update.get("http_status"))
    latest_result = (
        _text(recent_update.get("result"))
        or _text(provider.get("last_result"))
        or _text(provider.get("last_push_result"))
        or None
    )
    latest_observation_count = _to_int(recent_update.get("observation_count"))
    latest_mapped_observation_count = _to_int(recent_update.get("mapped_observation_count"))
    latest_dropped_observation_count = _to_int(recent_update.get("dropped_observation_count"))

    problems: list[str] = []
    if expected_enabled and not live_enabled:
        problems.append("disabled_in_live_status")
    if expected_enabled and stations_with_live == 0 and observation_rows == 0:
        problems.append("no_mapped_live_data")
    if latest_result == "error" or (latest_http_status is not None and latest_http_status >= 400):
        problems.append("latest_update_error")
    if expected_enabled and latest_observation_count > 0 and latest_mapped_observation_count == 0:
        problems.append("recent_observations_unmapped")
    if (
        expected_enabled
        and last_received_age_minutes is not None
        and stale_after_minutes > 0
        and last_received_age_minutes > stale_after_minutes
    ):
        problems.append("stale_last_received_update")

    return {
        "provider_uid": provider_uid,
        "display_name": _text(provider.get("display_name")),
        "publisher": _text(provider.get("publisher")),
        "expected_enabled": expected_enabled,
        "live_enabled": live_enabled,
        "stations_with_any_live_observation": stations_with_live,
        "observation_rows": observation_rows,
        "coverage_ratio": provider.get("coverage_ratio"),
        "last_result": _text(provider.get("last_result")) or None,
        "last_push_result": _text(provider.get("last_push_result")) or None,
        "last_received_update_at": last_received_update_at,
        "last_received_age_minutes": last_received_age_minutes,
        "last_source_update_at": _text(provider.get("last_source_update_at")) or None,
        "last_polled_at": _text(provider.get("last_polled_at")) or None,
        "last_push_received_at": _text(provider.get("last_push_received_at")) or None,
        "latest_http_status": latest_http_status,
        "latest_result": latest_result,
        "latest_observation_count": latest_observation_count,
        "latest_mapped_observation_count": latest_mapped_observation_count,
        "latest_dropped_observation_count": latest_dropped_observation_count,
        "problems": problems,
    }


def build_analysis(
    payload: dict[str, Any],
    *,
    expected_enabled_providers: list[ProviderTarget] | None = None,
    stale_after_minutes: int = 60,
    provider_filter: str = "",
    now: datetime | None = None,
    source: str = "",
) -> dict[str, Any]:
    status_payload = _require_status_payload(payload)
    now = now or datetime.now(timezone.utc)
    provider_filter = _text(provider_filter)
    expected_enabled_lookup = None
    if expected_enabled_providers is not None:
        expected_enabled_lookup = {
            provider.provider_uid
            for provider in expected_enabled_providers
            if not provider_filter or provider.provider_uid == provider_filter
        }

    providers = [provider for provider in status_payload.get("providers", []) if isinstance(provider, dict)]
    if provider_filter:
        providers = [provider for provider in providers if _text(provider.get("provider_uid")) == provider_filter]

    provider_analyses = [
        analyze_provider(
            provider,
            expected_enabled_uids=expected_enabled_lookup,
            stale_after_minutes=stale_after_minutes,
            now=now,
        )
        for provider in providers
    ]
    provider_analyses.sort(key=lambda item: (-len(item["problems"]), item["provider_uid"]))

    status_provider_uids = {provider["provider_uid"] for provider in provider_analyses}
    missing_expected_provider_uids: list[str] = []
    unexpected_provider_uids: list[str] = []
    if expected_enabled_lookup is not None:
        missing_expected_provider_uids = sorted(expected_enabled_lookup - status_provider_uids)
        unexpected_provider_uids = sorted(status_provider_uids - expected_enabled_lookup)

    problem_counts: dict[str, int] = {}
    for provider in provider_analyses:
        for problem in provider["problems"]:
            problem_counts[problem] = problem_counts.get(problem, 0) + 1

    return {
        "source": source or None,
        "bundle_station_count": _to_int(status_payload.get("bundle_station_count")),
        "stations_with_any_live_observation": _to_int(status_payload.get("stations_with_any_live_observation")),
        "stations_with_current_live_state": _to_int(status_payload.get("stations_with_current_live_state")),
        "coverage_ratio": float(status_payload.get("coverage_ratio") or 0.0),
        "last_received_update_at": _text(status_payload.get("last_received_update_at")) or None,
        "last_source_update_at": _text(status_payload.get("last_source_update_at")) or None,
        "providers_in_status_count": len(provider_analyses),
        "expected_enabled_provider_count": len(expected_enabled_lookup) if expected_enabled_lookup is not None else None,
        "present_expected_provider_count": (
            len(status_provider_uids & expected_enabled_lookup) if expected_enabled_lookup is not None else None
        ),
        "missing_expected_provider_uids": missing_expected_provider_uids,
        "unexpected_provider_uids": unexpected_provider_uids,
        "problem_counts": problem_counts,
        "providers_with_problems": [provider for provider in provider_analyses if provider["problems"]],
        "providers": provider_analyses,
    }


def _format_problem_list(problems: list[str]) -> str:
    return ", ".join(PROBLEM_LABELS.get(problem, problem) for problem in problems) or "none"


def _format_provider_line(provider: dict[str, Any]) -> str:
    last_received = provider["last_received_update_at"] or "n/a"
    latest_result = provider["latest_result"] or "n/a"
    latest_http_status = provider["latest_http_status"]
    observation_counts = (
        f"obs/mapped/dropped={provider['latest_observation_count']}/"
        f"{provider['latest_mapped_observation_count']}/{provider['latest_dropped_observation_count']}"
    )
    http_part = f"http={latest_http_status}" if latest_http_status is not None else "http=n/a"
    return (
        f"- {provider['provider_uid']}: {_format_problem_list(provider['problems'])}; "
        f"stations={provider['stations_with_any_live_observation']}, rows={provider['observation_rows']}, "
        f"latest_result={latest_result}, {http_part}, {observation_counts}, "
        f"last_received={last_received}"
    )


def format_human_analysis(analysis: dict[str, Any]) -> str:
    lines = [
        f"Source: {analysis['source'] or DEFAULT_STATUS_URL}",
        f"Bundle stations: {analysis['bundle_station_count']}",
        f"Stations with live observations: {analysis['stations_with_any_live_observation']} "
        f"({analysis['coverage_ratio'] * 100:.2f}%)",
        f"Stations with current live state: {analysis['stations_with_current_live_state']}",
        f"Providers in live status: {analysis['providers_in_status_count']}",
        f"Last received update: {analysis['last_received_update_at'] or 'n/a'}",
    ]

    if analysis["expected_enabled_provider_count"] is not None:
        lines.append(f"Expected enabled providers locally: {analysis['expected_enabled_provider_count']}")
        lines.append(f"Expected enabled providers present: {analysis['present_expected_provider_count']}")
        lines.append(
            "Missing expected providers: "
            + (", ".join(analysis["missing_expected_provider_uids"]) or "none")
        )
        lines.append(
            "Unexpected providers in live status: "
            + (", ".join(analysis["unexpected_provider_uids"]) or "none")
        )

    if analysis["problem_counts"]:
        lines.append("")
        lines.append("Problem counts:")
        for problem, count in sorted(analysis["problem_counts"].items()):
            lines.append(f"- {PROBLEM_LABELS.get(problem, problem)}: {count}")

    if analysis["providers_with_problems"]:
        lines.append("")
        lines.append("Providers with issues:")
        for provider in analysis["providers_with_problems"]:
            lines.append(_format_provider_line(provider))
    else:
        lines.append("")
        lines.append("Providers with issues: none")

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze the live status API and compare it with locally expected live providers",
    )
    parser.add_argument("--url", default=DEFAULT_STATUS_URL, help="Status endpoint URL to fetch")
    parser.add_argument(
        "--input",
        type=Path,
        default=None,
        help="Read status JSON from a local file instead of fetching. Use '-' for stdin.",
    )
    parser.add_argument("--provider", default="", help="Analyze only one provider UID")
    parser.add_argument(
        "--stale-after-minutes",
        type=int,
        default=60,
        help="Flag expected providers when their last received update is older than this threshold",
    )
    parser.add_argument("--timeout-seconds", type=int, default=20, help="Network timeout when fetching")
    parser.add_argument("--insecure", action="store_true", help="Disable TLS verification when fetching")
    parser.add_argument(
        "--skip-expected-providers",
        action="store_true",
        help="Skip the comparison against locally enabled providers",
    )
    parser.add_argument("--json", action="store_true", help="Print the analysis as JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = str(args.input) if args.input is not None else args.url
    payload = load_status_payload(
        args.input,
        url=args.url,
        timeout_seconds=args.timeout_seconds,
        insecure=args.insecure,
    )
    expected_enabled_providers = None if args.skip_expected_providers else load_expected_enabled_providers()
    analysis = build_analysis(
        payload,
        expected_enabled_providers=expected_enabled_providers,
        stale_after_minutes=args.stale_after_minutes,
        provider_filter=args.provider,
        source=source,
    )
    if args.json:
        print(json.dumps(analysis, ensure_ascii=False, indent=2))
        return
    print(format_human_analysis(analysis))


if __name__ == "__main__":
    main()
