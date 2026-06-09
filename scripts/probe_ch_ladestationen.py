#!/usr/bin/env python3
from __future__ import annotations

import json
import ssl
import urllib.request
import urllib.error
import gzip
import argparse
from collections import Counter
from pathlib import Path
import sys
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from commercial_backend.ch_ladestationen import (
    STATIC_DATA_URL,
    STATUS_DATA_URL,
    parse_static_payload,
    parse_status_payload,
)


def _build_summary(static_payload: dict[str, Any], status_payload: dict[str, Any]) -> dict[str, Any]:
    static_rows = parse_static_payload(static_payload)
    status_rows = parse_status_payload(status_payload)
    static_chargers = {row["charger_id"] for row in static_rows}
    status_chargers = {row["charger_id"] for row in status_rows}
    return {
        "static_url": STATIC_DATA_URL,
        "status_url": STATUS_DATA_URL,
        "static_record_count": len(static_rows),
        "status_record_count": len(status_rows),
        "static_without_status_count": len(static_chargers - status_chargers),
        "status_without_static_count": len(status_chargers - static_chargers),
        "status_values": dict(Counter(row["source_status"] for row in status_rows)),
        "availability_values": dict(Counter(row["availability_status"] for row in status_rows)),
    }


def _probe_alerts(
    summary: dict[str, Any],
    *,
    min_static_records: int,
    min_status_records: int,
    max_static_without_status: int,
    max_status_without_static: int,
) -> list[str]:
    alerts: list[str] = []
    if int(summary["static_record_count"]) < min_static_records:
        alerts.append(
            f"static_record_count_below_threshold:{summary['static_record_count']}<"
            f"{min_static_records}"
        )
    if int(summary["status_record_count"]) < min_status_records:
        alerts.append(
            f"status_record_count_below_threshold:{summary['status_record_count']}<"
            f"{min_status_records}"
        )
    if int(summary["static_without_status_count"]) > max_static_without_status:
        alerts.append(
            f"static_without_status_above_threshold:{summary['static_without_status_count']}>"
            f"{max_static_without_status}"
        )
    if int(summary["status_without_static_count"]) > max_status_without_static:
        alerts.append(
            f"status_without_static_above_threshold:{summary['status_without_static_count']}>"
            f"{max_status_without_static}"
        )
    return alerts


def _fetch_json(url: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "woladen.de CH source probe",
        },
    )
    def read_response(response) -> dict[str, Any]:
        raw = response.read()
        if response.headers.get("content-encoding", "").casefold() == "gzip":
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8"))

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return read_response(response)
    except (ssl.SSLError, urllib.error.URLError):
        # Some local Python builds do not know the system CA bundle. This probe
        # is diagnostic only; production ingestion must use normal TLS checks.
        context = ssl._create_unverified_context()
        with urllib.request.urlopen(request, timeout=30, context=context) as response:
            return read_response(response)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Probe CH Ladestationen static/status payload health.")
    parser.add_argument("--min-static-records", type=int, default=1)
    parser.add_argument("--min-status-records", type=int, default=1)
    parser.add_argument("--max-static-without-status", type=int, default=0)
    parser.add_argument("--max-status-without-static", type=int, default=0)
    args = parser.parse_args(argv)

    summary = _build_summary(_fetch_json(STATIC_DATA_URL), _fetch_json(STATUS_DATA_URL))
    alerts = _probe_alerts(
        summary,
        min_static_records=args.min_static_records,
        min_status_records=args.min_status_records,
        max_static_without_status=args.max_static_without_status,
        max_status_without_static=args.max_status_without_static,
    )
    summary["ok"] = not alerts
    summary["alerts"] = alerts
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 2 if alerts else 0


if __name__ == "__main__":
    raise SystemExit(main())
