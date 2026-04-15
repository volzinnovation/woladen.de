#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.config import AppConfig
from backend.service import IngestionService


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the woladen live ingester")
    parser.add_argument("--provider", default="", help="Only ingest a single provider UID")
    parser.add_argument("--max-providers", type=int, default=None, help="Only ingest the first N enabled providers")
    parser.add_argument("--bootstrap-only", action="store_true", help="Initialize the database and seed metadata")
    parser.add_argument("--loop", action="store_true", help="Continuously poll providers in round-robin order")
    parser.add_argument("--sleep-seconds", type=float, default=0.0, help="Sleep between loop iterations")
    return parser.parse_args()


def bootstrap_loop_if_missing(service: IngestionService) -> bool:
    db_missing = not service.config.db_path.exists()
    service.bootstrap()
    return db_missing


def main() -> None:
    args = parse_args()
    service = IngestionService(AppConfig())

    if args.bootstrap_only:
        service.bootstrap()
        print(json.dumps({"result": "bootstrapped"}))
        return

    if args.loop:
        bootstrap_loop_if_missing(service)
        while True:
            result = service.ingest_next_provider(bootstrap=False)
            if result is not None:
                print(json.dumps(result))
                if args.sleep_seconds > 0:
                    time.sleep(args.sleep_seconds)
                continue

            sleep_seconds = service.seconds_until_next_provider_due(bootstrap=False)
            if sleep_seconds is None:
                sleep_seconds = max(args.sleep_seconds, 1.0) if args.sleep_seconds > 0 else 1.0
                print(json.dumps({"result": "no_enabled_provider", "sleep_seconds": sleep_seconds}))
            else:
                sleep_seconds = min(sleep_seconds, float(service.config.poll_idle_sleep_max_seconds))
                if args.sleep_seconds > 0:
                    sleep_seconds = max(sleep_seconds, args.sleep_seconds)
                print(json.dumps({"result": "idle", "sleep_seconds": sleep_seconds}))
            time.sleep(sleep_seconds)
        return

    result = service.ingest_once(
        provider_uid=args.provider or None,
        max_providers=args.max_providers,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
