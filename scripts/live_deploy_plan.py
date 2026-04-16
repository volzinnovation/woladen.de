#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.deploy_plan import classify_deploy_plan, collect_changed_paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a runtime deployment plan for live.woladen.de")
    parser.add_argument("--candidate-root", required=True, help="Path to the extracted release bundle")
    parser.add_argument("--current-root", default="", help="Path to the currently deployed release")
    parser.add_argument("--shell", action="store_true", help="Print shell assignments instead of JSON")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    candidate_root = Path(args.candidate_root).expanduser()
    current_root = Path(args.current_root).expanduser() if str(args.current_root).strip() else None

    changed_paths = collect_changed_paths(current_root, candidate_root)
    plan = classify_deploy_plan(changed_paths)
    payload = {
        "changed_paths": list(plan.changed_paths),
        "restart_services": plan.restart_services,
        "bootstrap_runtime": plan.bootstrap_runtime,
        "reload_caddy": plan.reload_caddy,
        "daemon_reload": plan.daemon_reload,
        "refresh_venv": plan.refresh_venv,
        "reasons": list(plan.reasons),
        "summary": ",".join(plan.reasons) or "none",
    }

    if args.shell:
        print(f"DEPLOY_PLAN_RESTART_SERVICES={1 if plan.restart_services else 0}")
        print(f"DEPLOY_PLAN_BOOTSTRAP_RUNTIME={1 if plan.bootstrap_runtime else 0}")
        print(f"DEPLOY_PLAN_RELOAD_CADDY={1 if plan.reload_caddy else 0}")
        print(f"DEPLOY_PLAN_DAEMON_RELOAD={1 if plan.daemon_reload else 0}")
        print(f"DEPLOY_PLAN_REFRESH_VENV={1 if plan.refresh_venv else 0}")
        print(f"DEPLOY_PLAN_CHANGED_COUNT={len(plan.changed_paths)}")
        print(f"DEPLOY_PLAN_SUMMARY={payload['summary']}")
        return

    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

