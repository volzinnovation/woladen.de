from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import filecmp

IGNORED_COMPARE_PATHS = {
    "LICENSE",
    "deploy/ionos/README.md",
    "deploy/ionos/bootstrap-host.sh",
    "deploy/ionos/build-release.sh",
    "deploy/ionos/deploy-release.sh",
    "deploy/ionos/install-on-vps.sh",
    "deploy/ionos/push-secrets-and-start.sh",
    "deploy/ionos/woladen-live.env.example",
    "docs/live-api-mvp.md",
    "release.json",
}
RESTART_SERVICE_PATHS = {
    "requirements-live.txt",
    "scripts/live_api.py",
    "scripts/live_ingester.py",
}
BOOTSTRAP_RUNTIME_PATHS = {
    "data/chargers_fast.csv",
    "data/mobilithek_afir_provider_configs.json",
    "data/mobilithek_afir_static_matches.csv",
    "data/live_provider_overrides.json",
}
RELOAD_CADDY_PATHS = {"deploy/ionos/Caddyfile"}
DAEMON_RELOAD_PATHS = {
    "deploy/ionos/woladen-live-api.service",
    "deploy/ionos/woladen-live-ingester.service",
}


@dataclass(frozen=True)
class DeployPlan:
    changed_paths: tuple[str, ...]
    restart_services: bool
    bootstrap_runtime: bool
    reload_caddy: bool
    daemon_reload: bool
    refresh_venv: bool
    reasons: tuple[str, ...]


def _iter_files(root: Path) -> set[str]:
    return {
        path.relative_to(root).as_posix()
        for path in root.rglob("*")
        if path.is_file()
    }


def collect_changed_paths(current_root: Path | None, candidate_root: Path) -> tuple[str, ...]:
    candidate_root = candidate_root.resolve()
    current_root = current_root.resolve() if current_root else None

    candidate_files = _iter_files(candidate_root)
    current_files = _iter_files(current_root) if current_root and current_root.exists() else set()
    changed_paths: list[str] = []

    for relative_path in sorted(candidate_files | current_files):
        if relative_path in IGNORED_COMPARE_PATHS:
            continue
        candidate_path = candidate_root / relative_path
        current_path = (current_root / relative_path) if current_root else None
        if current_path is None or not current_path.exists() or not candidate_path.exists():
            changed_paths.append(relative_path)
            continue
        if not filecmp.cmp(candidate_path, current_path, shallow=False):
            changed_paths.append(relative_path)

    return tuple(changed_paths)


def _restart_required(changed_paths: tuple[str, ...]) -> bool:
    return any(path.startswith("backend/") or path in RESTART_SERVICE_PATHS for path in changed_paths)


def _bootstrap_required(changed_paths: tuple[str, ...]) -> bool:
    return any(path in BOOTSTRAP_RUNTIME_PATHS for path in changed_paths)


def classify_deploy_plan(changed_paths: tuple[str, ...]) -> DeployPlan:
    restart_services = _restart_required(changed_paths)
    daemon_reload = any(path in DAEMON_RELOAD_PATHS for path in changed_paths)
    reload_caddy = any(path in RELOAD_CADDY_PATHS for path in changed_paths)
    refresh_venv = "requirements-live.txt" in changed_paths
    bootstrap_runtime = False
    reasons: list[str] = []

    if refresh_venv:
        reasons.append("python-dependencies")
    if restart_services:
        reasons.append("runtime-code")
    if daemon_reload:
        reasons.append("systemd-units")
    if reload_caddy:
        reasons.append("caddy-config")
    if not restart_services and _bootstrap_required(changed_paths):
        bootstrap_runtime = True
        reasons.append("runtime-data")
    if not reasons and changed_paths:
        reasons.append("no-runtime-action")

    return DeployPlan(
        changed_paths=changed_paths,
        restart_services=restart_services or daemon_reload,
        bootstrap_runtime=bootstrap_runtime,
        reload_caddy=reload_caddy,
        daemon_reload=daemon_reload,
        refresh_venv=refresh_venv,
        reasons=tuple(reasons),
    )

