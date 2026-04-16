from __future__ import annotations

from pathlib import Path

from backend.deploy_plan import classify_deploy_plan, collect_changed_paths


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_collect_changed_paths_ignores_release_metadata(tmp_path: Path) -> None:
    current_root = tmp_path / "current"
    candidate_root = tmp_path / "candidate"

    _write_file(current_root / "release.json", '{"git_sha":"old"}\n')
    _write_file(candidate_root / "release.json", '{"git_sha":"new"}\n')
    _write_file(current_root / "data" / "chargers_fast.geojson", '{"type":"FeatureCollection","features":[]}\n')
    _write_file(candidate_root / "data" / "chargers_fast.geojson", '{"type":"FeatureCollection","features":[]}\n')

    assert collect_changed_paths(current_root, candidate_root) == ()


def test_classify_plan_restarts_for_runtime_code_changes(tmp_path: Path) -> None:
    current_root = tmp_path / "current"
    candidate_root = tmp_path / "candidate"

    _write_file(current_root / "backend" / "api.py", "old\n")
    _write_file(candidate_root / "backend" / "api.py", "new\n")

    changed_paths = collect_changed_paths(current_root, candidate_root)
    plan = classify_deploy_plan(changed_paths)

    assert changed_paths == ("backend/api.py",)
    assert plan.restart_services is True
    assert plan.bootstrap_runtime is False
    assert "runtime-code" in plan.reasons


def test_classify_plan_bootstraps_for_runtime_data_only(tmp_path: Path) -> None:
    current_root = tmp_path / "current"
    candidate_root = tmp_path / "candidate"

    _write_file(current_root / "data" / "chargers_fast.csv", "a\n")
    _write_file(candidate_root / "data" / "chargers_fast.csv", "b\n")

    changed_paths = collect_changed_paths(current_root, candidate_root)
    plan = classify_deploy_plan(changed_paths)

    assert changed_paths == ("data/chargers_fast.csv",)
    assert plan.restart_services is False
    assert plan.bootstrap_runtime is True
    assert plan.reasons == ("runtime-data",)


def test_classify_plan_reloads_caddy_without_service_restart(tmp_path: Path) -> None:
    current_root = tmp_path / "current"
    candidate_root = tmp_path / "candidate"

    _write_file(current_root / "deploy" / "ionos" / "Caddyfile", "old\n")
    _write_file(candidate_root / "deploy" / "ionos" / "Caddyfile", "new\n")

    changed_paths = collect_changed_paths(current_root, candidate_root)
    plan = classify_deploy_plan(changed_paths)

    assert changed_paths == ("deploy/ionos/Caddyfile",)
    assert plan.restart_services is False
    assert plan.bootstrap_runtime is False
    assert plan.reload_caddy is True
    assert plan.reasons == ("caddy-config",)


def test_classify_plan_for_initial_deploy_requires_restart(tmp_path: Path) -> None:
    candidate_root = tmp_path / "candidate"
    _write_file(candidate_root / "backend" / "service.py", "new\n")
    _write_file(candidate_root / "requirements-live.txt", "uvicorn>=0.30.0\n")

    changed_paths = collect_changed_paths(None, candidate_root)
    plan = classify_deploy_plan(changed_paths)

    assert set(changed_paths) == {"backend/service.py", "requirements-live.txt"}
    assert plan.restart_services is True
    assert plan.refresh_venv is True

