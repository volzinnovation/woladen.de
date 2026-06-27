from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_live_deploy_can_derive_known_hosts_when_secret_is_absent():
    workflow = (REPO_ROOT / ".github" / "workflows" / "live-deploy.yml").read_text(encoding="utf-8")

    assert "LIVE_DEPLOY_SSH_KNOWN_HOSTS" in workflow
    assert "ssh-keyscan" in workflow
    assert "LIVE_API_PUSH_TOKEN" not in workflow


def test_live_deploy_packages_and_refreshes_occupancy_sidecar():
    workflow = (REPO_ROOT / ".github" / "workflows" / "live-deploy.yml").read_text(encoding="utf-8")
    build_release = (REPO_ROOT / "deploy" / "ionos" / "build-release.sh").read_text(encoding="utf-8")
    deploy_release = (REPO_ROOT / "deploy" / "ionos" / "deploy-release.sh").read_text(encoding="utf-8")
    bootstrap = (REPO_ROOT / "deploy" / "ionos" / "bootstrap-host.sh").read_text(encoding="utf-8")
    install = (REPO_ROOT / "deploy" / "ionos" / "install-on-vps.sh").read_text(encoding="utf-8")
    cron = (REPO_ROOT / "deploy" / "ionos" / "woladen-live-occupancy-stats-refresh.cron").read_text(
        encoding="utf-8"
    )

    assert "scripts/refresh_occupancy_stats_sidecar.py" in workflow
    assert "scripts/refresh_occupancy_stats_sidecar.py" in build_release
    assert "refresh_occupancy_stats_sidecar" in deploy_release
    assert "command -v zstd" in deploy_release
    assert "zstd" in bootstrap
    assert "WOLADEN_LIVE_OCCUPANCY_STATS_SQLITE_PATH" in deploy_release
    assert "woladen-live-occupancy-stats-refresh.cron" in install
    assert "__CONFIG_DIR__/huggingface.token" in cron
    assert "/occupancy_stats.sqlite3" in cron
