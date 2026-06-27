from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_live_deploy_can_derive_known_hosts_when_secret_is_absent():
    workflow = (REPO_ROOT / ".github" / "workflows" / "live-deploy.yml").read_text(encoding="utf-8")

    assert "LIVE_DEPLOY_SSH_KNOWN_HOSTS" in workflow
    assert "ssh-keyscan" in workflow
    assert "LIVE_API_PUSH_TOKEN" not in workflow
