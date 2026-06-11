from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_live_deploy_uses_pinned_known_hosts_secret():
    workflow = (REPO_ROOT / ".github" / "workflows" / "live-deploy.yml").read_text(encoding="utf-8")

    assert "LIVE_DEPLOY_SSH_KNOWN_HOSTS" in workflow
    assert "ssh-keyscan" not in workflow
