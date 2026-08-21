from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "iphone/scripts/write_github_release_access_config.sh"


def _run_config(
    tmp_path: Path, *, extra_env: dict[str, str] | None = None, require: bool = False
):
    output = tmp_path / "GitHubReleaseAccess.json"
    env = {
        **os.environ,
        "WOLADEN_HF_RELEASE_TOKEN_FILE": str(tmp_path / "missing-token"),
        **(extra_env or {}),
    }
    command = [str(SCRIPT), "--output", str(output)]
    if require:
        command.insert(1, "--require")
    result = subprocess.run(command, text=True, capture_output=True, env=env)
    return result, output


def test_generates_hf_stable_discovery_config_without_legacy_github_pointer(tmp_path):
    result, output = _run_config(tmp_path)

    assert result.returncode == 0, result.stderr
    document = json.loads(output.read_text(encoding="utf-8"))
    assert document == {
        "schema_version": "woladen-open-static-release-access-v1",
        "hf_repo_id": "loffenauer/AFIR",
        "hf_repo_type": "dataset",
        "hf_prefix": "AFIR/open-static/releases",
        "hf_stable_alias": "open-static-ios-regional-latest",
        "source_repository": "volzinnovation/Woladen.de-analytics",
    }
    assert "release_tag" not in document
    assert "github_owner" not in document
    assert "github_repo" not in document


def test_includes_optional_hf_read_token(tmp_path):
    token_path = tmp_path / "hf-token"
    token_path.write_text("hf_read_token\n", encoding="utf-8")
    result, output = _run_config(
        tmp_path,
        extra_env={"WOLADEN_HF_RELEASE_TOKEN_FILE": str(token_path)},
    )

    assert result.returncode == 0, result.stderr
    document = json.loads(output.read_text(encoding="utf-8"))
    assert document["hf_read_token"] == "hf_read_token"


def test_required_hf_token_fails_closed_when_missing(tmp_path):
    result, output = _run_config(tmp_path, require=True)

    assert result.returncode == 1
    assert "Hugging Face release download token file missing" in result.stderr
    assert not output.exists()


def test_latest_alias_is_rejected(tmp_path):
    result, output = _run_config(
        tmp_path,
        extra_env={"WOLADEN_OPEN_STATIC_HF_STABLE_ALIAS": "latest"},
    )

    assert result.returncode == 1
    assert "Invalid Hugging Face stable alias" in result.stderr
    assert not output.exists()
