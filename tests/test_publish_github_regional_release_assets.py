from __future__ import annotations

import os
import subprocess
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "iphone"
    / "scripts"
    / "publish_github_regional_release_assets.sh"
)
WORKFLOW_URL = (
    "https://github.com/volzinnovation/Woladen.de-analytics/actions/workflows/"
    "build-open-static-sqlite-bundle.yml"
)


def test_retired_publisher_refuses_upload_without_invoking_gh(tmp_path: Path):
    invocation_log = tmp_path / "gh-invocations"
    fake_gh = tmp_path / "gh"
    fake_gh.write_text(
        "#!/usr/bin/env bash\n"
        'printf "%s\\n" "$*" >> "$WOLADEN_FAKE_GH_LOG"\n',
        encoding="utf-8",
    )
    fake_gh.chmod(0o755)
    env = os.environ.copy()
    env["PATH"] = f"{tmp_path}:{env['PATH']}"
    env["WOLADEN_FAKE_GH_LOG"] = str(invocation_log)

    result = subprocess.run(
        [str(SCRIPT), str(tmp_path / "assets"), str(tmp_path / "bundle")],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode != 0
    assert "release publisher is retired" in result.stderr
    assert WORKFLOW_URL in result.stderr
    assert not invocation_log.exists()


def test_retired_publisher_help_points_to_canonical_workflow():
    result = subprocess.run(
        [str(SCRIPT), "--help"],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "release publisher is retired" in result.stdout
    assert WORKFLOW_URL in result.stdout
