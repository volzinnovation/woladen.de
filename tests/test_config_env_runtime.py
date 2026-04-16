from __future__ import annotations

from pathlib import Path

from backend.config import AppConfig, load_env_file


def test_app_config_reads_loaded_env_file_at_instantiation_time(tmp_path, monkeypatch):
    env_file = tmp_path / "woladen-live.env"
    env_file.write_text(
        "\n".join(
            [
                "WOLADEN_LIVE_RAW_PAYLOAD_DIR=/var/lib/woladen/live_raw",
                "WOLADEN_LIVE_ARCHIVE_DIR=/var/lib/woladen/live_archives",
                "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID=loffenauer/AFIR",
                "WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE=/etc/woladen/huggingface.token",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("WOLADEN_LIVE_RAW_PAYLOAD_DIR", raising=False)
    monkeypatch.delenv("WOLADEN_LIVE_ARCHIVE_DIR", raising=False)
    monkeypatch.delenv("WOLADEN_LIVE_HF_ARCHIVE_REPO_ID", raising=False)
    monkeypatch.delenv("WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE", raising=False)

    load_env_file(
        env_file,
        allowed_keys={
            "WOLADEN_LIVE_RAW_PAYLOAD_DIR",
            "WOLADEN_LIVE_ARCHIVE_DIR",
            "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID",
            "WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE",
        },
    )

    config = AppConfig()

    assert config.raw_payload_dir == Path("/var/lib/woladen/live_raw")
    assert config.archive_dir == Path("/var/lib/woladen/live_archives")
    assert config.hf_archive_repo_id == "loffenauer/AFIR"
    assert config.hf_archive_token_file == Path("/etc/woladen/huggingface.token")
