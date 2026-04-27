from scripts.live_archive_logs import ARCHIVE_ENV_FILE_KEYS


def test_live_archive_logs_loads_queue_dir_from_env_file():
    assert "WOLADEN_LIVE_QUEUE_DIR" in ARCHIVE_ENV_FILE_KEYS
