from __future__ import annotations

from pathlib import Path

import pytest

from backend.config import AppConfig


@pytest.fixture()
def app_config(tmp_path: Path) -> AppConfig:
    cert_path = tmp_path / "certificate.p12"
    cert_path.write_bytes(b"dummy-p12")
    password_file = tmp_path / "pwd.txt"
    password_file.write_text("secret-pass\n", encoding="utf-8")
    return AppConfig(
        db_path=tmp_path / "live.sqlite3",
        chargers_geojson_path=tmp_path / "chargers_fast.geojson",
        raw_payload_dir=tmp_path / "raw",
        archive_dir=tmp_path / "archives",
        provider_config_path=tmp_path / "providers.json",
        site_match_path=tmp_path / "matches.csv",
        chargers_csv_path=tmp_path / "chargers.csv",
        subscription_registry_path=tmp_path / "subscriptions.json",
        machine_cert_p12=cert_path,
        machine_cert_password_file=password_file,
        provider_override_path=None,
        archive_timezone_name="UTC",
    )
