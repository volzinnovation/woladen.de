from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

REPO_ROOT = Path(__file__).resolve().parent.parent


def _env_path(name: str, default: Path) -> Path:
    value = str(os.environ.get(name, "")).strip()
    return Path(value).expanduser() if value else default


def _env_optional_path(name: str) -> Path | None:
    value = str(os.environ.get(name, "")).strip()
    return Path(value).expanduser() if value else None


def _env_csv(name: str) -> tuple[str, ...]:
    value = str(os.environ.get(name, "")).strip()
    if not value:
        return ()
    return tuple(part.strip() for part in value.split(",") if part.strip())


@dataclass(frozen=True)
class AppConfig:
    db_path: Path = _env_path("WOLADEN_LIVE_DB_PATH", REPO_ROOT / "data" / "live_state.sqlite3")
    chargers_geojson_path: Path = _env_path(
        "WOLADEN_LIVE_CHARGERS_GEOJSON_PATH",
        REPO_ROOT / "data" / "chargers_fast.geojson",
    )
    raw_payload_dir: Path = _env_path("WOLADEN_LIVE_RAW_PAYLOAD_DIR", REPO_ROOT / "data" / "live_raw")
    archive_dir: Path = _env_path("WOLADEN_LIVE_ARCHIVE_DIR", REPO_ROOT / "data" / "live_archives")
    provider_config_path: Path = _env_path(
        "WOLADEN_LIVE_PROVIDER_CONFIG_PATH",
        REPO_ROOT / "data" / "mobilithek_afir_provider_configs.json",
    )
    site_match_path: Path = _env_path(
        "WOLADEN_LIVE_SITE_MATCH_PATH",
        REPO_ROOT / "data" / "mobilithek_afir_static_matches.csv",
    )
    chargers_csv_path: Path = _env_path("WOLADEN_LIVE_CHARGERS_CSV_PATH", REPO_ROOT / "data" / "chargers_fast.csv")
    provider_override_path: Path | None = (
        _env_path("WOLADEN_LIVE_PROVIDER_OVERRIDE_PATH", REPO_ROOT / "data" / "live_provider_overrides.json")
        if str(os.environ.get("WOLADEN_LIVE_PROVIDER_OVERRIDE_PATH", "")).strip()
        else None
    )
    subscription_registry_path: Path = _env_path(
        "WOLADEN_LIVE_SUBSCRIPTION_REGISTRY_PATH",
        REPO_ROOT / "secret" / "mobilithek_subscriptions.json",
    )
    machine_cert_p12: Path = _env_path("WOLADEN_MACHINE_CERT_P12", REPO_ROOT / "secret" / "certificate.p12")
    machine_cert_password_file: Path = _env_path(
        "WOLADEN_MACHINE_CERT_PASSWORD_FILE",
        REPO_ROOT / "secret" / "pwd.txt",
    )
    api_host: str = str(os.environ.get("WOLADEN_LIVE_API_HOST", "127.0.0.1"))
    api_port: int = int(os.environ.get("WOLADEN_LIVE_API_PORT", "8001"))
    api_cors_allowed_origins: tuple[str, ...] = _env_csv("WOLADEN_LIVE_API_CORS_ALLOWED_ORIGINS")
    api_cors_allowed_origin_regex: str = str(
        os.environ.get(
            "WOLADEN_LIVE_API_CORS_ALLOW_ORIGIN_REGEX",
            r"https?://(localhost|127\.0\.0\.1)(:\d+)?$",
        )
    ).strip()
    poll_timeout_seconds: int = int(os.environ.get("WOLADEN_LIVE_POLL_TIMEOUT_SECONDS", "10"))
    poll_interval_delta_seconds: int = int(os.environ.get("WOLADEN_LIVE_POLL_INTERVAL_DELTA_SECONDS", "15"))
    poll_interval_snapshot_seconds: int = int(os.environ.get("WOLADEN_LIVE_POLL_INTERVAL_SNAPSHOT_SECONDS", "30"))
    poll_interval_no_data_max_seconds: int = int(
        os.environ.get("WOLADEN_LIVE_POLL_INTERVAL_NO_DATA_MAX_SECONDS", "600")
    )
    poll_interval_error_max_seconds: int = int(os.environ.get("WOLADEN_LIVE_POLL_INTERVAL_ERROR_MAX_SECONDS", "900"))
    poll_interval_unchanged_max_seconds: int = int(
        os.environ.get("WOLADEN_LIVE_POLL_INTERVAL_UNCHANGED_MAX_SECONDS", "300")
    )
    poll_idle_sleep_max_seconds: int = int(os.environ.get("WOLADEN_LIVE_POLL_IDLE_SLEEP_MAX_SECONDS", "30"))
    sqlite_busy_timeout_ms: int = int(os.environ.get("WOLADEN_LIVE_SQLITE_BUSY_TIMEOUT_MS", "5000"))
    archive_timezone_name: str = str(os.environ.get("WOLADEN_LIVE_ARCHIVE_TIMEZONE", "Europe/Berlin")).strip()
    hf_archive_repo_id: str = str(os.environ.get("WOLADEN_LIVE_HF_ARCHIVE_REPO_ID", "")).strip()
    hf_archive_repo_type: str = str(os.environ.get("WOLADEN_LIVE_HF_ARCHIVE_REPO_TYPE", "dataset")).strip() or "dataset"
    hf_archive_path_prefix: str = str(os.environ.get("WOLADEN_LIVE_HF_ARCHIVE_PATH_PREFIX", "daily")).strip().strip("/")
    hf_archive_token_file: Path | None = _env_optional_path("WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE")

    def cert_password(self) -> str:
        return self.machine_cert_password_file.read_text(encoding="utf-8").strip()

    def archive_timezone(self):
        timezone_name = self.archive_timezone_name or "UTC"
        try:
            return ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            return timezone.utc

    def hf_archive_token(self) -> str:
        for env_name in (
            "WOLADEN_LIVE_HF_ARCHIVE_TOKEN",
            "HF_TOKEN",
            "HUGGINGFACE_HUB_TOKEN",
            "HUGGINGFACE_TOKEN",
        ):
            value = str(os.environ.get(env_name, "")).strip()
            if value:
                return value
        if self.hf_archive_token_file and self.hf_archive_token_file.exists():
            return self.hf_archive_token_file.read_text(encoding="utf-8").strip()
        return ""

    def has_hf_archive_upload_config(self) -> bool:
        return bool(self.hf_archive_repo_id and self.hf_archive_token())
