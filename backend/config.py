from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import timezone
from pathlib import Path
from typing import Collection
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FAST_CHARGERS_CSV_PATH = REPO_ROOT / "data" / "chargers_fast.csv"
DEFAULT_FULL_CHARGERS_CSV_PATH = REPO_ROOT / "data" / "chargers_full.csv"


def _env_path(name: str, default: Path) -> Path:
    value = str(os.environ.get(name, "")).strip()
    return Path(value).expanduser() if value else default


def _env_optional_path(name: str) -> Path | None:
    value = str(os.environ.get(name, "")).strip()
    return Path(value).expanduser() if value else None


def _env_existing_path(name: str, default: Path) -> Path | None:
    value = str(os.environ.get(name, "")).strip()
    if value:
        return Path(value).expanduser()
    if default.exists():
        return default
    return None


def _env_csv(name: str) -> tuple[str, ...]:
    value = str(os.environ.get(name, "")).strip()
    if not value:
        return ()
    return tuple(part.strip() for part in value.split(",") if part.strip())


def _env_bool(name: str, default: bool) -> bool:
    value = str(os.environ.get(name, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def load_env_file(path: Path, *, allowed_keys: Collection[str] | None = None) -> None:
    """Load simple KEY=value assignments from a runtime env file.

    This loader is intentionally narrow. It supports the subset used by the
    archive CLI and can optionally ignore unrelated keys that may use
    shell-unsafe syntax.
    """

    allowed = set(allowed_keys) if allowed_keys is not None else None
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in raw_line:
            raise ValueError(f"Invalid env assignment in {path}:{line_number}")
        key, value = raw_line.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"Missing env key in {path}:{line_number}")
        if allowed is not None and key not in allowed:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ[key] = value


@dataclass(frozen=True)
class AppConfig:
    db_path: Path = field(default_factory=lambda: _env_path("WOLADEN_LIVE_DB_PATH", REPO_ROOT / "data" / "live_state.sqlite3"))
    chargers_geojson_path: Path = field(
        default_factory=lambda: _env_path(
            "WOLADEN_LIVE_CHARGERS_GEOJSON_PATH",
            REPO_ROOT / "data" / "chargers_fast.geojson",
        )
    )
    raw_payload_dir: Path = field(
        default_factory=lambda: _env_path("WOLADEN_LIVE_RAW_PAYLOAD_DIR", REPO_ROOT / "data" / "live_raw")
    )
    archive_dir: Path = field(
        default_factory=lambda: _env_path("WOLADEN_LIVE_ARCHIVE_DIR", REPO_ROOT / "data" / "live_archives")
    )
    queue_dir: Path = field(
        default_factory=lambda: _env_path("WOLADEN_LIVE_QUEUE_DIR", REPO_ROOT / "data" / "live_queue")
    )
    provider_config_path: Path = field(
        default_factory=lambda: _env_path(
            "WOLADEN_LIVE_PROVIDER_CONFIG_PATH",
            REPO_ROOT / "data" / "mobilithek_afir_provider_configs.json",
        )
    )
    site_match_path: Path = field(
        default_factory=lambda: _env_path(
            "WOLADEN_LIVE_SITE_MATCH_PATH",
            REPO_ROOT / "data" / "mobilithek_afir_static_matches.csv",
        )
    )
    chargers_csv_path: Path = field(
        default_factory=lambda: _env_path("WOLADEN_LIVE_CHARGERS_CSV_PATH", DEFAULT_FAST_CHARGERS_CSV_PATH)
    )
    full_chargers_csv_path: Path | None = field(
        default_factory=lambda: (
            _env_path("WOLADEN_LIVE_FULL_CHARGERS_CSV_PATH", DEFAULT_FULL_CHARGERS_CSV_PATH)
            if str(os.environ.get("WOLADEN_LIVE_FULL_CHARGERS_CSV_PATH", "")).strip()
            else None
        )
    )
    provider_override_path: Path | None = field(
        default_factory=lambda: _env_existing_path(
            "WOLADEN_LIVE_PROVIDER_OVERRIDE_PATH",
            REPO_ROOT / "data" / "live_provider_overrides.json",
        )
    )
    subscription_registry_path: Path = field(
        default_factory=lambda: _env_path(
            "WOLADEN_LIVE_SUBSCRIPTION_REGISTRY_PATH",
            REPO_ROOT / "secret" / "mobilithek_subscriptions.json",
        )
    )
    machine_cert_p12: Path = field(
        default_factory=lambda: _env_path("WOLADEN_MACHINE_CERT_P12", REPO_ROOT / "secret" / "certificate.p12")
    )
    machine_cert_password_file: Path = field(
        default_factory=lambda: _env_path(
            "WOLADEN_MACHINE_CERT_PASSWORD_FILE",
            REPO_ROOT / "secret" / "pwd.txt",
        )
    )
    api_host: str = field(default_factory=lambda: str(os.environ.get("WOLADEN_LIVE_API_HOST", "127.0.0.1")))
    api_port: int = field(default_factory=lambda: int(os.environ.get("WOLADEN_LIVE_API_PORT", "8001")))
    api_cors_allowed_origins: tuple[str, ...] = field(
        default_factory=lambda: _env_csv("WOLADEN_LIVE_API_CORS_ALLOWED_ORIGINS")
    )
    api_cors_allowed_origin_regex: str = field(
        default_factory=lambda: str(
            os.environ.get(
                "WOLADEN_LIVE_API_CORS_ALLOW_ORIGIN_REGEX",
                r"https?://(localhost|127\.0\.0\.1|0\.0\.0\.0|\[::1\])(:\d+)?$",
            )
        ).strip()
    )
    api_push_enabled: bool = field(default_factory=lambda: _env_bool("WOLADEN_LIVE_API_PUSH_ENABLED", True))
    poll_timeout_seconds: int = field(
        default_factory=lambda: int(os.environ.get("WOLADEN_LIVE_POLL_TIMEOUT_SECONDS", "10"))
    )
    poll_interval_delta_seconds: int = field(
        default_factory=lambda: int(os.environ.get("WOLADEN_LIVE_POLL_INTERVAL_DELTA_SECONDS", "15"))
    )
    poll_interval_snapshot_seconds: int = field(
        default_factory=lambda: int(os.environ.get("WOLADEN_LIVE_POLL_INTERVAL_SNAPSHOT_SECONDS", "30"))
    )
    poll_interval_no_data_max_seconds: int = field(
        default_factory=lambda: int(os.environ.get("WOLADEN_LIVE_POLL_INTERVAL_NO_DATA_MAX_SECONDS", "600"))
    )
    poll_interval_error_max_seconds: int = field(
        default_factory=lambda: int(os.environ.get("WOLADEN_LIVE_POLL_INTERVAL_ERROR_MAX_SECONDS", "900"))
    )
    poll_interval_unchanged_max_seconds: int = field(
        default_factory=lambda: int(os.environ.get("WOLADEN_LIVE_POLL_INTERVAL_UNCHANGED_MAX_SECONDS", "300"))
    )
    poll_idle_sleep_max_seconds: int = field(
        default_factory=lambda: int(os.environ.get("WOLADEN_LIVE_POLL_IDLE_SLEEP_MAX_SECONDS", "30"))
    )
    sqlite_busy_timeout_ms: int = field(
        default_factory=lambda: int(os.environ.get("WOLADEN_LIVE_SQLITE_BUSY_TIMEOUT_MS", "5000"))
    )
    sqlite_lock_retry_seconds: float = field(
        default_factory=lambda: float(os.environ.get("WOLADEN_LIVE_SQLITE_LOCK_RETRY_SECONDS", "30"))
    )
    queue_idle_sleep_seconds: float = field(
        default_factory=lambda: float(os.environ.get("WOLADEN_LIVE_QUEUE_IDLE_SLEEP_SECONDS", "1"))
    )
    queue_cleanup_interval_seconds: float = field(
        default_factory=lambda: float(os.environ.get("WOLADEN_LIVE_QUEUE_CLEANUP_INTERVAL_SECONDS", "300"))
    )
    queue_done_retention_seconds: float = field(
        default_factory=lambda: float(os.environ.get("WOLADEN_LIVE_QUEUE_DONE_RETENTION_SECONDS", "86400"))
    )
    queue_failed_retention_seconds: float = field(
        default_factory=lambda: float(os.environ.get("WOLADEN_LIVE_QUEUE_FAILED_RETENTION_SECONDS", "604800"))
    )
    archive_timezone_name: str = field(
        default_factory=lambda: str(os.environ.get("WOLADEN_LIVE_ARCHIVE_TIMEZONE", "Europe/Berlin")).strip()
    )
    hf_archive_repo_id: str = field(
        default_factory=lambda: str(os.environ.get("WOLADEN_LIVE_HF_ARCHIVE_REPO_ID", "")).strip()
    )
    hf_archive_repo_type: str = field(
        default_factory=lambda: str(os.environ.get("WOLADEN_LIVE_HF_ARCHIVE_REPO_TYPE", "dataset")).strip()
        or "dataset"
    )
    hf_archive_path_prefix: str = field(
        default_factory=lambda: str(os.environ.get("WOLADEN_LIVE_HF_ARCHIVE_PATH_PREFIX", "daily")).strip().strip("/")
    )
    hf_archive_token_file: Path | None = field(
        default_factory=lambda: _env_optional_path("WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE")
    )

    def __post_init__(self) -> None:
        if self.full_chargers_csv_path is None:
            if self.chargers_csv_path != DEFAULT_FAST_CHARGERS_CSV_PATH:
                object.__setattr__(self, "full_chargers_csv_path", self.chargers_csv_path)
            elif DEFAULT_FULL_CHARGERS_CSV_PATH.exists():
                object.__setattr__(self, "full_chargers_csv_path", DEFAULT_FULL_CHARGERS_CSV_PATH)
            else:
                object.__setattr__(self, "full_chargers_csv_path", self.chargers_csv_path)

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
