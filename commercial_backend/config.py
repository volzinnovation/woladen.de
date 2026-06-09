from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .country_sets import (
    BASELINE_COUNTRIES,
    DEFAULT_COUNTRIES,
    EU27_COMMERCIAL_COUNTRIES,
    EU27_COUNTRIES,
    EXPANSION_COUNTRIES,
    ONBOARDED_COUNTRIES,
    OPEN_DISCOVERY_COUNTRIES,
)

REPO_ROOT = Path(__file__).resolve().parent.parent

def _env_csv(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = str(os.environ.get(name, "")).strip()
    if not value:
        return default
    return tuple(part.strip().upper() for part in value.split(",") if part.strip())


def _env_bool(name: str, default: bool) -> bool:
    value = str(os.environ.get(name, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = str(os.environ.get(name, "")).strip()
    return int(value) if value else default


def _env_float(name: str, default: float) -> float:
    value = str(os.environ.get(name, "")).strip()
    return float(value) if value else default


def _env_path(name: str, default: Path) -> Path:
    value = str(os.environ.get(name, "")).strip()
    return Path(value).expanduser() if value else default


def _unquote_env_value(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def _read_hf_token_file(path: Path, *, env_names: tuple[str, ...]) -> str:
    if not path.exists() or not path.is_file():
        return ""
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return ""
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() in env_names:
            return _unquote_env_value(value)
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line and not line.startswith("#") and "=" not in line:
            return line
    return ""


def _default_hf_token_files() -> tuple[Path, ...]:
    return (
        REPO_ROOT / "secret" / "hf_token",
        REPO_ROOT / "secret" / "HF_TOKEN",
        REPO_ROOT / "secret" / "huggingface_token",
        REPO_ROOT.parent / "woladen.de" / "secrets" / "hf_token",
        REPO_ROOT.parent / "woladen.de" / "secrets" / "HF_TOKEN",
        REPO_ROOT.parent / "woladen.de" / "secrets" / "huggingface_token",
        Path("/run/secrets/woladen/hf_token"),
        Path("/run/secrets/woladen-local/hf_token"),
        Path("/run/secrets/woladen-de/hf_token"),
        Path("/run/secrets/woladen-de/HF_TOKEN"),
        Path("/run/secrets/woladen-de/huggingface_token"),
    )


@dataclass(frozen=True)
class AppConfig:
    store_backend: str = field(
        default_factory=lambda: str(os.environ.get("WOLADEN_COMMERCIAL_STORE_BACKEND", "sqlite")).strip().lower()
        or "sqlite"
    )
    database_url: str = field(
        default_factory=lambda: str(
            os.environ.get(
                "WOLADEN_COMMERCIAL_DATABASE_URL",
                os.environ.get(
                    "DATABASE_URL",
                    "postgresql://woladen:woladen@localhost:5432/woladen_commercial",
                ),
            )
        ).strip()
    )
    sqlite_path: Path = field(
        default_factory=lambda: _env_path(
            "WOLADEN_COMMERCIAL_SQLITE_PATH",
            REPO_ROOT / "data" / "commercial_state.sqlite3",
        )
    )
    occupancy_stats_sqlite_path: Path = field(
        default_factory=lambda: _env_path(
            "WOLADEN_COMMERCIAL_OCCUPANCY_SQLITE_PATH",
            _env_path(
                "WOLADEN_OCCUPANCY_STATS_SQLITE_PATH",
                REPO_ROOT / "data" / "occupancy_stats.sqlite3",
            ),
        )
    )
    allowed_country_codes: tuple[str, ...] = field(
        default_factory=lambda: _env_csv("WOLADEN_COMMERCIAL_ALLOWED_COUNTRIES", DEFAULT_COUNTRIES)
    )
    default_push_country_code: str = field(
        default_factory=lambda: str(os.environ.get("WOLADEN_COMMERCIAL_DEFAULT_PUSH_COUNTRY", "DE")).strip().upper()
    )
    raw_payload_dir: Path = field(
        default_factory=lambda: _env_path(
            "WOLADEN_COMMERCIAL_RAW_PAYLOAD_DIR",
            REPO_ROOT / "data" / "commercial_raw",
        )
    )
    archive_dir: Path = field(
        default_factory=lambda: _env_path(
            "WOLADEN_COMMERCIAL_ARCHIVE_DIR",
            REPO_ROOT / "data" / "commercial_archives",
        )
    )
    inline_payload_max_bytes: int = field(
        default_factory=lambda: _env_int("WOLADEN_COMMERCIAL_INLINE_PAYLOAD_MAX_BYTES", 0)
    )
    max_push_payload_bytes: int = field(
        default_factory=lambda: _env_int("WOLADEN_COMMERCIAL_MAX_PUSH_PAYLOAD_BYTES", 0)
    )
    accept_unknown_sources: bool = field(
        default_factory=lambda: _env_bool("WOLADEN_COMMERCIAL_ACCEPT_UNKNOWN_SOURCES", True)
    )
    debug_push_response: bool = field(
        default_factory=lambda: _env_bool("WOLADEN_COMMERCIAL_DEBUG_PUSH_RESPONSE", False)
    )
    task_processing_lease_seconds: float = field(
        default_factory=lambda: _env_float("WOLADEN_COMMERCIAL_TASK_PROCESSING_LEASE_SECONDS", 1800.0)
    )
    archive_timezone_name: str = field(
        default_factory=lambda: str(os.environ.get("WOLADEN_COMMERCIAL_ARCHIVE_TIMEZONE", "Europe/Berlin")).strip()
    )
    hf_archive_repo_id: str = field(
        default_factory=lambda: str(os.environ.get("WOLADEN_COMMERCIAL_HF_ARCHIVE_REPO_ID", "")).strip()
    )
    hf_archive_repo_type: str = field(
        default_factory=lambda: str(os.environ.get("WOLADEN_COMMERCIAL_HF_ARCHIVE_REPO_TYPE", "dataset")).strip()
        or "dataset"
    )
    hf_archive_path_prefix: str = field(
        default_factory=lambda: str(os.environ.get("WOLADEN_COMMERCIAL_HF_ARCHIVE_PATH_PREFIX", "AFIR/commercial")).strip().strip("/")
    )
    hf_archive_token_file: Path | None = field(
        default_factory=lambda: (
            _env_path("WOLADEN_COMMERCIAL_HF_ARCHIVE_TOKEN_FILE", Path())
            if str(os.environ.get("WOLADEN_COMMERCIAL_HF_ARCHIVE_TOKEN_FILE", "")).strip()
            else None
        )
    )

    def normalized_country_codes(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(code.strip().upper() for code in self.allowed_country_codes if code.strip()))

    def archive_timezone(self):
        timezone_name = self.archive_timezone_name or "UTC"
        try:
            return ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            return timezone.utc

    def hf_archive_token(self) -> str:
        env_names = (
            "WOLADEN_COMMERCIAL_HF_ARCHIVE_TOKEN",
            "HF_TOKEN",
            "HUGGINGFACE_HUB_TOKEN",
            "HUGGINGFACE_TOKEN",
        )
        for env_name in env_names:
            value = str(os.environ.get(env_name, "")).strip()
            if value:
                return value
        if self.hf_archive_token_file and self.hf_archive_token_file.exists():
            token = _read_hf_token_file(self.hf_archive_token_file, env_names=env_names)
            if token:
                return token
        for path in _default_hf_token_files():
            token = _read_hf_token_file(path, env_names=env_names)
            if token:
                return token
        return ""

    def has_hf_archive_upload_config(self) -> bool:
        return bool(self.hf_archive_repo_id and self.hf_archive_token())
