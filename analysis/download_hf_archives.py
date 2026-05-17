#!/usr/bin/env python3
"""Download daily AFIR provider-response archives from Hugging Face."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.archive import DailyResponseArchiveDownloader
from backend.config import AppConfig, load_env_file

DEFAULT_DAYS = 7
DEFAULT_HF_ARCHIVE_REPO_ID = "loffenauer/AFIR"
DEFAULT_HF_ARCHIVE_PATH_PREFIX = "provider-response-archives"
ARCHIVE_ENV_FILE_KEYS = frozenset(
    {
        "WOLADEN_LIVE_ARCHIVE_DIR",
        "WOLADEN_LIVE_ARCHIVE_TIMEZONE",
        "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID",
        "WOLADEN_LIVE_HF_ARCHIVE_REPO_TYPE",
        "WOLADEN_LIVE_HF_ARCHIVE_PATH_PREFIX",
        "WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE",
        "WOLADEN_LIVE_HF_ARCHIVE_TOKEN",
        "HF_TOKEN",
        "HUGGINGFACE_HUB_TOKEN",
        "HUGGINGFACE_TOKEN",
    }
)
TOKEN_ENV_NAMES = (
    "WOLADEN_LIVE_HF_ARCHIVE_TOKEN",
    "HF_TOKEN",
    "HUGGINGFACE_HUB_TOKEN",
    "HUGGINGFACE_TOKEN",
)
DEFAULT_HF_TOKEN_FILES = (
    REPO_ROOT / "secret" / "hf_private",
    REPO_ROOT / "secret" / "hf_private.txt",
    REPO_ROOT / "secret" / "HF_PRIVATE",
    REPO_ROOT / "secret" / "HF_PRIVATE.txt",
)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value.strip())


def date_window(end_date: date, days: int) -> list[date]:
    if days < 1:
        raise SystemExit("--days must be at least 1")
    return [end_date - timedelta(days=offset) for offset in range(days - 1, -1, -1)]


def discover_default_hf_token_file() -> Path | None:
    for candidate in DEFAULT_HF_TOKEN_FILES:
        if candidate.exists():
            return candidate
    return None


def configure_archive_environment(args: argparse.Namespace) -> None:
    if args.env_file is not None:
        load_env_file(args.env_file, allowed_keys=ARCHIVE_ENV_FILE_KEYS)

    if args.archive_dir is not None:
        os.environ["WOLADEN_LIVE_ARCHIVE_DIR"] = str(args.archive_dir)

    if args.hf_repo_id:
        os.environ["WOLADEN_LIVE_HF_ARCHIVE_REPO_ID"] = args.hf_repo_id
    elif not str(os.environ.get("WOLADEN_LIVE_HF_ARCHIVE_REPO_ID", "")).strip():
        os.environ["WOLADEN_LIVE_HF_ARCHIVE_REPO_ID"] = DEFAULT_HF_ARCHIVE_REPO_ID

    if args.hf_path_prefix is not None:
        os.environ["WOLADEN_LIVE_HF_ARCHIVE_PATH_PREFIX"] = args.hf_path_prefix.strip().strip("/")
    elif not str(os.environ.get("WOLADEN_LIVE_HF_ARCHIVE_PATH_PREFIX", "")).strip():
        os.environ["WOLADEN_LIVE_HF_ARCHIVE_PATH_PREFIX"] = DEFAULT_HF_ARCHIVE_PATH_PREFIX

    if args.hf_token_file is not None:
        os.environ["WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE"] = str(args.hf_token_file)
    elif (
        not str(os.environ.get("WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE", "")).strip()
        and not any(str(os.environ.get(env_name, "")).strip() for env_name in TOKEN_ENV_NAMES)
    ):
        token_file = discover_default_hf_token_file()
        if token_file is not None:
            os.environ["WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE"] = str(token_file)


def download_archive(
    downloader: DailyResponseArchiveDownloader,
    target_date: date,
    *,
    force: bool,
) -> tuple[dict[str, Any] | None, dict[str, str] | None]:
    try:
        result = downloader.download_date(target_date, force=force)
    except ModuleNotFoundError as exc:
        if exc.name == "huggingface_hub":
            raise SystemExit(
                "Downloading requires huggingface_hub. Install it with: "
                "python -m pip install -r requirements-live.txt"
            ) from exc
        raise
    except Exception as exc:
        if exc.__class__.__name__ in {"EntryNotFoundError", "RemoteEntryNotFoundError"}:
            return None, {
                "date": target_date.isoformat(),
                "error": f"{type(exc).__name__}: {exc}",
            }
        raise

    if result.get("result") == "skipped_missing_repo_config":
        raise SystemExit(
            "Missing Hugging Face archive repo config. Pass --hf-repo-id or set "
            "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID."
        )

    target_path_text = str(result.get("target_path") or "").strip()
    if target_path_text and not Path(target_path_text).exists():
        raise FileNotFoundError(f"download completed but archive is missing: {target_path_text}")
    return result, None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download AFIR response archives from Hugging Face")
    parser.add_argument(
        "--date",
        type=_parse_date,
        default=None,
        help="End date in YYYY-MM-DD. Defaults to yesterday in the configured archive timezone.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Number of trailing days to download, ending at --date. Defaults to {DEFAULT_DAYS}.",
    )
    parser.add_argument("--archive-dir", type=Path, default=None, help="Directory where downloaded tgz archives are stored")
    parser.add_argument("--env-file", type=Path, default=None, help="Optional env file with Hugging Face archive settings")
    parser.add_argument(
        "--hf-repo-id",
        default="",
        help=f"Hugging Face dataset repo id; defaults to {DEFAULT_HF_ARCHIVE_REPO_ID}",
    )
    parser.add_argument(
        "--hf-path-prefix",
        default=None,
        help=f"Archive path prefix in the dataset; defaults to {DEFAULT_HF_ARCHIVE_PATH_PREFIX}",
    )
    parser.add_argument(
        "--hf-token-file",
        type=Path,
        default=None,
        help="Optional local file containing a Hugging Face token; the token itself is never written to outputs",
    )
    parser.add_argument("--force-download", action="store_true", help="Download archives even when local copies exist")
    parser.add_argument("--require-complete", action="store_true", help="Exit with an error if any requested archive is unavailable")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_archive_environment(args)
    if not sys.stdout.isatty():
        os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")

    config = AppConfig()
    downloader = DailyResponseArchiveDownloader(config)
    end_date = args.date or downloader.default_target_date()
    target_dates = date_window(end_date, args.days)

    downloads: list[dict[str, Any]] = []
    missing_archives: list[dict[str, str]] = []
    for target_date in target_dates:
        result, missing = download_archive(downloader, target_date, force=args.force_download)
        if result is not None:
            downloads.append(result)
        if missing is not None:
            missing_archives.append(missing)

    summary = {
        "result": "complete" if not missing_archives else "partial",
        "start_date": target_dates[0].isoformat(),
        "end_date": end_date.isoformat(),
        "requested_days": args.days,
        "downloaded_days": len(downloads),
        "missing_archives": missing_archives,
        "archive_dir": str(config.archive_dir.resolve()),
        "downloads": downloads,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if missing_archives and args.require_complete:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
