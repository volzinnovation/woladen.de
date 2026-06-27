#!/usr/bin/env python3
"""Refresh the private occupancy stats SQLite sidecar atomically."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from datetime import date
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REPO_ID = "loffenauer/AFIR"
DEFAULT_REPO_TYPE = "dataset"
DEFAULT_PREFIX = "AFIR/commercial/analytics/occupancy/merged"
DEFAULT_ASSET_NAME = "occupancy_stats.sqlite3.zst"
REQUIRED_PROFILE_COLUMNS = {
    "station_id",
    "country_code",
    "measured_seconds",
    "occupied_seconds",
    "out_of_order_seconds",
    "occupancy_share",
    "out_of_order_share",
    "confidence_label",
}


def _env_value(*names: str) -> str:
    for name in names:
        value = str(os.environ.get(name, "")).strip()
        if value:
            return value
    return ""


def _default_output_path() -> Path:
    configured = _env_value(
        "WOLADEN_LIVE_OCCUPANCY_STATS_SQLITE_PATH",
        "WOLADEN_COMMERCIAL_OCCUPANCY_SQLITE_PATH",
        "WOLADEN_OCCUPANCY_STATS_SQLITE_PATH",
    )
    return Path(configured).expanduser() if configured else REPO_ROOT / "data" / "occupancy_stats.sqlite3"


def _token_from_env_or_file(token_file: str = "") -> str | None:
    token = _env_value(
        "WOLADEN_OCCUPANCY_STATS_HF_TOKEN",
        "HF_PRIVATE",
        "HF_TOKEN",
        "HUGGINGFACE_HUB_TOKEN",
    )
    if token:
        return token
    token_path = token_file or _env_value(
        "WOLADEN_OCCUPANCY_STATS_HF_TOKEN_FILE",
        "HF_TOKEN_FILE",
        "HUGGINGFACE_HUB_TOKEN_FILE",
    )
    if not token_path:
        return None
    try:
        loaded = Path(token_path).expanduser().read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return loaded or None


def _resolve_zstd() -> str:
    binary = shutil.which("zstd")
    if not binary:
        raise RuntimeError("zstd_binary_not_found_in_path")
    return binary


def _decompress_zstd(source_path: Path, output_path: Path) -> None:
    with output_path.open("wb") as handle:
        subprocess.run(
            [_resolve_zstd(), "-d", "-c", str(source_path)],
            stdout=handle,
            check=True,
        )


def _copy_or_decompress(source_path: Path, output_path: Path) -> None:
    if source_path.name.endswith(".zst"):
        _decompress_zstd(source_path, output_path)
    else:
        shutil.copyfile(source_path, output_path)


def _validate_sqlite(path: Path) -> dict[str, Any]:
    with sqlite3.connect(path) as connection:
        integrity = str(connection.execute("PRAGMA integrity_check").fetchone()[0])
        if integrity != "ok":
            raise RuntimeError(f"sqlite_integrity_check_failed:{integrity}")
        tables = {
            str(row[0])
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        }
        if "station_occupancy_profiles" not in tables:
            raise RuntimeError("missing_table:station_occupancy_profiles")
        profile_columns = {
            str(row[1])
            for row in connection.execute("PRAGMA table_info(station_occupancy_profiles)")
        }
        missing_columns = sorted(REQUIRED_PROFILE_COLUMNS - profile_columns)
        if missing_columns:
            raise RuntimeError(f"missing_profile_columns:{','.join(missing_columns)}")
        profile_count = int(
            connection.execute("SELECT COUNT(*) FROM station_occupancy_profiles").fetchone()[0] or 0
        )
        countries = [
            str(row[0])
            for row in connection.execute(
                """
                SELECT DISTINCT country_code
                FROM station_occupancy_profiles
                WHERE COALESCE(country_code, '') != ''
                ORDER BY country_code
                """
            ).fetchall()
        ]
    return {
        "integrity_check": integrity,
        "profile_count": profile_count,
        "countries": countries,
    }


def _publish_sqlite(source_path: Path, output_path: Path) -> dict[str, Any]:
    if not source_path.exists() or not source_path.is_file():
        raise FileNotFoundError(f"source_not_found:{source_path}")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=f".{output_path.name}.", dir=output_path.parent) as temp_dir:
        temp_sqlite = Path(temp_dir) / output_path.name
        _copy_or_decompress(source_path, temp_sqlite)
        validation = _validate_sqlite(temp_sqlite)
        temp_target = output_path.with_name(f".{output_path.name}.tmp")
        shutil.copyfile(temp_sqlite, temp_target)
        os.replace(temp_target, output_path)
    return validation


def _remote_asset_path(prefix: str, end_date: str, asset_name: str) -> str:
    return f"{prefix.strip('/')}/{end_date}/{asset_name}"


def _select_latest_remote_asset(files: Sequence[str], *, prefix: str, asset_name: str) -> tuple[str, str]:
    pattern = re.compile(rf"^{re.escape(prefix.strip('/'))}/(\d{{4}}-\d{{2}}-\d{{2}})/{re.escape(asset_name)}$")
    matches: list[tuple[date, str]] = []
    for filename in files:
        match = pattern.match(str(filename))
        if not match:
            continue
        try:
            artifact_date = date.fromisoformat(match.group(1))
        except ValueError:
            continue
        matches.append((artifact_date, str(filename)))
    if not matches:
        raise RuntimeError(f"occupancy_stats_artifact_not_found:{prefix.strip('/')}/{asset_name}")
    artifact_date, remote_path = max(matches, key=lambda item: item[0])
    return remote_path, artifact_date.isoformat()


def _download_hf_asset(
    *,
    repo_id: str,
    repo_type: str,
    prefix: str,
    end_date: str,
    asset_name: str,
    token: str | None,
    download_dir: Path,
) -> tuple[Path, str]:
    try:
        from huggingface_hub import HfApi, hf_hub_download
    except ImportError as exc:
        raise RuntimeError("huggingface_hub_not_installed") from exc

    if end_date:
        remote_path = _remote_asset_path(prefix, end_date, asset_name)
        resolved_date = end_date
    else:
        api = HfApi(token=token)
        files = api.list_repo_files(repo_id=repo_id, repo_type=repo_type)
        remote_path, resolved_date = _select_latest_remote_asset(files, prefix=prefix, asset_name=asset_name)

    downloaded = hf_hub_download(
        repo_id=repo_id,
        repo_type=repo_type,
        filename=remote_path,
        token=token,
        local_dir=str(download_dir),
        local_dir_use_symlinks=False,
    )
    return Path(downloaded), resolved_date


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh /var/lib/woladen/occupancy_stats.sqlite3 atomically.")
    parser.add_argument("--source-path", type=Path, default=None, help="Local .sqlite3 or .sqlite3.zst artifact.")
    parser.add_argument("--output-path", type=Path, default=_default_output_path())
    parser.add_argument("--repo-id", default=_env_value("WOLADEN_OCCUPANCY_STATS_HF_REPO_ID", "HF_DATASET_REPO_ID") or DEFAULT_REPO_ID)
    parser.add_argument("--repo-type", default=_env_value("WOLADEN_OCCUPANCY_STATS_HF_REPO_TYPE") or DEFAULT_REPO_TYPE)
    parser.add_argument("--prefix", default=_env_value("WOLADEN_OCCUPANCY_STATS_HF_PREFIX") or DEFAULT_PREFIX)
    parser.add_argument("--end-date", default=_env_value("WOLADEN_OCCUPANCY_STATS_END_DATE"))
    parser.add_argument("--asset-name", default=_env_value("WOLADEN_OCCUPANCY_STATS_ASSET_NAME") or DEFAULT_ASSET_NAME)
    parser.add_argument("--token-file", default="")
    parser.add_argument("--summary-path", type=Path, default=None)
    parser.add_argument("--allow-missing", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        source_path = args.source_path
        resolved_date = args.end_date or ""
        if source_path is None:
            with tempfile.TemporaryDirectory(prefix="occupancy-stats-hf-") as temp_dir:
                source_path, resolved_date = _download_hf_asset(
                    repo_id=args.repo_id,
                    repo_type=args.repo_type,
                    prefix=args.prefix,
                    end_date=args.end_date,
                    asset_name=args.asset_name,
                    token=_token_from_env_or_file(args.token_file),
                    download_dir=Path(temp_dir),
                )
                validation = _publish_sqlite(source_path, args.output_path)
        else:
            validation = _publish_sqlite(source_path, args.output_path)
        summary = {
            "ok": True,
            "output_path": str(args.output_path),
            "source_path": str(source_path),
            "artifact_date": resolved_date,
            **validation,
        }
    except Exception as exc:
        if args.allow_missing:
            summary = {"ok": False, "skipped": True, "error": str(exc), "output_path": str(args.output_path)}
        else:
            print(str(exc), file=sys.stderr)
            return 1

    encoded = json.dumps(summary, sort_keys=True)
    print(encoded)
    if args.summary_path is not None:
        args.summary_path.parent.mkdir(parents=True, exist_ok=True)
        args.summary_path.write_text(f"{encoded}\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
