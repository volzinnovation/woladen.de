#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import sys
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_open_static_sqlite_bundle import aggregate_sqlite_parts


SCHEMA_VERSION = 2
DEFAULT_OUTPUT_DIR = Path("iphone/dist/github-regional-packs")
COMPRESSION_ALGORITHM = "zlib"


@dataclass(frozen=True)
class RegionalPack:
    group_id: str
    display_name: str
    countries: tuple[str, ...]

    @property
    def asset_id(self) -> str:
        return f"open-static-{self.group_id}"

    @property
    def sqlite_file_name(self) -> str:
        return f"{self.asset_id}.sqlite3"

    @property
    def compressed_sqlite_file_name(self) -> str:
        return f"{self.sqlite_file_name}.{COMPRESSION_ALGORITHM}"

    @property
    def manifest_file_name(self) -> str:
        return f"{self.asset_id}.manifest.json"


REGIONAL_PACKS: tuple[RegionalPack, ...] = (
    RegionalPack("DACH", "DACH", ("DE", "AT", "CH")),
    RegionalPack("BENELUX", "Benelux", ("NL", "BE", "LU")),
    RegionalPack("ROMANIC", "Romanic", ("FR", "ES", "PT")),
    RegionalPack("NORDICS", "Nordics", ("DK", "SE", "NO", "FI")),
    RegionalPack("REST-EUROPE", "Rest of Europe", ("PL", "CZ", "LT", "GR", "CY", "SI", "MT", "HU", "LV")),
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _compress_zlib_path(source_path: Path, output_path: Path, *, level: int = 9) -> None:
    compressor = zlib.compressobj(level=level, method=zlib.DEFLATED, wbits=zlib.MAX_WBITS)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_name(f".{output_path.name}.tmp")
    try:
        with source_path.open("rb") as source, temp_path.open("wb") as output:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                data = compressor.compress(chunk)
                if data:
                    output.write(data)
            output.write(compressor.flush())
        temp_path.replace(output_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _release_asset_url(owner: str, repo: str, tag: str, asset_name: str) -> str | None:
    if not (owner and repo and tag):
        return None
    return f"https://github.com/{owner}/{repo}/releases/download/{tag}/{asset_name}"


def _part_for_country(parts_dir: Path, country: str) -> Path | None:
    for candidate in (
        parts_dir / f"open-static-{country}.sqlite3",
        parts_dir / f"{country}.sqlite3",
    ):
        if candidate.is_file():
            return candidate
    return None


def _sqlite_countries(path: Path) -> list[str]:
    with sqlite3.connect(path) as connection:
        return [
            str(row[0]).upper()
            for row in connection.execute("SELECT DISTINCT country_code FROM stations ORDER BY country_code")
            if row[0]
        ]


def build_regional_release_assets(
    *,
    parts_dir: Path,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    github_owner: str = "",
    github_repo: str = "",
    github_release_tag: str = "",
    fail_on_missing_country: bool = False,
    selected_group_ids: Sequence[str] = (),
) -> dict[str, Any]:
    if not parts_dir.is_dir():
        raise FileNotFoundError(f"country_parts_dir_missing:{parts_dir}")

    selected = {group_id.strip().upper() for group_id in selected_group_ids if group_id.strip()}
    packs = [pack for pack in REGIONAL_PACKS if not selected or pack.group_id in selected]
    if not packs:
        raise ValueError("no_regional_packs_selected")

    output_dir.mkdir(parents=True, exist_ok=True)
    for stale in output_dir.glob("open-static-*"):
        if stale.is_file():
            stale.unlink()
    for stale in output_dir.glob(".open-static-*.tmp"):
        if stale.is_file():
            stale.unlink()
    index_path = output_dir / "regional_pack_index.json"
    if index_path.exists():
        index_path.unlink()

    generated_at = _utc_now_iso()
    index_packs: list[dict[str, Any]] = []

    for pack in packs:
        part_paths: list[Path] = []
        included_countries: list[str] = []
        missing_countries: list[str] = []
        for country in pack.countries:
            part_path = _part_for_country(parts_dir, country)
            if part_path is None:
                missing_countries.append(country)
                continue
            part_paths.append(part_path)
            included_countries.append(country)

        if missing_countries and fail_on_missing_country:
            raise FileNotFoundError(f"regional_pack_missing_country_parts:{pack.group_id}:{','.join(missing_countries)}")
        if not part_paths:
            continue

        sqlite_path = output_dir / f".{pack.sqlite_file_name}.tmp"
        compressed_sqlite_path = output_dir / pack.compressed_sqlite_file_name
        result = aggregate_sqlite_parts(part_paths=part_paths, output_path=sqlite_path)
        sqlite_sha256 = _sha256_path(sqlite_path)
        sqlite_bytes = sqlite_path.stat().st_size
        _compress_zlib_path(sqlite_path, compressed_sqlite_path)

        checksum_path = output_dir / f"{pack.sqlite_file_name}.sha256"
        checksum_path.write_text(f"{sqlite_sha256}  {pack.sqlite_file_name}\n", encoding="utf-8")
        compressed_sha256 = _sha256_path(compressed_sqlite_path)
        compressed_checksum_path = output_dir / f"{pack.compressed_sqlite_file_name}.sha256"
        compressed_checksum_path.write_text(
            f"{compressed_sha256}  {pack.compressed_sqlite_file_name}\n",
            encoding="utf-8",
        )

        actual_countries = _sqlite_countries(sqlite_path)
        sqlite_payload = {
            "file": pack.sqlite_file_name,
            "bytes": sqlite_bytes,
            "sha256": sqlite_sha256,
        }
        compressed_sqlite_payload = {
            "file": compressed_sqlite_path.name,
            "bytes": compressed_sqlite_path.stat().st_size,
            "sha256": compressed_sha256,
            "algorithm": COMPRESSION_ALGORITHM,
            "uncompressedFile": pack.sqlite_file_name,
            "uncompressedBytes": sqlite_bytes,
            "uncompressedSHA256": sqlite_sha256,
            "url": _release_asset_url(github_owner, github_repo, github_release_tag, compressed_sqlite_path.name),
        }
        checksum_payload = {
            "file": checksum_path.name,
            "bytes": checksum_path.stat().st_size,
            "sha256": _sha256_path(checksum_path),
            "url": _release_asset_url(github_owner, github_repo, github_release_tag, checksum_path.name),
        }
        compressed_checksum_payload = {
            "file": compressed_checksum_path.name,
            "bytes": compressed_checksum_path.stat().st_size,
            "sha256": _sha256_path(compressed_checksum_path),
            "url": _release_asset_url(github_owner, github_repo, github_release_tag, compressed_checksum_path.name),
        }
        manifest = {
            "format": "woladen.open-static.regional-pack.manifest",
            "schemaVersion": SCHEMA_VERSION,
            "version": pack.asset_id,
            "generatedAt": generated_at,
            "schema": "open_static.sqlite3",
            "stationCount": int(result["station_count"]),
            "chargerCount": int(result["charger_count"]),
            "stationAmenityCount": int(result["station_amenity_count"]),
            "chargerAliasCount": int(result["charger_alias_count"]),
            "countries": actual_countries,
            "assetPackGroup": {
                "id": pack.group_id,
                "name": pack.display_name,
                "requestedCountries": list(pack.countries),
                "includedCountries": included_countries,
                "missingCountries": missing_countries,
            },
            "sqlite": sqlite_payload,
            "compressedSQLite": compressed_sqlite_payload,
            "checksum": checksum_payload,
            "compressedChecksum": compressed_checksum_payload,
            "release": {
                "owner": github_owner or None,
                "repo": github_repo or None,
                "tag": github_release_tag or None,
            },
        }
        manifest_path = output_dir / pack.manifest_file_name
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

        index_packs.append(
            {
                "groupID": pack.group_id,
                "displayName": pack.display_name,
                "assetPackID": pack.asset_id,
                "manifestFile": manifest_path.name,
                "sqliteFile": pack.sqlite_file_name,
                "compressedSQLiteFile": compressed_sqlite_path.name,
                "checksumFile": checksum_path.name,
                "compressedChecksumFile": compressed_checksum_path.name,
                "compression": COMPRESSION_ALGORITHM,
                "requestedCountries": list(pack.countries),
                "includedCountries": included_countries,
                "missingCountries": missing_countries,
                "countries": actual_countries,
                "stationCount": int(result["station_count"]),
                "chargerCount": int(result["charger_count"]),
                "sqliteBytes": sqlite_bytes,
                "compressedSQLiteBytes": compressed_sqlite_path.stat().st_size,
                "sqliteSHA256": sqlite_sha256,
                "compressedSQLiteSHA256": compressed_sha256,
            }
        )
        sqlite_path.unlink(missing_ok=True)

    index = {
        "format": "woladen.open-static.regional-pack.index",
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": generated_at,
        "githubOwner": github_owner or None,
        "githubRepo": github_repo or None,
        "githubReleaseTag": github_release_tag or None,
        "packs": index_packs,
    }
    index_path.write_text(json.dumps(index, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {
        "output_dir": str(output_dir.resolve()),
        "pack_count": len(index_packs),
        "packs": index_packs,
        "index_path": str(index_path.resolve()),
    }


def _country_codes_from_text(value: str) -> list[str]:
    return [item.strip().upper() for item in value.split(",") if item.strip()]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build GitHub Release assets for Woladen regional iPhone packages.")
    parser.add_argument("--parts-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--github-owner", default="")
    parser.add_argument("--github-repo", default="")
    parser.add_argument("--github-release-tag", default="")
    parser.add_argument("--group", action="append", default=[], help="Optional group ID to build; repeatable.")
    parser.add_argument("--groups", default="", help="Optional comma-separated group IDs to build.")
    parser.add_argument("--fail-on-missing-country", action="store_true")
    args = parser.parse_args(argv)

    selected_groups = [*args.group, *_country_codes_from_text(args.groups)]
    result = build_regional_release_assets(
        parts_dir=args.parts_dir,
        output_dir=args.output_dir,
        github_owner=args.github_owner,
        github_repo=args.github_repo,
        github_release_tag=args.github_release_tag,
        fail_on_missing_country=args.fail_on_missing_country,
        selected_group_ids=selected_groups,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
