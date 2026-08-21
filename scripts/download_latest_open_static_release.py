#!/usr/bin/env python3
"""Download one checksum-bound asset from an immutable open-static release."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import ssl
import subprocess
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import zlib
from pathlib import Path
from typing import Any, BinaryIO, Callable, Mapping, Sequence


DEFAULT_ASSET_NAME = "open_static.sqlite3.zst"
DEFAULT_OUTPUT_PATH = Path("data/eu27_ch_static/open_static.sqlite3")
DEFAULT_SOURCE_REPOSITORY = "volzinnovation/Woladen.de-analytics"
DEFAULT_HF_REPO = "loffenauer/AFIR"
DEFAULT_HF_PREFIX = "AFIR/open-static/releases"
DEFAULT_HF_STABLE_ALIAS = "open-static-ios-regional-latest"
HF_API = "https://huggingface.co/api"
HF_ROOT = "https://huggingface.co"
GITHUB_API = "https://api.github.com"
HF_MIRROR_SCHEMA_VERSION = "woladen-open-static-hf-release-mirror-v2"
RELEASE_MANIFEST_SCHEMA_VERSION = "woladen-open-static-release-v1"
REGIONAL_MANIFEST_FORMAT = "woladen.open-static.regional-pack.manifest"
REGIONAL_MANIFEST_SCHEMA_VERSION = 2
FULL_GIT_REVISION_PATTERN = re.compile(r"[0-9a-f]{40}")
FULL_HF_REVISION_PATTERN = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")


def _repo_from_git_remote() -> str:
    try:
        remote = subprocess.check_output(
            ["git", "config", "--get", "remote.origin.url"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        remote = ""
    return _repo_from_remote_url(remote)


def _repo_from_remote_url(remote_url: str) -> str:
    text = remote_url.strip()
    if not text:
        return ""
    match = re.search(r"github\.com[:/]([^/\s]+/[^/\s]+?)(?:\.git)?$", text)
    return match.group(1) if match else ""


def _resolve_source_repository(explicit_repo: str | None) -> str:
    repo = (
        explicit_repo
        or os.getenv("WOLADEN_OPEN_STATIC_RELEASE_REPO")
        or DEFAULT_SOURCE_REPOSITORY
    ).strip()
    if not re.fullmatch(r"[^/\s]+/[^/\s]+", repo):
        raise ValueError("repo must be owner/name; pass --repo")
    return repo


def _github_headers(
    token: str | None = None,
    accept: str = "application/vnd.github+json",
) -> dict[str, str]:
    headers = {
        "Accept": accept,
        "User-Agent": "woladen-open-static-release-downloader",
        "X-GitHub-Api-Version": "2026-03-10",
    }
    value = (
        token
        or os.getenv("WOLADEN_REPO_TOKEN")
        or os.getenv("GH_TOKEN")
        or os.getenv("GITHUB_TOKEN")
        or ""
    ).strip()
    if value:
        headers["Authorization"] = f"Bearer {value}"
    return headers


def _hf_headers(token: str | None = None) -> dict[str, str]:
    headers = {"User-Agent": "woladen-open-static-release-downloader"}
    value = (
        token
        or os.getenv("WOLADEN_OPEN_STATIC_HF_TOKEN")
        or os.getenv("HF_PRIVATE")
        or os.getenv("HF_TOKEN")
        or ""
    ).strip()
    if value:
        headers["Authorization"] = f"Bearer {value}"
    return headers


def _default_urlopen(request: urllib.request.Request) -> BinaryIO:
    try:
        return urllib.request.urlopen(request)
    except urllib.error.URLError as exc:
        if not isinstance(getattr(exc, "reason", None), ssl.SSLCertVerificationError):
            raise
        try:
            import certifi
        except ImportError:
            raise
        context = ssl.create_default_context(cafile=certifi.where())
        return urllib.request.urlopen(request, context=context)


def _read_url(
    url: str,
    *,
    headers: Mapping[str, str],
    opener: Callable[..., BinaryIO] | None,
    error_prefix: str,
) -> bytes:
    request = urllib.request.Request(url, headers=dict(headers))
    open_fn = opener or _default_urlopen
    try:
        with open_fn(request) as response:
            return response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{error_prefix}:{exc.code}:{url}:{body}") from exc


def _download(
    url: str,
    output_path: Path,
    *,
    headers: Mapping[str, str],
    opener: Callable[..., BinaryIO] | None,
    error_prefix: str,
) -> int:
    request = urllib.request.Request(url, headers=dict(headers))
    open_fn = opener or _default_urlopen
    output_path.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    try:
        with open_fn(request) as response, output_path.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                handle.write(chunk)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{error_prefix}:{exc.code}:{url}:{body}") from exc
    return total


def _strict_json_object(payload: bytes, *, label: str) -> dict[str, Any]:
    def object_pairs_hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{label}_duplicate_key:{key}")
            result[key] = value
        return result

    try:
        document = json.loads(
            payload.decode("utf-8"), object_pairs_hook=object_pairs_hook
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label}_invalid_json") from exc
    if not isinstance(document, dict):
        raise ValueError(f"{label}_object_required")
    return document


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _parse_sha256(payload: bytes, *, expected_name: str) -> str:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"invalid_sha256_file:{expected_name}") from exc
    match = re.fullmatch(
        r"\s*([0-9a-fA-F]{64})[ \t]+\*?([^\s]+)\s*",
        text,
    )
    if match is None or match.group(2) != expected_name:
        raise ValueError(f"invalid_sha256_file:{expected_name}")
    return match.group(1).lower()


def _manifest_inventory(
    document: Mapping[str, Any], *, label: str
) -> dict[str, dict[str, Any]]:
    rows = document.get("assets")
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"{label}_assets_invalid")
    inventory: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or set(row) != {
            "name",
            "size_bytes",
            "sha256",
        }:
            raise ValueError(f"{label}_asset_invalid")
        name = row.get("name")
        size_bytes = row.get("size_bytes")
        sha256 = row.get("sha256")
        if (
            not isinstance(name, str)
            or not name
            or name in {".", ".."}
            or "/" in name
            or "\\" in name
            or name in inventory
            or not isinstance(size_bytes, int)
            or isinstance(size_bytes, bool)
            or size_bytes <= 0
            or not isinstance(sha256, str)
            or re.fullmatch(r"[0-9a-f]{64}", sha256) is None
        ):
            raise ValueError(f"{label}_asset_invalid")
        inventory[name] = dict(row)
    return inventory


def _validate_mirror_manifest(
    document: Mapping[str, Any],
    *,
    source_repository: str,
    stable_alias: str,
) -> tuple[str, str, dict[str, dict[str, Any]]]:
    if set(document) != {
        "schema_version",
        "release_tag",
        "source_repository",
        "source_commit",
        "assets",
    }:
        raise ValueError("hf_mirror_manifest_shape_invalid")
    if document.get("schema_version") != HF_MIRROR_SCHEMA_VERSION:
        raise ValueError("hf_mirror_manifest_schema_unsupported")
    if document.get("source_repository") != source_repository:
        raise ValueError("hf_mirror_manifest_repository_mismatch")

    source_commit = document.get("source_commit")
    release_tag = document.get("release_tag")
    if (
        not isinstance(source_commit, str)
        or FULL_GIT_REVISION_PATTERN.fullmatch(source_commit) is None
    ):
        raise ValueError("hf_mirror_manifest_source_commit_invalid")
    expected_tag = f"{stable_alias}-{source_commit}"
    if release_tag != expected_tag:
        raise ValueError("hf_mirror_manifest_release_tag_invalid")

    inventory = _manifest_inventory(document, label="hf_mirror_manifest")
    if "open_static_release_manifest.json" not in inventory:
        raise ValueError("hf_mirror_release_manifest_missing")
    return release_tag, source_commit, inventory


def _validate_release_manifest(
    document: Mapping[str, Any],
    *,
    release_tag: str,
    source_repository: str,
    source_commit: str,
    mirror_inventory: Mapping[str, dict[str, Any]],
) -> None:
    if set(document) != {
        "schema_version",
        "release_tag",
        "source_repository",
        "source_revision",
        "assets",
    }:
        raise ValueError("open_static_release_manifest_shape_invalid")
    if document.get("schema_version") != RELEASE_MANIFEST_SCHEMA_VERSION:
        raise ValueError("open_static_release_manifest_schema_unsupported")
    if document.get("release_tag") != release_tag:
        raise ValueError("open_static_release_manifest_tag_mismatch")
    if document.get("source_repository") != source_repository:
        raise ValueError("open_static_release_manifest_repository_mismatch")
    if document.get("source_revision") != source_commit:
        raise ValueError("open_static_release_manifest_revision_mismatch")
    release_inventory = _manifest_inventory(
        document, label="open_static_release_manifest"
    )
    expected_inventory = {
        name: row
        for name, row in mirror_inventory.items()
        if name != "open_static_release_manifest.json"
    }
    if release_inventory != expected_inventory:
        raise ValueError("open_static_release_manifest_inventory_mismatch")


def _verify_inventory_payload(
    payload: bytes,
    *,
    name: str,
    inventory: Mapping[str, dict[str, Any]],
) -> None:
    row = inventory.get(name)
    if row is None:
        raise FileNotFoundError(f"release_manifest_asset_missing:{name}")
    if len(payload) != row["size_bytes"]:
        raise RuntimeError(
            f"release_asset_size_mismatch:{name}:"
            f"expected={row['size_bytes']}:actual={len(payload)}"
        )
    actual = _sha256_bytes(payload)
    if actual != row["sha256"]:
        raise RuntimeError(
            f"release_asset_sha256_mismatch:{name}:"
            f"expected={row['sha256']}:actual={actual}"
        )


def _verify_inventory_file(
    path: Path,
    *,
    name: str,
    inventory: Mapping[str, dict[str, Any]],
) -> None:
    row = inventory.get(name)
    if row is None:
        raise FileNotFoundError(f"release_manifest_asset_missing:{name}")
    actual_size = path.stat().st_size
    if actual_size != row["size_bytes"]:
        raise RuntimeError(
            f"release_asset_size_mismatch:{name}:"
            f"expected={row['size_bytes']}:actual={actual_size}"
        )
    actual_sha256 = _sha256_file(path)
    if actual_sha256 != row["sha256"]:
        raise RuntimeError(
            f"release_asset_sha256_mismatch:{name}:"
            f"expected={row['sha256']}:actual={actual_sha256}"
        )


def _hf_api_revision_url(repo_id: str, revision: str) -> str:
    repo = urllib.parse.quote(repo_id.strip(), safe="/")
    encoded_revision = urllib.parse.quote(revision.strip(), safe="")
    return f"{HF_API}/datasets/{repo}/revision/{encoded_revision}"


def _hf_resolve_url(repo_id: str, revision: str, remote_path: str) -> str:
    repo = urllib.parse.quote(repo_id.strip(), safe="/")
    encoded_revision = urllib.parse.quote(revision.strip(), safe="")
    encoded_path = urllib.parse.quote(remote_path.strip("/"), safe="/")
    return f"{HF_ROOT}/datasets/{repo}/resolve/{encoded_revision}/{encoded_path}"


def _hf_revision_oid(
    *,
    repo_id: str,
    revision: str,
    token: str | None,
    opener: Callable[..., BinaryIO] | None,
) -> str:
    payload = _read_url(
        _hf_api_revision_url(repo_id, revision),
        headers=_hf_headers(token),
        opener=opener,
        error_prefix="hf_revision_api_error",
    )
    document = _strict_json_object(payload, label="hf_revision")
    oid = str(document.get("sha") or "").strip().lower()
    if FULL_HF_REVISION_PATTERN.fullmatch(oid) is None:
        raise RuntimeError(f"hf_revision_oid_invalid:{revision}")
    return oid


def _hf_file_payload(
    *,
    repo_id: str,
    revision: str,
    remote_path: str,
    token: str | None,
    opener: Callable[..., BinaryIO] | None,
) -> bytes:
    return _read_url(
        _hf_resolve_url(repo_id, revision, remote_path),
        headers=_hf_headers(token),
        opener=opener,
        error_prefix="hf_asset_download_error",
    )


def _hf_download_file(
    *,
    repo_id: str,
    revision: str,
    remote_path: str,
    output_path: Path,
    token: str | None,
    opener: Callable[..., BinaryIO] | None,
) -> int:
    return _download(
        _hf_resolve_url(repo_id, revision, remote_path),
        output_path,
        headers=_hf_headers(token),
        opener=opener,
        error_prefix="hf_asset_download_error",
    )


def _release_path(prefix: str, alias: str, name: str) -> str:
    clean_prefix = prefix.strip("/")
    clean_alias = alias.strip("/")
    if not clean_prefix or not clean_alias:
        raise ValueError("hf_release_path_invalid")
    if any(part in {"", ".", ".."} for part in clean_prefix.split("/")):
        raise ValueError("hf_prefix_invalid")
    if "/" in clean_alias or clean_alias in {".", ".."}:
        raise ValueError("hf_alias_invalid")
    return f"{clean_prefix}/{clean_alias}/{name}"


def discover_immutable_hf_release(
    *,
    repo_id: str,
    prefix: str,
    stable_alias: str,
    source_repository: str,
    token: str | None = None,
    opener: Callable[..., BinaryIO] | None = None,
) -> dict[str, Any]:
    """Resolve a mutable channel once, then return only immutable coordinates."""

    if not re.fullmatch(r"[^/\s]+/[^/\s]+", repo_id.strip()):
        raise ValueError("hf_repo_must_be_owner_name")
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", stable_alias) is None:
        raise ValueError("hf_stable_alias_invalid")
    if stable_alias == "latest":
        raise ValueError("hf_latest_alias_not_allowed")

    stable_revision = _hf_revision_oid(
        repo_id=repo_id,
        revision="main",
        token=token,
        opener=opener,
    )
    stable_manifest_payload = _hf_file_payload(
        repo_id=repo_id,
        revision=stable_revision,
        remote_path=_release_path(prefix, stable_alias, "manifest.json"),
        token=token,
        opener=opener,
    )
    stable_manifest = _strict_json_object(
        stable_manifest_payload, label="hf_stable_manifest"
    )
    release_tag, source_commit, inventory = _validate_mirror_manifest(
        stable_manifest,
        source_repository=source_repository,
        stable_alias=stable_alias,
    )

    immutable_revision = _hf_revision_oid(
        repo_id=repo_id,
        revision=release_tag,
        token=token,
        opener=opener,
    )
    immutable_manifest_payload = _hf_file_payload(
        repo_id=repo_id,
        revision=immutable_revision,
        remote_path=_release_path(prefix, release_tag, "manifest.json"),
        token=token,
        opener=opener,
    )
    commit_manifest_payload = _hf_file_payload(
        repo_id=repo_id,
        revision=immutable_revision,
        remote_path=(f"{prefix.strip('/')}/commits/{source_commit}/manifest.json"),
        token=token,
        opener=opener,
    )
    if not (
        stable_manifest_payload == immutable_manifest_payload == commit_manifest_payload
    ):
        raise RuntimeError("hf_release_manifest_alias_mismatch")

    immutable_manifest = _strict_json_object(
        immutable_manifest_payload, label="hf_immutable_manifest"
    )
    immutable_tag, immutable_source_commit, immutable_inventory = (
        _validate_mirror_manifest(
            immutable_manifest,
            source_repository=source_repository,
            stable_alias=stable_alias,
        )
    )
    if (
        immutable_tag != release_tag
        or immutable_source_commit != source_commit
        or immutable_inventory != inventory
    ):
        raise RuntimeError("hf_release_manifest_identity_mismatch")

    release_manifest_payload = _hf_file_payload(
        repo_id=repo_id,
        revision=immutable_revision,
        remote_path=_release_path(
            prefix, release_tag, "open_static_release_manifest.json"
        ),
        token=token,
        opener=opener,
    )
    _verify_inventory_payload(
        release_manifest_payload,
        name="open_static_release_manifest.json",
        inventory=inventory,
    )
    release_manifest = _strict_json_object(
        release_manifest_payload, label="open_static_release_manifest"
    )
    _validate_release_manifest(
        release_manifest,
        release_tag=release_tag,
        source_repository=source_repository,
        source_commit=source_commit,
        mirror_inventory=inventory,
    )

    final_tag_revision = _hf_revision_oid(
        repo_id=repo_id,
        revision=release_tag,
        token=token,
        opener=opener,
    )
    if final_tag_revision != immutable_revision:
        raise RuntimeError("hf_immutable_tag_moved_during_discovery")

    return {
        "schema_version": "woladen-open-static-discovery-v1",
        "hf_repo": repo_id,
        "hf_prefix": prefix.strip("/"),
        "hf_stable_alias": stable_alias,
        "stable_revision": stable_revision,
        "release_tag": release_tag,
        "source_repository": source_repository,
        "source_commit": source_commit,
        "immutable_revision": immutable_revision,
        "inventory": inventory,
    }


def _compression_suffix(asset_name: str) -> str:
    for suffix in (".zst", ".zlib"):
        if asset_name.endswith(suffix):
            return suffix
    return ""


def _decompressed_asset_name(asset_name: str) -> str:
    suffix = _compression_suffix(asset_name)
    return asset_name[: -len(suffix)] if suffix else asset_name


def _resolve_zstd_binary() -> str:
    binary = shutil.which("zstd")
    if not binary:
        raise RuntimeError("zstd_binary_not_found_in_path")
    return binary


def _decompress_asset(
    *, compressed_path: Path, output_path: Path, asset_name: str
) -> None:
    suffix = _compression_suffix(asset_name)
    if suffix == ".zst":
        with output_path.open("wb") as handle:
            subprocess.run(
                [_resolve_zstd_binary(), "-d", "-c", str(compressed_path)],
                check=True,
                stdout=handle,
            )
        return
    if suffix == ".zlib":
        decompressor = zlib.decompressobj()
        with compressed_path.open("rb") as source, output_path.open("wb") as output:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                output.write(decompressor.decompress(chunk))
            output.write(decompressor.flush())
        if not decompressor.eof:
            raise RuntimeError("zlib_stream_incomplete")
        return
    raise ValueError("decompress requires a .zst or .zlib asset")


def _verify_checksum_payload(
    payload: bytes,
    *,
    checksum_name: str,
    target_name: str,
    target_path: Path,
    inventory: Mapping[str, dict[str, Any]],
) -> str:
    _verify_inventory_payload(payload, name=checksum_name, inventory=inventory)
    expected = _parse_sha256(payload, expected_name=target_name)
    actual = _sha256_file(target_path)
    if actual != expected:
        raise RuntimeError(
            f"sha256_mismatch:{target_name}:expected={expected}:actual={actual}"
        )
    return expected


def _validate_regional_manifest(
    document: Mapping[str, Any],
    *,
    asset_name: str,
    release_tag: str,
    source_repository: str,
    source_commit: str,
    inventory: Mapping[str, dict[str, Any]],
) -> None:
    match = re.fullmatch(r"(open-static-[A-Z-]+)\.sqlite3\.zlib", asset_name)
    if match is None:
        return
    asset_id = match.group(1)
    manifest_name = f"{asset_id}.manifest.json"
    if document.get("format") != REGIONAL_MANIFEST_FORMAT:
        raise ValueError("regional_manifest_format_invalid")
    if document.get("schemaVersion") != REGIONAL_MANIFEST_SCHEMA_VERSION:
        raise ValueError("regional_manifest_schema_unsupported")
    if document.get("sourceRevision") != source_commit:
        raise ValueError("regional_manifest_source_revision_mismatch")
    if document.get("version") != asset_id:
        raise ValueError("regional_manifest_version_mismatch")

    sqlite_name = _decompressed_asset_name(asset_name)
    checksum_name = f"{sqlite_name}.sha256"
    compressed_checksum_name = f"{asset_name}.sha256"
    required_names = {
        manifest_name,
        asset_name,
        checksum_name,
        compressed_checksum_name,
    }
    if not required_names.issubset(inventory):
        raise ValueError("regional_manifest_inventory_incomplete")

    compressed = document.get("compressedSQLite")
    sqlite = document.get("sqlite")
    checksum = document.get("checksum")
    compressed_checksum = document.get("compressedChecksum")
    release = document.get("release")
    if not all(
        isinstance(value, dict)
        for value in (compressed, sqlite, checksum, compressed_checksum, release)
    ):
        raise ValueError("regional_manifest_shape_invalid")

    compressed_row = inventory[asset_name]
    sqlite_checksum_payload = inventory[checksum_name]
    compressed_checksum_payload = inventory[compressed_checksum_name]
    if (
        compressed.get("file") != asset_name
        or compressed.get("bytes") != compressed_row["size_bytes"]
        or compressed.get("sha256") != compressed_row["sha256"]
        or compressed.get("algorithm") != "zlib"
        or compressed.get("uncompressedFile") != sqlite_name
        or sqlite.get("file") != sqlite_name
        or compressed.get("uncompressedBytes") != sqlite.get("bytes")
        or compressed.get("uncompressedSHA256") != sqlite.get("sha256")
        or checksum.get("file") != checksum_name
        or checksum.get("bytes") != sqlite_checksum_payload["size_bytes"]
        or checksum.get("sha256") != sqlite_checksum_payload["sha256"]
        or compressed_checksum.get("file") != compressed_checksum_name
        or compressed_checksum.get("bytes") != compressed_checksum_payload["size_bytes"]
        or compressed_checksum.get("sha256") != compressed_checksum_payload["sha256"]
    ):
        raise ValueError("regional_manifest_asset_binding_mismatch")

    owner, repo = source_repository.split("/", 1)
    expected_asset_url = (
        f"https://github.com/{source_repository}/releases/download/"
        f"{release_tag}/{asset_name}"
    )
    expected_checksum_url = (
        f"https://github.com/{source_repository}/releases/download/"
        f"{release_tag}/{checksum_name}"
    )
    expected_compressed_checksum_url = (
        f"https://github.com/{source_repository}/releases/download/"
        f"{release_tag}/{compressed_checksum_name}"
    )
    if (
        release.get("owner") != owner
        or release.get("repo") != repo
        or release.get("tag") != release_tag
        or compressed.get("url") != expected_asset_url
        or checksum.get("url") != expected_checksum_url
        or compressed_checksum.get("url") != expected_compressed_checksum_url
    ):
        raise ValueError("regional_manifest_release_binding_mismatch")


def _download_hf_release_asset(
    *,
    discovery: Mapping[str, Any],
    output_path: Path,
    asset_name: str,
    decompress: bool | None,
    keep_compressed: bool,
    token: str | None,
    opener: Callable[..., BinaryIO] | None,
) -> dict[str, Any]:
    inventory = discovery["inventory"]
    if asset_name not in inventory:
        available = ",".join(sorted(inventory))
        raise FileNotFoundError(
            f"asset_not_found:{asset_name}:release={discovery['release_tag']}:"
            f"available={available}"
        )

    suffix = _compression_suffix(asset_name)
    should_decompress = bool(suffix) if decompress is None else decompress
    if should_decompress and not suffix:
        raise ValueError("decompress requires a .zst or .zlib asset")

    final_output_path = output_path
    compressed_output_path = output_path
    if should_decompress:
        if output_path.name.endswith(suffix):
            compressed_output_path = output_path
            final_output_path = output_path.with_name(output_path.name[: -len(suffix)])
        else:
            compressed_output_path = output_path.with_name(
                f"{output_path.name}{suffix}"
            )

    final_output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".{final_output_path.name}.download-",
        dir=final_output_path.parent,
    ) as temporary_directory:
        temporary_root = Path(temporary_directory)
        downloaded_path = temporary_root / asset_name
        size_bytes = _hf_download_file(
            repo_id=discovery["hf_repo"],
            revision=discovery["immutable_revision"],
            remote_path=_release_path(
                discovery["hf_prefix"], discovery["release_tag"], asset_name
            ),
            output_path=downloaded_path,
            token=token,
            opener=opener,
        )
        _verify_inventory_file(downloaded_path, name=asset_name, inventory=inventory)

        compressed_checksum_name = f"{asset_name}.sha256"
        compressed_checksum_payload = _hf_file_payload(
            repo_id=discovery["hf_repo"],
            revision=discovery["immutable_revision"],
            remote_path=_release_path(
                discovery["hf_prefix"],
                discovery["release_tag"],
                compressed_checksum_name,
            ),
            token=token,
            opener=opener,
        )
        _verify_checksum_payload(
            compressed_checksum_payload,
            checksum_name=compressed_checksum_name,
            target_name=asset_name,
            target_path=downloaded_path,
            inventory=inventory,
        )

        regional_match = re.fullmatch(
            r"(open-static-[A-Z-]+)\.sqlite3\.zlib", asset_name
        )
        if regional_match is not None:
            regional_manifest_name = f"{regional_match.group(1)}.manifest.json"
            regional_manifest_payload = _hf_file_payload(
                repo_id=discovery["hf_repo"],
                revision=discovery["immutable_revision"],
                remote_path=_release_path(
                    discovery["hf_prefix"],
                    discovery["release_tag"],
                    regional_manifest_name,
                ),
                token=token,
                opener=opener,
            )
            _verify_inventory_payload(
                regional_manifest_payload,
                name=regional_manifest_name,
                inventory=inventory,
            )
            regional_manifest = _strict_json_object(
                regional_manifest_payload, label="regional_manifest"
            )
            _validate_regional_manifest(
                regional_manifest,
                asset_name=asset_name,
                release_tag=discovery["release_tag"],
                source_repository=discovery["source_repository"],
                source_commit=discovery["source_commit"],
                inventory=inventory,
            )

        expanded_checksum_payload: bytes | None = None
        if should_decompress:
            expanded_name = _decompressed_asset_name(asset_name)
            expanded_path = temporary_root / expanded_name
            _decompress_asset(
                compressed_path=downloaded_path,
                output_path=expanded_path,
                asset_name=asset_name,
            )
            expanded_checksum_name = f"{expanded_name}.sha256"
            expanded_checksum_payload = _hf_file_payload(
                repo_id=discovery["hf_repo"],
                revision=discovery["immutable_revision"],
                remote_path=_release_path(
                    discovery["hf_prefix"],
                    discovery["release_tag"],
                    expanded_checksum_name,
                ),
                token=token,
                opener=opener,
            )
            expanded_sha256 = _verify_checksum_payload(
                expanded_checksum_payload,
                checksum_name=expanded_checksum_name,
                target_name=expanded_name,
                target_path=expanded_path,
                inventory=inventory,
            )
            if regional_match is not None:
                regional_sqlite = regional_manifest.get("sqlite")
                if (
                    not isinstance(regional_sqlite, dict)
                    or regional_sqlite.get("bytes") != expanded_path.stat().st_size
                    or regional_sqlite.get("sha256") != expanded_sha256
                ):
                    raise RuntimeError("regional_manifest_expanded_asset_mismatch")
            os.replace(expanded_path, final_output_path)
            final_checksum_path = final_output_path.with_name(
                f"{final_output_path.name}.sha256"
            )
            final_checksum_path.write_bytes(expanded_checksum_payload)
            if keep_compressed:
                os.replace(downloaded_path, compressed_output_path)
                compressed_output_path.with_name(
                    f"{compressed_output_path.name}.sha256"
                ).write_bytes(compressed_checksum_payload)
        else:
            os.replace(downloaded_path, final_output_path)
            final_output_path.with_name(f"{final_output_path.name}.sha256").write_bytes(
                compressed_checksum_payload
            )

    return {
        "repo": discovery["source_repository"],
        "hf_repo": discovery["hf_repo"],
        "hf_prefix": discovery["hf_prefix"],
        "hf_stable_alias": discovery["hf_stable_alias"],
        "hf_stable_revision": discovery["stable_revision"],
        "hf_immutable_revision": discovery["immutable_revision"],
        "source": "hf_immutable_release",
        "release_tag": discovery["release_tag"],
        "source_commit": discovery["source_commit"],
        "requested_asset_name": asset_name,
        "asset_name": asset_name,
        "compressed": bool(suffix),
        "decompressed": should_decompress,
        "download_path": str(compressed_output_path.resolve()),
        "output_path": str(final_output_path.resolve()),
        "size_bytes": size_bytes,
        "checksum_verified": True,
        "download_checksum_verified": True,
        "decompressed_checksum_verified": should_decompress,
        "manifest_verified": True,
    }


def _release_api_url(repo: str, tag: str | None) -> str:
    if not tag:
        raise ValueError("github_immutable_release_tag_required")
    encoded_tag = urllib.parse.quote(tag, safe="")
    return f"{GITHUB_API}/repos/{repo}/releases/tags/{encoded_tag}"


def _asset_by_name(release: Mapping[str, Any], asset_name: str) -> dict[str, Any]:
    assets = release.get("assets")
    if not isinstance(assets, list):
        raise RuntimeError("release_assets_missing")
    for asset in assets:
        if isinstance(asset, dict) and asset.get("name") == asset_name:
            return asset
    tag = release.get("tag_name") or "unknown"
    available = sorted(
        str(asset.get("name", "")) for asset in assets if isinstance(asset, dict)
    )
    raise FileNotFoundError(
        f"asset_not_found:{asset_name}:release={tag}:available={','.join(available)}"
    )


def _validate_github_release_inventory(
    release: Mapping[str, Any],
    *,
    expected_inventory: Mapping[str, dict[str, Any]],
) -> None:
    assets = release.get("assets")
    if not isinstance(assets, list):
        raise RuntimeError("release_assets_missing")
    actual: dict[str, dict[str, Any]] = {}
    for asset in assets:
        if not isinstance(asset, dict):
            raise RuntimeError("github_release_asset_invalid")
        name = asset.get("name")
        if not isinstance(name, str) or not name or name in actual:
            raise RuntimeError("github_release_asset_invalid")
        if asset.get("state") != "uploaded":
            raise RuntimeError(f"github_release_asset_not_uploaded:{name}")
        actual[name] = asset
    if set(actual) != set(expected_inventory):
        raise RuntimeError("github_release_inventory_mismatch")
    for name, expected in expected_inventory.items():
        asset = actual[name]
        if (
            asset.get("size") != expected["size_bytes"]
            or asset.get("digest") != f"sha256:{expected['sha256']}"
        ):
            raise RuntimeError(f"github_release_asset_binding_mismatch:{name}")


def _download_github_immutable_release(
    *,
    repo: str,
    tag: str,
    output_path: Path,
    asset_name: str,
    decompress: bool | None,
    keep_compressed: bool,
    token: str | None,
    opener: Callable[..., BinaryIO] | None,
) -> dict[str, Any]:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{40}", tag) is None:
        raise ValueError("github_revision_scoped_release_tag_required")
    release_payload = _read_url(
        _release_api_url(repo, tag),
        headers=_github_headers(token),
        opener=opener,
        error_prefix="github_api_error",
    )
    release = _strict_json_object(release_payload, label="github_release")
    if (
        release.get("tag_name") != tag
        or release.get("draft") is not False
        or release.get("immutable") is not True
    ):
        raise RuntimeError("github_release_not_verified_immutable")

    manifest_asset = _asset_by_name(release, "open_static_release_manifest.json")
    manifest_url = str(manifest_asset.get("url") or "")
    if not manifest_url:
        raise RuntimeError(
            "release_asset_url_missing:open_static_release_manifest.json"
        )
    manifest_payload = _read_url(
        manifest_url,
        headers=_github_headers(token, accept="application/octet-stream"),
        opener=opener,
        error_prefix="github_asset_download_error",
    )
    manifest = _strict_json_object(
        manifest_payload, label="open_static_release_manifest"
    )
    source_commit = str(manifest.get("source_revision") or "")
    if FULL_GIT_REVISION_PATTERN.fullmatch(source_commit) is None or not tag.endswith(
        f"-{source_commit}"
    ):
        raise ValueError("github_revision_scoped_release_tag_required")
    release_inventory = _manifest_inventory(
        manifest, label="open_static_release_manifest"
    )
    mirror_inventory = {
        "open_static_release_manifest.json": {
            "name": "open_static_release_manifest.json",
            "size_bytes": len(manifest_payload),
            "sha256": _sha256_bytes(manifest_payload),
        },
        **release_inventory,
    }
    _validate_release_manifest(
        manifest,
        release_tag=tag,
        source_repository=repo,
        source_commit=source_commit,
        mirror_inventory=mirror_inventory,
    )
    _validate_github_release_inventory(
        release,
        expected_inventory=mirror_inventory,
    )

    # GitHub is an explicit immutable-release mode only. Reuse the same local
    # verification semantics without pretending it is an HF discovery result.
    suffix = _compression_suffix(asset_name)
    should_decompress = bool(suffix) if decompress is None else decompress
    if should_decompress and not suffix:
        raise ValueError("decompress requires a .zst or .zlib asset")
    asset = _asset_by_name(release, asset_name)
    checksum = _asset_by_name(release, f"{asset_name}.sha256")
    if not asset.get("url") or not checksum.get("url"):
        raise RuntimeError(f"release_asset_url_missing:{asset_name}")

    final_output_path = output_path
    compressed_output_path = output_path
    if should_decompress:
        if output_path.name.endswith(suffix):
            compressed_output_path = output_path
            final_output_path = output_path.with_name(output_path.name[: -len(suffix)])
        else:
            compressed_output_path = output_path.with_name(
                f"{output_path.name}{suffix}"
            )
    final_output_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".{final_output_path.name}.download-",
        dir=final_output_path.parent,
    ) as temporary_directory:
        temporary_root = Path(temporary_directory)
        downloaded_path = temporary_root / asset_name
        size_bytes = _download(
            str(asset["url"]),
            downloaded_path,
            headers=_github_headers(token, accept="application/octet-stream"),
            opener=opener,
            error_prefix="github_asset_download_error",
        )
        _verify_inventory_file(
            downloaded_path, name=asset_name, inventory=release_inventory
        )
        checksum_payload = _read_url(
            str(checksum["url"]),
            headers=_github_headers(token, accept="application/octet-stream"),
            opener=opener,
            error_prefix="github_asset_download_error",
        )
        _verify_checksum_payload(
            checksum_payload,
            checksum_name=f"{asset_name}.sha256",
            target_name=asset_name,
            target_path=downloaded_path,
            inventory=release_inventory,
        )

        regional_match = re.fullmatch(
            r"(open-static-[A-Z-]+)\.sqlite3\.zlib", asset_name
        )
        regional_manifest: Mapping[str, Any] | None = None
        if regional_match is not None:
            regional_manifest_name = f"{regional_match.group(1)}.manifest.json"
            regional_manifest_asset = _asset_by_name(release, regional_manifest_name)
            regional_manifest_url = str(regional_manifest_asset.get("url") or "")
            if not regional_manifest_url:
                raise RuntimeError(
                    f"release_asset_url_missing:{regional_manifest_name}"
                )
            regional_manifest_payload = _read_url(
                regional_manifest_url,
                headers=_github_headers(token, accept="application/octet-stream"),
                opener=opener,
                error_prefix="github_asset_download_error",
            )
            _verify_inventory_payload(
                regional_manifest_payload,
                name=regional_manifest_name,
                inventory=release_inventory,
            )
            regional_manifest = _strict_json_object(
                regional_manifest_payload, label="regional_manifest"
            )
            _validate_regional_manifest(
                regional_manifest,
                asset_name=asset_name,
                release_tag=tag,
                source_repository=repo,
                source_commit=source_commit,
                inventory=release_inventory,
            )

        if should_decompress:
            expanded_name = _decompressed_asset_name(asset_name)
            expanded_path = temporary_root / expanded_name
            _decompress_asset(
                compressed_path=downloaded_path,
                output_path=expanded_path,
                asset_name=asset_name,
            )
            expanded_checksum_asset = _asset_by_name(release, f"{expanded_name}.sha256")
            expanded_checksum_url = str(expanded_checksum_asset.get("url") or "")
            if not expanded_checksum_url:
                raise RuntimeError(f"release_asset_url_missing:{expanded_name}.sha256")
            expanded_checksum_payload = _read_url(
                expanded_checksum_url,
                headers=_github_headers(token, accept="application/octet-stream"),
                opener=opener,
                error_prefix="github_asset_download_error",
            )
            _verify_checksum_payload(
                expanded_checksum_payload,
                checksum_name=f"{expanded_name}.sha256",
                target_name=expanded_name,
                target_path=expanded_path,
                inventory=release_inventory,
            )
            if regional_manifest is not None:
                regional_sqlite = regional_manifest.get("sqlite")
                if (
                    not isinstance(regional_sqlite, dict)
                    or regional_sqlite.get("bytes") != expanded_path.stat().st_size
                    or regional_sqlite.get("sha256") != _sha256_file(expanded_path)
                ):
                    raise RuntimeError("regional_manifest_expanded_asset_mismatch")
            os.replace(expanded_path, final_output_path)
            final_output_path.with_name(f"{final_output_path.name}.sha256").write_bytes(
                expanded_checksum_payload
            )
            if keep_compressed:
                os.replace(downloaded_path, compressed_output_path)
                compressed_output_path.with_name(
                    f"{compressed_output_path.name}.sha256"
                ).write_bytes(checksum_payload)
        else:
            os.replace(downloaded_path, final_output_path)
            final_output_path.with_name(f"{final_output_path.name}.sha256").write_bytes(
                checksum_payload
            )

    return {
        "repo": repo,
        "hf_repo": "",
        "source": "github_immutable_release",
        "release_tag": tag,
        "source_commit": source_commit,
        "requested_asset_name": asset_name,
        "asset_name": asset_name,
        "compressed": bool(suffix),
        "decompressed": should_decompress,
        "download_path": str(compressed_output_path.resolve()),
        "output_path": str(final_output_path.resolve()),
        "size_bytes": size_bytes,
        "checksum_verified": True,
        "download_checksum_verified": True,
        "decompressed_checksum_verified": should_decompress,
        "manifest_verified": True,
    }


def download_latest_open_static_release(
    *,
    repo: str | None,
    output_path: Path,
    asset_name: str = DEFAULT_ASSET_NAME,
    tag: str | None = None,
    hf_repo: str | None = None,
    hf_prefix: str = DEFAULT_HF_PREFIX,
    hf_stable_alias: str = DEFAULT_HF_STABLE_ALIAS,
    prefer_hf_mirror: bool = True,
    require_checksum: bool = True,
    decompress: bool | None = None,
    keep_compressed: bool = False,
    token: str | None = None,
    hf_token: str | None = None,
    opener: Callable[..., BinaryIO] | None = None,
) -> dict[str, Any]:
    """Download from HF discovery, or an explicitly selected immutable GitHub tag.

    ``require_checksum`` remains in the API for callers of the old helper, but
    disabling verification is no longer permitted.
    """

    if not require_checksum:
        raise ValueError("open_static_checksum_verification_required")
    source_repository = _resolve_source_repository(repo)
    resolved_hf_repo = (
        hf_repo
        if hf_repo is not None
        else os.getenv("WOLADEN_OPEN_STATIC_HF_REPO") or DEFAULT_HF_REPO
    ).strip()
    if prefer_hf_mirror:
        if not resolved_hf_repo:
            raise ValueError("hf_repo_required_for_stable_release_discovery")
        if tag:
            raise ValueError("hf_stable_discovery_does_not_accept_release_tag")
        discovery = discover_immutable_hf_release(
            repo_id=resolved_hf_repo,
            prefix=hf_prefix,
            stable_alias=hf_stable_alias,
            source_repository=source_repository,
            token=hf_token,
            opener=opener,
        )
        return _download_hf_release_asset(
            discovery=discovery,
            output_path=output_path,
            asset_name=asset_name,
            decompress=decompress,
            keep_compressed=keep_compressed,
            token=hf_token,
            opener=opener,
        )

    if not tag:
        raise ValueError("github_immutable_release_tag_required")
    return _download_github_immutable_release(
        repo=source_repository,
        tag=tag,
        output_path=output_path,
        asset_name=asset_name,
        decompress=decompress,
        keep_compressed=keep_compressed,
        token=token,
        opener=opener,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--repo",
        default=os.getenv("WOLADEN_OPEN_STATIC_RELEASE_REPO")
        or DEFAULT_SOURCE_REPOSITORY,
        help="Expected source repository in owner/name form.",
    )
    parser.add_argument(
        "--tag",
        default=os.getenv("WOLADEN_OPEN_STATIC_RELEASE_TAG") or None,
        help=(
            "Exact immutable GitHub release tag; valid only with "
            "--no-hf-mirror. GitHub Latest and mutable tags are not supported."
        ),
    )
    parser.add_argument("--asset-name", default=DEFAULT_ASSET_NAME)
    parser.add_argument(
        "--hf-repo",
        default=os.getenv("WOLADEN_OPEN_STATIC_HF_REPO") or DEFAULT_HF_REPO,
        help="HF dataset repo holding the publisher mirror.",
    )
    parser.add_argument(
        "--hf-prefix",
        default=os.getenv("WOLADEN_OPEN_STATIC_HF_PREFIX") or DEFAULT_HF_PREFIX,
        help="Prefix for mirrored release assets in the HF dataset.",
    )
    parser.add_argument(
        "--hf-stable-alias",
        default=os.getenv("WOLADEN_OPEN_STATIC_HF_STABLE_ALIAS")
        or DEFAULT_HF_STABLE_ALIAS,
        help="Publisher-promoted HF stable channel directory (never 'latest').",
    )
    parser.add_argument(
        "--no-hf-mirror",
        action="store_true",
        help="Use only the exact immutable GitHub tag supplied with --tag.",
    )
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument(
        "--require-checksum",
        action="store_true",
        default=True,
        help="Retained for compatibility; checksum and manifest verification are mandatory.",
    )
    parser.add_argument(
        "--no-decompress",
        action="store_true",
        help="Keep the verified compressed asset as --output-path.",
    )
    parser.add_argument(
        "--keep-compressed",
        action="store_true",
        help="Keep the verified compressed download beside the expanded output.",
    )
    args = parser.parse_args(argv)

    result = download_latest_open_static_release(
        repo=args.repo,
        tag=args.tag,
        hf_repo=args.hf_repo,
        hf_prefix=args.hf_prefix,
        hf_stable_alias=args.hf_stable_alias,
        prefer_hf_mirror=not args.no_hf_mirror,
        asset_name=args.asset_name,
        output_path=args.output_path,
        require_checksum=args.require_checksum,
        decompress=False if args.no_decompress else None,
        keep_compressed=args.keep_compressed,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
