#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import ssl
import subprocess
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, BinaryIO, Callable, Sequence


DEFAULT_ASSET_NAME = "open_static.sqlite3.zst"
DEFAULT_OUTPUT_PATH = Path("data/eu27_ch_static/open_static.sqlite3")
GITHUB_API = "https://api.github.com"


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


def _resolve_repo(explicit_repo: str | None) -> str:
    repo = (explicit_repo or os.getenv("GITHUB_REPOSITORY") or "").strip()
    if not repo:
        repo = _repo_from_git_remote()
    if not re.fullmatch(r"[^/\s]+/[^/\s]+", repo):
        raise ValueError("repo must be owner/name; pass --repo or set GITHUB_REPOSITORY")
    return repo


def _github_headers(token: str | None = None, accept: str = "application/vnd.github+json") -> dict[str, str]:
    headers = {
        "Accept": accept,
        "User-Agent": "woladen-open-static-release-downloader",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    value = (token or os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN") or "").strip()
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


def _urlopen_json(url: str, headers: dict[str, str], opener: Callable[..., BinaryIO] | None = None) -> dict[str, Any]:
    request = urllib.request.Request(url, headers=headers)
    open_fn = opener or _default_urlopen
    try:
        with open_fn(request) as response:
            value = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"github_api_error:{exc.code}:{url}:{body}") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"github_api_unexpected_response:{url}")
    return value


def _download(url: str, output_path: Path, headers: dict[str, str], opener: Callable[..., BinaryIO] | None = None) -> int:
    request = urllib.request.Request(url, headers=headers)
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
        raise RuntimeError(f"github_asset_download_error:{exc.code}:{url}:{body}") from exc
    return total


def _release_api_url(repo: str, tag: str | None) -> str:
    if tag:
        return f"{GITHUB_API}/repos/{repo}/releases/tags/{tag}"
    return f"{GITHUB_API}/repos/{repo}/releases/latest"


def _asset_by_name(release: dict[str, Any], asset_name: str) -> dict[str, Any]:
    assets = release.get("assets")
    if not isinstance(assets, list):
        raise RuntimeError("release_assets_missing")
    for asset in assets:
        if isinstance(asset, dict) and asset.get("name") == asset_name:
            return asset
    tag = release.get("tag_name") or "unknown"
    available = sorted(str(asset.get("name", "")) for asset in assets if isinstance(asset, dict))
    raise FileNotFoundError(f"asset_not_found:{asset_name}:release={tag}:available={','.join(available)}")


def _parse_sha256(text: str) -> str:
    first = text.strip().split()[0] if text.strip() else ""
    if not re.fullmatch(r"[a-fA-F0-9]{64}", first):
        raise ValueError("invalid_sha256_file")
    return first.lower()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _is_zstd_asset(asset_name: str) -> bool:
    return asset_name.endswith(".zst")


def _decompressed_asset_name(asset_name: str) -> str:
    return asset_name[:-4] if _is_zstd_asset(asset_name) else asset_name


def _resolve_zstd_binary() -> str:
    binary = shutil.which("zstd")
    if not binary:
        raise RuntimeError("zstd_binary_not_found_in_path")
    return binary


def _decompress_zstd(compressed_path: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_name(f".{output_path.name}.tmp")
    try:
        with temp_path.open("wb") as handle:
            subprocess.run([_resolve_zstd_binary(), "-d", "-c", str(compressed_path)], check=True, stdout=handle)
        temp_path.replace(output_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _download_and_verify_checksum(
    *,
    release: dict[str, Any],
    checksum_name: str,
    checksum_path: Path,
    target_path: Path,
    require_checksum: bool,
    token: str | None,
    opener: Callable[..., BinaryIO] | None,
) -> bool:
    try:
        checksum_asset = _asset_by_name(release, checksum_name)
    except FileNotFoundError:
        if require_checksum:
            raise
        return False

    checksum_url = str(checksum_asset.get("url") or "")
    if not checksum_url:
        raise RuntimeError(f"release_asset_url_missing:{checksum_name}")
    _download(
        checksum_url,
        checksum_path,
        headers=_github_headers(token, accept="application/octet-stream"),
        opener=opener,
    )
    expected = _parse_sha256(checksum_path.read_text(encoding="utf-8"))
    actual = _sha256_file(target_path)
    if actual != expected:
        raise RuntimeError(f"sha256_mismatch:{target_path}:expected={expected}:actual={actual}")
    return True


def download_latest_open_static_release(
    *,
    repo: str | None,
    output_path: Path,
    asset_name: str = DEFAULT_ASSET_NAME,
    tag: str | None = None,
    require_checksum: bool = False,
    decompress: bool | None = None,
    keep_compressed: bool = False,
    token: str | None = None,
    opener: Callable[..., BinaryIO] | None = None,
) -> dict[str, Any]:
    resolved_repo = _resolve_repo(repo)
    headers = _github_headers(token)
    release = _urlopen_json(_release_api_url(resolved_repo, tag), headers=headers, opener=opener)
    requested_asset_name = asset_name
    try:
        asset = _asset_by_name(release, asset_name)
    except FileNotFoundError:
        if asset_name != DEFAULT_ASSET_NAME:
            raise
        asset_name = _decompressed_asset_name(DEFAULT_ASSET_NAME)
        asset = _asset_by_name(release, asset_name)
    asset_url = str(asset.get("url") or "")
    if not asset_url:
        raise RuntimeError(f"release_asset_url_missing:{asset_name}")

    compressed_asset = _is_zstd_asset(asset_name)
    should_decompress = compressed_asset if decompress is None else decompress
    if should_decompress and not compressed_asset:
        raise ValueError("decompress requires a .zst asset")

    final_output_path = output_path
    download_path = output_path
    if should_decompress:
        if output_path.name.endswith(".zst"):
            download_path = output_path
            final_output_path = output_path.with_name(output_path.name[:-4])
        else:
            download_path = output_path.with_name(f"{output_path.name}.zst")

    size_bytes = _download(
        asset_url,
        download_path,
        headers=_github_headers(token, accept="application/octet-stream"),
        opener=opener,
    )

    checksum_name = f"{asset_name}.sha256"
    download_checksum_path = download_path.with_name(f"{download_path.name}.sha256")
    download_checksum_verified = _download_and_verify_checksum(
        release=release,
        checksum_name=checksum_name,
        checksum_path=download_checksum_path,
        target_path=download_path,
        require_checksum=require_checksum,
        token=token,
        opener=opener,
    )

    decompressed_checksum_verified = False
    if should_decompress:
        _decompress_zstd(download_path, final_output_path)
        raw_checksum_name = f"{_decompressed_asset_name(asset_name)}.sha256"
        raw_checksum_path = final_output_path.with_name(f"{final_output_path.name}.sha256")
        decompressed_checksum_verified = _download_and_verify_checksum(
            release=release,
            checksum_name=raw_checksum_name,
            checksum_path=raw_checksum_path,
            target_path=final_output_path,
            require_checksum=require_checksum,
            token=token,
            opener=opener,
        )
        if not keep_compressed:
            download_path.unlink(missing_ok=True)
            download_checksum_path.unlink(missing_ok=True)

    return {
        "repo": resolved_repo,
        "release_tag": release.get("tag_name"),
        "requested_asset_name": requested_asset_name,
        "asset_name": asset_name,
        "compressed": compressed_asset,
        "decompressed": should_decompress,
        "download_path": str(download_path.resolve()),
        "output_path": str(final_output_path.resolve()),
        "size_bytes": size_bytes,
        "checksum_verified": download_checksum_verified or decompressed_checksum_verified,
        "download_checksum_verified": download_checksum_verified,
        "decompressed_checksum_verified": decompressed_checksum_verified,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Download the latest Woladen open-static SQLite bundle release.")
    parser.add_argument(
        "--repo",
        default=os.getenv("WOLADEN_OPEN_STATIC_RELEASE_REPO"),
        help="GitHub repository in owner/name form. Defaults to WOLADEN_OPEN_STATIC_RELEASE_REPO, GITHUB_REPOSITORY, or origin.",
    )
    parser.add_argument(
        "--tag",
        default=os.getenv("WOLADEN_OPEN_STATIC_RELEASE_TAG") or None,
        help="Download from a specific release tag instead of /releases/latest. Defaults to WOLADEN_OPEN_STATIC_RELEASE_TAG.",
    )
    parser.add_argument("--asset-name", default=DEFAULT_ASSET_NAME)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--require-checksum", action="store_true")
    parser.add_argument(
        "--no-decompress",
        action="store_true",
        help="Keep the downloaded asset as-is. By default .zst assets are decompressed to --output-path.",
    )
    parser.add_argument(
        "--keep-compressed",
        action="store_true",
        help="When decompressing a .zst asset, keep the compressed download beside the output file.",
    )
    args = parser.parse_args(argv)

    result = download_latest_open_static_release(
        repo=args.repo,
        tag=args.tag,
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
