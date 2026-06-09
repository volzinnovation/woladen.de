from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import urllib.request

import pytest

from scripts.download_latest_open_static_release import (
    _repo_from_remote_url,
    download_latest_open_static_release,
)


class _FakeResponse:
    def __init__(self, payload: bytes):
        self._payload = payload
        self._offset = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            size = len(self._payload) - self._offset
        chunk = self._payload[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


def test_repo_from_remote_url_supports_https_and_ssh():
    assert _repo_from_remote_url("https://github.com/volzinnovation/woladen.de.git") == (
        "volzinnovation/woladen.de"
    )
    assert _repo_from_remote_url("git@github.com:volzinnovation/woladen.de.git") == (
        "volzinnovation/woladen.de"
    )


def _zstd_compress(payload_path):
    zstd = shutil.which("zstd")
    if not zstd:
        pytest.skip("zstd is not installed")
    subprocess.run([zstd, "-19", "-f", str(payload_path), "-o", f"{payload_path}.zst"], check=True)
    return payload_path.with_name(f"{payload_path.name}.zst")


def test_downloads_compressed_latest_release_asset_and_verifies_checksums(tmp_path):
    bundle = b"sqlite bundle"
    bundle_path = tmp_path / "source.sqlite3"
    bundle_path.write_bytes(bundle)
    compressed_path = _zstd_compress(bundle_path)
    compressed = compressed_path.read_bytes()
    raw_digest = hashlib.sha256(bundle).hexdigest()
    compressed_digest = hashlib.sha256(compressed).hexdigest()
    release = {
        "tag_name": "open-static-sqlite-42-abcdef0",
        "assets": [
            {"name": "open_static.sqlite3.zst", "url": "https://api.github.com/assets/1"},
            {"name": "open_static.sqlite3.zst.sha256", "url": "https://api.github.com/assets/2"},
            {"name": "open_static.sqlite3.sha256", "url": "https://api.github.com/assets/3"},
        ],
    }
    payloads = {
        "https://api.github.com/repos/volzinnovation/woladen.de/releases/latest": json.dumps(
            release
        ).encode("utf-8"),
        "https://api.github.com/assets/1": compressed,
        "https://api.github.com/assets/2": f"{compressed_digest}  open_static.sqlite3.zst\n".encode("utf-8"),
        "https://api.github.com/assets/3": f"{raw_digest}  open_static.sqlite3\n".encode("utf-8"),
    }

    def opener(request: urllib.request.Request):
        return _FakeResponse(payloads[request.full_url])

    output_path = tmp_path / "open_static.sqlite3"
    result = download_latest_open_static_release(
        repo="volzinnovation/woladen.de",
        output_path=output_path,
        opener=opener,
    )

    assert output_path.read_bytes() == bundle
    assert result["release_tag"] == "open-static-sqlite-42-abcdef0"
    assert result["checksum_verified"] is True
    assert result["download_checksum_verified"] is True
    assert result["decompressed_checksum_verified"] is True
    assert result["decompressed"] is True
    assert result["size_bytes"] == len(compressed)
    assert not output_path.with_name("open_static.sqlite3.zst").exists()


def test_downloads_legacy_uncompressed_release_asset_when_requested(tmp_path):
    bundle = b"sqlite bundle"
    digest = hashlib.sha256(bundle).hexdigest()
    release = {
        "tag_name": "open-static-sqlite-42-abcdef0",
        "assets": [
            {"name": "open_static.sqlite3", "url": "https://api.github.com/assets/1"},
            {"name": "open_static.sqlite3.sha256", "url": "https://api.github.com/assets/2"},
        ],
    }
    payloads = {
        "https://api.github.com/repos/volzinnovation/woladen.de/releases/latest": json.dumps(
            release
        ).encode("utf-8"),
        "https://api.github.com/assets/1": bundle,
        "https://api.github.com/assets/2": f"{digest}  open_static.sqlite3\n".encode("utf-8"),
    }

    def opener(request: urllib.request.Request):
        return _FakeResponse(payloads[request.full_url])

    output_path = tmp_path / "open_static.sqlite3"
    result = download_latest_open_static_release(
        repo="volzinnovation/woladen.de",
        asset_name="open_static.sqlite3",
        output_path=output_path,
        opener=opener,
    )

    assert output_path.read_bytes() == bundle
    assert result["checksum_verified"] is True
    assert result["download_checksum_verified"] is True
    assert result["decompressed"] is False


def test_default_download_falls_back_to_legacy_uncompressed_asset(tmp_path):
    bundle = b"sqlite bundle"
    digest = hashlib.sha256(bundle).hexdigest()
    release = {
        "tag_name": "open-static-sqlite-42-abcdef0",
        "assets": [
            {"name": "open_static.sqlite3", "url": "https://api.github.com/assets/1"},
            {"name": "open_static.sqlite3.sha256", "url": "https://api.github.com/assets/2"},
        ],
    }
    payloads = {
        "https://api.github.com/repos/volzinnovation/woladen.de/releases/latest": json.dumps(
            release
        ).encode("utf-8"),
        "https://api.github.com/assets/1": bundle,
        "https://api.github.com/assets/2": f"{digest}  open_static.sqlite3\n".encode("utf-8"),
    }

    def opener(request: urllib.request.Request):
        return _FakeResponse(payloads[request.full_url])

    output_path = tmp_path / "open_static.sqlite3"
    result = download_latest_open_static_release(
        repo="volzinnovation/woladen.de",
        output_path=output_path,
        opener=opener,
    )

    assert output_path.read_bytes() == bundle
    assert result["requested_asset_name"] == "open_static.sqlite3.zst"
    assert result["asset_name"] == "open_static.sqlite3"
    assert result["decompressed"] is False


def test_requires_checksum_when_requested(tmp_path):
    release = {
        "tag_name": "open-static-sqlite-42-abcdef0",
        "assets": [{"name": "open_static.sqlite3.zst", "url": "https://api.github.com/assets/1"}],
    }
    payloads = {
        "https://api.github.com/repos/volzinnovation/woladen.de/releases/latest": json.dumps(
            release
        ).encode("utf-8"),
        "https://api.github.com/assets/1": b"compressed sqlite bundle",
    }

    def opener(request: urllib.request.Request):
        return _FakeResponse(payloads[request.full_url])

    with pytest.raises(FileNotFoundError):
        download_latest_open_static_release(
            repo="volzinnovation/woladen.de",
            output_path=tmp_path / "open_static.sqlite3",
            require_checksum=True,
            opener=opener,
        )
