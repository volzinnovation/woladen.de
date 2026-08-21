from __future__ import annotations

import hashlib
import json
import urllib.request
import zlib

import pytest

from scripts.download_latest_open_static_release import (
    DEFAULT_HF_PREFIX,
    DEFAULT_HF_STABLE_ALIAS,
    DEFAULT_SOURCE_REPOSITORY,
    _hf_api_revision_url,
    _hf_resolve_url,
    _release_api_url,
    _repo_from_remote_url,
    discover_immutable_hf_release,
    download_latest_open_static_release,
)


SOURCE_COMMIT = "a" * 40
STABLE_REVISION = "b" * 40
IMMUTABLE_REVISION = "c" * 40
HF_REPO = "loffenauer/AFIR"
RELEASE_TAG = f"{DEFAULT_HF_STABLE_ALIAS}-{SOURCE_COMMIT}"


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


class _FakeOpener:
    def __init__(self, payloads: dict[str, bytes]):
        self.payloads = payloads
        self.calls: list[str] = []

    def __call__(self, request: urllib.request.Request):
        self.calls.append(request.full_url)
        try:
            payload = self.payloads[request.full_url]
        except KeyError as exc:
            raise AssertionError(f"unexpected URL: {request.full_url}") from exc
        return _FakeResponse(payload)


def _json_bytes(document: dict) -> bytes:
    return (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()


def _inventory(payloads: dict[str, bytes]) -> list[dict[str, object]]:
    return [
        {
            "name": name,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "size_bytes": len(payload),
        }
        for name, payload in sorted(payloads.items())
    ]


def _release_documents(
    assets: dict[str, bytes],
) -> tuple[bytes, bytes, dict[str, bytes]]:
    release_manifest = _json_bytes(
        {
            "schema_version": "woladen-open-static-release-v1",
            "release_tag": RELEASE_TAG,
            "source_repository": DEFAULT_SOURCE_REPOSITORY,
            "source_revision": SOURCE_COMMIT,
            "assets": _inventory(assets),
        }
    )
    mirrored_assets = {
        **assets,
        "open_static_release_manifest.json": release_manifest,
    }
    mirror_manifest = _json_bytes(
        {
            "schema_version": "woladen-open-static-hf-release-mirror-v2",
            "release_tag": RELEASE_TAG,
            "source_repository": DEFAULT_SOURCE_REPOSITORY,
            "source_commit": SOURCE_COMMIT,
            "assets": _inventory(mirrored_assets),
        }
    )
    return release_manifest, mirror_manifest, mirrored_assets


def _hf_payloads(
    assets: dict[str, bytes],
    *,
    immutable_manifest: bytes | None = None,
    actual_assets: dict[str, bytes] | None = None,
) -> dict[str, bytes]:
    release_manifest, mirror_manifest, _mirrored_assets = _release_documents(assets)
    immutable_manifest = immutable_manifest or mirror_manifest
    actual_assets = actual_assets or assets
    immutable_base = f"{DEFAULT_HF_PREFIX}/{RELEASE_TAG}"
    return {
        _hf_api_revision_url(HF_REPO, "main"): _json_bytes({"sha": STABLE_REVISION}),
        _hf_api_revision_url(HF_REPO, RELEASE_TAG): _json_bytes(
            {"sha": IMMUTABLE_REVISION}
        ),
        _hf_resolve_url(
            HF_REPO,
            STABLE_REVISION,
            f"{DEFAULT_HF_PREFIX}/{DEFAULT_HF_STABLE_ALIAS}/manifest.json",
        ): mirror_manifest,
        _hf_resolve_url(
            HF_REPO,
            IMMUTABLE_REVISION,
            f"{immutable_base}/manifest.json",
        ): immutable_manifest,
        _hf_resolve_url(
            HF_REPO,
            IMMUTABLE_REVISION,
            f"{DEFAULT_HF_PREFIX}/commits/{SOURCE_COMMIT}/manifest.json",
        ): immutable_manifest,
        _hf_resolve_url(
            HF_REPO,
            IMMUTABLE_REVISION,
            f"{immutable_base}/open_static_release_manifest.json",
        ): release_manifest,
        **{
            _hf_resolve_url(
                HF_REPO,
                IMMUTABLE_REVISION,
                f"{immutable_base}/{name}",
            ): payload
            for name, payload in actual_assets.items()
        },
    }


def _checksum(payload: bytes, name: str) -> bytes:
    return f"{hashlib.sha256(payload).hexdigest()}  {name}\n".encode()


def test_repo_from_remote_url_supports_https_and_ssh():
    assert _repo_from_remote_url(
        "https://github.com/volzinnovation/Woladen.de-analytics.git"
    ) == ("volzinnovation/Woladen.de-analytics")
    assert _repo_from_remote_url(
        "git@github.com:volzinnovation/Woladen.de-analytics.git"
    ) == ("volzinnovation/Woladen.de-analytics")


def test_hf_stable_alias_resolves_and_downloads_only_from_immutable_revision(
    tmp_path,
):
    bundle = b"sqlite bundle"
    assets = {
        "open_static.sqlite3": bundle,
        "open_static.sqlite3.sha256": _checksum(bundle, "open_static.sqlite3"),
    }
    opener = _FakeOpener(_hf_payloads(assets))
    output_path = tmp_path / "open_static.sqlite3"

    result = download_latest_open_static_release(
        repo=DEFAULT_SOURCE_REPOSITORY,
        hf_repo=HF_REPO,
        output_path=output_path,
        asset_name="open_static.sqlite3",
        opener=opener,
    )

    assert output_path.read_bytes() == bundle
    assert result["source"] == "hf_immutable_release"
    assert result["release_tag"] == RELEASE_TAG
    assert result["source_commit"] == SOURCE_COMMIT
    assert result["hf_stable_revision"] == STABLE_REVISION
    assert result["hf_immutable_revision"] == IMMUTABLE_REVISION
    assert result["checksum_verified"] is True
    assert result["manifest_verified"] is True
    assert not any(
        "api.github.com" in url or "/releases/latest" in url for url in opener.calls
    )

    asset_calls = [
        url
        for url in opener.calls
        if "/resolve/" in url
        and not url.endswith(
            f"/{DEFAULT_HF_PREFIX}/{DEFAULT_HF_STABLE_ALIAS}/manifest.json"
        )
    ]
    assert asset_calls
    assert all(f"/resolve/{IMMUTABLE_REVISION}/" in url for url in asset_calls)


def test_regional_zlib_download_verifies_package_manifest_and_expanded_checksum(
    tmp_path,
):
    sqlite = b"regional sqlite bundle"
    asset_name = "open-static-DACH.sqlite3.zlib"
    sqlite_name = "open-static-DACH.sqlite3"
    compressed = zlib.compress(sqlite, level=9)
    sqlite_checksum = _checksum(sqlite, sqlite_name)
    compressed_checksum = _checksum(compressed, asset_name)
    manifest_name = "open-static-DACH.manifest.json"
    manifest = _json_bytes(
        {
            "format": "woladen.open-static.regional-pack.manifest",
            "schemaVersion": 2,
            "sourceRevision": SOURCE_COMMIT,
            "version": "open-static-DACH",
            "generatedAt": "2026-08-14T00:00:00Z",
            "schema": "open_static.sqlite3",
            "stationCount": 1,
            "chargerCount": 1,
            "stationAmenityCount": 1,
            "chargerAliasCount": 0,
            "countries": ["DE"],
            "assetPackGroup": {
                "id": "DACH",
                "name": "DACH",
                "requestedCountries": ["DE", "AT", "CH"],
                "includedCountries": ["DE"],
                "missingCountries": ["AT", "CH"],
            },
            "sqlite": {
                "file": sqlite_name,
                "bytes": len(sqlite),
                "sha256": hashlib.sha256(sqlite).hexdigest(),
            },
            "compressedSQLite": {
                "file": asset_name,
                "bytes": len(compressed),
                "sha256": hashlib.sha256(compressed).hexdigest(),
                "algorithm": "zlib",
                "uncompressedFile": sqlite_name,
                "uncompressedBytes": len(sqlite),
                "uncompressedSHA256": hashlib.sha256(sqlite).hexdigest(),
                "url": (
                    f"https://github.com/{DEFAULT_SOURCE_REPOSITORY}/releases/"
                    f"download/{RELEASE_TAG}/{asset_name}"
                ),
            },
            "checksum": {
                "file": f"{sqlite_name}.sha256",
                "bytes": len(sqlite_checksum),
                "sha256": hashlib.sha256(sqlite_checksum).hexdigest(),
                "url": (
                    f"https://github.com/{DEFAULT_SOURCE_REPOSITORY}/releases/"
                    f"download/{RELEASE_TAG}/{sqlite_name}.sha256"
                ),
            },
            "compressedChecksum": {
                "file": f"{asset_name}.sha256",
                "bytes": len(compressed_checksum),
                "sha256": hashlib.sha256(compressed_checksum).hexdigest(),
                "url": (
                    f"https://github.com/{DEFAULT_SOURCE_REPOSITORY}/releases/"
                    f"download/{RELEASE_TAG}/{asset_name}.sha256"
                ),
            },
            "release": {
                "owner": "volzinnovation",
                "repo": "Woladen.de-analytics",
                "tag": RELEASE_TAG,
            },
        }
    )
    assets = {
        asset_name: compressed,
        f"{asset_name}.sha256": compressed_checksum,
        f"{sqlite_name}.sha256": sqlite_checksum,
        manifest_name: manifest,
    }
    opener = _FakeOpener(_hf_payloads(assets))
    output_path = tmp_path / sqlite_name

    result = download_latest_open_static_release(
        repo=DEFAULT_SOURCE_REPOSITORY,
        hf_repo=HF_REPO,
        output_path=output_path,
        asset_name=asset_name,
        keep_compressed=True,
        opener=opener,
    )

    assert output_path.read_bytes() == sqlite
    assert output_path.with_name(asset_name).read_bytes() == compressed
    assert result["decompressed"] is True
    assert result["download_checksum_verified"] is True
    assert result["decompressed_checksum_verified"] is True


def test_discovery_rejects_mutable_github_style_tag_in_stable_manifest():
    release_manifest, mirror_manifest, mirrored_assets = _release_documents(
        {
            "open_static.sqlite3": b"sqlite",
            "open_static.sqlite3.sha256": _checksum(b"sqlite", "open_static.sqlite3"),
        }
    )
    del release_manifest, mirrored_assets
    document = json.loads(mirror_manifest)
    document["release_tag"] = DEFAULT_HF_STABLE_ALIAS
    bad_manifest = _json_bytes(document)
    payloads = {
        _hf_api_revision_url(HF_REPO, "main"): _json_bytes({"sha": STABLE_REVISION}),
        _hf_resolve_url(
            HF_REPO,
            STABLE_REVISION,
            f"{DEFAULT_HF_PREFIX}/{DEFAULT_HF_STABLE_ALIAS}/manifest.json",
        ): bad_manifest,
    }
    opener = _FakeOpener(payloads)

    with pytest.raises(ValueError, match="hf_mirror_manifest_release_tag_invalid"):
        discover_immutable_hf_release(
            repo_id=HF_REPO,
            prefix=DEFAULT_HF_PREFIX,
            stable_alias=DEFAULT_HF_STABLE_ALIAS,
            source_repository=DEFAULT_SOURCE_REPOSITORY,
            opener=opener,
        )

    assert not any(
        "api.github.com" in url or "/releases/latest" in url for url in opener.calls
    )


def test_discovery_rejects_stable_and_immutable_manifest_mismatch():
    assets = {
        "open_static.sqlite3": b"sqlite",
        "open_static.sqlite3.sha256": _checksum(b"sqlite", "open_static.sqlite3"),
    }
    _release_manifest, mirror_manifest, _mirrored_assets = _release_documents(assets)
    opener = _FakeOpener(
        _hf_payloads(assets, immutable_manifest=mirror_manifest + b" ")
    )

    with pytest.raises(RuntimeError, match="hf_release_manifest_alias_mismatch"):
        discover_immutable_hf_release(
            repo_id=HF_REPO,
            prefix=DEFAULT_HF_PREFIX,
            stable_alias=DEFAULT_HF_STABLE_ALIAS,
            source_repository=DEFAULT_SOURCE_REPOSITORY,
            opener=opener,
        )


def test_tampered_asset_fails_without_replacing_existing_output(tmp_path):
    expected = b"sqlite"
    assets = {
        "open_static.sqlite3": expected,
        "open_static.sqlite3.sha256": _checksum(expected, "open_static.sqlite3"),
    }
    opener = _FakeOpener(
        _hf_payloads(
            assets,
            actual_assets={
                **assets,
                "open_static.sqlite3": b"tamper",
            },
        )
    )
    output_path = tmp_path / "open_static.sqlite3"
    output_path.write_bytes(b"previous verified bundle")

    with pytest.raises(RuntimeError, match="release_asset_sha256_mismatch"):
        download_latest_open_static_release(
            repo=DEFAULT_SOURCE_REPOSITORY,
            hf_repo=HF_REPO,
            output_path=output_path,
            asset_name="open_static.sqlite3",
            opener=opener,
        )

    assert output_path.read_bytes() == b"previous verified bundle"


def test_github_mode_never_uses_latest_or_an_implicit_tag(tmp_path):
    def unexpected_open(_request: urllib.request.Request):
        raise AssertionError("network request must not start without an immutable tag")

    with pytest.raises(ValueError, match="github_immutable_release_tag_required"):
        download_latest_open_static_release(
            repo=DEFAULT_SOURCE_REPOSITORY,
            hf_repo="",
            prefer_hf_mirror=False,
            output_path=tmp_path / "open_static.sqlite3",
            opener=unexpected_open,
        )


def test_explicit_github_mode_requires_and_verifies_an_immutable_inventory(tmp_path):
    bundle = b"immutable github sqlite"
    assets = {
        "open_static.sqlite3": bundle,
        "open_static.sqlite3.sha256": _checksum(bundle, "open_static.sqlite3"),
    }
    release_manifest, _mirror_manifest, mirrored_assets = _release_documents(assets)
    urls = {
        name: f"https://api.github.test/assets/{index}"
        for index, name in enumerate(sorted(mirrored_assets), start=1)
    }
    release = {
        "tag_name": RELEASE_TAG,
        "draft": False,
        "immutable": True,
        "assets": [
            {
                "name": name,
                "url": urls[name],
                "state": "uploaded",
                "size": len(payload),
                "digest": f"sha256:{hashlib.sha256(payload).hexdigest()}",
            }
            for name, payload in sorted(mirrored_assets.items())
        ],
    }
    opener = _FakeOpener(
        {
            _release_api_url(DEFAULT_SOURCE_REPOSITORY, RELEASE_TAG): _json_bytes(
                release
            ),
            urls["open_static_release_manifest.json"]: release_manifest,
            **{urls[name]: payload for name, payload in assets.items()},
        }
    )
    output_path = tmp_path / "open_static.sqlite3"

    result = download_latest_open_static_release(
        repo=DEFAULT_SOURCE_REPOSITORY,
        hf_repo="",
        prefer_hf_mirror=False,
        tag=RELEASE_TAG,
        output_path=output_path,
        asset_name="open_static.sqlite3",
        opener=opener,
    )

    assert output_path.read_bytes() == bundle
    assert result["source"] == "github_immutable_release"
    assert result["release_tag"] == RELEASE_TAG
    assert not any("/releases/latest" in url for url in opener.calls)


def test_checksum_verification_cannot_be_disabled(tmp_path):
    with pytest.raises(ValueError, match="open_static_checksum_verification_required"):
        download_latest_open_static_release(
            repo=DEFAULT_SOURCE_REPOSITORY,
            hf_repo=HF_REPO,
            output_path=tmp_path / "open_static.sqlite3",
            require_checksum=False,
        )
