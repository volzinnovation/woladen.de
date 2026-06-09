from __future__ import annotations

import hashlib
import io
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import pytest

from commercial_backend.http_fetch import stream_request_to_file


@dataclass
class FakeHeaders:
    values: dict[str, str]

    def items(self):
        return self.values.items()


class FakeResponse:
    def __init__(self, body: bytes, *, status: int = 200, headers: dict[str, str] | None = None):
        self._body = io.BytesIO(body)
        self.status = status
        self.headers = FakeHeaders(headers or {"Content-Type": "application/json"})
        self.url = "https://example.test/final"

    def read(self, size: int = -1) -> bytes:
        return self._body.read(size)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


def test_stream_request_to_file_hashes_and_records_headers(tmp_path: Path) -> None:
    request = urllib.request.Request("https://example.test/feed")

    def opener(request, timeout=None):
        assert timeout == 5
        return FakeResponse(b'{"ok":true}', headers={"Content-Encoding": "gzip"})

    result = stream_request_to_file(
        request,
        tmp_path / "payload.bin",
        timeout_seconds=5,
        opener=opener,
    )

    assert result.payload_path.read_bytes() == b'{"ok":true}'
    assert result.payload_sha256 == hashlib.sha256(b'{"ok":true}').hexdigest()
    assert result.byte_length == len(b'{"ok":true}')
    assert result.headers == {"content-encoding": "gzip"}
    assert result.final_url == "https://example.test/final"


def test_stream_request_to_file_retries_retryable_http_errors(tmp_path: Path, monkeypatch) -> None:
    request = urllib.request.Request("https://example.test/feed")
    calls = 0
    monkeypatch.setattr("commercial_backend.http_fetch.time.sleep", lambda _seconds: None)

    def opener(request, timeout=None):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise urllib.error.HTTPError(
                request.full_url,
                503,
                "Service Unavailable",
                hdrs={},
                fp=io.BytesIO(b"retry later"),
            )
        return FakeResponse(b"ok")

    result = stream_request_to_file(
        request,
        tmp_path / "payload.bin",
        timeout_seconds=5,
        opener=opener,
        retry_backoff_seconds=0,
    )

    assert calls == 2
    assert result.payload_path.read_bytes() == b"ok"


def test_stream_request_to_file_deletes_partial_payload_after_final_failure(
    tmp_path: Path,
    monkeypatch,
) -> None:
    request = urllib.request.Request("https://example.test/feed")
    target = tmp_path / "payload.bin"
    monkeypatch.setattr("commercial_backend.http_fetch.time.sleep", lambda _seconds: None)

    def opener(request, timeout=None):
        target.write_bytes(b"partial")
        raise urllib.error.URLError("network down")

    with pytest.raises(urllib.error.URLError):
        stream_request_to_file(
            request,
            target,
            timeout_seconds=5,
            opener=opener,
            max_attempts=2,
            retry_backoff_seconds=0,
        )

    assert not target.exists()
