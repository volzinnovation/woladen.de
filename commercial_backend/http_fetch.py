from __future__ import annotations

import hashlib
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


RETRYABLE_HTTP_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


@dataclass(frozen=True)
class StreamedFetchResult:
    payload_path: Path
    payload_sha256: str
    byte_length: int
    headers: dict[str, str]
    status_code: int
    final_url: str


def stream_request_to_file(
    request: urllib.request.Request,
    payload_path: Path,
    *,
    timeout_seconds: float,
    opener: Callable[..., object] = urllib.request.urlopen,
    max_attempts: int = 3,
    retry_backoff_seconds: float = 0.75,
    allow_insecure_tls_retry: bool = False,
) -> StreamedFetchResult:
    attempts = max(1, int(max_attempts))
    insecure_context = ssl._create_unverified_context() if allow_insecure_tls_retry else None
    last_error: BaseException | None = None
    for attempt_index in range(attempts):
        context = insecure_context if attempt_index > 0 and insecure_context is not None else None
        try:
            return _stream_once(
                request,
                payload_path,
                timeout_seconds=timeout_seconds,
                opener=opener,
                context=context,
            )
        except urllib.error.HTTPError as exc:
            last_error = exc
            retryable = exc.code in RETRYABLE_HTTP_STATUS_CODES
        except (TimeoutError, urllib.error.URLError, ssl.SSLError, OSError) as exc:
            last_error = exc
            retryable = True

        payload_path.unlink(missing_ok=True)
        if not retryable or attempt_index >= attempts - 1:
            break
        time.sleep(retry_backoff_seconds * (2**attempt_index))

    assert last_error is not None
    raise last_error


def _stream_once(
    request: urllib.request.Request,
    payload_path: Path,
    *,
    timeout_seconds: float,
    opener: Callable[..., object],
    context: ssl.SSLContext | None,
) -> StreamedFetchResult:
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    response = _open_request(opener, request, timeout_seconds=timeout_seconds, context=context)
    with response:
        payload_sha256 = hashlib.sha256()
        byte_length = 0
        with payload_path.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)
                payload_sha256.update(chunk)
                byte_length += len(chunk)
        headers = {key.lower(): value for key, value in response.headers.items()}
        return StreamedFetchResult(
            payload_path=payload_path,
            payload_sha256=payload_sha256.hexdigest(),
            byte_length=byte_length,
            headers=headers,
            status_code=int(getattr(response, "status", 200) or 200),
            final_url=str(getattr(response, "url", request.full_url) or request.full_url),
        )


def _open_request(
    opener: Callable[..., object],
    request: urllib.request.Request,
    *,
    timeout_seconds: float,
    context: ssl.SSLContext | None,
) -> object:
    if context is None:
        return opener(request, timeout=timeout_seconds)
    return opener(request, timeout=timeout_seconds, context=context)
