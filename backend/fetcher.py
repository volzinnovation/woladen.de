from __future__ import annotations

import shutil
import subprocess
import tempfile
from pathlib import Path

from .config import AppConfig
from .models import FetchResponse, ProviderTarget

MTLS_PROBE_URL = "https://mobilithek.info:8443/mobilithek/api/v1.0/subscription/datexv3?subscriptionID=0"


def _parse_status_code(header_text: str) -> int:
    for line in header_text.splitlines():
        if line.startswith("HTTP/"):
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                return int(parts[1])
    return 0


def _parse_content_type(header_text: str) -> str:
    for line in header_text.splitlines():
        if line.lower().startswith("content-type:"):
            return line.split(":", 1)[1].strip()
    return ""


class CurlFetcher:
    def __init__(self, config: AppConfig):
        self.config = config
        self.curl_path = shutil.which("curl") or "/usr/bin/curl"
        if not Path(self.curl_path).exists():
            raise RuntimeError("curl_not_found")

    def fetch(self, provider: ProviderTarget) -> FetchResponse:
        if provider.fetch_kind == "publication_file_auth":
            raise RuntimeError("auth_publication_fetch_not_configured")
        return self.fetch_url(provider.fetch_url, use_mtls=provider.fetch_kind.startswith("mtls_"))

    def fetch_url(self, url: str, *, use_mtls: bool, allowed_statuses: set[int] | None = None) -> FetchResponse:
        password = ""
        with tempfile.TemporaryDirectory(prefix="woladen-live-fetch-") as temp_dir:
            header_path = Path(temp_dir) / "headers.txt"
            body_path = Path(temp_dir) / "body.bin"
            command = [
                self.curl_path,
                "-sS",
                "-L",
                "--max-time",
                str(self.config.poll_timeout_seconds),
                "-D",
                str(header_path),
                "-o",
                str(body_path),
                "-H",
                "Accept: application/json, application/octet-stream",
                "-H",
                "Accept-Encoding: gzip",
            ]

            if use_mtls:
                password = self.config.cert_password()
                command.extend(
                    [
                        "--cert-type",
                        "P12",
                        "--cert",
                        f"{self.config.machine_cert_p12}:{password}",
                    ]
                )

            command.append(url)

            try:
                result = subprocess.run(
                    command,
                    check=False,
                    capture_output=True,
                    timeout=self.config.poll_timeout_seconds + 2,
                )
            except subprocess.TimeoutExpired as exc:
                raise TimeoutError(f"{provider.provider_uid}: timeout") from exc

            if result.returncode != 0:
                stderr = result.stderr.decode("utf-8", errors="replace").strip()
                if "Operation timed out" in stderr or result.returncode == 28:
                    raise TimeoutError(f"{url}: timeout")
                raise RuntimeError(stderr or f"curl_exit_{result.returncode}")

            header_text = header_path.read_text(encoding="utf-8", errors="replace") if header_path.exists() else ""
            body = body_path.read_bytes() if body_path.exists() else b""
            status_code = _parse_status_code(header_text)

            return FetchResponse(
                body=body,
                content_type=_parse_content_type(header_text),
                http_status=status_code or 200,
                headers_text=header_text,
            )

    def probe_certificate(self) -> FetchResponse:
        return self.fetch_url(MTLS_PROBE_URL, use_mtls=True, allowed_statuses={403, 404})
