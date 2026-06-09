from __future__ import annotations

from scripts import commercial_fetch_be_transportdata as fetch_be


def _energyvision_spec(url: str) -> fetch_be.SourceSpec:
    return fetch_be.SourceSpec(
        key="energyvision-locations",
        source_uid="be_energyvision_ocpi_locations",
        display_name="BE EnergyVision OCPI locations and EVSE status",
        url=url,
        source_kind="afir_ocpi_locations_with_status",
        task_kind="parse_dynamic_payload",
        auth_kind="energyvision",
        stream_parser=lambda *_args, **_kwargs: iter(()),
    )


def test_energyvision_staging_url_uses_staging_token(monkeypatch):
    monkeypatch.setenv("TRANSPORTDATA_BE_ENERGYVISION_STAGING_TOKEN", "staging-token")
    monkeypatch.delenv("TRANSPORTDATA_BE_ENERGYVISION_API_KEY", raising=False)
    monkeypatch.delenv("WOLADEN_TRANSPORTDATA_BE_API_KEY", raising=False)
    spec = _energyvision_spec("https://ocpi.myev-dev.be/cpo/2.2.1/locations/")

    headers = fetch_be._energyvision_headers(spec)

    assert headers["Authorization"] == "Token staging-token"


def test_energyvision_header_preserves_explicit_scheme(monkeypatch):
    monkeypatch.setenv("TRANSPORTDATA_BE_ENERGYVISION_STAGING_TOKEN", "Bearer staging-token")
    spec = _energyvision_spec("https://ocpi.myev-dev.be/cpo/2.2.1/locations/")

    headers = fetch_be._energyvision_headers(spec)

    assert headers["Authorization"] == "Bearer staging-token"


def test_energyvision_base_url_override_builds_versioned_module_url(monkeypatch):
    monkeypatch.setenv("TRANSPORTDATA_BE_ENERGYVISION_BASE_URL", "https://ocpi.myev-dev.be/cpo/")
    monkeypatch.delenv("TRANSPORTDATA_BE_ENERGYVISION_OCPI_VERSION", raising=False)

    assert fetch_be._energyvision_source_url(
        module="locations",
        default_url="https://ocpi.energyvision.be/cpo/2.1.1/locations/",
    ) == "https://ocpi.myev-dev.be/cpo/2.2.1/locations/"


def test_energyvision_credentials_exchange_uses_token_a_and_returns_token_c(monkeypatch):
    calls = []

    class FakeResponse:
        headers = {}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{"data":{"token":"token-c"},"status_code":1000,"status_message":"Success"}'

    def fake_urlopen(request, timeout=None, context=None):
        calls.append((request, timeout, context))
        return FakeResponse()

    monkeypatch.setenv("TRANSPORTDATA_BE_ENERGYVISION_PROD_TOKEN", "token-a")
    monkeypatch.setenv("TRANSPORTDATA_BE_ENERGYVISION_EMSP_TOKEN_B", "token-b")
    monkeypatch.setenv("TRANSPORTDATA_BE_ENERGYVISION_EMSP_VERSIONS_URL", "https://emsp.example.test/ocpi/emsp/versions/")
    monkeypatch.setenv("TRANSPORTDATA_BE_ENERGYVISION_EMSP_COUNTRY_CODE", "DE")
    monkeypatch.setenv("TRANSPORTDATA_BE_ENERGYVISION_EMSP_PARTY_ID", "WLA")
    monkeypatch.delenv("TRANSPORTDATA_BE_ENERGYVISION_PROD_TOKEN_C", raising=False)
    monkeypatch.setattr(fetch_be.urllib.request, "urlopen", fake_urlopen)
    spec = _energyvision_spec("https://ocpi.energyvision.be/cpo/2.1.1/locations/")

    headers = fetch_be._energyvision_headers(spec, timeout_seconds=5)

    assert headers["Authorization"] == "Token token-c"
    request, timeout, context = calls[0]
    assert timeout == 5
    assert context is None
    assert request.full_url == "https://ocpi.energyvision.be/cpo/2.1.1/credentials/"
    assert request.headers["Authorization"] == "Token token-a"
    body = fetch_be.json.loads(request.data.decode("utf-8"))
    assert body["token"] == "token-b"
    assert body["url"] == "https://emsp.example.test/ocpi/emsp/versions/"
    assert body["roles"][0]["country_code"] == "DE"
    assert body["roles"][0]["party_id"] == "WLA"


def test_dynamic_configured_excludes_static_only_sources(monkeypatch):
    monkeypatch.setattr(fetch_be, "_has_credentials", lambda _spec: True)

    selected, skipped = fetch_be._selected_sources("dynamic-configured")

    selected_keys = {spec.key for spec in selected}
    assert "eco-static" not in selected_keys
    assert "indigo-static" not in selected_keys
    assert "energyvision-locations" in selected_keys
    assert "road-locations" in selected_keys
    assert any(
        item["source"] == "eco-static"
        and item["reason"] == "static_source_excluded_from_dynamic_cycle"
        for item in skipped
    )
    assert any(
        item["source"] == "indigo-static"
        and item["reason"] == "static_source_excluded_from_dynamic_cycle"
        for item in skipped
    )
