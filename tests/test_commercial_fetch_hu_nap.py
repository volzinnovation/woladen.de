from __future__ import annotations

from scripts import commercial_fetch_hu_nap as fetch_hu


def test_hu_fetch_static_real_selects_non_empty_static_subscriptions():
    specs = fetch_hu._selected_sources("static-real")

    assert [spec.key for spec in specs] == ["eco-movement-static", "mobiliti-static"]
    assert [spec.task_kind for spec in specs] == ["parse_static_payload", "parse_static_payload"]


def test_hu_fetch_all_active_includes_ack_only_test_and_dynamic_subscriptions():
    specs = fetch_hu._selected_sources("all-active")

    assert [spec.key for spec in specs] == [
        "eco-movement-static",
        "mobiliti-static",
        "ampeco-test-static",
        "ampeco-test-dynamic",
        "magyar-kozut-static",
    ]
    assert specs[3].task_kind == "parse_dynamic_payload"


def test_hu_credentials_use_standard_email_and_env_password():
    email, password = fetch_hu._credentials_from_env({"HU_NAP_PASSWORD": "secret"})

    assert email == "raphael.volz@hs-pforzheim.de"
    assert password == "secret"


def test_hu_resolves_runtime_subscription_urls_from_portal_profile_summaries():
    specs = fetch_hu._selected_sources("static-real")
    profiles = [
        {
            "serviceProviderUserProfileId": specs[0].profile_id,
            "dataAccesses": [{"url": "https://napphub.kozut.hu/hub-web/datex2/3_3/eco/pullSnapshotData"}],
        },
        {
            "serviceProviderUserProfileId": specs[1].profile_id,
            "dataAccesses": [{"url": "https://napphub.kozut.hu/hub-web/datex2/3_3/mobiliti/pullSnapshotData"}],
        },
    ]

    resolved = fetch_hu._resolve_source_urls(specs, profiles)

    assert [spec.url.rsplit("/", 2)[-2] for spec in resolved] == ["eco", "mobiliti"]
    assert all("napphub.kozut.hu" in spec.url for spec in resolved)


def test_hu_open_url_retries_after_temporary_url_error(monkeypatch):
    calls = []
    sleep_seconds = []
    sentinel = object()

    def fake_urlopen(request, timeout=None, context=None):
        calls.append(context is not None)
        if len(calls) < 3:
            raise fetch_hu.urllib.error.URLError("Temporary failure in name resolution")
        return sentinel

    monkeypatch.setattr(fetch_hu.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(fetch_hu.time, "sleep", lambda seconds: sleep_seconds.append(seconds))

    request = fetch_hu.urllib.request.Request("https://example.test")

    assert fetch_hu._open_url(request, timeout_seconds=1) is sentinel
    assert calls == [False, True, False]
    assert sleep_seconds == [2.0]
