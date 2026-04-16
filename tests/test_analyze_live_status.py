from __future__ import annotations

import importlib.util
import sys
from datetime import datetime, timezone
from pathlib import Path

from backend.models import ProviderTarget


def _load_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "analyze_live_status.py"
    spec = importlib.util.spec_from_file_location("analyze_live_status_module", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


analyze_live_status = _load_module()


def _provider_target(uid: str) -> ProviderTarget:
    return ProviderTarget(
        provider_uid=uid,
        display_name=uid,
        publisher=uid,
        publication_id=f"publication-{uid}",
        access_mode="auth",
        fetch_kind="mtls_subscription",
        fetch_url=f"https://example.invalid/{uid}",
        subscription_id=f"subscription-{uid}",
        enabled=True,
        delta_delivery=False,
        retention_period_minutes=None,
    )


def test_build_analysis_detects_missing_unexpected_and_problem_providers():
    payload = {
        "bundle_station_count": 100,
        "stations_with_any_live_observation": 25,
        "stations_with_current_live_state": 25,
        "coverage_ratio": 0.25,
        "last_received_update_at": "2026-04-16T10:00:00+00:00",
        "providers": [
            {
                "provider_uid": "good",
                "enabled": True,
                "stations_with_any_live_observation": 12,
                "observation_rows": 40,
                "last_result": "ok",
                "last_received_update_at": "2026-04-16T10:00:00+00:00",
                "recent_updates": [
                    {
                        "result": "ok",
                        "http_status": 200,
                        "observation_count": 40,
                        "mapped_observation_count": 40,
                        "dropped_observation_count": 0,
                    }
                ],
            },
            {
                "provider_uid": "broken",
                "enabled": True,
                "stations_with_any_live_observation": 0,
                "observation_rows": 0,
                "last_result": "error",
                "last_received_update_at": "2026-04-16T08:00:00+00:00",
                "recent_updates": [
                    {
                        "result": "error",
                        "http_status": 404,
                        "observation_count": 0,
                        "mapped_observation_count": 0,
                        "dropped_observation_count": 0,
                    }
                ],
            },
            {
                "provider_uid": "unmapped",
                "enabled": True,
                "stations_with_any_live_observation": 0,
                "observation_rows": 0,
                "last_result": "ok",
                "last_received_update_at": "2026-04-16T09:55:00+00:00",
                "recent_updates": [
                    {
                        "result": "ok",
                        "http_status": 200,
                        "observation_count": 200,
                        "mapped_observation_count": 0,
                        "dropped_observation_count": 200,
                    }
                ],
            },
            {
                "provider_uid": "deprecated_chargecloud",
                "enabled": True,
                "stations_with_any_live_observation": 3,
                "observation_rows": 10,
                "last_result": "ok",
                "last_received_update_at": "2026-04-16T10:00:00+00:00",
                "recent_updates": [],
            },
        ],
    }

    analysis = analyze_live_status.build_analysis(
        payload,
        expected_enabled_providers=[
            _provider_target("good"),
            _provider_target("broken"),
            _provider_target("unmapped"),
            _provider_target("missing"),
        ],
        stale_after_minutes=60,
        now=datetime(2026, 4, 16, 10, 30, tzinfo=timezone.utc),
        source="fixture",
    )

    assert analysis["missing_expected_provider_uids"] == ["missing"]
    assert analysis["unexpected_provider_uids"] == ["deprecated_chargecloud"]

    provider_by_uid = {provider["provider_uid"]: provider for provider in analysis["providers"]}
    assert provider_by_uid["good"]["problems"] == []
    assert provider_by_uid["broken"]["problems"] == [
        "no_mapped_live_data",
        "latest_update_error",
        "stale_last_received_update",
    ]
    assert provider_by_uid["unmapped"]["problems"] == [
        "no_mapped_live_data",
        "recent_observations_unmapped",
    ]


def test_format_human_analysis_lists_key_findings():
    analysis = {
        "source": "fixture",
        "bundle_station_count": 10,
        "stations_with_any_live_observation": 3,
        "stations_with_current_live_state": 3,
        "coverage_ratio": 0.3,
        "last_received_update_at": "2026-04-16T10:00:00+00:00",
        "providers_in_status_count": 2,
        "expected_enabled_provider_count": 3,
        "present_expected_provider_count": 2,
        "missing_expected_provider_uids": ["missing"],
        "unexpected_provider_uids": ["deprecated_chargecloud"],
        "problem_counts": {"latest_update_error": 1, "recent_observations_unmapped": 1},
        "providers_with_problems": [
            {
                "provider_uid": "mobidata_bw_datex",
                "problems": ["recent_observations_unmapped"],
                "stations_with_any_live_observation": 0,
                "observation_rows": 0,
                "latest_result": "ok",
                "latest_http_status": 200,
                "latest_observation_count": 200,
                "latest_mapped_observation_count": 0,
                "latest_dropped_observation_count": 200,
                "last_received_update_at": None,
            },
            {
                "provider_uid": "smatrics",
                "problems": ["latest_update_error"],
                "stations_with_any_live_observation": 0,
                "observation_rows": 0,
                "latest_result": "error",
                "latest_http_status": 404,
                "latest_observation_count": 0,
                "latest_mapped_observation_count": 0,
                "latest_dropped_observation_count": 0,
                "last_received_update_at": None,
            },
        ],
    }

    report = analyze_live_status.format_human_analysis(analysis)

    assert "Missing expected providers: missing" in report
    assert "Unexpected providers in live status: deprecated_chargecloud" in report
    assert "mobidata_bw_datex: recent observations unmapped" in report
    assert "smatrics: latest update error" in report
