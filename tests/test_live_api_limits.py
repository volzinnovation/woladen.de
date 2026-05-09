from __future__ import annotations

from fastapi.testclient import TestClient

from backend.api import create_app


def test_station_lookup_rejects_more_than_twenty_station_ids(app_config):
    client = TestClient(create_app(app_config))

    response = client.post(
        "/v1/stations/lookup",
        json={"station_ids": [f"station-{index}" for index in range(21)]},
    )

    assert response.status_code == 422


def test_station_lookup_strips_public_namespace_for_internal_lookup(app_config):
    app = create_app(app_config)
    captured: dict[str, list[str] | str] = {}

    def fake_list_station_summaries_by_ids(station_ids, *, timings=None):
        captured["lookup_ids"] = list(station_ids)
        return [{"station_id": "47d719c1b62c750", "provider_uid": "test-provider"}]

    def fake_get_station_detail(station_id, *, timings=None):
        captured["detail_id"] = station_id
        return {
            "station": {"station_id": station_id, "provider_uid": "test-provider"},
            "evses": [{"station_id": station_id, "provider_uid": "test-provider"}],
            "recent_observations": [{"station_id": station_id, "provider_uid": "test-provider"}],
        }

    app.state.store.list_station_summaries_by_ids = fake_list_station_summaries_by_ids
    app.state.store.get_station_detail = fake_get_station_detail
    client = TestClient(app)

    lookup = client.post(
        "/v1/stations/lookup",
        json={"station_ids": ["DE:47d719c1b62c750"]},
    )
    detail = client.get("/v1/stations/DE:47d719c1b62c750")

    assert lookup.status_code == 200
    assert lookup.json()["stations"][0]["station_id"] == "DE:47d719c1b62c750"
    assert lookup.json()["missing_station_ids"] == []
    assert captured["lookup_ids"] == ["47d719c1b62c750"]
    assert detail.status_code == 200
    assert detail.json()["station"]["station_id"] == "DE:47d719c1b62c750"
    assert detail.json()["evses"][0]["station_id"] == "DE:47d719c1b62c750"
    assert captured["detail_id"] == "47d719c1b62c750"


def test_station_lookup_preserves_legacy_unprefixed_response_ids(app_config):
    app = create_app(app_config)
    captured: dict[str, list[str] | str] = {}

    def fake_list_station_summaries_by_ids(station_ids, *, timings=None):
        captured["lookup_ids"] = list(station_ids)
        return [{"station_id": "47d719c1b62c750", "provider_uid": "test-provider"}]

    def fake_get_station_detail(station_id, *, timings=None):
        captured["detail_id"] = station_id
        return {
            "station": {"station_id": station_id, "provider_uid": "test-provider"},
            "evses": [{"station_id": station_id, "provider_uid": "test-provider"}],
            "recent_observations": [{"station_id": station_id, "provider_uid": "test-provider"}],
        }

    app.state.store.list_station_summaries_by_ids = fake_list_station_summaries_by_ids
    app.state.store.get_station_detail = fake_get_station_detail
    client = TestClient(app)

    lookup = client.post(
        "/v1/stations/lookup",
        json={"station_ids": ["47d719c1b62c750"]},
    )
    detail = client.get("/v1/stations/47d719c1b62c750")

    assert lookup.status_code == 200
    assert lookup.json()["stations"][0]["station_id"] == "47d719c1b62c750"
    assert lookup.json()["missing_station_ids"] == []
    assert captured["lookup_ids"] == ["47d719c1b62c750"]
    assert detail.status_code == 200
    assert detail.json()["station"]["station_id"] == "47d719c1b62c750"
    assert detail.json()["evses"][0]["station_id"] == "47d719c1b62c750"
    assert captured["detail_id"] == "47d719c1b62c750"
