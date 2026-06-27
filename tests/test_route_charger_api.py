from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest
import requests
from fastapi.testclient import TestClient

from backend.api import create_app
from backend.routing import NormalizedRoute, OpenRouteServiceClient, RouteEndpoint, RouteNotFound, RouteProviderError


class FakeORSClient:
    def __init__(self, origin_distances: dict[str, float], destination_distances: dict[str, float]):
        self.origin_distances = origin_distances
        self.destination_distances = destination_distances
        self.one_to_many_batches: list[list[str]] = []
        self.many_to_one_batches: list[list[str]] = []

    def directions(self, origin, destination, *, profile: str = "driving-car") -> NormalizedRoute:
        return NormalizedRoute(
            source="openrouteservice",
            profile=profile,
            distance_m=10_000.0,
            duration_s=900.0,
            geometry={"type": "LineString", "coordinates": [[0.0, 0.0], [0.0, 0.1]]},
            points=[{"lat": 0.0, "lon": 0.0}, {"lat": 0.1, "lon": 0.0}],
        )

    def matrix_one_to_many(self, origin, destinations, *, profile: str = "driving-car"):
        labels = [destination.label for destination in destinations]
        self.one_to_many_batches.append(labels)
        return [self.origin_distances.get(label) for label in labels]

    def matrix_many_to_one(self, origins, destination, *, profile: str = "driving-car"):
        labels = [origin.label for origin in origins]
        self.many_to_one_batches.append(labels)
        return [self.destination_distances.get(label) for label in labels]


class FakeORSResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self.payload = payload
        self.text = text

    def json(self):
        if self.payload is None:
            raise ValueError("no json")
        return self.payload


class FakeORSSession:
    def __init__(self, response: FakeORSResponse):
        self.response = response
        self.requests: list[dict] = []

    def post(self, url, *, json, headers, timeout):
        self.requests.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        return self.response


class SequencedORSSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests: list[dict] = []

    def post(self, url, *, json, headers, timeout):
        self.requests.append({"url": url, "json": json, "headers": headers, "timeout": timeout})
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _create_route_sqlite(path: Path, stations: list[dict]) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE stations (
            station_uid INTEGER PRIMARY KEY,
            country_code TEXT NOT NULL,
            station_id TEXT NOT NULL UNIQUE,
            operator_name TEXT,
            station_name TEXT,
            address TEXT,
            postal_code TEXT,
            city TEXT,
            latitude REAL,
            longitude REAL,
            charger_count INTEGER,
            max_power_kw REAL,
            connector_types TEXT
        );
        CREATE TABLE station_amenities (
            station_uid INTEGER PRIMARY KEY,
            amenities_total INTEGER,
            amenity_category_counts_json TEXT,
            amenity_examples_json TEXT,
            nearest_amenity_kind TEXT,
            nearest_amenity_name TEXT,
            nearest_amenity_distance_m REAL
        );
        CREATE VIRTUAL TABLE station_rtree USING rtree(
            station_uid,
            min_lon,
            max_lon,
            min_lat,
            max_lat
        );
        """
    )
    for index, station in enumerate(stations, start=1):
        connection.execute(
            """
            INSERT INTO stations (
                station_uid,
                country_code,
                station_id,
                operator_name,
                station_name,
                address,
                postal_code,
                city,
                latitude,
                longitude,
                charger_count,
                max_power_kw,
                connector_types
            )
            VALUES (?, 'DE', ?, ?, ?, '', '', '', ?, ?, 2, ?, 'ccs')
            """,
            (
                index,
                station["station_id"],
                station.get("operator_name", "Fast Operator"),
                station.get("station_name", station["station_id"]),
                station["latitude"],
                station["longitude"],
                station.get("max_power_kw", 150.0),
            ),
        )
        examples = station.get("amenity_examples", [])
        counts = station.get("amenity_category_counts", {})
        nearest = examples[0] if examples else {}
        connection.execute(
            """
            INSERT INTO station_amenities (
                station_uid,
                amenities_total,
                amenity_category_counts_json,
                amenity_examples_json,
                nearest_amenity_kind,
                nearest_amenity_name,
                nearest_amenity_distance_m
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                index,
                station.get("amenities_total", 0),
                json.dumps(counts),
                json.dumps(examples),
                nearest.get("category", ""),
                nearest.get("name", ""),
                nearest.get("distance_m"),
            ),
        )
        connection.execute(
            """
            INSERT INTO station_rtree (station_uid, min_lon, max_lon, min_lat, max_lat)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                index,
                station["longitude"],
                station["longitude"],
                station["latitude"],
                station["latitude"],
            ),
        )
    connection.commit()
    connection.close()


def _route_request(filters: dict | None = None) -> dict:
    return {
        "origin": {"lat": 0.0, "lon": 0.0, "label": "Origin"},
        "destination": {"lat": 0.1, "lon": 0.0, "label": "Destination"},
        "filters": filters or {"min_power_kw": 50},
        "filter_mode": "route_calculation",
    }


def test_ors_client_maps_quota_403_to_route_capacity_error(app_config):
    client = OpenRouteServiceClient(
        replace(app_config, ors_base_url="https://api.openrouteservice.org", ors_api_key="test-key"),
        session=FakeORSSession(FakeORSResponse(403, {"error": "Quota exceeded"})),
    )

    with pytest.raises(RouteProviderError) as exc_info:
        client.matrix_one_to_many(
            RouteEndpoint(lat=0.0, lon=0.0),
            [RouteEndpoint(lat=0.1, lon=0.0)],
        )

    assert exc_info.value.detail == "route_provider_quota_exhausted"
    assert exc_info.value.status_code == 503


def test_ors_client_keeps_non_quota_403_as_auth_failure(app_config):
    client = OpenRouteServiceClient(
        replace(app_config, ors_base_url="https://api.openrouteservice.org", ors_api_key="test-key"),
        session=FakeORSSession(FakeORSResponse(403, {"error": "Forbidden"})),
    )

    with pytest.raises(RouteProviderError) as exc_info:
        client.matrix_one_to_many(
            RouteEndpoint(lat=0.0, lon=0.0),
            [RouteEndpoint(lat=0.1, lon=0.0)],
        )

    assert exc_info.value.detail == "route_provider_auth_failed"
    assert exc_info.value.status_code == 503


def test_ors_client_falls_back_when_primary_request_fails(app_config):
    session = SequencedORSSession(
        [
            requests.ConnectionError("primary down"),
            FakeORSResponse(200, {"distances": [[1234.0]]}),
        ]
    )
    client = OpenRouteServiceClient(
        replace(
            app_config,
            ors_base_url="http://private-ors.test/ors",
            ors_api_key="",
            ors_fallback_base_url="https://api.openrouteservice.org",
            ors_fallback_api_key="fallback-key",
        ),
        session=session,
    )

    distances = client.matrix_one_to_many(
        RouteEndpoint(lat=0.0, lon=0.0),
        [RouteEndpoint(lat=0.1, lon=0.0)],
    )

    assert distances == [1234.0]
    assert [request["url"] for request in session.requests] == [
        "http://private-ors.test/ors/v2/matrix/driving-car",
        "https://api.openrouteservice.org/v2/matrix/driving-car",
    ]
    assert "Authorization" not in session.requests[0]["headers"]
    assert session.requests[1]["headers"]["Authorization"] == "fallback-key"


def test_ors_client_does_not_fall_back_for_route_not_found(app_config):
    session = SequencedORSSession(
        [
            FakeORSResponse(404, {"error": "Route not found"}),
            FakeORSResponse(200, {"distances": [[1234.0]]}),
        ]
    )
    client = OpenRouteServiceClient(
        replace(
            app_config,
            ors_base_url="http://private-ors.test/ors",
            ors_fallback_base_url="https://api.openrouteservice.org",
            ors_fallback_api_key="fallback-key",
        ),
        session=session,
    )

    with pytest.raises(RouteNotFound):
        client.matrix_one_to_many(
            RouteEndpoint(lat=0.0, lon=0.0),
            [RouteEndpoint(lat=0.1, lon=0.0)],
        )

    assert [request["url"] for request in session.requests] == [
        "http://private-ors.test/ors/v2/matrix/driving-car"
    ]


def test_route_charger_search_validates_detour_distance_and_prunes_slow_candidates(app_config, tmp_path: Path):
    sqlite_path = tmp_path / "open_static.sqlite3"
    _create_route_sqlite(
        sqlite_path,
        [
            {
                "station_id": "test:near",
                "latitude": 0.05,
                "longitude": 0.005,
                "max_power_kw": 150,
                "amenities_total": 3,
                "amenity_category_counts": {"amenity_restaurant": 1},
                "amenity_examples": [{"category": "restaurant", "name": "Food", "opening_hours": "24/7"}],
            },
            {
                "station_id": "test:close-but-bad-detour",
                "latitude": 0.052,
                "longitude": 0.006,
                "max_power_kw": 150,
                "amenities_total": 3,
                "amenity_category_counts": {"amenity_restaurant": 1},
                "amenity_examples": [{"category": "restaurant", "name": "Food"}],
            },
            {
                "station_id": "test:slow",
                "latitude": 0.051,
                "longitude": 0.004,
                "max_power_kw": 11,
            },
        ],
    )
    app = create_app(
        replace(
            app_config,
            open_static_sqlite_path=sqlite_path,
            route_corridor_radius_m=2_000,
            route_candidate_radius_m=3_000,
        )
    )
    fake_ors = FakeORSClient(
        origin_distances={
            "test:near": 5_200,
            "test:close-but-bad-detour": 8_000,
        },
        destination_distances={
            "test:near": 5_600,
            "test:close-but-bad-detour": 8_000,
        },
    )
    app.state.route_charger_service.ors_client = fake_ors
    client = TestClient(app)

    response = client.post("/v1/routes/chargers", json=_route_request())

    assert response.status_code == 200
    payload = response.json()
    assert [item["station"]["station_id"] for item in payload["stations"]] == ["test:near"]
    assert payload["stations"][0]["route"]["drive_distance_to_route_m"] == 400
    assert payload["stations"][0]["route"]["route_detour_m"] == 800
    validated_labels = [label for batch in fake_ors.one_to_many_batches for label in batch]
    assert "test:slow" not in validated_labels
    assert "test:close-but-bad-detour" in validated_labels


def test_route_charger_search_applies_static_filters_before_matrix_validation(app_config, tmp_path: Path):
    sqlite_path = tmp_path / "open_static.sqlite3"
    _create_route_sqlite(
        sqlite_path,
        [
            {
                "station_id": "test:matching",
                "latitude": 0.05,
                "longitude": 0.005,
                "operator_name": "Filter Operator",
                "max_power_kw": 300,
                "amenities_total": 12,
                "amenity_category_counts": {"amenity_restaurant": 1, "amenity_cafe": 1},
                "amenity_examples": [{"category": "restaurant", "name": "McDonalds Cafe"}],
            },
            {
                "station_id": "test:wrong-operator",
                "latitude": 0.051,
                "longitude": 0.005,
                "operator_name": "Other Operator",
                "max_power_kw": 300,
                "amenities_total": 12,
                "amenity_category_counts": {"amenity_restaurant": 1, "amenity_cafe": 1},
                "amenity_examples": [{"category": "restaurant", "name": "McDonalds Cafe"}],
            },
            {
                "station_id": "test:wrong-amenity-name",
                "latitude": 0.052,
                "longitude": 0.005,
                "operator_name": "Filter Operator",
                "max_power_kw": 300,
                "amenities_total": 12,
                "amenity_category_counts": {"amenity_restaurant": 1, "amenity_cafe": 1},
                "amenity_examples": [{"category": "restaurant", "name": "Burger Place"}],
            },
        ],
    )
    app = create_app(
        replace(
            app_config,
            open_static_sqlite_path=sqlite_path,
            route_corridor_radius_m=2_000,
            route_candidate_radius_m=3_000,
        )
    )
    fake_ors = FakeORSClient(
        origin_distances={"test:matching": 5_000},
        destination_distances={"test:matching": 5_500},
    )
    app.state.route_charger_service.ors_client = fake_ors
    client = TestClient(app)

    response = client.post(
        "/v1/routes/chargers",
        json=_route_request(
            {
                "operator": "Filter Operator",
                "min_power_kw": 150,
                "min_amenities_total": 11,
                "selected_amenities": ["restaurant", "amenity_cafe"],
                "amenity_name_query": "mcdonalds",
            }
        ),
    )

    assert response.status_code == 200
    payload = response.json()
    assert [item["station"]["station_id"] for item in payload["stations"]] == ["test:matching"]
    assert fake_ors.one_to_many_batches == [["test:matching"]]


def test_route_charger_search_caps_results_to_closest_one_hundred(app_config, tmp_path: Path):
    sqlite_path = tmp_path / "open_static.sqlite3"
    stations = [
        {
            "station_id": f"test:station-{index:03d}",
            "latitude": 0.001 + (index * 0.0008),
            "longitude": 0.001,
            "max_power_kw": 150,
        }
        for index in range(105)
    ]
    _create_route_sqlite(sqlite_path, stations)
    labels = [station["station_id"] for station in stations]
    app = create_app(
        replace(
            app_config,
            open_static_sqlite_path=sqlite_path,
            route_corridor_radius_m=2_000,
            route_candidate_radius_m=3_000,
            route_matrix_batch_size=200,
        )
    )
    fake_ors = FakeORSClient(
        origin_distances={label: 5_000 + index for index, label in enumerate(labels)},
        destination_distances={label: 5_000 + index for index, label in enumerate(labels)},
    )
    app.state.route_charger_service.ors_client = fake_ors
    client = TestClient(app)

    response = client.post("/v1/routes/chargers", json=_route_request())

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["stations"]) == 100
    assert [item["station"]["station_id"] for item in payload["stations"][:3]] == [
        "test:station-000",
        "test:station-001",
        "test:station-002",
    ]
    assert payload["query"]["returned_count"] == 100


def test_route_charger_search_applies_currently_open_filter_after_validation(app_config, tmp_path: Path):
    sqlite_path = tmp_path / "open_static.sqlite3"
    _create_route_sqlite(
        sqlite_path,
        [
            {
                "station_id": "test:open",
                "latitude": 0.05,
                "longitude": 0.005,
                "max_power_kw": 150,
                "amenities_total": 1,
                "amenity_category_counts": {"amenity_restaurant": 1},
                "amenity_examples": [{"category": "restaurant", "name": "Open Food", "opening_hours": "open"}],
            },
            {
                "station_id": "test:closed",
                "latitude": 0.052,
                "longitude": 0.005,
                "max_power_kw": 150,
                "amenities_total": 1,
                "amenity_category_counts": {"amenity_restaurant": 1},
                "amenity_examples": [{"category": "restaurant", "name": "Closed Food", "opening_hours": "closed"}],
            },
        ],
    )
    app = create_app(
        replace(
            app_config,
            open_static_sqlite_path=sqlite_path,
            route_corridor_radius_m=2_000,
            route_candidate_radius_m=3_000,
        )
    )
    fake_ors = FakeORSClient(
        origin_distances={"test:open": 5_000, "test:closed": 5_000},
        destination_distances={"test:open": 5_500, "test:closed": 5_500},
    )
    app.state.route_charger_service.ors_client = fake_ors
    client = TestClient(app)

    response = client.post(
        "/v1/routes/chargers",
        json=_route_request({"min_power_kw": 50, "currently_open_only": True}),
    )

    assert response.status_code == 200
    payload = response.json()
    assert [item["station"]["station_id"] for item in payload["stations"]] == ["test:open"]


def test_route_charger_search_returns_503_when_route_provider_is_unconfigured(app_config, tmp_path: Path):
    sqlite_path = tmp_path / "open_static.sqlite3"
    _create_route_sqlite(
        sqlite_path,
        [{"station_id": "test:near", "latitude": 0.05, "longitude": 0.005, "max_power_kw": 150}],
    )
    client = TestClient(create_app(replace(app_config, open_static_sqlite_path=sqlite_path)))

    response = client.post("/v1/routes/chargers", json=_route_request())

    assert response.status_code == 503
    assert response.json()["detail"] == "route_provider_unavailable"
