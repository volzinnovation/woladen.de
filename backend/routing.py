from __future__ import annotations

import hashlib
import json
import math
import re
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import requests

from .config import AppConfig
from .open_catalog import OpenCatalogStore
from .store import LiveStore

ROUTE_PROFILE = "driving-car"
ROUTE_SOURCE = "openrouteservice"
AMENITY_PREFIX = "amenity_"
OPENING_DAY_KEYS = ("Mo", "Tu", "We", "Th", "Fr", "Sa", "Su")
OPENING_DAY_SELECTOR_RE = re.compile(
    r"^((?:Mo|Tu|We|Th|Fr|Sa|Su)(?:\s*-\s*(?:Mo|Tu|We|Th|Fr|Sa|Su))?"
    r"(?:\s*,\s*(?:Mo|Tu|We|Th|Fr|Sa|Su)(?:\s*-\s*(?:Mo|Tu|We|Th|Fr|Sa|Su))?)*)\s+(.+)$"
)


class RouteProviderUnavailable(RuntimeError):
    pass


class RouteProviderError(RuntimeError):
    def __init__(self, detail: str, *, status_code: int = 502):
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code


class RouteNotFound(RuntimeError):
    pass


class RouteTooLong(RuntimeError):
    pass


@dataclass(frozen=True)
class RouteEndpoint:
    lat: float
    lon: float
    label: str = ""


@dataclass(frozen=True)
class RouteFilters:
    operator: str = ""
    min_power_kw: float = 50.0
    min_amenities_total: int = 0
    selected_amenities: tuple[str, ...] = ()
    amenity_name_query: str = ""
    available_only: bool = False
    currently_open_only: bool = False

    @property
    def normalized_selected_amenities(self) -> tuple[str, ...]:
        normalized = [_normalize_amenity_key(value) for value in self.selected_amenities]
        return tuple(dict.fromkeys(value for value in normalized if value))


@dataclass(frozen=True)
class NormalizedRoute:
    source: str
    profile: str
    distance_m: float
    duration_s: float
    geometry: dict[str, Any]
    points: list[dict[str, float]]


@dataclass(frozen=True)
class RouteProjection:
    straight_line_distance_to_route_m: float
    route_position_m: float
    nearest_route_point: dict[str, float]


@dataclass(frozen=True)
class CandidateRouteInfo:
    station: dict[str, Any]
    projection: RouteProjection
    origin_distance_m: float
    destination_distance_m: float
    route_detour_m: float
    drive_distance_to_route_m: float


@dataclass(frozen=True)
class OpeningRange:
    start: int
    end: int
    open_ended: bool


@dataclass(frozen=True)
class OpeningClause:
    selected_days: set[str] | None
    mode: str
    ranges: list[OpeningRange]


@dataclass
class OpenRouteServiceClient:
    config: AppConfig
    session: requests.Session = field(default_factory=requests.Session)

    @property
    def available(self) -> bool:
        return bool(self.config.ors_base_url)

    def directions(self, origin: RouteEndpoint, destination: RouteEndpoint, *, profile: str = ROUTE_PROFILE) -> NormalizedRoute:
        self._require_available()
        payload = {
            "coordinates": [[origin.lon, origin.lat], [destination.lon, destination.lat]],
            "instructions": False,
            "preference": "fastest",
        }
        response_payload = self._post_json(f"/v2/directions/{profile}/geojson", payload)
        return normalize_directions_response(response_payload, profile=profile)

    def matrix_one_to_many(
        self,
        origin: RouteEndpoint,
        destinations: list[RouteEndpoint],
        *,
        profile: str = ROUTE_PROFILE,
    ) -> list[float | None]:
        if not destinations:
            return []
        locations = [[origin.lon, origin.lat]] + [[item.lon, item.lat] for item in destinations]
        payload = {
            "locations": locations,
            "sources": [0],
            "destinations": list(range(1, len(locations))),
            "metrics": ["distance"],
        }
        response_payload = self._post_json(f"/v2/matrix/{profile}", payload)
        distances = response_payload.get("distances")
        if not isinstance(distances, list) or not distances:
            raise RouteProviderError("route_matrix_invalid_response")
        row = distances[0]
        if not isinstance(row, list):
            raise RouteProviderError("route_matrix_invalid_response")
        return [_float_or_none(value) for value in row[: len(destinations)]]

    def matrix_many_to_one(
        self,
        origins: list[RouteEndpoint],
        destination: RouteEndpoint,
        *,
        profile: str = ROUTE_PROFILE,
    ) -> list[float | None]:
        if not origins:
            return []
        locations = [[item.lon, item.lat] for item in origins] + [[destination.lon, destination.lat]]
        payload = {
            "locations": locations,
            "sources": list(range(len(origins))),
            "destinations": [len(origins)],
            "metrics": ["distance"],
        }
        response_payload = self._post_json(f"/v2/matrix/{profile}", payload)
        distances = response_payload.get("distances")
        if not isinstance(distances, list):
            raise RouteProviderError("route_matrix_invalid_response")
        values = []
        for row in distances[: len(origins)]:
            if not isinstance(row, list) or not row:
                values.append(None)
            else:
                values.append(_float_or_none(row[0]))
        return values

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        self._require_available()
        url = f"{self.config.ors_base_url}{path}"
        headers = {"Content-Type": "application/json"}
        if self.config.ors_api_key:
            headers["Authorization"] = self.config.ors_api_key
        try:
            response = self.session.post(
                url,
                json=payload,
                headers=headers,
                timeout=max(float(self.config.ors_timeout_seconds), 0.1),
            )
        except requests.Timeout as exc:
            raise RouteProviderError("route_provider_timeout", status_code=504) from exc
        except requests.RequestException as exc:
            raise RouteProviderError("route_provider_request_failed", status_code=502) from exc

        if response.status_code in {403, 429} and _response_mentions_quota_exceeded(response):
            raise RouteProviderError("route_provider_quota_exhausted", status_code=503)
        if response.status_code == 429:
            raise RouteProviderError("route_provider_rate_limited", status_code=429)
        if response.status_code in {401, 403}:
            raise RouteProviderError("route_provider_auth_failed", status_code=503)
        if response.status_code == 404:
            raise RouteNotFound("route_not_found")
        if response.status_code >= 400:
            raise RouteProviderError("route_provider_error", status_code=502)

        try:
            decoded = response.json()
        except ValueError as exc:
            raise RouteProviderError("route_provider_invalid_json") from exc
        if not isinstance(decoded, dict):
            raise RouteProviderError("route_provider_invalid_json")
        return decoded

    def _require_available(self) -> None:
        if not self.available:
            raise RouteProviderUnavailable("route_provider_unavailable")


@dataclass
class RouteChargerService:
    config: AppConfig
    catalog_store: OpenCatalogStore
    live_store: LiveStore
    ors_client: OpenRouteServiceClient | None = None

    def __post_init__(self) -> None:
        if self.ors_client is None:
            self.ors_client = OpenRouteServiceClient(self.config)
        self._route_cache: dict[str, tuple[float, NormalizedRoute]] = {}

    def search(
        self,
        *,
        origin: RouteEndpoint,
        destination: RouteEndpoint,
        filters: RouteFilters,
        filter_mode: str = "route_calculation",
    ) -> dict[str, Any]:
        route = self._base_route(origin=origin, destination=destination)
        if self.config.route_max_distance_m > 0 and route.distance_m > self.config.route_max_distance_m:
            raise RouteTooLong("route_too_long")

        static_candidates = self._static_corridor_candidates(route=route, filters=filters)
        validation_pool = self._validation_pool(route=route, candidates=static_candidates, filters=filters)
        validated = self._validate_candidates(
            origin=origin,
            destination=destination,
            route=route,
            validation_pool=validation_pool,
        )
        final_candidates = self._apply_post_validation_filters(validated, filters=filters)
        final_candidates.sort(key=_route_result_sort_key)
        max_results = max(1, int(self.config.route_max_results))
        stations = [
            {
                "station": item.station,
                "route": {
                    "drive_distance_to_route_m": round(item.drive_distance_to_route_m),
                    "route_detour_m": round(item.route_detour_m),
                    "straight_line_distance_to_route_m": round(
                        item.projection.straight_line_distance_to_route_m
                    ),
                    "route_position_m": round(item.projection.route_position_m),
                    "nearest_route_point": item.projection.nearest_route_point,
                },
            }
            for item in final_candidates[:max_results]
        ]
        return {
            "route": {
                "source": route.source,
                "profile": route.profile,
                "distance_m": round(route.distance_m),
                "duration_s": round(route.duration_s),
                "geometry": route.geometry,
            },
            "stations": stations,
            "query": {
                "corridor_radius_m": max(1, int(self.config.route_corridor_radius_m)),
                "candidate_radius_m": max(1, int(self.config.route_candidate_radius_m)),
                "min_power_kw": filters.min_power_kw,
                "min_amenities_total": filters.min_amenities_total,
                "selected_amenities": list(filters.normalized_selected_amenities),
                "amenity_name_query": filters.amenity_name_query,
                "operator": filters.operator,
                "available_only": filters.available_only,
                "currently_open_only": filters.currently_open_only,
                "filter_mode": filter_mode,
                "validation_candidate_count": len(validation_pool),
                "validated_candidate_count": len(validated),
                "returned_count": len(stations),
            },
            "source": "open_static.sqlite3+openrouteservice",
        }

    def _base_route(self, *, origin: RouteEndpoint, destination: RouteEndpoint) -> NormalizedRoute:
        route_key = _route_cache_key(origin, destination)
        cached = self._route_cache.get(route_key)
        now = time.monotonic()
        if cached is not None:
            stored_at, route = cached
            if now - stored_at <= max(float(self.config.route_cache_ttl_seconds), 0.0):
                return route
        assert self.ors_client is not None
        route = self.ors_client.directions(origin, destination, profile=ROUTE_PROFILE)
        self._route_cache[route_key] = (now, route)
        return route

    def _static_corridor_candidates(
        self,
        *,
        route: NormalizedRoute,
        filters: RouteFilters,
    ) -> list[dict[str, Any]]:
        candidate_limit = max(
            int(self.config.route_max_validation_candidates) * 20,
            int(self.config.route_max_validation_candidates),
        )
        return self.catalog_store.route_corridor_candidates(
            route_points=route.points,
            radius_m=max(1, int(self.config.route_candidate_radius_m)),
            candidate_limit=candidate_limit,
            min_power_kw=filters.min_power_kw,
            operator_query=filters.operator,
        )

    def _validation_pool(
        self,
        *,
        route: NormalizedRoute,
        candidates: list[dict[str, Any]],
        filters: RouteFilters,
    ) -> list[tuple[dict[str, Any], RouteProjection]]:
        pool = []
        seen_station_ids: set[str] = set()
        candidate_radius_m = max(1, int(self.config.route_candidate_radius_m))
        for station in candidates:
            station_id = str(station.get("station_id") or "")
            if not station_id or station_id in seen_station_ids:
                continue
            seen_station_ids.add(station_id)
            if not _station_matches_static_filters(station, filters):
                continue
            projection = project_station_to_route(station, route.points)
            if projection.straight_line_distance_to_route_m > candidate_radius_m:
                continue
            pool.append((station, projection))
        pool.sort(key=lambda item: _validation_pool_sort_key(item[0], item[1]))
        return pool[: max(1, int(self.config.route_max_validation_candidates))]

    def _validate_candidates(
        self,
        *,
        origin: RouteEndpoint,
        destination: RouteEndpoint,
        route: NormalizedRoute,
        validation_pool: list[tuple[dict[str, Any], RouteProjection]],
    ) -> list[CandidateRouteInfo]:
        if not validation_pool:
            return []
        assert self.ors_client is not None
        batch_size = max(1, int(self.config.route_matrix_batch_size))
        max_drive_distance_m = max(1, int(self.config.route_corridor_radius_m))
        validated: list[CandidateRouteInfo] = []
        for batch_start in range(0, len(validation_pool), batch_size):
            batch = validation_pool[batch_start : batch_start + batch_size]
            endpoints = [
                RouteEndpoint(
                    lat=float(station["latitude"]),
                    lon=float(station["longitude"]),
                    label=str(station.get("station_id") or ""),
                )
                for station, _projection in batch
            ]
            origin_distances = self.ors_client.matrix_one_to_many(origin, endpoints, profile=ROUTE_PROFILE)
            destination_distances = self.ors_client.matrix_many_to_one(endpoints, destination, profile=ROUTE_PROFILE)
            for (station, projection), origin_distance, destination_distance in zip(
                batch,
                origin_distances,
                destination_distances,
            ):
                if origin_distance is None or destination_distance is None:
                    continue
                route_detour_m = max(0.0, float(origin_distance) + float(destination_distance) - route.distance_m)
                drive_distance_to_route_m = route_detour_m / 2.0
                if drive_distance_to_route_m > max_drive_distance_m:
                    continue
                validated.append(
                    CandidateRouteInfo(
                        station=station,
                        projection=projection,
                        origin_distance_m=float(origin_distance),
                        destination_distance_m=float(destination_distance),
                        route_detour_m=route_detour_m,
                        drive_distance_to_route_m=drive_distance_to_route_m,
                    )
                )
        return validated

    def _apply_post_validation_filters(
        self,
        candidates: list[CandidateRouteInfo],
        *,
        filters: RouteFilters,
    ) -> list[CandidateRouteInfo]:
        filtered = [
            item
            for item in candidates
            if not filters.currently_open_only or _station_has_open_amenity(item.station)
        ]
        if not filters.available_only or not filtered:
            return filtered if not filters.available_only else []
        live_by_station_id = self._live_availability_by_station_id(
            [str(item.station.get("station_id") or "") for item in filtered]
        )
        return [
            item
            for item in filtered
            if live_by_station_id.get(str(item.station.get("station_id") or ""), False)
        ]

    def _live_availability_by_station_id(self, station_ids: list[str]) -> dict[str, bool]:
        normalized_ids = [station_id for station_id in dict.fromkeys(station_ids) if station_id]
        if not normalized_ids:
            return {}
        rows = self.live_store.list_station_summaries_by_ids(normalized_ids)
        availability: dict[str, bool] = {}
        for row in rows:
            station_id = str(row.get("station_id") or "")
            total_evses = int(row.get("total_evses") or 0)
            available_evses = int(row.get("available_evses") or 0)
            availability[station_id] = total_evses > 0 and available_evses > 0
        return availability


def normalize_directions_response(payload: dict[str, Any], *, profile: str = ROUTE_PROFILE) -> NormalizedRoute:
    feature = _first_feature(payload)
    geometry = feature.get("geometry") if isinstance(feature, dict) else None
    if not isinstance(geometry, dict):
        raise RouteProviderError("route_geometry_missing")
    if geometry.get("type") != "LineString":
        raise RouteProviderError("route_geometry_invalid")
    coordinates = geometry.get("coordinates")
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        raise RouteProviderError("route_geometry_invalid")
    normalized_coordinates = []
    points = []
    for item in coordinates:
        if not isinstance(item, list | tuple) or len(item) < 2:
            continue
        lon = _float_or_none(item[0])
        lat = _float_or_none(item[1])
        if lat is None or lon is None:
            continue
        normalized_coordinates.append([lon, lat])
        points.append({"lat": lat, "lon": lon})
    if len(points) < 2:
        raise RouteProviderError("route_geometry_invalid")

    properties = feature.get("properties") if isinstance(feature.get("properties"), dict) else {}
    summary = properties.get("summary") if isinstance(properties.get("summary"), dict) else {}
    distance_m = _float_or_none(summary.get("distance")) if isinstance(summary, dict) else None
    duration_s = _float_or_none(summary.get("duration")) if isinstance(summary, dict) else None
    if distance_m is None:
        distance_m = _route_distance_m(points)
    if duration_s is None:
        duration_s = 0.0

    return NormalizedRoute(
        source=ROUTE_SOURCE,
        profile=profile,
        distance_m=distance_m,
        duration_s=duration_s,
        geometry={"type": "LineString", "coordinates": normalized_coordinates},
        points=points,
    )


def project_station_to_route(station: dict[str, Any], route_points: list[dict[str, float]]) -> RouteProjection:
    station_lat = float(station["latitude"])
    station_lon = float(station["longitude"])
    cumulative = 0.0
    best_distance = math.inf
    best_position = 0.0
    best_point = {"lat": route_points[0]["lat"], "lon": route_points[0]["lon"]}
    for start, end in zip(route_points, route_points[1:]):
        segment_length = _haversine_m(start["lat"], start["lon"], end["lat"], end["lon"])
        distance_m, t, nearest = _point_segment_distance_m(station_lat, station_lon, start, end)
        if distance_m < best_distance:
            best_distance = distance_m
            best_position = cumulative + segment_length * t
            best_point = nearest
        cumulative += segment_length
    return RouteProjection(
        straight_line_distance_to_route_m=best_distance,
        route_position_m=best_position,
        nearest_route_point={
            "lat": round(best_point["lat"], 6),
            "lon": round(best_point["lon"], 6),
        },
    )


def _first_feature(payload: dict[str, Any]) -> dict[str, Any]:
    features = payload.get("features")
    if isinstance(features, list) and features:
        feature = features[0]
        if isinstance(feature, dict):
            return feature
    raise RouteNotFound("route_not_found")


def _response_mentions_quota_exceeded(response: requests.Response) -> bool:
    return "quota exceeded" in _response_error_text(response).lower()


def _response_error_text(response: requests.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return str(getattr(response, "text", "") or "")
    if isinstance(payload, dict):
        fragments: list[str] = []
        for key in ("error", "message", "detail"):
            value = payload.get(key)
            if isinstance(value, str):
                fragments.append(value)
            elif isinstance(value, dict):
                fragments.extend(str(item) for item in value.values())
            elif isinstance(value, list):
                fragments.extend(str(item) for item in value)
        return " ".join(fragments)
    return str(payload)


def _route_cache_key(origin: RouteEndpoint, destination: RouteEndpoint) -> str:
    payload = {
        "profile": ROUTE_PROFILE,
        "origin": [round(origin.lat, 5), round(origin.lon, 5)],
        "destination": [round(destination.lat, 5), round(destination.lon, 5)],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _route_distance_m(points: list[dict[str, float]]) -> float:
    return sum(
        _haversine_m(start["lat"], start["lon"], end["lat"], end["lon"])
        for start, end in zip(points, points[1:])
    )


def _point_segment_distance_m(
    lat: float,
    lon: float,
    start: dict[str, float],
    end: dict[str, float],
) -> tuple[float, float, dict[str, float]]:
    ref_lat = (lat + start["lat"] + end["lat"]) / 3.0
    px, py = _project_xy(lat, lon, ref_lat)
    ax, ay = _project_xy(start["lat"], start["lon"], ref_lat)
    bx, by = _project_xy(end["lat"], end["lon"], ref_lat)
    dx = bx - ax
    dy = by - ay
    length_sq = dx * dx + dy * dy
    if length_sq <= 0:
        return math.hypot(px - ax, py - ay), 0.0, {"lat": start["lat"], "lon": start["lon"]}
    t = max(0.0, min(1.0, ((px - ax) * dx + (py - ay) * dy) / length_sq))
    nearest_x = ax + t * dx
    nearest_y = ay + t * dy
    nearest = {
        "lat": start["lat"] + (end["lat"] - start["lat"]) * t,
        "lon": start["lon"] + (end["lon"] - start["lon"]) * t,
    }
    return math.hypot(px - nearest_x, py - nearest_y), t, nearest


def _project_xy(lat: float, lon: float, ref_lat: float) -> tuple[float, float]:
    return (
        lon * 111_320.0 * max(math.cos(math.radians(ref_lat)), 0.01),
        lat * 111_320.0,
    )


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6_371_000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = (
        math.sin(delta_phi / 2) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    )
    return radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _station_matches_static_filters(station: dict[str, Any], filters: RouteFilters) -> bool:
    if filters.operator and str(station.get("operator_name") or "") != filters.operator:
        return False
    max_power_kw = _float_or_none(station.get("max_power_kw")) or 0.0
    if max_power_kw < filters.min_power_kw:
        return False
    if int(station.get("amenities_total") or 0) < filters.min_amenities_total:
        return False
    counts = _normalized_amenity_counts(station.get("amenity_category_counts"))
    for key in filters.normalized_selected_amenities:
        if counts.get(key, 0) <= 0:
            return False
    return _station_matches_amenity_name_query(station, filters.amenity_name_query)


def _station_matches_amenity_name_query(station: dict[str, Any], query: str) -> bool:
    normalized_query = _normalize_amenity_name_query(query)
    if not normalized_query:
        return True
    for example in _amenity_examples(station):
        name = str(example.get("name") or "")
        if normalized_query in _normalize_amenity_name_query(name):
            return True
    return False


def _station_has_open_amenity(station: dict[str, Any]) -> bool:
    for example in _amenity_examples(station):
        opening_hours = str(example.get("opening_hours") or example.get("openingHours") or "").strip()
        if _is_amenity_open(opening_hours):
            return True
    return False


def _is_amenity_open(opening_hours: str, now: datetime | None = None) -> bool:
    normalized = re.sub(r"\s+", " ", str(opening_hours or "").strip())
    if not normalized:
        return False
    if normalized.lower() == "24/7":
        return True
    if re.match(r"^(?:off|closed)$", normalized, flags=re.IGNORECASE):
        return False
    if normalized.lower() == "open":
        return True

    effective_now = now or datetime.now(ZoneInfo("Europe/Berlin"))
    day_index = effective_now.weekday()
    day_key = OPENING_DAY_KEYS[day_index]
    previous_day_key = OPENING_DAY_KEYS[(day_index + 6) % len(OPENING_DAY_KEYS)]
    minute_of_day = effective_now.hour * 60 + effective_now.minute
    clauses = [
        clause
        for clause in (_parse_opening_clause(part) for part in normalized.split(";"))
        if clause is not None
    ]
    if not clauses:
        return False

    current_state: str | None = None
    for clause in clauses:
        state = _opening_state(clause, day_key=day_key, minute_of_day=minute_of_day, previous_day=False)
        if state is not None:
            current_state = state
    if current_state == "open":
        return True
    if current_state == "unknown":
        return False

    return any(
        _opening_state(
            clause,
            day_key=previous_day_key,
            minute_of_day=minute_of_day,
            previous_day=True,
        )
        == "open"
        for clause in clauses
    )


def _parse_opening_clause(value: str) -> OpeningClause | None:
    trimmed = str(value or "").strip()
    if not trimmed:
        return None
    match = OPENING_DAY_SELECTOR_RE.match(trimmed)
    selector = match.group(1) if match else None
    body = (match.group(2) if match else trimmed).strip()
    if re.match(r"^(?:off|closed)$", body, flags=re.IGNORECASE):
        return OpeningClause(_selected_opening_days(selector), "closed", [])
    if body.lower() == "open":
        return OpeningClause(_selected_opening_days(selector), "open", [])
    ranges = [
        parsed
        for parsed in (_parse_opening_range(part) for part in body.split(","))
        if parsed is not None
    ]
    if not ranges:
        return OpeningClause(_selected_opening_days(selector), "unknown", [])
    return OpeningClause(_selected_opening_days(selector), "times", ranges)


def _selected_opening_days(selector: str | None) -> set[str] | None:
    if not selector:
        return None
    selected: set[str] = set()
    for raw_part in selector.split(","):
        part = raw_part.strip()
        if part == "PH":
            continue
        if "-" in part:
            bounds = [item.strip() for item in part.split("-")]
            if len(bounds) != 2 or bounds[0] not in OPENING_DAY_KEYS or bounds[1] not in OPENING_DAY_KEYS:
                continue
            start = OPENING_DAY_KEYS.index(bounds[0])
            end = OPENING_DAY_KEYS.index(bounds[1])
            for offset in range(len(OPENING_DAY_KEYS)):
                index = (start + offset) % len(OPENING_DAY_KEYS)
                selected.add(OPENING_DAY_KEYS[index])
                if index == end:
                    break
        elif part in OPENING_DAY_KEYS:
            selected.add(part)
    return selected


def _parse_opening_range(value: str) -> OpeningRange | None:
    compact = re.sub(r"\s+", "", str(value or ""))
    if compact.endswith("+") and "-" not in compact:
        start = _parse_opening_minute(compact[:-1])
        if start is None:
            return None
        return OpeningRange(start=start, end=24 * 60, open_ended=True)
    parts = compact.replace("+", "").split("-")
    if len(parts) != 2:
        return None
    start = _parse_opening_minute(parts[0])
    end = _parse_opening_minute(parts[1])
    if start is None or end is None:
        return None
    return OpeningRange(start=start, end=end, open_ended=False)


def _parse_opening_minute(value: str) -> int | None:
    pieces = str(value or "").split(":")
    if len(pieces) != 2:
        return None
    try:
        hour = int(pieces[0])
        minute = int(pieces[1])
    except ValueError:
        return None
    if hour < 0 or hour > 24 or minute < 0 or minute > 59 or (hour == 24 and minute != 0):
        return None
    return hour * 60 + minute


def _opening_state(
    clause: OpeningClause,
    *,
    day_key: str,
    minute_of_day: int,
    previous_day: bool,
) -> str | None:
    if clause.selected_days is not None and day_key not in clause.selected_days:
        return None
    if clause.mode == "closed":
        return None if previous_day else "closed"
    if clause.mode == "open":
        return "open"
    if clause.mode == "unknown":
        return None if previous_day else "unknown"
    if clause.mode == "times":
        if any(_is_within_opening_range(item, minute_of_day=minute_of_day, previous_day=previous_day) for item in clause.ranges):
            return "open"
        return None if previous_day else "closed"
    return None


def _is_within_opening_range(
    opening_range: OpeningRange,
    *,
    minute_of_day: int,
    previous_day: bool,
) -> bool:
    if opening_range.open_ended:
        return minute_of_day < 6 * 60 if previous_day else minute_of_day >= opening_range.start
    if opening_range.start == opening_range.end:
        return True
    if opening_range.start < opening_range.end:
        return not previous_day and opening_range.start <= minute_of_day < opening_range.end
    return minute_of_day < opening_range.end if previous_day else minute_of_day >= opening_range.start


def _amenity_examples(station: dict[str, Any]) -> list[dict[str, Any]]:
    examples = station.get("amenity_examples")
    if isinstance(examples, list):
        return [example for example in examples if isinstance(example, dict)]
    return []


def _normalized_amenity_counts(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, int] = {}
    for key, count in value.items():
        normalized_key = _normalize_amenity_key(str(key))
        if not normalized_key:
            continue
        normalized[normalized_key] = normalized.get(normalized_key, 0) + int(count or 0)
    return normalized


def _normalize_amenity_key(value: str) -> str:
    key = str(value or "").strip().lower()
    if not key:
        return ""
    return key if key.startswith(AMENITY_PREFIX) else f"{AMENITY_PREFIX}{key}"


def _normalize_amenity_name_query(value: str) -> str:
    text = str(value or "").strip().lower().replace("ß", "ss")
    decomposed = unicodedata.normalize("NFD", text)
    return "".join(
        char
        for char in decomposed
        if char.isalnum() and unicodedata.category(char) != "Mn"
    )


def _validation_pool_sort_key(station: dict[str, Any], projection: RouteProjection) -> tuple[float, int, float, float, str]:
    return (
        projection.straight_line_distance_to_route_m,
        -_amenity_tier(station),
        -(_float_or_none(station.get("max_power_kw")) or 0.0),
        projection.route_position_m,
        str(station.get("station_id") or ""),
    )


def _route_result_sort_key(item: CandidateRouteInfo) -> tuple[float, int, float, float, str]:
    return (
        item.drive_distance_to_route_m,
        -_amenity_tier(item.station),
        -(_float_or_none(item.station.get("max_power_kw")) or 0.0),
        item.projection.route_position_m,
        str(item.station.get("station_id") or ""),
    )


def _amenity_tier(station: dict[str, Any]) -> int:
    total = int(station.get("amenities_total") or 0)
    if total >= 11:
        return 3
    if total >= 6:
        return 2
    if total >= 1:
        return 1
    return 0


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None
