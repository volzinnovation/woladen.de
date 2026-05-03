from __future__ import annotations

import re
import time

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .config import AppConfig
from .service import IngestionService
from .store import LiveStore

STATIC_STATION_RESPONSE_FIELDS = {
    "operator",
    "address",
    "postcode",
    "city",
    "lat",
    "lon",
    "charging_points_count",
    "max_power_kw",
    "provider_uid",
}
PROVIDER_LOOKUP_KEYS = ("provider_uid", "provider")
SUBSCRIPTION_LOOKUP_KEYS = (
    "subscription_id",
    "subscriptionid",
    "x-subscription-id",
    "x-mobilithek-subscription-id",
)
PUBLICATION_LOOKUP_KEYS = (
    "publication_id",
    "publicationid",
    "x-publication-id",
    "x-mobilithek-publication-id",
)
PROFILE_FLAG_VALUES = {"1", "true", "yes", "on"}
PROFILE_HEADER_NAMES = ("Server-Timing", "Timing-Allow-Origin", "Content-Length")
MAX_STATION_LOOKUP_IDS = 20
MAX_RATING_LOOKUP_IDS = 50


class StationLookupRequest(BaseModel):
    station_ids: list[str] = Field(default_factory=list, max_length=MAX_STATION_LOOKUP_IDS)


class RatingLookupRequest(BaseModel):
    station_ids: list[str] = Field(default_factory=list, max_length=MAX_RATING_LOOKUP_IDS)


class StationRatingRequest(BaseModel):
    station_id: str = Field(min_length=1, max_length=128)
    rating: int = Field(ge=1, le=5)
    client_id: str = Field(min_length=16, max_length=128)


def _strip_fields(payload: dict, excluded_fields: set[str]) -> dict:
    return {key: value for key, value in payload.items() if key not in excluded_fields}


def _serialize_station_summary(payload: dict) -> dict:
    return _strip_fields(payload, STATIC_STATION_RESPONSE_FIELDS)


def _serialize_station_detail(payload: dict) -> dict:
    return {
        "station": _serialize_station_summary(payload["station"]),
        "evses": [_strip_fields(item, {"provider_uid"}) for item in payload["evses"]],
        "recent_observations": [
            _strip_fields(item, {"provider_uid"}) for item in payload["recent_observations"]
        ],
    }


def _serialize_evse_detail(payload: dict) -> dict:
    return {
        "current": _strip_fields(payload["current"], {"provider_uid"}),
        "recent_observations": [
            _strip_fields(item, {"provider_uid"}) for item in payload["recent_observations"]
        ],
    }


def _normalize_lookup_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _request_lookup_value(request: Request, keys: tuple[str, ...]) -> str:
    normalized_keys = {_normalize_lookup_key(key) for key in keys}
    for mapping in (request.query_params.multi_items(), request.headers.items()):
        for key, value in mapping:
            if _normalize_lookup_key(key) not in normalized_keys:
                continue
            text = str(value or "").strip()
            if text:
                return text
    return ""


def _profiling_enabled(request: Request) -> bool:
    profile_value = (
        request.query_params.get("profile")
        or request.headers.get("x-woladen-profile")
        or ""
    )
    return str(profile_value).strip().lower() in PROFILE_FLAG_VALUES


def _record_profile_metric(request: Request, metric_name: str, duration_ms: float, description: str = "") -> None:
    if not getattr(request.state, "profiling_enabled", False):
        return
    metrics = getattr(request.state, "profiling_metrics", None)
    if metrics is None:
        metrics = {}
        request.state.profiling_metrics = metrics
    metric = metrics.setdefault(metric_name, {"duration_ms": 0.0, "description": description})
    metric["duration_ms"] += max(float(duration_ms), 0.0)
    if description and not metric.get("description"):
        metric["description"] = description


def _record_store_timings(request: Request, timings: dict[str, float] | None) -> None:
    if not timings:
        return
    metric_map = {
        "db_query_ms": ("db-query", "SQLite query"),
        "db_decode_ms": ("db-decode", "SQLite row decode"),
    }
    for key, duration_ms in timings.items():
        metric_name, description = metric_map.get(key, (key.replace("_", "-"), ""))
        _record_profile_metric(request, metric_name, duration_ms, description)


def _server_timing_header_value(request: Request) -> str:
    metrics = getattr(request.state, "profiling_metrics", {}) or {}
    header_parts: list[str] = []
    for metric_name, metric in metrics.items():
        part = f"{metric_name};dur={metric['duration_ms']:.3f}"
        description = str(metric.get("description") or "").strip()
        if description:
            escaped = description.replace("\\", "\\\\").replace('"', '\\"')
            part += f';desc="{escaped}"'
        header_parts.append(part)
    return ", ".join(header_parts)


def _json_response(request: Request, payload: object, *, status_code: int = 200) -> JSONResponse:
    encode_started_at = time.perf_counter()
    response = JSONResponse(content=payload, status_code=status_code)
    _record_profile_metric(
        request,
        "json-encode",
        (time.perf_counter() - encode_started_at) * 1000.0,
        "JSON encode",
    )
    return response


def create_app(config: AppConfig | None = None) -> FastAPI:
    effective_config = config or AppConfig()
    store = LiveStore(effective_config)
    store.initialize()
    ingestion_service = IngestionService(effective_config, store=store)
    station_catalog_path = effective_config.full_chargers_csv_path or effective_config.chargers_csv_path
    if (
        effective_config.provider_config_path.exists()
        and effective_config.site_match_path.exists()
        and station_catalog_path.exists()
    ):
        ingestion_service.bootstrap()
    else:
        ingestion_service.receipt_queue.initialize()
    cors_origin_regex = effective_config.api_cors_allowed_origin_regex or None

    app = FastAPI(title="woladen live API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(effective_config.api_cors_allowed_origins),
        allow_origin_regex=cors_origin_regex,
        allow_methods=["*"],
        allow_headers=["*"],
        expose_headers=list(PROFILE_HEADER_NAMES),
    )
    app.state.config = effective_config
    app.state.store = store
    app.state.ingestion_service = ingestion_service
    app.state.receipt_queue = ingestion_service.receipt_queue

    @app.middleware("http")
    async def add_request_timing(request: Request, call_next):
        request.state.profiling_enabled = _profiling_enabled(request)
        request.state.profiling_metrics = {}
        request_started_at = time.perf_counter()
        response = await call_next(request)
        response.headers.setdefault("Timing-Allow-Origin", "*")
        if request.state.profiling_enabled:
            _record_profile_metric(
                request,
                "app",
                (time.perf_counter() - request_started_at) * 1000.0,
                "Total app time",
            )
            server_timing = _server_timing_header_value(request)
            if server_timing:
                response.headers["Server-Timing"] = server_timing
        return response

    @app.get("/healthz")
    def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/status")
    @app.get("/v1/status")
    def status() -> dict:
        raise HTTPException(status_code=404, detail="status_endpoint_disabled")

    @app.head("/v1/push")
    @app.head("/v1/push/{provider_uid}")
    def push_healthcheck(provider_uid: str = "") -> Response:
        return Response(status_code=200)

    @app.get("/v1/push")
    @app.get("/v1/push/{provider_uid}")
    def push_probe(provider_uid: str = "") -> dict[str, bool | str | None]:
        return {
            "ok": True,
            "provider_uid": provider_uid or None,
        }

    @app.post("/v1/push")
    @app.post("/v1/push/{provider_uid}")
    async def push_ingest(request: Request, provider_uid: str = "") -> Response:
        payload_bytes = await request.body()
        resolved_provider_uid = provider_uid or _request_lookup_value(request, PROVIDER_LOOKUP_KEYS)
        subscription_id = _request_lookup_value(request, SUBSCRIPTION_LOOKUP_KEYS)
        publication_id = _request_lookup_value(request, PUBLICATION_LOOKUP_KEYS)

        try:
            app.state.ingestion_service.receive_push(
                provider_uid=resolved_provider_uid,
                subscription_id=subscription_id,
                publication_id=publication_id,
                payload_bytes=payload_bytes,
                content_type=request.headers.get("content-type", ""),
                content_encoding=request.headers.get("content-encoding", ""),
                request_path=request.url.path,
                request_query=request.url.query,
                request_headers=dict(request.headers),
            )
        except KeyError as exc:
            detail = str(exc)
            if detail.startswith("'") and detail.endswith("'"):
                detail = detail[1:-1]
            raise HTTPException(status_code=404, detail=detail) from exc
        except ValueError as exc:
            status_code = 400 if str(exc) == "missing_push_provider_hint" else 422
            raise HTTPException(status_code=status_code, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

        return Response(status_code=200)

    @app.get("/v1/providers")
    def list_providers() -> list[dict]:
        return app.state.store.list_providers()

    @app.get("/v1/stations")
    def list_stations(
        request: Request,
        provider_uid: str = Query(default=""),
        status: str = Query(default="", pattern="^(|free|occupied|out_of_order|unknown)$"),
        limit: int = Query(default=100, ge=1),
        offset: int = Query(default=0, ge=0),
    ) -> JSONResponse:
        timings: dict[str, float] | None = {} if request.state.profiling_enabled else None
        rows = app.state.store.list_station_summaries(
            provider_uid=provider_uid,
            status=status,
            limit=min(limit, 100),
            offset=offset,
            timings=timings,
        )
        _record_store_timings(request, timings)
        payload_started_at = time.perf_counter()
        payload = [_serialize_station_summary(row) for row in rows]
        _record_profile_metric(
            request,
            "payload",
            (time.perf_counter() - payload_started_at) * 1000.0,
            "Response shaping",
        )
        return _json_response(request, payload)

    @app.post("/v1/stations/lookup")
    def lookup_stations(request: Request, payload: StationLookupRequest) -> JSONResponse:
        station_ids = [str(station_id or "").strip() for station_id in payload.station_ids]
        station_ids = [station_id for station_id in station_ids if station_id]
        timings: dict[str, float] | None = {} if request.state.profiling_enabled else None
        stations = app.state.store.list_station_summaries_by_ids(station_ids, timings=timings)
        _record_store_timings(request, timings)
        payload_started_at = time.perf_counter()
        found_station_ids = {str(station["station_id"]) for station in stations}
        missing_station_ids = [station_id for station_id in station_ids if station_id not in found_station_ids]
        response_payload = {
            "stations": [_serialize_station_summary(station) for station in stations],
            "missing_station_ids": missing_station_ids,
        }
        _record_profile_metric(
            request,
            "payload",
            (time.perf_counter() - payload_started_at) * 1000.0,
            "Response shaping",
        )
        return _json_response(request, response_payload)

    @app.post("/v1/ratings/lookup")
    def lookup_ratings(request: Request, payload: RatingLookupRequest) -> JSONResponse:
        station_ids = [str(station_id or "").strip() for station_id in payload.station_ids]
        station_ids = [station_id for station_id in station_ids if station_id]
        timings: dict[str, float] | None = {} if request.state.profiling_enabled else None
        ratings = app.state.store.list_station_rating_summaries_by_ids(station_ids, timings=timings)
        _record_store_timings(request, timings)
        payload_started_at = time.perf_counter()
        found_station_ids = {str(rating["station_id"]) for rating in ratings}
        missing_station_ids = [station_id for station_id in station_ids if station_id not in found_station_ids]
        response_payload = {
            "ratings": ratings,
            "missing_station_ids": missing_station_ids,
        }
        _record_profile_metric(
            request,
            "payload",
            (time.perf_counter() - payload_started_at) * 1000.0,
            "Response shaping",
        )
        return _json_response(request, response_payload)

    @app.post("/v1/ratings")
    def submit_rating(request: Request, payload: StationRatingRequest) -> JSONResponse:
        try:
            rating = app.state.store.upsert_station_rating(
                station_id=payload.station_id,
                rating=payload.rating,
                client_id=payload.client_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        response_payload = {
            "rating": rating,
            "user_rating": payload.rating,
        }
        return _json_response(request, response_payload)

    @app.get("/v1/stations/{station_id}")
    def station_detail(request: Request, station_id: str) -> JSONResponse:
        timings: dict[str, float] | None = {} if request.state.profiling_enabled else None
        payload = app.state.store.get_station_detail(station_id, timings=timings)
        _record_store_timings(request, timings)
        if payload is None:
            raise HTTPException(status_code=404, detail="station_not_found")
        payload_started_at = time.perf_counter()
        response_payload = _serialize_station_detail(payload)
        _record_profile_metric(
            request,
            "payload",
            (time.perf_counter() - payload_started_at) * 1000.0,
            "Response shaping",
        )
        return _json_response(request, response_payload)

    @app.get("/v1/evses/{provider_uid}/{provider_evse_id}")
    def evse_detail(
        request: Request,
        provider_uid: str,
        provider_evse_id: str,
    ) -> JSONResponse:
        timings: dict[str, float] | None = {} if request.state.profiling_enabled else None
        payload = app.state.store.get_evse_detail(provider_uid, provider_evse_id, timings=timings)
        _record_store_timings(request, timings)
        if payload is None:
            raise HTTPException(status_code=404, detail="evse_not_found")
        payload_started_at = time.perf_counter()
        response_payload = _serialize_evse_detail(payload)
        _record_profile_metric(
            request,
            "payload",
            (time.perf_counter() - payload_started_at) * 1000.0,
            "Response shaping",
        )
        return _json_response(request, response_payload)

    return app
