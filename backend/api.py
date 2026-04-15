from __future__ import annotations

import re

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .config import AppConfig
from .service import IngestionService
from .status import build_bundle_live_status_report
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


class StationLookupRequest(BaseModel):
    station_ids: list[str] = Field(default_factory=list, max_length=200)


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


def create_app(config: AppConfig | None = None) -> FastAPI:
    effective_config = config or AppConfig()
    store = LiveStore(effective_config)
    store.initialize()
    ingestion_service = IngestionService(effective_config, store=store)
    cors_origin_regex = effective_config.api_cors_allowed_origin_regex or None

    app = FastAPI(title="woladen live API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(effective_config.api_cors_allowed_origins),
        allow_origin_regex=cors_origin_regex,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.state.config = effective_config
    app.state.store = store
    app.state.ingestion_service = ingestion_service

    @app.get("/healthz")
    def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/status")
    @app.get("/v1/status")
    def status() -> dict:
        return build_bundle_live_status_report(
            store=app.state.store,
            geojson_path=app.state.config.chargers_geojson_path,
        )

    @app.head("/v1/push")
    @app.head("/v1/push/{provider_uid}")
    def push_healthcheck(provider_uid: str = "") -> Response:
        return Response(status_code=200)

    @app.post("/v1/push")
    @app.post("/v1/push/{provider_uid}")
    async def push_ingest(request: Request, provider_uid: str = "") -> Response:
        payload_bytes = await request.body()
        resolved_provider_uid = provider_uid or _request_lookup_value(request, PROVIDER_LOOKUP_KEYS)
        subscription_id = _request_lookup_value(request, SUBSCRIPTION_LOOKUP_KEYS)
        publication_id = _request_lookup_value(request, PUBLICATION_LOOKUP_KEYS)

        try:
            app.state.ingestion_service.ingest_push(
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
        provider_uid: str = Query(default=""),
        status: str = Query(default="", pattern="^(|free|occupied|out_of_order|unknown)$"),
        limit: int = Query(default=100, ge=1),
        offset: int = Query(default=0, ge=0),
    ) -> list[dict]:
        rows = app.state.store.list_station_summaries(
            provider_uid=provider_uid,
            status=status,
            limit=min(limit, 100),
            offset=offset,
        )
        return [_serialize_station_summary(row) for row in rows]

    @app.post("/v1/stations/lookup")
    def lookup_stations(payload: StationLookupRequest) -> dict[str, list[dict] | list[str]]:
        station_ids = [str(station_id or "").strip() for station_id in payload.station_ids]
        station_ids = [station_id for station_id in station_ids if station_id]
        stations = app.state.store.list_station_summaries_by_ids(station_ids)
        found_station_ids = {str(station["station_id"]) for station in stations}
        missing_station_ids = [station_id for station_id in station_ids if station_id not in found_station_ids]
        return {
            "stations": [_serialize_station_summary(station) for station in stations],
            "missing_station_ids": missing_station_ids,
        }

    @app.get("/v1/stations/{station_id}")
    def station_detail(station_id: str) -> dict:
        payload = app.state.store.get_station_detail(station_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="station_not_found")
        return _serialize_station_detail(payload)

    @app.get("/v1/evses/{provider_uid}/{provider_evse_id}")
    def evse_detail(
        provider_uid: str,
        provider_evse_id: str,
    ) -> dict:
        payload = app.state.store.get_evse_detail(provider_uid, provider_evse_id)
        if payload is None:
            raise HTTPException(status_code=404, detail="evse_not_found")
        return _serialize_evse_detail(payload)

    return app
