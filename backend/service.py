from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .archive import ResponseLogWriter
from .config import AppConfig
from .datex import decode_json_payload, extract_dynamic_facts
from .fetcher import CurlFetcher
from .loaders import load_evse_matches, load_provider_targets, load_site_matches, load_station_records
from .models import FetchResponse
from .store import LiveStore, utc_now_iso


class IngestionService:
    def __init__(
        self,
        config: AppConfig,
        *,
        store: LiveStore | None = None,
        fetcher: Any | None = None,
        response_log_writer: ResponseLogWriter | None = None,
    ):
        self.config = config
        self.store = store or LiveStore(config)
        self.fetcher = fetcher or CurlFetcher(config)
        self.response_log_writer = response_log_writer or ResponseLogWriter(config)

    def bootstrap(self) -> None:
        self.store.initialize()
        self.store.upsert_provider_targets(
            load_provider_targets(
                self.config.provider_config_path,
                self.config.provider_override_path,
                self.config.subscription_registry_path,
            )
        )
        self.store.upsert_site_matches(load_site_matches(self.config.site_match_path, self.config.chargers_csv_path))
        self.store.upsert_evse_matches(load_evse_matches(self.config.chargers_csv_path))
        self.store.reconcile_station_ids_from_site_matches()
        self.store.upsert_stations(load_station_records(self.config.chargers_csv_path))

    def _persist_payload(
        self,
        *,
        provider_uid: str,
        fetched_at: str,
        payload_bytes: bytes,
        content_type: str,
    ) -> tuple[str, int, int]:
        payload = decode_json_payload(payload_bytes)
        facts = extract_dynamic_facts(
            payload,
            provider_uid,
            self.store.get_site_station_map(provider_uid),
            self.store.get_evse_station_map(provider_uid),
        )
        return self.store.persist_provider_observations(
            provider_uid=provider_uid,
            facts=facts,
            fetched_at=fetched_at,
            payload_bytes=payload_bytes,
            content_type=content_type,
        )

    def _resolve_provider_for_push(
        self,
        *,
        provider_uid: str = "",
        subscription_id: str = "",
        publication_id: str = "",
    ) -> dict[str, Any]:
        provider_uid = str(provider_uid or "").strip()
        subscription_id = str(subscription_id or "").strip()
        publication_id = str(publication_id or "").strip()

        if provider_uid:
            provider = self.store.get_provider(provider_uid)
            if provider is None:
                raise KeyError(f"unknown_provider:{provider_uid}")
            return provider

        if subscription_id:
            provider = self.store.get_provider_by_subscription_id(subscription_id)
            if provider is None:
                raise KeyError(f"unknown_subscription_id:{subscription_id}")
            return provider

        if publication_id:
            provider = self.store.get_provider_by_publication_id(publication_id)
            if provider is None:
                raise KeyError(f"unknown_publication_id:{publication_id}")
            return provider

        raise ValueError("missing_push_provider_hint")

    def ingest_provider(self, provider_uid: str, *, bootstrap: bool = True) -> dict[str, Any]:
        if bootstrap:
            self.bootstrap()
        provider = self.store.get_provider(provider_uid)
        if provider is None:
            raise KeyError(f"unknown_provider:{provider_uid}")

        poll_run_id = self.store.start_poll_run(provider_uid)
        fetched_at = utc_now_iso()
        fetch_response: FetchResponse | None = None
        response_log_attempted = False

        try:
            fetch_response = self.fetcher.fetch(type("ProviderRow", (), provider))
            response_log_attempted = True
            self.response_log_writer.write_http_response(
                provider_uid=provider_uid,
                fetched_at=fetched_at,
                response=fetch_response,
            )
            if fetch_response.http_status in (204, 304):
                result = "no_data" if fetch_response.http_status == 204 else "not_modified"
                self.store.finish_poll_run(
                    poll_run_id,
                    provider_uid=provider_uid,
                    result=result,
                    fetched_at=fetched_at,
                    http_status=fetch_response.http_status,
                )
                return {
                    "provider_uid": provider_uid,
                    "result": result,
                    "fetched_at": fetched_at,
                    "http_status": fetch_response.http_status,
                    "observation_count": 0,
                    "changed_observation_count": 0,
                }
            if fetch_response.http_status >= 400:
                error_text = f"http_{fetch_response.http_status}"
                self.store.finish_poll_run(
                    poll_run_id,
                    provider_uid=provider_uid,
                    result="error",
                    fetched_at=fetched_at,
                    http_status=fetch_response.http_status,
                    error_text=error_text,
                )
                return {
                    "provider_uid": provider_uid,
                    "result": "error",
                    "fetched_at": fetched_at,
                    "http_status": fetch_response.http_status,
                    "error": error_text,
                }
            payload_sha256, observation_count, changed_count = self._persist_payload(
                provider_uid=provider_uid,
                fetched_at=fetched_at,
                payload_bytes=fetch_response.body,
                content_type=fetch_response.content_type,
            )
            self.store.finish_poll_run(
                poll_run_id,
                provider_uid=provider_uid,
                result="ok",
                fetched_at=fetched_at,
                http_status=fetch_response.http_status,
                payload_sha256=payload_sha256,
                observation_count=observation_count,
                changed_observation_count=changed_count,
            )
            return {
                "provider_uid": provider_uid,
                "result": "ok",
                "fetched_at": fetched_at,
                "http_status": fetch_response.http_status,
                "observation_count": observation_count,
                "changed_observation_count": changed_count,
            }
        except TimeoutError as exc:
            if not response_log_attempted:
                self.response_log_writer.write_fetch_failure(
                    provider_uid=provider_uid,
                    fetched_at=fetched_at,
                    failure_kind="timeout",
                    error_text=str(exc),
                )
            self.store.finish_poll_run(
                poll_run_id,
                provider_uid=provider_uid,
                result="timeout",
                fetched_at=fetched_at,
                error_text=str(exc),
            )
            return {"provider_uid": provider_uid, "result": "timeout", "fetched_at": fetched_at, "error": str(exc)}
        except Exception as exc:
            if not response_log_attempted:
                failure_kind = "error"
                if fetch_response is not None:
                    response_log_attempted = True
                    self.response_log_writer.write_http_response(
                        provider_uid=provider_uid,
                        fetched_at=fetched_at,
                        response=fetch_response,
                    )
                else:
                    self.response_log_writer.write_fetch_failure(
                        provider_uid=provider_uid,
                        fetched_at=fetched_at,
                        failure_kind=failure_kind,
                        error_text=str(exc),
                    )
            self.store.finish_poll_run(
                poll_run_id,
                provider_uid=provider_uid,
                result="error",
                fetched_at=fetched_at,
                error_text=str(exc),
            )
            return {"provider_uid": provider_uid, "result": "error", "fetched_at": fetched_at, "error": str(exc)}

    def ingest_push(
        self,
        *,
        provider_uid: str = "",
        subscription_id: str = "",
        publication_id: str = "",
        payload_bytes: bytes,
        content_type: str = "",
        content_encoding: str = "",
        request_path: str = "",
        request_query: str = "",
        request_headers: Mapping[str, Any] | None = None,
        bootstrap: bool = True,
    ) -> dict[str, Any]:
        if bootstrap:
            self.bootstrap()

        received_at = utc_now_iso()
        resolved_provider_uid = str(provider_uid or "").strip() or "unknown-provider"
        push_run_id: int | None = None

        try:
            provider = self._resolve_provider_for_push(
                provider_uid=provider_uid,
                subscription_id=subscription_id,
                publication_id=publication_id,
            )
            resolved_provider_uid = str(provider["provider_uid"])
            self.response_log_writer.write_push_request(
                provider_uid=resolved_provider_uid,
                received_at=received_at,
                payload_bytes=payload_bytes,
                content_type=content_type,
                content_encoding=content_encoding,
                subscription_id=subscription_id,
                publication_id=publication_id,
                request_path=request_path,
                request_query=request_query,
                request_headers=request_headers,
            )
            push_run_id = self.store.start_push_run(
                resolved_provider_uid,
                subscription_id=subscription_id,
                publication_id=publication_id,
                received_at=received_at,
                content_type=content_type,
                content_encoding=content_encoding,
                request_path=request_path,
                request_query=request_query,
            )
            payload_sha256, observation_count, changed_count = self._persist_payload(
                provider_uid=resolved_provider_uid,
                fetched_at=received_at,
                payload_bytes=payload_bytes,
                content_type=content_type,
            )
            self.store.finish_push_run(
                push_run_id,
                provider_uid=resolved_provider_uid,
                result="ok",
                received_at=received_at,
                payload_sha256=payload_sha256,
                observation_count=observation_count,
                changed_observation_count=changed_count,
            )
            return {
                "provider_uid": resolved_provider_uid,
                "subscription_id": subscription_id,
                "publication_id": publication_id,
                "result": "ok",
                "received_at": received_at,
                "observation_count": observation_count,
                "changed_observation_count": changed_count,
            }
        except Exception as exc:
            if push_run_id is not None:
                self.store.finish_push_run(
                    push_run_id,
                    provider_uid=resolved_provider_uid,
                    result="error",
                    received_at=received_at,
                    error_text=str(exc),
                )
            if push_run_id is None:
                self.response_log_writer.write_push_request(
                    provider_uid=resolved_provider_uid,
                    received_at=received_at,
                    payload_bytes=payload_bytes,
                    content_type=content_type,
                    content_encoding=content_encoding,
                    subscription_id=subscription_id,
                    publication_id=publication_id,
                    request_path=request_path,
                    request_query=request_query,
                    request_headers=request_headers,
                )
            raise

    def ingest_once(self, provider_uid: str | None = None, max_providers: int | None = None) -> list[dict[str, Any]]:
        self.bootstrap()
        if provider_uid:
            return [self.ingest_provider(provider_uid, bootstrap=False)]

        providers = self.store.list_providers(enabled_only=True)
        if max_providers is not None:
            providers = providers[:max_providers]
        return [self.ingest_provider(str(provider["provider_uid"]), bootstrap=False) for provider in providers]

    def ingest_next_provider(self, *, bootstrap: bool = True) -> dict[str, Any] | None:
        if bootstrap:
            self.bootstrap()
        provider = self.store.get_next_provider_for_round_robin()
        if provider is None:
            return None
        return self.ingest_provider(str(provider["provider_uid"]), bootstrap=False)

    def seconds_until_next_provider_due(self, *, bootstrap: bool = True) -> float | None:
        if bootstrap:
            self.bootstrap()
        return self.store.seconds_until_next_provider_due()
