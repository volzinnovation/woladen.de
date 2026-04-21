from __future__ import annotations

import hashlib
import json
import threading
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .archive import ResponseLogWriter
from .config import AppConfig
from .datex import decode_json_payload, extract_dynamic_facts
from .fetcher import CurlFetcher
from .loaders import load_evse_matches, load_provider_targets, load_site_matches, load_station_records
from .models import EvseMatch, FetchResponse, SiteMatch
from .receipt_queue import ReceiptQueue, ReceiptTask
from .store import LiveStore, utc_now_iso


class IngestionService:
    def __init__(
        self,
        config: AppConfig,
        *,
        store: LiveStore | None = None,
        fetcher: Any | None = None,
        response_log_writer: ResponseLogWriter | None = None,
        receipt_queue: ReceiptQueue | None = None,
    ):
        self.config = config
        self.store = store or LiveStore(config)
        self.fetcher = fetcher or CurlFetcher(config)
        self.response_log_writer = response_log_writer or ResponseLogWriter(config)
        self.receipt_queue = receipt_queue or ReceiptQueue(config)
        self._bootstrap_lock = threading.Lock()
        self._bootstrapped = False
        self._bootstrap_signature: tuple[tuple[str, float | None], ...] | None = None
        self._site_station_maps: dict[str, dict[str, str]] = {}
        self._evse_station_maps: dict[str, dict[str, dict[str, str]]] = {}

    def bootstrap(self) -> None:
        current_signature = self._metadata_signature()
        if self._bootstrapped and self._bootstrap_signature == current_signature:
            return
        with self._bootstrap_lock:
            current_signature = self._metadata_signature()
            if self._bootstrapped and self._bootstrap_signature == current_signature:
                return
            self.store.initialize()
            self.receipt_queue.initialize()
            station_catalog_path = self.config.full_chargers_csv_path or self.config.chargers_csv_path
            provider_targets = load_provider_targets(
                self.config.provider_config_path,
                self.config.provider_override_path,
                self.config.subscription_registry_path,
            )
            site_matches = load_site_matches(self.config.site_match_path, station_catalog_path)
            evse_matches = load_evse_matches(station_catalog_path, self.config.site_match_path)
            self.store.upsert_provider_targets(provider_targets)
            self.store.upsert_site_matches(site_matches)
            self.store.upsert_evse_matches(evse_matches)
            self.store.reconcile_station_ids_from_site_matches()
            self.store.upsert_stations(load_station_records(station_catalog_path))
            self._site_station_maps = self._build_site_station_maps(site_matches)
            self._evse_station_maps = self._build_evse_station_maps(evse_matches)
            self._bootstrapped = True
            self._bootstrap_signature = current_signature

    def _metadata_signature(self) -> tuple[tuple[str, float | None], ...]:
        station_catalog_path = self.config.full_chargers_csv_path or self.config.chargers_csv_path
        source_paths = (
            self.config.provider_config_path,
            self.config.provider_override_path,
            self.config.subscription_registry_path,
            self.config.site_match_path,
            station_catalog_path,
        )
        signature: list[tuple[str, float | None]] = []
        for path in source_paths:
            if path is None:
                signature.append(("", None))
                continue
            try:
                stat = path.stat()
            except FileNotFoundError:
                signature.append((str(path), None))
                continue
            signature.append((str(path), stat.st_mtime_ns))
        return tuple(signature)

    def _build_site_station_maps(self, matches: list[SiteMatch]) -> dict[str, dict[str, str]]:
        by_provider: dict[str, dict[str, str]] = {}
        for match in matches:
            by_provider.setdefault(match.provider_uid, {})[match.site_id] = match.station_id
        return by_provider

    def _build_evse_station_maps(self, matches: list[EvseMatch]) -> dict[str, dict[str, dict[str, str]]]:
        by_provider: dict[str, dict[str, dict[str, str]]] = {}
        for match in matches:
            by_provider.setdefault(match.provider_uid, {})[match.evse_id] = {
                "station_id": match.station_id,
                "site_id": match.site_id,
                "station_ref": match.station_ref,
            }
        return by_provider

    def _site_station_map(self, provider_uid: str) -> dict[str, str]:
        cached = self._site_station_maps.get(provider_uid)
        if cached is not None:
            return cached
        loaded = self.store.get_site_station_map(provider_uid)
        self._site_station_maps[provider_uid] = loaded
        return loaded

    def _evse_station_map(self, provider_uid: str) -> dict[str, dict[str, str]]:
        cached = self._evse_station_maps.get(provider_uid)
        if cached is not None:
            return cached
        loaded = self.store.get_evse_station_map(provider_uid)
        self._evse_station_maps[provider_uid] = loaded
        return loaded

    def _persist_payload(
        self,
        *,
        provider_uid: str,
        fetched_at: str,
        payload_bytes: bytes,
        content_type: str,
    ) -> dict[str, Any]:
        payload = decode_json_payload(payload_bytes)
        facts = extract_dynamic_facts(
            payload,
            provider_uid,
            self._site_station_map(provider_uid),
            self._evse_station_map(provider_uid),
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

    def _queued_result_payload(
        self,
        *,
        provider_uid: str,
        receipt_at: str,
        task: ReceiptTask,
        http_status: int = 0,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = {
            "provider_uid": provider_uid,
            "result": "queued",
            "queue_task_id": task.task_id,
            "observation_count": 0,
            "mapped_observation_count": 0,
            "dropped_observation_count": 0,
            "changed_observation_count": 0,
            "changed_mapped_observation_count": 0,
            "changed_dropped_observation_count": 0,
        }
        if task.task_kind == "poll":
            payload["fetched_at"] = receipt_at
            payload["http_status"] = http_status
        else:
            payload["received_at"] = receipt_at
            payload["subscription_id"] = task.subscription_id
            payload["publication_id"] = task.publication_id
        if extra:
            payload.update(extra)
        return payload

    def _duplicate_push_result_payload(
        self,
        *,
        provider_uid: str,
        receipt_at: str,
        subscription_id: str = "",
        publication_id: str = "",
        duplicate_of_push_run_id: int | None = None,
        payload_sha256: str = "",
    ) -> dict[str, Any]:
        payload = {
            "provider_uid": provider_uid,
            "subscription_id": subscription_id,
            "publication_id": publication_id,
            "result": "duplicate",
            "received_at": receipt_at,
            "observation_count": 0,
            "mapped_observation_count": 0,
            "dropped_observation_count": 0,
            "changed_observation_count": 0,
            "changed_mapped_observation_count": 0,
            "changed_dropped_observation_count": 0,
        }
        if duplicate_of_push_run_id is not None:
            payload["duplicate_of_push_run_id"] = duplicate_of_push_run_id
        if payload_sha256:
            payload["payload_sha256"] = payload_sha256
        return payload

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
                    "mapped_observation_count": 0,
                    "dropped_observation_count": 0,
                    "changed_observation_count": 0,
                    "changed_mapped_observation_count": 0,
                    "changed_dropped_observation_count": 0,
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
            ingest_stats = self._persist_payload(
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
                payload_sha256=str(ingest_stats["payload_sha256"]),
                observation_count=int(ingest_stats["observation_count"]),
                mapped_observation_count=int(ingest_stats["mapped_observation_count"]),
                dropped_observation_count=int(ingest_stats["dropped_observation_count"]),
                changed_observation_count=int(ingest_stats["changed_observation_count"]),
                changed_mapped_observation_count=int(ingest_stats["changed_mapped_observation_count"]),
                changed_dropped_observation_count=int(ingest_stats["changed_dropped_observation_count"]),
            )
            return {
                "provider_uid": provider_uid,
                "result": "ok",
                "fetched_at": fetched_at,
                "http_status": fetch_response.http_status,
                "observation_count": int(ingest_stats["observation_count"]),
                "mapped_observation_count": int(ingest_stats["mapped_observation_count"]),
                "dropped_observation_count": int(ingest_stats["dropped_observation_count"]),
                "changed_observation_count": int(ingest_stats["changed_observation_count"]),
                "changed_mapped_observation_count": int(ingest_stats["changed_mapped_observation_count"]),
                "changed_dropped_observation_count": int(ingest_stats["changed_dropped_observation_count"]),
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
                if fetch_response is not None:
                    self.response_log_writer.write_http_response(
                        provider_uid=provider_uid,
                        fetched_at=fetched_at,
                        response=fetch_response,
                    )
                else:
                    self.response_log_writer.write_fetch_failure(
                        provider_uid=provider_uid,
                        fetched_at=fetched_at,
                        failure_kind="error",
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

    def receive_provider(self, provider_uid: str, *, bootstrap: bool = True) -> dict[str, Any]:
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
            response_log_path = self.response_log_writer.write_http_response(
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
                    "mapped_observation_count": 0,
                    "dropped_observation_count": 0,
                    "changed_observation_count": 0,
                    "changed_mapped_observation_count": 0,
                    "changed_dropped_observation_count": 0,
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
            payload_sha256 = hashlib.sha256(fetch_response.body).hexdigest()
            task = self.receipt_queue.build_task(
                task_kind="poll",
                provider_uid=provider_uid,
                run_id=poll_run_id,
                receipt_log_path=response_log_path,
                receipt_at=fetched_at,
                content_type=fetch_response.content_type,
                http_status=fetch_response.http_status,
            )
            self.receipt_queue.enqueue(task)
            self.store.queue_poll_run(
                poll_run_id,
                provider_uid=provider_uid,
                fetched_at=fetched_at,
                http_status=fetch_response.http_status,
                payload_sha256=payload_sha256,
            )
            return self._queued_result_payload(
                provider_uid=provider_uid,
                receipt_at=fetched_at,
                task=task,
                http_status=fetch_response.http_status,
            )
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
                if fetch_response is not None:
                    self.response_log_writer.write_http_response(
                        provider_uid=provider_uid,
                        fetched_at=fetched_at,
                        response=fetch_response,
                    )
                else:
                    self.response_log_writer.write_fetch_failure(
                        provider_uid=provider_uid,
                        fetched_at=fetched_at,
                        failure_kind="error",
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
            ingest_stats = self._persist_payload(
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
                payload_sha256=str(ingest_stats["payload_sha256"]),
                observation_count=int(ingest_stats["observation_count"]),
                mapped_observation_count=int(ingest_stats["mapped_observation_count"]),
                dropped_observation_count=int(ingest_stats["dropped_observation_count"]),
                changed_observation_count=int(ingest_stats["changed_observation_count"]),
                changed_mapped_observation_count=int(ingest_stats["changed_mapped_observation_count"]),
                changed_dropped_observation_count=int(ingest_stats["changed_dropped_observation_count"]),
            )
            return {
                "provider_uid": resolved_provider_uid,
                "subscription_id": subscription_id,
                "publication_id": publication_id,
                "result": "ok",
                "received_at": received_at,
                "observation_count": int(ingest_stats["observation_count"]),
                "mapped_observation_count": int(ingest_stats["mapped_observation_count"]),
                "dropped_observation_count": int(ingest_stats["dropped_observation_count"]),
                "changed_observation_count": int(ingest_stats["changed_observation_count"]),
                "changed_mapped_observation_count": int(ingest_stats["changed_mapped_observation_count"]),
                "changed_dropped_observation_count": int(ingest_stats["changed_dropped_observation_count"]),
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

    def receive_push(
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
            payload_sha256 = hashlib.sha256(payload_bytes).hexdigest()
            request_log_path = self.response_log_writer.write_push_request(
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
            duplicate_run = self.store.find_recent_push_run(
                resolved_provider_uid,
                payload_sha256=payload_sha256,
                received_at=received_at,
                within_seconds=self.store.push_duplicate_window_seconds(provider),
            )
            if duplicate_run is not None:
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
                self.store.finish_push_run(
                    push_run_id,
                    provider_uid=resolved_provider_uid,
                    result="duplicate",
                    received_at=received_at,
                    payload_sha256=payload_sha256,
                )
                return self._duplicate_push_result_payload(
                    provider_uid=resolved_provider_uid,
                    receipt_at=received_at,
                    subscription_id=subscription_id,
                    publication_id=publication_id,
                    duplicate_of_push_run_id=int(duplicate_run["id"]),
                    payload_sha256=payload_sha256,
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
            task = self.receipt_queue.build_task(
                task_kind="push",
                provider_uid=resolved_provider_uid,
                run_id=push_run_id,
                receipt_log_path=request_log_path,
                receipt_at=received_at,
                content_type=content_type,
                subscription_id=subscription_id,
                publication_id=publication_id,
            )
            self.receipt_queue.enqueue(task)
            self.store.queue_push_run(
                push_run_id,
                provider_uid=resolved_provider_uid,
                received_at=received_at,
                payload_sha256=payload_sha256,
            )
            return self._queued_result_payload(
                provider_uid=resolved_provider_uid,
                receipt_at=received_at,
                task=task,
            )
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

    def _payload_from_receipt_log(self, task: ReceiptTask) -> tuple[bytes, str]:
        record_path = Path(task.receipt_log_path)
        payload = json.loads(record_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"invalid_receipt_log:{record_path}")
        body_text = payload.get("body_text")
        if not isinstance(body_text, str):
            raise ValueError(f"missing_body_text:{record_path}")
        content_type = str(payload.get("content_type") or task.content_type or "").strip()
        return body_text.encode("utf-8"), content_type

    def process_next_receipt(self, *, bootstrap: bool = True) -> dict[str, Any] | None:
        if bootstrap:
            self.bootstrap()
        task = self.receipt_queue.claim_next()
        if task is None:
            return None

        try:
            payload_bytes, content_type = self._payload_from_receipt_log(task)
            ingest_stats = self._persist_payload(
                provider_uid=task.provider_uid,
                fetched_at=task.receipt_at,
                payload_bytes=payload_bytes,
                content_type=content_type,
            )
            if task.task_kind == "poll":
                self.store.complete_poll_run(
                    task.run_id,
                    provider_uid=task.provider_uid,
                    result="ok",
                    fetched_at=task.receipt_at,
                    http_status=task.http_status,
                    payload_sha256=str(ingest_stats["payload_sha256"]),
                    observation_count=int(ingest_stats["observation_count"]),
                    mapped_observation_count=int(ingest_stats["mapped_observation_count"]),
                    dropped_observation_count=int(ingest_stats["dropped_observation_count"]),
                    changed_observation_count=int(ingest_stats["changed_observation_count"]),
                    changed_mapped_observation_count=int(ingest_stats["changed_mapped_observation_count"]),
                    changed_dropped_observation_count=int(ingest_stats["changed_dropped_observation_count"]),
                )
            else:
                self.store.finish_push_run(
                    task.run_id,
                    provider_uid=task.provider_uid,
                    result="ok",
                    received_at=task.receipt_at,
                    payload_sha256=str(ingest_stats["payload_sha256"]),
                    observation_count=int(ingest_stats["observation_count"]),
                    mapped_observation_count=int(ingest_stats["mapped_observation_count"]),
                    dropped_observation_count=int(ingest_stats["dropped_observation_count"]),
                    changed_observation_count=int(ingest_stats["changed_observation_count"]),
                    changed_mapped_observation_count=int(ingest_stats["changed_mapped_observation_count"]),
                    changed_dropped_observation_count=int(ingest_stats["changed_dropped_observation_count"]),
                )
            self.receipt_queue.mark_done(task)
            return {
                "provider_uid": task.provider_uid,
                "task_kind": task.task_kind,
                "result": "ok",
                "queue_task_id": task.task_id,
                "receipt_at": task.receipt_at,
                "observation_count": int(ingest_stats["observation_count"]),
                "mapped_observation_count": int(ingest_stats["mapped_observation_count"]),
                "dropped_observation_count": int(ingest_stats["dropped_observation_count"]),
                "changed_observation_count": int(ingest_stats["changed_observation_count"]),
                "changed_mapped_observation_count": int(ingest_stats["changed_mapped_observation_count"]),
                "changed_dropped_observation_count": int(ingest_stats["changed_dropped_observation_count"]),
            }
        except Exception as exc:
            if task.task_kind == "poll":
                self.store.complete_poll_run(
                    task.run_id,
                    provider_uid=task.provider_uid,
                    result="error",
                    fetched_at=task.receipt_at,
                    http_status=task.http_status,
                    error_text=str(exc),
                )
            else:
                self.store.finish_push_run(
                    task.run_id,
                    provider_uid=task.provider_uid,
                    result="error",
                    received_at=task.receipt_at,
                    error_text=str(exc),
                )
            self.receipt_queue.mark_failed(task, error_text=str(exc))
            return {
                "provider_uid": task.provider_uid,
                "task_kind": task.task_kind,
                "result": "error",
                "queue_task_id": task.task_id,
                "receipt_at": task.receipt_at,
                "error": str(exc),
            }

    def drain_receipt_queue(
        self,
        *,
        max_items: int | None = None,
        bootstrap: bool = True,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        while max_items is None or len(results) < max_items:
            item = self.process_next_receipt(bootstrap=bootstrap and not results)
            if item is None:
                break
            results.append(item)
        return results

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

    def receive_next_provider(self, *, bootstrap: bool = True) -> dict[str, Any] | None:
        if bootstrap:
            self.bootstrap()
        provider = self.store.get_next_provider_for_round_robin()
        if provider is None:
            return None
        return self.receive_provider(str(provider["provider_uid"]), bootstrap=False)

    def seconds_until_next_provider_due(self, *, bootstrap: bool = True) -> float | None:
        if bootstrap:
            self.bootstrap()
        return self.store.seconds_until_next_provider_due()
