# woladen Backend

This package contains the live-data backend for AFIR dynamic charging availability and pricing.
It ingests DATEX II v3 payloads from Mobilithek subscriptions, persists normalized EVSE and station
state in SQLite, exposes a read API via FastAPI, and stores raw payload logs for audit and archiving.

For deployment-specific instructions, see [deploy/ionos/README.md](/Users/raphaelvolz/Github/woladen.de/deploy/ionos/README.md).
For local VPS-like disk and inode testing, see
[deploy/local-live-constrained/README.md](/Users/raphaelvolz/Github/woladen.de/deploy/local-live-constrained/README.md).
For the higher-level product note, see [docs/live-api-mvp.md](/Users/raphaelvolz/Github/woladen.de/docs/live-api-mvp.md).
For the version history and rollout evidence, see [docs/live-backend-evolution.md](/Users/raphaelvolz/Github/woladen.de/docs/live-backend-evolution.md).

## Components

- `config.py`: environment-driven runtime configuration.
- `loaders.py`: loads provider metadata, static site-to-station matches, and charger records.
- `fetcher.py`: polling client for Mobilithek pull and mTLS subscription endpoints.
- `datex.py`: DATEX II v3 JSON/XML decoding and normalization into internal facts.
- `store.py`: SQLite schema, persistence, scheduler state, and query helpers.
- `service.py`: ingestion orchestration for polling and push delivery.
- `api.py`: FastAPI app with push ingestion and read endpoints.
- `status.py`: bundle coverage and provider-level live status reporting for the API and CLI.
- `archive.py`: raw request/response logging and daily archive bundling.
- `archive.py`: raw request/response logging plus daily archive upload/download helpers.
- `subscriptions.py`: helpers for syncing subscription IDs from Mobilithek account data.

## Data Flow

1. Provider and site metadata are bootstrapped from repo data files.
2. Dynamic payloads arrive either through polling or through Mobilithek push delivery.
3. `datex.py` converts the raw DATEX payload into normalized EVSE facts.
4. `store.py` updates `evse_current_state` and refreshes aggregated station state.
5. `status.py` intersects the current live state with the bundled charger GeoJSON and derives coverage/status metadata.
6. `api.py` serves the current station, EVSE, and bundle-status state from SQLite.
7. `archive.py` writes timestamped raw payload logs and can bundle them into daily `.tgz` archives.

## Installation

Install the live backend runtime dependencies:

```bash
python3 -m pip install -r /Users/raphaelvolz/Github/woladen.de/requirements-live.txt
```

## Production Deployment

The live backend is deployed on `https://live.woladen.de` using the IONOS VPS packaging in
[deploy/ionos/README.md](/Users/raphaelvolz/Github/woladen.de/deploy/ionos/README.md).
The production host currently runs Debian 12.

Use two different Unix accounts on the VPS:

- `deploy`: SSH operator account with `sudo` for installation, secret upload, and service restarts
- `woladen`: non-login service account created by the installer for the API and ingester

Typical deployment flow:

```bash
./deploy/ionos/build-release.sh
scp /Users/raphaelvolz/Github/woladen.de/tmp/ionos-release/woladen-live-backend-*.tar.gz deploy@live.woladen.de:~
ssh deploy@live.woladen.de
tar -xzf ~/woladen-live-backend-*.tar.gz
cd ~/woladen-live-backend-*
sudo ./deploy/ionos/bootstrap-host.sh
sudo ./deploy/ionos/install-on-vps.sh
```

Upload the required Mobilithek secrets from the local machine and restart the services:

```bash
./deploy/ionos/push-secrets-and-start.sh \
  --host live.woladen.de \
  --ssh-user deploy \
  --cert /path/to/certificate.p12 \
  --password-file /path/to/pwd.txt \
  --subscriptions /path/to/mobilithek_subscriptions.json
```

If `caddy.service` is installed, `install-on-vps.sh` now manages the active Caddy config automatically by either writing `/etc/caddy/Caddyfile` or appending an import for `/etc/woladen/live.woladen.de.Caddyfile`, then validating and reloading or starting Caddy.

Quick production verification:

```bash
curl -fsS https://live.woladen.de/healthz
sudo systemctl status woladen-live-api.service woladen-live-ingester.service --no-pager
sudo journalctl -u woladen-live-api.service -u woladen-live-ingester.service -n 100 --no-pager
```

## Scaling Rollout Plan

The current live deployment is intentionally simple:

- one `live_ingester.py` loop
- one `live_api.py` service behind Caddy
- one local SQLite database in WAL mode

That architecture scales well on a single host as long as write concurrency stays low. It is a poor fit for active-active multi-node deployment because the ingester and API share a local SQLite file and SQLite still allows only one writer per database file.

Use the following staged rollout plan instead of jumping directly to many small servers.

### Phase 1: Scale Up The Single Host

When the first pressure comes from CPU, request volume, or a busier ingestion loop, prefer a larger VPS first.

- Move to more CPU and RAM before changing the topology.
- Keep a single ingester process and keep SQLite local to the same machine.
- Add API workers on the same host if read traffic grows, while keeping one Caddy entry point.
- Prefer fast local SSD or NVMe storage over network-attached database files.

This is the default path while the bottleneck is one hot ingester process or moderate API traffic.

### Phase 2: Measure Before Splitting

Before any architecture change, collect evidence about which part is actually saturated.

- API latency and error rate
- request rate and concurrent connections
- ingester cycle duration and provider-specific processing cost
- SQLite lock contention or busy timeouts
- memory growth, WAL size, and disk I/O

If the API is the bottleneck, increase local API workers first. If the ingester is the bottleneck, profile provider fetch, decode, match, and write cost before introducing more moving parts.

### Open Production Latency Note

As of April 19, 2026, the live API path appears roughly an order of magnitude faster than before after the `station_current_state.evses_json` materialization work and the `station_id` lookup improvements. Treat that as a backend win, not as proof that the full browser path improved by the same factor.

- Treat browser-observed latency and backend compute latency as separate measurements.
- Do not assume the browser-visible wait matches the backend speedup one-to-one.
- The current backend path for station detail is intentionally simple: one indexed `station_id` lookup plus JSON decode.
- If the browser still feels slow, measure the full path: TLS, Caddy, transfer size, compression, JSON encode, browser download, browser JSON parse, and frontend render/update cost.
- In particular, large `lookup` batches and large detail payloads can make network transfer and client-side parsing/rendering look like “API slowness” even when SQLite is already fast.
- Always compare:
  - `Server-Timing` (`db-query`, `db-decode`, `payload`, `json-encode`, `app`)
  - browser resource timing (`requestStart`, `responseStart`, `responseEnd`, total `duration`)
  - payload size (`Content-Length`)
  - frontend work after the response arrives
- If the browser still lags well behind `app` time, focus next on payload size, request fanout, response compression, and frontend main-thread work before planning a database migration.

### Phase 3: Separate Storage Before Horizontal Scaling

Do not add multiple app servers that depend on the same SQLite file. When the product needs real horizontal scaling or higher availability, move the live state to a client/server database first.

- Migrate live state from SQLite to PostgreSQL.
- Keep the API stateless so multiple API instances can run behind Caddy or another load balancer.
- Continue with one ingester first, then shard ingestion by provider group only after the database supports concurrent writers safely.
- Add at least two API instances only after the shared-state layer is no longer SQLite-bound.

This is the point where many smaller servers start to make sense.

### Decision Rule

- If one core is busy but the host still has headroom, buy a bigger box.
- If the API needs more throughput on the same host, add local API workers.
- If the deployment needs multiple API nodes, HA failover, or multiple concurrent writers, migrate to PostgreSQL first and only then scale out horizontally.

## Runtime Configuration

The backend uses `AppConfig` in [config.py](/Users/raphaelvolz/Github/woladen.de/backend/config.py). Important variables:

- `WOLADEN_LIVE_DB_PATH`: SQLite database path. Default: `data/live_state.sqlite3`
- `WOLADEN_LIVE_RAW_PAYLOAD_DIR`: raw push/poll provider/day JSONL journal directory. Default: `data/live_raw`
- `WOLADEN_LIVE_ARCHIVE_DIR`: archive output directory. Default: `data/live_archives`
- `WOLADEN_LIVE_PROVIDER_CONFIG_PATH`: provider metadata JSON. Default: `data/mobilithek_afir_provider_configs.json`
- `WOLADEN_LIVE_SITE_MATCH_PATH`: site-to-station match CSV. Default: `data/mobilithek_afir_static_matches.csv`
- `WOLADEN_LIVE_CHARGERS_CSV_PATH`: charger baseline CSV. Default: `data/chargers_fast.csv`
- `WOLADEN_LIVE_FULL_CHARGERS_CSV_PATH`: canonical full-registry charger CSV for backend matching and `/status` diagnostics. Default: `data/chargers_full.csv` when present, otherwise `data/chargers_fast.csv`
- `WOLADEN_LIVE_CHARGERS_GEOJSON_PATH`: bundled charger GeoJSON used by `/status`. Default: `data/chargers_fast.geojson`
- `WOLADEN_LIVE_PROVIDER_OVERRIDE_PATH`: optional provider override JSON
- `WOLADEN_LIVE_SUBSCRIPTION_REGISTRY_PATH`: subscription registry JSON. Default: `secret/mobilithek_subscriptions.json`
- `WOLADEN_MACHINE_CERT_P12`: Mobilithek machine certificate for mTLS polling
- `WOLADEN_MACHINE_CERT_PASSWORD_FILE`: password file for the PKCS#12 certificate
- `WOLADEN_LIVE_API_HOST`: FastAPI bind host. Default: `127.0.0.1`
- `WOLADEN_LIVE_API_PORT`: FastAPI bind port. Default: `8001`
- `WOLADEN_LIVE_API_PUSH_ENABLED`: enable Mobilithek push routes. Default: `1`
- `WOLADEN_LIVE_API_CORS_ALLOWED_ORIGINS`: comma-separated explicit CORS allowlist
- `WOLADEN_LIVE_API_CORS_ALLOW_ORIGIN_REGEX`: regex fallback for local development (`localhost`, `127.0.0.1`, `0.0.0.0`, `[::1]` by default)
- `WOLADEN_LIVE_POLL_TIMEOUT_SECONDS`: fetch timeout
- `WOLADEN_LIVE_POLL_INTERVAL_DELTA_SECONDS`: base interval for delta feeds
- `WOLADEN_LIVE_POLL_INTERVAL_SNAPSHOT_SECONDS`: base interval for snapshot feeds
- `WOLADEN_LIVE_POLL_INTERVAL_NO_DATA_MAX_SECONDS`: backoff cap for `204` or `304`
- `WOLADEN_LIVE_POLL_INTERVAL_ERROR_MAX_SECONDS`: backoff cap for poll failures
- `WOLADEN_LIVE_POLL_INTERVAL_UNCHANGED_MAX_SECONDS`: backoff cap for unchanged snapshot payloads
- `WOLADEN_LIVE_POLL_IDLE_SLEEP_MAX_SECONDS`: max sleep while no provider is due
- `WOLADEN_LIVE_SQLITE_BUSY_TIMEOUT_MS`: SQLite busy timeout
- `WOLADEN_LIVE_SQLITE_LOCK_RETRY_SECONDS`: additional retry budget for transient SQLite lock contention on receipt-path writes
- `WOLADEN_LIVE_QUEUE_CLEANUP_INTERVAL_SECONDS`: how often the queue worker prunes completed SQLite queue rows
- `WOLADEN_LIVE_QUEUE_DONE_RETENTION_SECONDS`: retention window for processed queue rows
- `WOLADEN_LIVE_QUEUE_FAILED_RETENTION_SECONDS`: retention window for failed queue rows
- `WOLADEN_LIVE_ARCHIVE_TIMEZONE`: timezone for archive day grouping
- `WOLADEN_LIVE_HF_ARCHIVE_REPO_ID`, `WOLADEN_LIVE_HF_ARCHIVE_REPO_TYPE`, `WOLADEN_LIVE_HF_ARCHIVE_PATH_PREFIX`, `WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE`: optional Hugging Face archive upload config

## Usage

Bootstrap the database and seed metadata:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_ingester.py --bootstrap-only
```

Run one ingestion pass across all enabled providers:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_ingester.py
```

Poll only one provider:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_ingester.py --provider qwello
```

Run the polling loop:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_ingester.py --loop --sleep-seconds 1
```

Run the API server:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_api.py
```

Audit legacy flat-file receipt queue state without changing it:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_queue_maintenance.py --env-file /etc/woladen/woladen-live.env
```

Any legacy queue mutation requires `--apply` and a `--backup-path`. For example, after confirming the report, active legacy tasks whose raw payload still exists can be migrated into the SQLite queue with `--migrate-active`; stale active references whose raw payload is already gone can be deleted with `--delete-stale-uploaded` only when the archive date is confirmed on Hugging Face.

Local end-to-end smoke test against the local API instead of `https://live.woladen.de`:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_ingester.py --provider edri
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_api.py
python3 -m http.server 4173 --directory /Users/raphaelvolz/Github/woladen.de/site
```

Then open:

```text
http://127.0.0.1:4173/?liveApiBaseUrl=http://127.0.0.1:8001
```

The `liveApiBaseUrl` query parameter forces the frontend to talk to the local API for that session without changing the default production mapping.

Print the current bundle/live coverage report from the same logic used by `/status`:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_bundle_coverage.py --json
```

Validate the machine certificate and current mTLS subscriptions:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_subscription_registry.py --probe-certificate
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_subscription_registry.py
```

Sync active subscription IDs into the registry:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/sync_mobilithek_subscriptions.py
```

Archive one day of raw logs:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_archive_logs.py --date 2026-04-14
```

List remote `.tgz` archives visible in the configured Hugging Face dataset:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_download_archive.py --list-available
```

Download the newest visible `.tgz` archive from the configured Hugging Face dataset:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_download_archive.py --latest-available
```

Or download yesterday's `.tgz` archive directly:

```bash
python3 /Users/raphaelvolz/Github/woladen.de/scripts/live_download_archive.py
```

## Storage

The backend persists into SQLite and keeps raw payload logs on disk.

- Database: `data/live_state.sqlite3` by default
- Raw poll logs: `data/live_raw/<provider_uid>/<YYYY-MM-DD>/*.json`
- Raw push logs: `data/live_raw/<provider_uid>/<YYYY-MM-DD>/*-push-*.json`
- Daily archives: `data/live_archives/live-provider-responses-<YYYY-MM-DD>.tgz` during local-only or retryable runs; removed after a successful Hugging Face upload

Important tables:

- `providers`: provider metadata, poll scheduler state, last poll result, last push result
- `provider_site_matches`: mapping from provider `site_id` to internal `station_id`
- `provider_poll_runs`: poll execution history
- `provider_push_runs`: push delivery history
- `evse_current_state`: latest normalized state per provider EVSE
- `station_current_state`: aggregated station availability and pricing summary

## API

The FastAPI app is created by [api.py](/Users/raphaelvolz/Github/woladen.de/backend/api.py). When running locally, the defaults are:

- Base URL: `http://127.0.0.1:8001`
- OpenAPI JSON: `GET /openapi.json`
- Swagger UI: `GET /docs`

### `GET /healthz`

Health probe.

Use `GET`, not `HEAD`. For example, `curl -I https://live.woladen.de/healthz` will return `405`
because the route is defined as `GET` only.

Response:

```json
{"ok": true}
```

### `GET /status`
### `GET /v1/status`

Returns a JSON status report derived from `evse_current_state`, `station_current_state`,
`providers`, the canonical station catalog, and the bundled station IDs in `chargers_fast.geojson`.
The primary denominators are full-registry station counts; bundle coverage remains available as
secondary counters.

Top-level fields:

- `db_path`
- `geojson_path`
- `station_count`
- `full_registry_station_count`
- `bundle_feature_count`
- `bundle_station_count`
- `bundle_duplicate_station_id_count`
- `stations_with_any_live_observation`
- `stations_with_current_live_state`
- `coverage_ratio`
- `bundle_stations_with_any_live_observation`
- `bundle_stations_with_current_live_state`
- `bundle_coverage_ratio`
- `last_received_update_at`: latest `fetched_at` seen anywhere in the full registry
- `latest_updated_station_id`: internal `station_id` for the latest received dynamic update
- `last_source_update_at`: latest source-provided observation timestamp across the full registry
- `providers_with_any_live_observation`
- `providers_with_any_live_observation_in_bundle`
- `observed_station_ids_not_in_full_registry`
- `current_state_station_ids_not_in_full_registry`
- `observed_station_ids_not_in_bundle`
- `current_state_station_ids_not_in_bundle`
- `stations_with_any_live_observation_outside_bundle`
- `stations_with_current_live_state_outside_bundle`
- `provider_station_count_sum`
- `provider_station_overlap_excess`
- `provider_bundle_station_count_sum`
- `provider_bundle_station_overlap_excess`
- `providers`

Provider item fields:

- `provider_uid`
- `display_name`
- `publisher`
- `enabled`
- `fetch_kind`
- `delta_delivery`
- `stations_with_any_live_observation`
- `stations_with_any_live_observation_in_bundle`
- `observation_rows`: current EVSE rows contributing to the provider summary
- `coverage_ratio`
- `bundle_coverage_ratio`
- `station_ids_outside_bundle`
- `station_ids_not_in_full_registry`
- `last_received_update_at`
- `last_source_update_at`
- `latest_updated_station_id`
- `latest_attribute_updates`
- `last_polled_at`
- `last_result`
- `last_push_received_at`
- `last_push_result`
- `recent_updates`

`latest_attribute_updates` is keyed by dynamic attribute and currently includes:

- `availability_status`
- `operational_status`
- `price_display`
- `price_currency`
- `price_energy_eur_kwh_min`
- `price_energy_eur_kwh_max`
- `price_time_eur_min_min`
- `price_time_eur_min_max`

Each attribute entry contains:

- `station_id`: bundle station whose latest observation carried this attribute value
- `fetched_at`: backend receive time for that update
- `source_observed_at`: provider timestamp from the DATEX payload when present
- `value`: latest non-empty value observed for that attribute

`recent_updates` is a reverse-chronological list of recent poll/push ingests for the provider.
Each item includes:

- `update_kind`: `poll` or `push`
- `update_at`
- `started_at`
- `ended_at`
- `fetched_at`
- `received_at`
- `result`
- `http_status`
- `observation_count`
- `mapped_observation_count`
- `dropped_observation_count`
- `changed_observation_count`
- `changed_mapped_observation_count`
- `changed_dropped_observation_count`
- `error_text`

Example:

```bash
curl http://127.0.0.1:8001/status
```

Representative response fragment:

```json
{
  "stations_with_any_live_observation": 2037,
  "last_received_update_at": "2026-04-15T13:35:57+00:00",
  "latest_updated_station_id": "ddb76167ba605597",
  "providers": [
    {
      "provider_uid": "enbwmobility",
      "stations_with_any_live_observation": 399,
      "last_received_update_at": "2026-04-15T13:35:57+00:00",
      "latest_updated_station_id": "ddb76167ba605597",
      "latest_attribute_updates": {
        "availability_status": {
          "station_id": "ddb76167ba605597",
          "fetched_at": "2026-04-15T13:35:57+00:00",
          "source_observed_at": "2026-04-15T12:29:47.013+02:00",
          "value": "free"
        },
        "operational_status": {
          "station_id": "ddb76167ba605597",
          "fetched_at": "2026-04-15T13:35:57+00:00",
          "source_observed_at": "2026-04-15T12:29:47.013+02:00",
          "value": "AVAILABLE"
        }
      }
    }
  ]
}
```

Notes:

- Coverage is counted against the station IDs in the bundled GeoJSON, not against all rows ever seen in SQLite.
- Provider counts are not additive because multiple providers can update the same bundled station.
- Per-attribute latest values are tracked independently. For a provider with multiple EVSEs at one station, the latest value for one attribute can come from a different EVSE observation than another attribute at the same timestamp.

### `HEAD /v1/push`
### `HEAD /v1/push/{provider_uid}`

Reachability probe for Mobilithek push delivery. Always responds with `200 OK` and an empty body.

Example:

```bash
curl -I http://127.0.0.1:8001/v1/push/enbwmobility
```

### `POST /v1/push`
### `POST /v1/push/{provider_uid}`

Push ingestion endpoint for Mobilithek subscriber delivery.

Request body:

- Raw DATEX II v3 JSON or XML payload as delivered by Mobilithek

Provider resolution:

1. Path parameter `provider_uid` on `/v1/push/{provider_uid}`
2. Query parameter or header matching `provider_uid` or `provider`
3. Query parameter or header matching `subscription_id`, `subscriptionID`, `x-subscription-id`, or `x-mobilithek-subscription-id`
4. Query parameter or header matching `publication_id`, `publicationID`, `x-publication-id`, or `x-mobilithek-publication-id`

Expected behavior:

- `200 OK` with empty body on successful ingestion
- `400` when no provider hint is available
- `404` when the referenced provider, subscription, or publication cannot be resolved
- `422` when the payload cannot be decoded as a valid DATEX payload
- `500` for unexpected internal failures

Recommended Mobilithek callback URLs:

```text
https://live.woladen.de/v1/push/enbwmobility
https://live.woladen.de/v1/push/wirelane
```

Example:

```bash
curl -X POST http://127.0.0.1:8001/v1/push/qwello \
  -H 'Content-Type: application/json' \
  --data-binary @payload.json
```

### `GET /v1/providers`

Returns the raw provider rows from SQLite, including configuration and scheduler metadata.

Representative fields:

- `provider_uid`
- `display_name`
- `publisher`
- `publication_id`
- `access_mode`
- `fetch_kind`
- `fetch_url`
- `subscription_id`
- `enabled`
- `delta_delivery`
- `retention_period_minutes`
- `last_polled_at`
- `next_poll_at`
- `last_result`
- `last_error_text`
- `last_push_received_at`
- `last_push_result`
- `last_push_error_text`

### `GET /v1/stations`

Returns station summaries with current aggregated live state.

Query parameters:

- `provider_uid`: optional provider filter
- `status`: optional availability filter, one of `free`, `occupied`, `out_of_order`, `unknown`
- `limit`: default `100`, hard-capped to `100`
- `offset`: default `0`

Response fields:

- `station_id`
- `availability_status`
- `available_evses`
- `occupied_evses`
- `out_of_order_evses`
- `unknown_evses`
- `total_evses`
- `price_display`
- `price_currency`
- `price_energy_eur_kwh_min`
- `price_energy_eur_kwh_max`
- `price_time_eur_min_min`
- `price_time_eur_min_max`
- `price_complex`
- `source_observed_at`
- `fetched_at`
- `ingested_at`

Example:

```bash
curl 'http://127.0.0.1:8001/v1/stations?status=free&limit=20'
```

### `POST /v1/stations/lookup`

Looks up a specific list of station IDs.

Request body:

```json
{
  "station_ids": ["station-1", "station-2"]
}
```

Response:

- `stations`: found station summaries in request order, deduplicated
- `missing_station_ids`: requested IDs that were not found

### `GET /v1/stations/{station_id}`

Returns a station detail payload with current EVSE state.

Response shape:

- `station`: station summary
- `evses`: list of current EVSE rows for the station
- `recent_observations`: always an empty list; detailed history now lives in raw log files instead of SQLite

Current EVSE fields include:

- `provider_site_id`
- `provider_station_ref`
- `provider_evse_id`
- `station_id`
- `availability_status`
- `operational_status`
- `price_display`
- `price_currency`
- `price_energy_eur_kwh_min`
- `price_energy_eur_kwh_max`
- `price_time_eur_min_min`
- `price_time_eur_min_max`
- `price_quality`
- `price_complex`
- `source_observed_at`
- `fetched_at`
- `ingested_at`
- `payload_sha256`

### `GET /v1/evses/{provider_uid}/{provider_evse_id}`

Returns current state for one provider EVSE.

Response shape:

- `current`: latest EVSE state
- `recent_observations`: always an empty list; detailed history now lives in raw log files instead of SQLite

## Notes

- Polling and push use the same normalization and persistence path in `IngestionService`.
- Push is usually the better delivery mode for sparse station updates because it avoids constant re-polling of mostly unchanged subscriptions.
- If a provider is meant to be push-only, disable polling for it in `live_provider_overrides.json` but keep its subscription entry in `mobilithek_subscriptions.json`.
- The backend accepts DATEX II v3 JSON and XML payloads and does not currently expose write endpoints other than push ingestion.
