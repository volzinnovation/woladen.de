# woladen Backend

This package contains the live-data backend for AFIR dynamic charging availability and pricing.
It ingests DATEX II v3 payloads from Mobilithek subscriptions, persists normalized EVSE and station
state in SQLite, exposes a read API via FastAPI, and stores raw payload logs for audit and archiving.

For deployment-specific instructions, see [deploy/ionos/README.md](/Users/raphaelvolz/Github/woladen.de/deploy/ionos/README.md).
For the higher-level product note, see [docs/live-api-mvp.md](/Users/raphaelvolz/Github/woladen.de/docs/live-api-mvp.md).

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

## Runtime Configuration

The backend uses `AppConfig` in [config.py](/Users/raphaelvolz/Github/woladen.de/backend/config.py). Important variables:

- `WOLADEN_LIVE_DB_PATH`: SQLite database path. Default: `data/live_state.sqlite3`
- `WOLADEN_LIVE_RAW_PAYLOAD_DIR`: raw push/poll log directory. Default: `data/live_raw`
- `WOLADEN_LIVE_ARCHIVE_DIR`: archive output directory. Default: `data/live_archives`
- `WOLADEN_LIVE_PROVIDER_CONFIG_PATH`: provider metadata JSON. Default: `data/mobilithek_afir_provider_configs.json`
- `WOLADEN_LIVE_SITE_MATCH_PATH`: site-to-station match CSV. Default: `data/mobilithek_afir_static_matches.csv`
- `WOLADEN_LIVE_CHARGERS_CSV_PATH`: charger baseline CSV. Default: `data/chargers_fast.csv`
- `WOLADEN_LIVE_CHARGERS_GEOJSON_PATH`: bundled charger GeoJSON used by `/status`. Default: `data/chargers_fast.geojson`
- `WOLADEN_LIVE_PROVIDER_OVERRIDE_PATH`: optional provider override JSON
- `WOLADEN_LIVE_SUBSCRIPTION_REGISTRY_PATH`: subscription registry JSON. Default: `secret/mobilithek_subscriptions.json`
- `WOLADEN_MACHINE_CERT_P12`: Mobilithek machine certificate for mTLS polling
- `WOLADEN_MACHINE_CERT_PASSWORD_FILE`: password file for the PKCS#12 certificate
- `WOLADEN_LIVE_API_HOST`: FastAPI bind host. Default: `127.0.0.1`
- `WOLADEN_LIVE_API_PORT`: FastAPI bind port. Default: `8001`
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

Returns a JSON status report for the current GeoJSON bundle, derived from `evse_current_state`,
`station_current_state`, `providers`, and the bundled station IDs in `chargers_fast.geojson`.

Top-level fields:

- `db_path`
- `geojson_path`
- `bundle_feature_count`
- `bundle_station_count`
- `bundle_duplicate_station_id_count`
- `stations_with_any_live_observation`
- `stations_with_current_live_state`
- `coverage_ratio`
- `last_received_update_at`: latest `fetched_at` seen anywhere in the bundle
- `latest_updated_station_id`: bundle `station_id` for the latest received dynamic update
- `last_source_update_at`: latest source-provided observation timestamp across the bundle
- `providers_with_any_live_observation`
- `observed_station_ids_not_in_bundle`
- `current_state_station_ids_not_in_bundle`
- `provider_station_count_sum`
- `provider_station_overlap_excess`
- `providers`

Provider item fields:

- `provider_uid`
- `display_name`
- `publisher`
- `enabled`
- `fetch_kind`
- `delta_delivery`
- `stations_with_any_live_observation`
- `observation_rows`: current EVSE rows contributing to the provider summary
- `coverage_ratio`
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
