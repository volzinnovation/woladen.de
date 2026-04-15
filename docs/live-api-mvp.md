# Live API MVP

This repo now contains a local SQLite-based live-data backend for AFIR dynamic data.
SQLite holds the live state, while raw provider responses are written to timestamped
JSON files and archived separately once per day.

## Services

- `scripts/live_ingester.py`: seeds metadata, polls dynamic providers, stores current state in SQLite, and writes one raw response log file per fetch.
- `scripts/live_api.py`: serves the read API on `127.0.0.1:8001` by default and also exposes the push-ingestion endpoint used by Mobilithek subscriber push delivery.
- `scripts/live_archive_logs.py`: bundles one day of provider response logs into a `.tgz` and uploads it to Hugging Face Hub.

## Local Usage

Install the live backend runtime only:

```bash
python3 -m pip install -r requirements-live.txt
```

Bootstrap the database:

```bash
python3 scripts/live_ingester.py --bootstrap-only
```

Run one ingestion pass across enabled providers:

```bash
python3 scripts/live_ingester.py
```

Run the round-robin ingester loop:

```bash
python3 scripts/live_ingester.py --loop --sleep-seconds 1
```

Run the API:

```bash
python3 scripts/live_api.py
```

Mobilithek push reachability probe:

```bash
curl -I http://127.0.0.1:8001/v1/push/qwello
```

Archive and upload one day of response logs manually:

```bash
python3 scripts/live_archive_logs.py --date 2026-04-14
```

## Defaults

- SQLite DB: `data/live_state.sqlite3`
- Provider response logs: `data/live_raw/<provider>/<YYYY-MM-DD>/*.json`
- Daily response archives: `data/live_archives/live-provider-responses-<YYYY-MM-DD>.tgz`
- Machine certificate: `secret/certificate.p12`
- Machine certificate password: `secret/pwd.txt`
- mTLS subscription registry: `secret/mobilithek_subscriptions.json`

The default provider seed currently enables `noauth` dynamic feeds from `data/mobilithek_afir_provider_configs.json`. mTLS providers become active by adding `subscriptionID` entries to `secret/mobilithek_subscriptions.json`.

Example:

```json
{
  "enbwmobility": {
    "enabled": true,
    "fetch_kind": "mtls_subscription",
    "subscription_id": "2000001"
  },
  "wirelane": {
    "enabled": true,
    "fetch_kind": "mtls_subscription",
    "subscription_id": "2000002"
  }
}
```

## Push Delivery

The current Mobilithek interface description documents subscriber push delivery as:

- an HTTPS `POST` to the subscriber URL configured in the subscription
- the raw payload in the request body, gzip-compressed on the wire
- `200 OK` with an empty body as the acknowledgement
- `HEAD` probes against the same URL while Mobilithek considers the subscriber temporarily unreachable

Relevant official docs:

- [Technische Schnittstellenbeschreibung (current)](https://mobilithek.info/cms/downloads/tssb-de)
- [Mobilithek technical downloads](https://mobilithek.info/help/download)

This backend accepts push deliveries on:

- `POST /v1/push/<provider_uid>`
- `POST /v1/push`

The provider-specific path is the safest option because Mobilithek does not append subscription parameters to the configured URL. If you prefer a shared endpoint, the app can also resolve the target provider from a configured query parameter or from subscription/publication identifiers sent in request headers.

Examples:

```text
https://live.woladen.de/v1/push/enbwmobility
https://live.woladen.de/v1/push/wirelane
```

If a provider should be push-only, disable it for polling in `data/live_provider_overrides.json` while keeping its subscription entry in `secret/mobilithek_subscriptions.json`.

Validate the certificate and configured subscription pulls:

```bash
python3 scripts/live_subscription_registry.py --probe-certificate
python3 scripts/live_subscription_registry.py
```

Sync the active dynamic DATEX subscription IDs from your Mobilithek account into the secret registry:

```bash
python3 scripts/sync_mobilithek_subscriptions.py
```

The sync currently targets the dynamic DATEX subscriptions from the repo docs: `elu_mobility`, `enbwmobility`, `eround`, `ladenetz_de_ladestationsdaten`, `m8mit`, `vaylens`, and `wirelane`.

## IONOS Packaging

Deployment packaging for an IONOS VPS is provided in `deploy/ionos/`.
The default production mapping assumes the live API is exposed at `https://live.woladen.de`
while the static frontend remains on `https://woladen.de`.
