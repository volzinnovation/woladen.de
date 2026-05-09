

# Projekt0

Fast chargers in Germany with nearby amenities from OpenStreetMap, now with AFIR-based live data, where available.

## What This Repo Does

- Ingests the official Bundesnetzagentur charging registry.
  Source discovery starts from the BNetzA E-Mobilitaet start page (`Downloads und Formulare`) and selects the newest CSV/XLSX link.
- Filters to active chargers with at least `50 kW` nominal power.
- Augments matched stations with live occupancy from the MobiData BW OCPI feeds.
- Exposes a live backend API for AFIR push/poll ingestion and station status at `https://live.woladen.de`.
- Enriches each charger with nearby amenities (`100m` radius) from OSM
  using either local `germany-latest.osm.pbf` or Overpass fallback.
- Publishes a mobile-ready static web map with filters (operator + amenities).
- Runs monthly via GitHub Actions on day 1 at `00:00 UTC` (`01:00 CET`).

## Project Structure

- `scripts/build_data.py`: End-to-end data pipeline.
- `scripts/build_site.py`: Creates deployable `site/` bundle.
- `backend/`: Live backend for DATEX ingestion, SQLite persistence, and FastAPI endpoints.
- `deploy/ionos/`: Packaging and install scripts for the IONOS VPS that serves `live.woladen.de`.
- `web/`: Frontend app (Leaflet + vanilla JS/CSS/HTML).
- `iphone/`: Native iPhone app (SwiftUI + MapKit).
- `android/`: Native Android app (Jetpack Compose + OSMDroid).
- `data/`: Cached source and generated analytics outputs.
- `.github/workflows/daily-data-generation.yml`: Daily data generation + commit.
- `.github/workflows/pages-deploy.yml`: GitHub Pages build + deploy.

## GitHub Setup

1. In repository settings, set GitHub Pages source to `GitHub Actions`.
2. Ensure the default branch allows `github-actions[bot]` pushes (for generated artifacts).
3. Keep DNS for `woladen.de` pointed to GitHub Pages.

## Backend Deployment

The static frontend and the live backend are deployed separately:

- `https://woladen.de`: static frontend
- `https://live.woladen.de`: FastAPI backend for AFIR dynamic data on chargers, receiving DATEX II v3 pull subscriptions from Mobilithek, where available

Backend docs:

- [backend/README.md](/Users/raphaelvolz/Github/woladen.de/backend/README.md)
- [deploy/ionos/README.md](/Users/raphaelvolz/Github/woladen.de/deploy/ionos/README.md)

Useful public backend endpoints:

- `GET https://live.woladen.de/healthz`
- `GET https://live.woladen.de/v1/status`

## Local Usage

Install dependencies:

```bash
python3 -m pip install -r requirements.txt
```

Build data:

```bash
python scripts/build_data.py \
  --min-power-kw 50 \
  --radius-m 100 \
  --amenity-backend osm-pbf \
  --osm-pbf-path data/germany-latest.osm.pbf \
  --download-osm-pbf
```

Overpass fallback:

```bash
python scripts/build_data.py \
  --min-power-kw 50 \
  --radius-m 100 \
  --amenity-backend overpass \
  --query-budget 500 \
  --refresh-days 30
```

Build site bundle:

```bash
python scripts/build_site.py
```

Test the built web app locally against `https://live.woladen.de`:

```bash
python scripts/build_site.py
python3 -m http.server 4173 --directory site
```

Then open `http://localhost:4173/`.

## Notes

- `--amenity-backend auto` (default) uses local `data/germany-latest.osm.pbf` if present, otherwise Overpass.
- `--query-budget`, `--refresh-days`, and `--overpass-delay-ms` only apply to the Overpass backend.
- If BNetzA fetch fails and no local cache exists, the pipeline fails intentionally.
- On first successful run, artifacts in `data/` are updated and committed by CI.

<!-- DATA_STATUS_START -->
## Data Build Status

- Last build (UTC): `2026-05-09T01:38:17+00:00`
- Source: `https://data.bundesnetzagentur.de/Bundesnetzagentur/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenregister_BNetzA_2026-04-22.csv`
- Full registry stations: `72155`
- Fast chargers (>= 50.0 kW): `15211`
- Fast chargers with live occupancy: `405`
- Fast chargers with static AFIR details: `9461` (price: `7678`, opening hours: `10969`)
- Chargers with >=1 nearby amenity: `15211`
- Occupancy sources scanned: `29` (matched EVSEs: `2778`)
- Static AFIR sources used: `19` (helpdesk phones: `5566`)
- Amenity backend: `osm-pbf`
- Live amenity lookups this run: `0` (cache hits: `0`, deferred: `0`)

Generated files:
- `data/bnetza_cache.csv`
- `data/chargers_full.csv`
- `data/chargers_fast.csv`
- `data/chargers_fast.geojson`
- `data/operators.json`
- `data/summary.json`
<!-- DATA_STATUS_END -->
