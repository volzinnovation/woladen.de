# woladen.de

# About woladen.de
Purpose: **Find charging stations in Europe where charging your EV is fun.** Fun is defined as: Stations should be reliable and available and humans have someting to do in the vicinity of the charger. Chargers are classified into Gold/Silver/Bronce depending on the number of amenities nearby-

[woladen.de](https://woladen.de/) allows you to discover recharging stations in Europe, we currently have data for 20+ countries and aim to pick up national data sources, as soon as they become available ( AFIR is in effect since Apr 2026, but adoption is slow in some countries).

This repository contains the public web frontend, native app source trees, and frontend/app assets. Backend ingestion, deployment, provider configuration, open-static bundle generation, analytics, and management reporting live in the private sister repository `volzinnovation/Woladen.de-analytics` (and is not open source).

## Current data coverage

![Data coverage](web/img/chargers_naturalearth_purple.png)

Our aim is cover all of EU27 (subject to AFIR regulation), CH and NO.

## What This Repo Does

- Finds European recharging stations depending on user criteria
- Augments stations with nearby amenities, e.h. Restaurants, etc., such that humans have something to do while charging the car.
- The commercial backend provides AFIR live status data from the various NAP in the EU and national open data providers.
- Displays live occupancy from `https://live-eu.woladen.de`, where available.
- Includes a web charging-stop planner that matches visible stations to a planned stop duration and needed kWh.
- Ships an installable PWA shell with conservative static caching; live, API, and data feeds remain network-first.
- Provides web, iPhone and Android app source and assets. Web site works perfectly fine, e.g. on a Tesla or NIO in-car browser.

## Project Structure

- `scripts/build_site.py`: Creates deployable `site/` bundle.
- `web/`: Frontend app (Leaflet + vanilla JS/CSS/HTML).
- `iphone/`: Native iPhone app (SwiftUI + MapKit).
- `android/`: Native Android app (Jetpack Compose + OSMDroid).
- `docs/`: Technical plans, native app notes, audits, and historical project docs.
- `site/`: Generated locally by `scripts/build_site.py` and deployed by GitHub
  Pages; it is intentionally ignored by git.
- `output/` and `test-results/`: Local generated screenshots, release assets,
  reports, and test artifacts; they are intentionally ignored by git.
- `.github/workflows/pages-deploy.yml`: GitHub Pages build + deploy.

## Data Sources

The data product is built from the national NAP charging registry, and data published by CPOs on their NAP accounts. We consume AFIR static metadata, and consume live occupancy feeds where possible, and combine this with open data from OpenStreetMap for amenities.

The open static bundle currently supports these country sources:

| Country | Primary source used by the bundle |
| --- | --- |
| AT | E-Control DATEX energy infrastructure table publication |
| BE | EnergyVision OCPI locations, Road OCPI locations, Group INDIGO DATEX static data, and Monta AFIR charge-point table |
| CH | Swiss BFE Ladestationen static OICP JSON |
| CY | Traffic4Cyprus/FixCyprus DATEX II chargers |
| CZ | MPO public charging-station register XLSX |
| DE | `woladen.de` Bundesnetzagentur-derived static bundle rows |
| DK | Monta AFIR charge-point table |
| ES | DGT electrolineras DATEX II static charging infrastructure |
| FI | Digitraffic DATEX locations |
| FR | Base nationale IRVE static consolidation |
| GR | Electrokinisi IDRO static charging-station JSON ZIP |
| HU | NAP subscription DATEX II static snapshots for Eco-Movement and MVM Mobiliti |
| IT | PUN public web-app signed static charging API aggregate |
| LT | Via Lietuva DATEX II public charging infrastructure table |
| LU | Public electrical charging stations WFS GeoJSON |
| LV | Transportdata Eco-Movement and LVC DATEX energy infrastructure snapshots |
| MT | Transport Malta eGIS Charging Points ArcGIS layer |
| NL | NDW OCPI locations |
| NO | NOBIL API v3 static charging-station datadump |
| PL | EIPA reader static JSON files, with browser pages as fallback evidence |
| PT | MOBI.E DATEX II v3 Energy Infrastructure Table Publication |
| SE | NOBIL API v3 static charging-station datadump |
| SI | NAP Prometej IDACS Energy Infrastructure Table Publication |
| OSM | OpenStreetMap/Geofabrik PBFs for nearby amenities |

## Live Data

The web, iPhone, and Android clients obtain catalog data, station details, live
status, ratings, routing, geocoding, and bundle statistics from
`https://live-eu.woladen.de`. Bundle production and data-quality reporting are
owned by `Woladen.de-analytics`; this repository does not publish or package
catalog snapshots.

## GitHub Setup

1. In repository settings, set GitHub Pages source to `GitHub Actions`.
2. Ensure the default branch allows `github-actions[bot]` pushes (for generated artifacts).
3. Keep DNS for `woladen.de` pointed to GitHub Pages.

Provider credentials, source credentials, raw private payloads, live deployment
material, and backend deploy secrets belong in `Woladen.de-analytics`.

## Backend Boundary

The static frontend and live backends are deployed separately:

- `https://woladen.de`: static frontend from this repository
- `https://live-eu.woladen.de`: EU catalog/routing and Germany/Mobilithek
  live backend from `Woladen.de-analytics`

Useful backend smoke endpoints:

- `GET https://live-eu.woladen.de/healthz`

## Local Usage

Use Python 3.12. Create an isolated environment and install the base dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

Build site bundle:

```bash
python3 scripts/build_site.py
```

Test the built web app locally against `https://live-eu.woladen.de`:

```bash
python3 scripts/build_site.py
python3 -m http.server 4173 --directory site
```

Then open `http://localhost:4173/`.

Run the focused frontend checks after web changes:

```bash
node --test web/filtering.test.mjs web/location.test.mjs
```

## Workflow Notes

- `pages-deploy.yml` builds and deploys only the static GitHub Pages site.
- Backend/data/bundle/deploy workflows live in private `Woladen.de-analytics`.
- Native app notes live in `docs/android.md` and `docs/iphone.md`.

## Notes

- Rebuild and smoke-test generated `site/` after web changes, but do not commit
  the generated bundle.
- Keep user-facing docs/help in German and technical docs in English.
- Keep any local analytics checkout or generated data under ignored paths; do
  not commit it to this repository.
