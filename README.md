# woladen.de

Fast chargers in Germany with nearby amenities from OpenStreetMap.

## What This Repo Does

- Ingests the official Bundesnetzagentur charging registry.
  Source discovery starts from the BNetzA E-Mobilitaet start page (`Downloads und Formulare`) and selects the newest CSV/XLSX link.
- Filters to active chargers with at least `50 kW` nominal power.
- Enriches each charger with nearby amenities (`100m` radius) from OSM Overpass.
- Publishes a mobile-ready static web map with filters (operator + amenities).
- Runs daily via GitHub Actions at `11:00 UTC`.

## Project Structure

- `scripts/build_data.py`: End-to-end data pipeline.
- `scripts/build_site.py`: Creates deployable `site/` bundle.
- `web/`: Frontend app (Leaflet + vanilla JS/CSS/HTML).
- `data/`: Cached source and generated analytics outputs.
- `.github/workflows/daily-data-and-pages.yml`: Daily build + GitHub Pages deploy.

## GitHub Setup

1. In repository settings, set GitHub Pages source to `GitHub Actions`.
2. Ensure the default branch allows `github-actions[bot]` pushes (for generated artifacts).
3. Keep DNS for `woladen.de` pointed to GitHub Pages.

## Local Usage

Install dependencies:

```bash
pip install -r requirements.txt
```

Build data:

```bash
python scripts/build_data.py --min-power-kw 50 --radius-m 100 --query-budget 500 --refresh-days 30
```

Build site bundle:

```bash
python scripts/build_site.py
```

## Notes

- The pipeline is cache-first for OSM amenity lookups and refreshes stale cache entries.
- If BNetzA fetch fails and no local cache exists, the pipeline fails intentionally.
- On first successful run, artifacts in `data/` are updated and committed by CI.

<!-- DATA_STATUS_START -->
## Data Build Status

- Last build (UTC): `2026-02-11T13:30:16+00:00`
- Source: `https://data.bundesnetzagentur.de/Bundesnetzagentur/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenregister_BNetzA_2026-01-28.csv`
- Fast chargers (>= 50.0 kW): `19816`
- Chargers with >=1 nearby amenity: `0`
- Overpass queries this run: `0` (cache hits: `0`, deferred: `19816`)

Generated files:
- `data/bnetza_cache.csv`
- `data/chargers_fast.csv`
- `data/chargers_fast.geojson`
- `data/summary.json`
<!-- DATA_STATUS_END -->
