# Sprint Kick-Off

Date: 2026-02-11
Project: `woladen.de`

## Project Goals

Find EV charging stations across the European Union plus Switzerland and Norway, with useful amenities nearby. The open-static European charging catalog is the product baseline, while Bundesnetzagentur/Mobilithek remains one Germany-specific source path. Filter to charging points with at least 50 kW power by default and augment station data with nearby amenities.

Regularly (e.g. once a month) build a database based in static files.

Create a good-looking mobile-ready web frontend in Javascript and HTML that displays charging points on a map and allows to filter by operator brand as well as amenities that are sought for.   Idealy it also contains geolocation of the user and allows to find an optimal car route from the users position to suitable charging points. 

Set up automated data-analysis pipelines that:

- pull supported public European charging datasets,
- compute amenities from OpenStreetMap data around charging stations,
- versions both source snapshot cache and derived analytics outputs in Git,
- publishes  the app via Github pages
- batch job runs on GitHub Actions at fixed noon GMT+1 (`11:00 UTC`).

A (broken) code example from a past student project that attempted this is found in 2025_06_schnelllader_umfeldanalyse.py

Set github repo needs to be setup to serve Github Pages using the domain woladen.de

## What Worked Well

- GitHub Actions is sufficient for low-volume recurring analysis tasks.
- Versioning both raw snapshot and derived output gives reproducibility and reviewability.
- Small CLI options (`--minimum`, `--lookback-days`, etc.) make the job reusable.
- Marker/section-based README update keeps docs current without manual steps.



## Reusable Pattern From Past Projects

1. Ingest:
   - fetch public dataset,
   - persist latest raw snapshot to `data/<source>_cache.csv`,
   - fallback to cache if fetch fails.
2. Transform/Analyze:
   - normalize schema defensively,
   - compute deterministic outputs,
   - append run-level row(s) to `data/<analysis>.csv`.
3. Publish:
   - update one README section in-place with latest narrative summary.
4. Automate:
   - schedule Action,
   - commit only changed generated artifacts.
5. Operate:
   - keep run logs readable,
   - fail hard only when no network and no cache.



## Suggested Upgrades Later

- Routing functionality based on suitable online service
- iPhone app
- Android app
