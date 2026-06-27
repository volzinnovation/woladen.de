# AGENTS.md

This file is the operational guide for coding agents working in this repository.

`AGENT.md` is historical project context. Use this file for day-to-day guardrails.

## Non-Negotiables

- Work directly on `main`.
- Never create or switch branches unless the user explicitly asks.
- Never open a pull request unless the user explicitly asks.
- Prefer small, surgical fixes. If the request is to fix a bug, do not refactor unrelated parts of the app.
- Do not overwrite unrelated user changes. This repository is often dirty because generated artifacts and analysis outputs are tracked.

## Product Invariants

- `woladen.de` is a Germany-focused fast-charger product.
- The core static dataset comes from the Bundesnetzagentur charging registry.
- The default fast-charger threshold is `>= 50 kW`.
- The default amenity radius is `250 m`.
- Live AFIR / OCPI data enriches the product, but the baseline static experience must remain useful when live data is missing.
- Generated data files are product outputs. A bad `summary.json` or `chargers_fast.geojson` is a user-facing regression, not harmless build noise.

## Source Of Truth

- Edit `web/` for frontend source code.
- Treat `site/` as generated output created by `python3 scripts/build_site.py`.
- Do not hand-edit `site/` unless the user explicitly asks for generated-only surgery.
- Edit `scripts/` for data generation and site-build logic.
- Edit `backend/` for the live API and ingestion pipeline.
- Treat `data/chargers_fast.geojson`, `data/operators.json`, `data/summary.json`, and `site/data/` as generated artifacts.
- Treat generated data schema as a shared contract across the web app, generated site, analysis scripts, and native clients. If you rename or remove fields, audit consumers first.
- Treat `data/mobilithek_afir_provider_configs.json` as the canonical provider catalog for AFIR/static-live onboarding.
- Treat `data/mobilithek_afir_static_matches.csv` as the canonical reviewed site-to-station match inventory consumed by the live backend.

## Workflow Expectations

- Frontend change:
  Run `node --test web/filtering.test.mjs web/location.test.mjs`.
  Then run `python3 scripts/build_site.py`.
- Data-pipeline change:
  Run targeted tests with `python3 -m pytest`, at minimum the affected data tests such as `tests/test_build_data.py`.
- Backend change:
  Run targeted tests with `python3 -m pytest`, especially `tests/test_live_backend.py` and related backend tests.
- Provider onboarding or mapping change:
  Run targeted tests with `python3 -m pytest`, especially `tests/test_live_backend.py`, `tests/test_provider_mapping_gap_report.py`, and the affected build-data tests.
- If the site bundle or generated data changes:
  Smoke-test locally with `python3 -m http.server 4173 --directory site` and inspect the relevant flow in a browser.
- Prefer targeted validation over assumptions. If you did not run a relevant test or smoke check, say so explicitly.
- Develop features for web app first, when prompted to port to Android and iPhone stick as faithfully as possible to the web app design and features.

## Live Deployment Boundary

- Keep `live.woladen.de` and `live-eu.woladen.de` mentally separate.
- `live.woladen.de` is the Germany/Mobilithek live-ingester deployment from this repository. It is deployed by `.github/workflows/live-deploy.yml` via `deploy/ionos/*`, uses systemd services on the IONOS host, and expects Mobilithek machine certificate, subscription registry, and SSH secrets.
- `live-eu.woladen.de` is the EU/open-static routing and catalog API from the private sister repository `volzinnovation/Woladen.de-analytics`. It is a Docker Compose stack under `deploy/onboarded-ingest/` in that repo and must not be deployed with this repository's `deploy/ionos/*` scripts.
- A failing `Live Deploy` run in this repository is a `live.woladen.de` deployment problem unless proven otherwise. Do not treat it as a `live-eu.woladen.de` deployment failure just because the public frontend defaults to `live-eu` for catalog/routing calls.
- If `live-eu.woladen.de/v1/providers` returns an empty list, that alone is not evidence of a broken EU deployment; `live-eu` is not the Mobilithek provider ingester. Use `/healthz`, `/v1/catalog/search`, geocoding, and routing endpoints for EU API smoke checks.
- `LIVE_DEPLOY_SSH_KNOWN_HOSTS` is optional. The workflow may derive `known_hosts` with `ssh-keyscan` from `LIVE_DEPLOY_HOST`, matching the historical deployment setup.
- Do not add `LIVE_API_PUSH_TOKEN` back as a required deploy secret. Mobilithek subscriber push calls the registered callback URL directly; the old `push_auth_not_configured` failure was a Woladen-side shared-token gate, not a Mobilithek-auth change.

## Provider Onboarding And Device Mapping

- New provider work usually touches `data/mobilithek_afir_provider_configs.json`, `data/mobilithek_afir_static_matches.csv`, `backend/loaders.py`, and analysis outputs under `analysis/output/reports/`.
- The goal is not just to ingest a provider feed. The goal is to map its sites and EVSEs onto existing internal `station_id` records with evidence that survives future payload changes.
- Prefer exact identifier reconciliation before any location approximation.
- Matching priority order:
  1. Exact or normalized `provider_evse_id` / charge-point identifiers.
  2. Exact or normalized `site_id`, `station_ref`, `datex_station_ids`, and other provider station identifiers.
  3. Existing alias rules already captured in loaders, static match CSVs, or tests.
  4. Only then geospatial approximation using distance, postcode, operator similarity, address similarity, and EVSE-count overlap.
- Never widen distance thresholds before exhausting identifier-based matching.
- Never let a nearby coordinate silently override contradictory identifier evidence.
- If location-based approximation is needed, treat it as a bootstrap or review-stage mapping, not as strong truth unless follow-up identifier evidence confirms it.
- When a provider exposes usable EVSE IDs or station refs in dynamic payloads, expand the identifier inventory and matching rules first instead of relying on nearest-station heuristics.
- Avoid many-to-one mappings caused only by proximity. Check station count, EVSE overlap, and operator/address consistency.
- Keep new providers out of competitive comparison until mapping quality is demonstrated by the analysis reports, not just by a successful fetch.
- Use `analysis.provider_mapping_gap_report` first for unmapped EVSE evidence and remediation category.
- Use `analysis.provider_quality_report` to decide whether a provider is `eligible`, `review`, or still `exclude`.

## Known Regression Traps

- Never leave `web/` and `site/` out of sync after a frontend change.
- Do not patch generated JSON or GeoJSON to hide an upstream bug. Fix the producer in `scripts/build_data.py` or `scripts/build_site.py`.
- Sanity-check amenity coverage after pipeline changes. This project has already regressed once when the build fell back to `overpass` with an ineffective query budget and almost all stations lost amenities.
- Treat `data/summary.json`, `data/operators.json`, and `data/chargers_fast.geojson` as contract artifacts. If one changes shape unexpectedly, expect frontend regressions.
- Provider mapping regressions are product regressions. A feed can look healthy while coverage silently collapses because identifiers stopped matching.
- Do not “fix” poor provider coverage by loosening location heuristics first. Identifier reconciliation is preferred over approximation of location.
- Map and modal work is regression-prone. If you touch those areas, verify the first-open detail minimap zoom and the locate button behavior.
- Leaflet inside hidden or flex layouts needs explicit size stabilization before fit / zoom operations.
- Be careful with DOM event handlers in `web/app.js`. Passing a raw event object into a boolean or options parameter has broken behavior here before.
- Favicon, social-card, and metadata changes are cache-sensitive. Keep versioned asset URLs aligned.
- Do not modify `iphone/` or `android/` for a web or data fix unless the user explicitly asks.

## Release Discipline

- The safe release order is:
  `build_data -> build_site -> local smoke check`.
- Before finishing, review `git status --short` and make sure only intended files changed.
- Mention regenerated artifacts explicitly in the final update.
- Do not deploy, push, or clean up branches unless the user explicitly asks.

## Communication

- Always respond in English in a concise and professional manner, sarcasm is ok, too.

## Documentation

- Keep user facing documentation and help or info files in German
- Keep technical documentation in English
