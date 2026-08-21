# AGENTS.md

This file is the single operational guide for coding agents working in this
repository.

## Non-Negotiables

- Work directly on `main`.
- Never create or switch branches unless the user explicitly asks.
- Never open a pull request unless the user explicitly asks.
- Prefer small, surgical fixes. If the request is to fix a bug, do not refactor unrelated parts of the app.
- Do not overwrite unrelated user changes. This repository is often dirty because generated data contract artifacts are tracked.
- Keep only `README.md` and this `AGENTS.md` in the repository root. Put other
  Markdown documentation in `docs/`.

## Product Invariants

- `woladen.de` is an EU-wide EV charging product covering the European Union plus Switzerland and Norway.
- The canonical brand slogan is `The human side of charging`; the canonical subtitle is `Because charging time is your time`.
- Do not use or revive `Plugs for Cars. Perks for People.` or variants such as `Plugs for cars, Perks for humans`.
- The core static dataset comes from the open-static European charging catalog; Germany/Mobilithek/Bundesnetzagentur is one source path, not the product boundary.
- The default fast-charger threshold is `>= 50 kW`.
- The default amenity radius is `250 m`.
- Web and native clients consume catalog and live data only from
  `https://live-eu.woladen.de`; do not add packaged catalog fallbacks.

## Source Of Truth

- Edit `web/` for frontend source code.
- Treat `site/` as generated output created by `python3 scripts/build_site.py`.
- Do not hand-edit `site/` unless the user explicitly asks for generated-only surgery.
- Edit `iphone/` and `android/` for native app source code and assets.
- Edit `scripts/build_site.py` and frontend/native asset helpers for public
  web/app bundle work.
- Treat `site/`, `output/`, and `test-results/` as local/generated outputs.
  They must stay ignored and must not be committed.
- Do not add backend runtime, provider ingestion, deployment, credential, or
  analytics code here. Those belong in the private sister repository
  `Woladen.de-analytics`.
- Do not commit generated catalog, management, occupancy, or spa payloads. They
  are served by `live-eu` and produced by `Woladen.de-analytics`.
- German/Mobilithek and EU provider catalogs, static-live matches,
  subscriptions, raw payloads, live state, open-static bundle generation,
  derived station characteristics, and management reports are owned by
  `Woladen.de-analytics`.

## Workflow Expectations

- Frontend change:
  Run `node --test web/filtering.test.mjs web/location.test.mjs`.
  Then run `python3 scripts/build_site.py`.
- Backend, data-pipeline, provider onboarding, provider mapping, deployment, or
  management-report change:
  Work in `Woladen.de-analytics` and run the targeted backend/data tests there.
- If web code changes:
  Build the local `site/` bundle, smoke-test it with
  `python3 -m http.server 4173 --directory site`, and inspect the relevant flow
  in a browser. Do not commit the generated `site/` directory.
- Prefer targeted validation over assumptions. If you did not run a relevant test or smoke check, say so explicitly.
- Develop features for web app first, when prompted to port to Android and iPhone stick as faithfully as possible to the web app design and features.

## Live Deployment Boundary

- `live-eu.woladen.de` is the single production backend for EU/open-static
  routing, catalog queries, and Germany/Mobilithek live ingestion. It is owned
  by `Woladen.de-analytics` via `deploy/onboarded-ingest/*`.
- `live.woladen.de` is a retired legacy host, retained offline only until its
  backup evidence is complete. Do not add it to clients, status checks, or
  deployment workflows.
- Do not add or revive live backend deploy workflows in this frontend repo.
- A failing backend deploy is an analytics repository problem unless proven
  otherwise. Do not treat it as a frontend regression just because the public
  frontend defaults to `live-eu` for catalog/routing calls.
- If `live-eu.woladen.de/v1/providers` returns an empty list, that alone is not evidence of a broken EU deployment; `live-eu` is not the Mobilithek provider ingester. Use `/healthz`, `/v1/catalog/search`, geocoding, and routing endpoints for EU API smoke checks.
- `LIVE_DEPLOY_SSH_KNOWN_HOSTS` is optional in the analytics deploy workflow.
- Do not add `LIVE_API_PUSH_TOKEN` back as a required deploy secret. Mobilithek
  subscriber push calls the registered callback URL directly; the old
  `push_auth_not_configured` failure was a Woladen-side shared-token gate, not a
  Mobilithek-auth change.

## Provider Onboarding And Device Mapping

- New provider work belongs in `Woladen.de-analytics`.
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
- Use the analytics repository's provider mapping and quality reports for
  unmapped EVSE evidence, remediation category, and eligibility decisions.

## Known Regression Traps

- Never finish frontend work without confirming the generated `site/` shell can
  be rebuilt from `web/`.
- Sanity-check amenity coverage after pipeline changes. This project has already regressed once when the build fell back to `overpass` with an ineffective query budget and almost all stations lost amenities.
- Provider mapping regressions are product regressions. A feed can look healthy while coverage silently collapses because identifiers stopped matching.
- Do not “fix” poor provider coverage by loosening location heuristics first. Identifier reconciliation is preferred over approximation of location.
- Map and modal work is regression-prone. If you touch those areas, verify the first-open detail minimap zoom and the locate button behavior.
- Leaflet inside hidden or flex layouts needs explicit size stabilization before fit / zoom operations.
- Be careful with DOM event handlers in `web/app.js`. Passing a raw event object into a boolean or options parameter has broken behavior here before.
- Favicon, social-card, and metadata changes are cache-sensitive. Keep versioned asset URLs aligned.
- Do not modify `iphone/` or `android/` for a web or data fix unless the user explicitly asks.

## Release Discipline

- The safe frontend release order is:
  `analytics API ready -> build_site -> local smoke check`.
- Before finishing, review `git status --short` and make sure only intended files changed.
- Mention regenerated local artifacts explicitly in the final update.
- Do not deploy, push, or clean up branches unless the user explicitly asks.

## Historical Context

- The original project goal was a mobile-ready web frontend that helps EV
  drivers find fast chargers with useful nearby amenities.
- Static bundle generation is owned by `Woladen.de-analytics`; this repository
  publishes only the frontend shell through GitHub Pages.
- GitHub Actions remains suitable for recurring low-volume frontend/data bundle
  publication, while provider ingestion and analytics execution live in
  `Woladen.de-analytics`.

## Communication

- Always respond in English in a concise and professional manner, sarcasm is ok, too.

## Documentation

- Keep user facing documentation and help or info files in German
- Keep technical documentation in English
