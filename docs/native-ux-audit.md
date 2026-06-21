# Native App UX Audit and Follow-up Plan

Date: 2026-06-21

Scope: iPhone, iPad, Android phone, and Android tablet after the native API-backed migration to `https://live-eu.woladen.de`.

## Audit Method

The review used the OpenAI product design audit lenses: task entry, information architecture, flow friction, visual hierarchy, trust, default and empty states, consistency, accessibility, responsive reflow, and input ergonomics. Each pass used real simulator or emulator screenshots before deciding whether to continue iterating.

## Screenshots Reviewed

Primary final screenshots:

- `analysis/output/native-ux-audit/screenshots/ios-phone-70-list-grid.png`
- `analysis/output/native-ux-audit/screenshots/ios-tablet-70-list-grid.png`
- `analysis/output/native-ux-audit/screenshots/android-phone-80-cold-default-list.png`
- `analysis/output/native-ux-audit/screenshots/android-tablet-80-cold-default-list.png`
- `analysis/output/native-ux-audit/screenshots/android-phone-71-list-grid-after-wait.png`
- `analysis/output/native-ux-audit/screenshots/android-tablet-71-list-grid-after-wait.png`

Earlier comparison screenshots covered list, map, detail, favorites, and info screens on both platforms.

## Implementation Status

Implemented:

- Native startup now uses the API-backed catalog path and keeps the default backend at `live-eu.woladen.de`.
- Static station detail remains API-backed through `/v1/catalog/stations/{station_id}`.
- Repository-level bounded caches are present for catalog search and station detail.
- Native i18n resources were generated for the web language set.
- Phone and tablet list rows now use larger, more readable typography.
- List screens now use station cards, matching the web app's card-based result presentation.
- Tablet list screens now use adaptive card grids instead of a combined list/map split.
- Tablet map remains a separate tab.
- Detail remains a combined station-properties and map/detail context, which matches the web app direction without inventing a new flow.
- Android cold startup now falls back to a Berlin catalog center, avoiding a misleading empty result from a bounded Germany-centroid search before location is available.
- Android tablet detail now uses an inline side pane on wide layouts instead of opening the compact bottom sheet over the list or map.
- Manual native reloads now invalidate repository caches before re-querying the API.
- Core native screens now consume generated i18n resources for navigation, filters, lists, favorites, map controls, station detail, availability summaries, static detail labels, and Info content.

## Audit Findings

Strengths:

- The primary task is clearer: users land on a charger card list instead of an oversized map-first tablet view.
- Cards make operator, city, power, availability, price, and nearby amenities scannable on both platforms.
- Larger type improves readability for elderly users and aligns better with accessibility expectations.
- Tablet layouts now use screen estate with multi-column result grids while preserving separate list and map modes.
- iOS no longer prompts for elevated location access on passive startup; location prompts are tied to explicit user actions.

Issues fixed during the audit:

- Android tablet list/map split gave the map too much prominence. It is now separate tabs plus adaptive card grids.
- Android list rows accumulated stale results from previous nearby searches. The displayed list is now bounded to the current nearest results.
- Android phone and tablet filter buttons previously obscured the first card. List content now starts below the floating filter control.
- iOS bottom navigation used a floating overlay that could visually collide with content. It now uses an opaque safe-area tab bar.
- Native fallback catalog startup could briefly show no chargers. Android and iOS catalog fallback now query Berlin for bounded API search while map cameras can stay Germany-wide.
- iOS iPad detail could leak into unrelated tabs. The wide detail pane is now limited to list, map, and favorites.
- Favorites could appear empty while saved stations were still being hydrated from `/v1/catalog/stations/{station_id}`. Both native apps now show a loading state when saved favorite IDs exist but station detail rows have not resolved yet.
- Several model-derived native labels ignored generated localization resources. Availability, station detail metadata, live rows, and static detail rows now use native localized resources where generated web keys exist.

Residual risks:

- Some operational/source labels remain intentionally raw because they are backend provider names, legal source titles, API schema names, or app-store/CarPlay scaffolding rather than translated UI chrome.
- Android and iOS do not yet have automated visual regression tests for phone and tablet screenshots.
- Android cold launch still depends on network latency for the first API-backed list because persistent disk caching is not yet implemented.
- Detail layouts were not redesigned beyond readability improvements; this is acceptable for now because the user explicitly asked to stick close to the web app.

## Follow-up Implementation Plan

1. Finish native i18n wiring.
   Run locale smoke checks for the generated web language set and decide whether legal/source titles or CarPlay scaffolding need dedicated translation keys.

2. Add screenshot smoke tests.
   Add deterministic simulator/emulator screenshot scripts for list, map, detail, favorites, and info on phone and tablet widths. Store only reviewed baseline artifacts or CI summaries to avoid noisy binary churn.

3. Add persistent catalog cache.
   Keep the current bounded in-memory caches, then add a small disk-backed stale cache for the most recent catalog search and opened station details so a cold restart has useful content before the network returns.

4. Refine detail tablet ergonomics only if needed.
   Keep the combined detail map/properties flow, but cap the map region to roughly half of the detail surface on tablet if future screenshots show it crowding station facts.

5. Audit dynamic type and font scaling.
   Verify iOS Dynamic Type and Android font-scale behavior at larger accessibility sizes, especially card title truncation, chips, bottom tabs, and detail stat cards.

Current UX assessment: acceptable for this slice after iteration. The remaining work is polish and robustness, not a blocker for the API-backed migration.
