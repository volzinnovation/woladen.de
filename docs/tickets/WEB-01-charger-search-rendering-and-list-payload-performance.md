# WEB-01 Charger Search Rendering And List Payload Performance

Status: Proposed
Owner repo: `woladen.de`
Supporting repo: `Woladen.de-analytics`
Related backend ticket: `COM-28` in `Woladen.de-analytics`

## Problem

Manual charger search on the local deployment is delayed primarily by browser
work after the API response arrives. A Berlin search returns 100 stations. The
local catalog API responds in roughly 112 ms with a 262 KB payload, while the
browser takes roughly 3 seconds to render the 100 complex station cards. Map
view transitions also take roughly 3 seconds because markers are rebuilt for
the result set.

The list payload contains station-level detail fields and up to 12 amenity
examples per station even though the initial card only needs a small summary.
The current client also schedules additional live-summary lookups after a
catalog response that already contains live summary fields.

## Goal

Reduce time to first useful charger result while preserving filtering, map
behavior, live status, amenity summaries, and complete detail hydration.

## Scope

- Define and consume a summary-only catalog shape for list/map rendering. Keep
  the fields required by filters and cards: stable station ID, coordinates,
  operator/city, charger count, maximum power, distance, amenity totals and
  category counts, price display, live availability counts/status, and daily
  analysis flags used by card state.
- Leave address, contact, connector/payment metadata, opening-hours detail,
  amenity examples, and charger-level rows to the existing station-detail
  hydration path.
- Render a bounded initial list window or use list virtualization/progressive
  rendering so the first visible cards do not wait for all 100 cards to be
  constructed.
- Bound or cluster map marker creation consistently with the result volume.
- Avoid issuing live-summary lookups when the catalog result already carries a
  usable live summary. Preserve refresh behavior when the summary is absent or
  stale.
- When detail hydration completes, update the modal and selected feature
  without rebuilding the entire list or map unnecessarily.

## Acceptance Criteria

- A local search can show the first visible charger cards without blocking on
  construction of all returned cards.
- On a repeatable 100-station fixture, first-visible-card rendering is under
  500 ms on the investigation environment and full-result rendering is
  progressive or otherwise non-blocking.
- Opening a station detail still loads address, charger-level data, amenities,
  live detail, and the existing detail map correctly.
- Detail hydration does not trigger a full 100-card list rebuild while the
  detail modal is open.
- Existing filters for power, operator, availability, open amenities, amenity
  categories, and amenity-name search retain their current semantics.
- No packaged catalog fallback or new backend/data-provider path is added.
- Add focused regression coverage for summary-only features, lazy detail
  hydration, and render-window behavior.
- Run the documented frontend tests, rebuild the local `site/` bundle, and
  smoke-test the search flow in a browser.

## Evidence And Implementation Notes

- Search result cap: `CATALOG_SEARCH_LIMIT = 100` in `web/app.js`.
- Every list item is synchronously passed through `createStationCard()`.
- `applyFilters()` rebuilds map markers and rerenders the active list.
- `openDetail()` already starts catalog and live detail hydration, so the
  product has an existing lazy-detail seam.
- Detail completion currently calls `applyFilters()`, which must be narrowed
  as part of this work to avoid reintroducing the rendering stall.
- A projection of the current local response containing only list/filter fields
  was approximately 82 KB versus 262 KB for the full response. This is a
  payload improvement, but virtualization/progressive rendering is expected to
  provide the larger user-visible gain.

## Validation

- Compare browser timings for autocomplete, catalog response, first visible
  card, full list, map markers, and detail hydration.
- Compare raw and compressed response sizes for the full and summary-only
  catalog shapes.
- Confirm no duplicate live-summary requests are made for rows whose catalog
  response already includes live data.
