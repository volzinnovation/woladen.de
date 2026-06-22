# Native Route Charger Search Plan

Status: planning
Scope: native route-planning feature for Android and iPhone only, intended as a future premium surface if usage justifies it. The web app should not expose this feature unless explicitly requested later.

## Goal

Let an app user enter an origin and destination, where either endpoint can be the current location or an address search result, then show chargers along the fastest standard car route. A charger qualifies when it is reachable within 2 km of actual driving access from the route, not merely within 2 km straight-line distance.

The route feature should reuse the data model already used by native list/map views:

- station summaries from the live catalog API
- live occupancy enrichment through station lookup
- existing amenity color semantics
- existing power and amenity category filters, extended with a minimum amenity-count slider

## Current State

- The native apps already discover stations through `GET /v1/catalog/search` around a center point, then rank/filter locally.
- The backend catalog endpoint caps each search to 100 results and currently supports point-radius search, not route-corridor search.
- The catalog station schema already exposes `amenities_total`, `amenity_examples`, and category counts. Native marker colors are currently:
  - `gray`: 0 amenities
  - `bronze`: 1-5 amenities
  - `silver`: 6-10 amenities
  - `gold`: 11+ amenities
- Existing native filters include operator, minimum kW, selected amenity categories, amenity name, availability, and currently-open amenity.
- The iPhone power slider already labels `0`, `50`, `150`, and `300+`; Android has the same text labels but should be upgraded to clearer tick/mark behavior with the new amenity-count slider.
- The web app points to the deployed ORS/Pelias geocoding proxy at `https://live-eu.woladen.de/v1/geocode/autocomplete`.
- Native address search is not yet aligned with that proxy:
  - iPhone currently uses `MKLocalSearch`.
  - Android currently uses `android.location.Geocoder`.

## External Routing Source

Use openrouteservice for route geometry because the project already attributes ORS/Pelias geocoding and ORS supports car directions. Keep ORS credentials server-side.

Recommended server request:

- ORS endpoint: directions with profile `driving-car`
- route preference: default/fastest standard route
- options: no avoid rules, no alternatives, no EV-specific routing for the first version
- geometry: return a GeoJSON line or decoded coordinate list
- instructions: disabled unless later needed for navigation-style UI

Known ORS limits relevant to this feature:

- driving routes are supported globally
- two-waypoint origin/destination requests are comfortably inside the 50-waypoint limit
- the Matrix endpoint can calculate profile-specific distance/time between selected sources and destinations, which is a better fit than hundreds of individual directions calls for candidate validation
- ORS publishes API restrictions and route distance limits, so the backend should handle route errors and quota/rate failures gracefully

References:

- https://openrouteservice.org/
- https://openrouteservice.org/restrictions/
- https://giscience.github.io/openrouteservice/api-reference/endpoints/matrix/
- https://giscience.github.io/openrouteservice-r/articles/openrouteservice.html

## ORS Deployment Strategy

The candidate-validation design creates a lot of routing work. Do not rely on the public ORS API for broad release if every route search validates hundreds of charger candidates.

Recommended rollout:

1. Prototype and low-volume beta can use the public ORS API with strict route-search throttles.
2. Before broad native release, run our own private ORS service for Europe routing.
3. Keep ORS behind the live API; native apps should never call ORS directly.
4. Configure only what this feature needs at first: `driving-car`, distances/durations, no elevation, no alternative routes, no pedestrian/bike profiles, no extra info.
5. Use the public ORS API only as an emergency fallback if the private service is unhealthy and rate limits allow it.

Initial private deployment shape:

- dedicated ORS routing service, not colocated with the live API process
- Europe Geofabrik PBF as source data
- graph build job on a large builder instance
- serving instance or instance pool reading prebuilt graphs
- blue/green graph rollout so a failed rebuild never takes routing offline
- private network access from the live API only
- `/ors/v2/health` and status monitoring

Sizing notes:

- As of 2026-06-22, Geofabrik lists `europe-latest.osm.pbf` at 32.2 GB.
- ORS documents RAM as the main constraint; large areas need substantially more memory, and their public planet setup uses 128 GB per profile.
- Treat Europe `driving-car` as a large build. Budget at least a 128 GB RAM class builder/server for the first spike, then measure graph build time, graph size, JVM heap, and query latency.
- If Europe in one graph is too expensive or unstable, evaluate regional sharding as a fallback. Because the product is Germany-focused but can route across borders, a DACH or Germany-plus-neighboring-countries shard may be a useful intermediate stage.

References:

- https://giscience.github.io/openrouteservice/run-instance/system-requirements
- https://giscience.github.io/openrouteservice/run-instance/running-with-docker
- https://download.geofabrik.de/europe.html

## Recommended Architecture

Build this as a backend-assisted feature, not as dozens of native catalog calls.

### Backend

Add a native-app route search endpoint to the live API. Premium entitlement wiring is deliberately deferred until a later release, and only if user volume justifies the commercial packaging work.

```http
POST /v1/routes/chargers
```

Request:

```json
{
  "origin": { "lat": 52.5200, "lon": 13.4050, "label": "Berlin" },
  "destination": { "lat": 48.1372, "lon": 11.5755, "label": "Munich" },
  "filters": {
    "min_power_kw": 50,
    "min_amenities_total": 0,
    "selected_amenities": [],
    "operator": ""
  }
}
```

Response:

```json
{
  "route": {
    "source": "openrouteservice",
    "profile": "driving-car",
    "distance_m": 585000,
    "duration_s": 21500,
    "geometry": {
      "type": "LineString",
      "coordinates": [[13.4050, 52.5200], [13.42, 52.51]]
    }
  },
  "stations": [
    {
      "station": { "...": "existing catalog station summary shape" },
      "route": {
        "drive_distance_to_route_m": 830,
        "route_detour_m": 1660,
        "straight_line_distance_to_route_m": 420,
        "route_position_m": 132000,
        "nearest_route_point": { "lat": 51.8, "lon": 11.9 }
      }
    }
  ],
  "query": {
    "corridor_radius_m": 2000,
    "min_power_kw": 50,
    "min_amenities_total": 0
  }
}
```

Keep route metadata outside the station object so the existing catalog station contract stays stable.

### Why Server-Side Corridor Search

Client-side route sampling against `/v1/catalog/search` would work for a prototype, but it creates poor request behavior on long routes. A 500 km route sampled every few km can easily become 100+ catalog requests per search, and each request is capped to 100 stations.

The backend can do the expensive part more cleanly:

- call ORS once per route
- query the local SQLite catalog with spatial indexes
- deduplicate stations by `station_id`
- run coarse spatial filtering, then driving-route validation for candidate stations
- return a bounded, route-ranked result set
- enforce route-search rate limits in one place
- cache repeated route searches without involving the app stores' network stacks

## Corridor Algorithm

Use coarse spatial filtering first, then validate reachability with actual driving routes. This matters most along highways: a charger can be close as the crow flies but require a long detour because the next exit or crossing is far away.

1. Fetch the ORS route geometry.
2. Normalize route coordinates as WGS84 `lat/lon` points.
3. Build route segments and cumulative route distance.
4. Collect candidate stations from the catalog spatial index with an expanded bounding box or grid tiles around each segment.
5. Apply static filters early: minimum kW, operator, selected amenity categories, and minimum amenity count.
6. Deduplicate candidate station IDs.
7. Keep a bounded validation pool, initially the best 250 coarse candidates by straight-line route distance and amenity/power tie-breakers.
8. Validate candidates with ORS Matrix first, not one directions request per charger:
   - request `origin -> candidate chargers` distances
   - request `candidate chargers -> destination` distances
   - use the base route distance from the original directions response
9. Compute `route_detour_m = origin_to_candidate_m + candidate_to_destination_m - base_route_distance_m`.
10. Convert that to an estimated one-way route access distance. For the first implementation use `drive_distance_to_route_m = route_detour_m / 2`, because visiting a charger normally means leaving and rejoining the main route.
11. Keep only stations with `drive_distance_to_route_m <= 2000`.
12. For ambiguous cases near the threshold, optionally confirm with a full waypoint directions request, e.g. origin -> charger -> destination.
13. Compute `route_position_m` from the closest projected point on the base route for display, but not as the primary sort.
14. Return the closest 100 stations after driving-route validation, sorted by `drive_distance_to_route_m`, then amenity tier, max kW, and route position.

If the first implementation must use existing point-radius catalog search internally, sample route points every 2 km, query with a padded radius around 3 km, deduplicate, then run the same matrix-first driving-route validation. The padded radius prevents false negatives between samples; it is not the final eligibility test.

Matrix validation reduces request count substantially. For 250 candidates, the backend can validate detour distance with two matrix calls plus one base directions call instead of roughly 250 waypoint directions calls. Full directions calls should be reserved for threshold-edge cases, debugging, or future UI needs where the actual charger detour geometry is displayed.

## Native Product Flow

Add the route planner as a dedicated native `Route` tab. Do not bury the first version behind the existing map toolbar; the route flow has enough endpoint, filter, loading, and result state to deserve its own tab.

Recommended first screen:

- Origin field
  - default action: current location
  - alternative: address search
- Destination field
  - address search
  - optional later: map pick
- Swap origin/destination action
- Route search button
- Filter button using the existing native filter sheet, extended for route-specific thresholds

Route results view:

- route line on the map
- charger markers using the existing amenity colors
- list of route chargers sorted by validated driving distance to the route
- station row metadata:
  - operator and city/address
  - max kW
  - availability if known
  - amenity count and color tier
  - actual driving distance from route, e.g. `0.8 km from route`
  - approximate route kilometer, e.g. `km 132`
- station detail opens the existing detail sheet/view
- navigation action opens Google/Apple Maps as today

Empty states:

- no route found
- route too long or unsupported by provider
- no chargers match the filters
- address search unavailable

## Address Search Alignment

Implement a native `GeocodingClient` on both platforms that calls the same deployed geocoder proxy shape already used by the web app:

```http
GET /v1/geocode/autocomplete?q={query}&lat={focusLat}&lon={focusLon}&limit=5
```

Use current location or current map center as focus. Keep platform-native geocoders only as an explicit fallback if the proxy is down, because route search should behave consistently across Android and iPhone.

## Filters

### Amenity Color Filter

The current marker colors are already derived from `amenities_total`. For route search, make the color filter a user-facing quick preset over a canonical minimum amenity-count filter:

- Any: `min_amenities_total = 0`
- Bronze+: `min_amenities_total = 1`
- Silver+: `min_amenities_total = 6`
- Gold+: `min_amenities_total = 11`

This preserves the current legend thresholds.

### Amenity Count Slider

Add `minAmenityCount` to the shared native filter state:

- range: `0...25`
- step: `1`
- default: `0`
- clamp stations with more than 25 amenities to the end of the scale for display only; filtering still uses the real `amenities_total`
- marks:
  - `0`
  - `Bronze` at 1
  - `Silver` at 6
  - `Gold` at 11
  - `25+`

Filtering rule:

```text
station.amenities_total >= filter.minAmenityCount
```

This should apply to normal nearby search as well as route search so the filter model stays predictable.

Keep category-specific amenity chips in an advanced filter section in the route UI. The primary route filter surface should expose amenity color/count and kW first; category chips are useful but secondary.

### Power Slider

Keep the current native range and default unless product decides otherwise:

- range: `0...350`
- step: `10`
- default: `50`
- marks:
  - `0`
  - `50`
  - `150`
  - `300 (HPC)`

Filtering rule remains:

```text
station.max_power_kw >= filter.minPowerKw
```

## Native Implementation Points

### Shared Model Changes

Android:

- `android/app/src/main/java/de/woladen/android/model/FilterState.kt`
- `android/app/src/main/java/de/woladen/android/model/FilterMatching.kt`
- `android/app/src/main/java/de/woladen/android/ui/FilterSheetView.kt`
- `android/app/src/main/java/de/woladen/android/viewmodel/AppViewModel.kt`
- `android/app/src/main/java/de/woladen/android/repository/ChargerRepository.kt`
- `android/app/src/main/java/de/woladen/android/service/LiveApiClient.kt`

iPhone:

- `iphone/Woladen/Models/FilterState.swift`
- `iphone/Woladen/Models/FilterMatching.swift`
- `iphone/Woladen/Views/FilterSheetView.swift`
- `iphone/Woladen/ViewModels/AppViewModel.swift`
- `iphone/Woladen/Services/ChargerRepository.swift`
- `iphone/Woladen/Services/LiveAPIClient.swift`

### New Native Route State

Add route-specific state without overloading nearby discovery state:

- `RouteEndpoint`: current location or geocoded place
- `RouteSearchState`: idle/loading/loaded/error
- `RouteResult`: route geometry, stations, route metadata
- `RouteStation`: existing feature plus route metadata

Do not mix route result ordering into `discoveredFeatures`; keep route results separate and reuse station detail/live summary hydration helpers where possible.

## Premium Entitlement

Defer premium entitlement source, request format, and backend enforcement to a later release, and only implement it if usage justifies a paid tier. The first route-planning release should still keep the feature native-only, but it should not block on subscription or entitlement infrastructure.

When/if premium enforcement is added later, it must be enforced in both layers:

- Native UI hides or locks the route feature for non-premium users.
- Backend route endpoints require an app auth/entitlement signal before calling ORS or returning route-corridor results.

The backend check will matter at that point because otherwise non-premium clients could call the route endpoint directly.

## Caching And Request Discipline

Backend:

- cache ORS route geometry by rounded origin/destination coordinates and profile
- cache matrix validation results by route hash and candidate station set hash
- cache optional per-candidate waypoint directions validation by route hash and station ID when threshold-edge confirmation is used
- cache route-corridor station results by route hash plus static filters after driving-route validation
- set short TTLs because live availability is hydrated separately
- rate-limit route searches per user/device
- time out ORS calls quickly and return a typed error

Native:

- debounce address autocomplete
- cancel in-flight autocomplete when fields change
- cancel route searches when endpoints change
- reuse previous route result when only local filters change and the route geometry is unchanged
- hydrate live summaries in batches using the existing station lookup flow

## Testing Plan

Backend:

- ORS response normalization with fixture geometry
- route corridor coarse candidate collection, including stations near segment midpoints
- matrix-based candidate validation and `drive_distance_to_route_m` calculation
- optional waypoint directions confirmation for threshold-edge cases
- exclusion of stations that are geographically close but require more than 2 km actual driving access from the route
- deduplication when a station is near multiple route segments
- final cap of the closest 100 validated stations
- result ordering by `drive_distance_to_route_m`
- filter behavior for min kW and min amenity count
- ORS failure and timeout handling

Android:

- `FilterState` persistence migration for `minAmenityCount`
- `ChargerProperties.matches` with amenity-count threshold
- route response parsing
- route view model state transitions
- address autocomplete client parsing
- filter sheet slider semantics

iPhone:

- `FilterStateStore` backwards-compatible decode with `minAmenityCount`
- `ChargerProperties.matches` with amenity-count threshold
- route response parsing
- route view model state transitions
- address autocomplete client parsing
- filter sheet slider semantics

Manual smoke checks:

- current location to searched destination
- searched origin to searched destination
- long Autobahn route with many chargers
- highway-side charger that is geographically close but not reachable within 2 km of actual driving from the route
- no results after Gold+ and high kW filters
- route with ORS unavailable

## Rollout

1. Add backend ORS routing configuration and base route normalization.
2. Add backend `/v1/routes/chargers` with coarse corridor search, matrix-based candidate validation, final 100-result cap, and tests.
3. Run a Europe self-hosted ORS spike before broad rollout: graph build, heap sizing, query latency, matrix throughput, monitoring, and blue/green graph replacement.
4. Add native geocoding client on both platforms, replacing platform-only address search for this flow.
5. Add `minAmenityCount` to native filter models and UI.
6. Add dedicated route tab, route search state, and route results UI on iPhone.
7. Port the same flow to Android.
8. Run native tests and route-specific manual smoke checks.
9. Later, if user volume justifies a paid tier, add premium entitlement gate and backend enforcement.

## Resolved Decisions And Deferred Items

- Premium entitlement source and backend request format are deferred to a later release, only if enough users justify a paid tier.
- The first route UI is a dedicated native tab.
- The backend may validate up to 250 coarse candidates, but returns only the closest 100 stations after actual driving-route validation.
- "Closest" means closest by real driving access from the route, not straight-line distance.
- Route station sorting uses real driving distance to the route.
- Category-specific amenity chips live in an advanced filter section.
- Broad rollout should use a private Europe ORS instance; public ORS is acceptable only for prototype or tightly throttled beta traffic.
- Candidate validation should use ORS Matrix first, with individual waypoint directions only for threshold-edge confirmation or future detour geometry display.
