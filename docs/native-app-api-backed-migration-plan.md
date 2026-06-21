# Native App API-Backed Migration Plan

Last updated: 2026-06-21

## Scope

Migrate the iPhone/iPad and Android apps to the current web product direction while keeping native clients API-backed only. The apps should support the same European catalog, live backend behavior, tablet/widescreen layouts, filtering improvements, detail-view improvements, and internationalization coverage as the web app.

This plan is implementation guidance for the next native-app work session. It does not change the repo by itself.

## Decisions

- Use `https://live-eu.woladen.de` as the default native API base.
- Treat the European SQLite bundle from `/Users/raphaelvolz/Github/woladen.de-analytics` as a backend-side data source for `live-eu`, not as a mobile app asset.
- Do not ship or download native regional SQLite packs.
- Keep native clients functional with bounded local API caches, not with an offline full catalog.
- Support every language currently supported by the web app.
- Implement data/API/i18n foundations before tablet UI polish.

## Reassessment After Recent Web Improvements

Recent web commits after the original planning pass tightened favorite behavior:

- Favorited stations use a star marker on the map, including the detail mini-map.
- Favorited stations show a star in station cards instead of the normal amenity/status dot.
- Toggling a favorite in the detail view refreshes visible list cards and markers immediately.
- Favorite card state has a Playwright regression test: the card gets the star, the amenity dot is absent, and the station card remains the click target rather than adding a nested favorite button.
- The Info legend includes a favorite-station marker entry.

Native implication: favorite visibility is now part of the first parity slice. It should ship with the API/i18n foundation work rather than waiting for later detail polish.

## Implementation Status: 2026-06-21

Completed in the native migration slice:

- iOS and Android now default to `https://live-eu.woladen.de`.
- App startup and nearby refresh use `GET /v1/catalog/search`.
- Static station hydration uses `GET /v1/catalog/stations/{station_id}`.
- Live summary refresh keeps `POST /v1/stations/lookup` with max-20 station ID batches.
- Live station detail continues through `GET /v1/stations/{station_id}`.
- Native clients no longer package or load local `chargers_fast.geojson`, `operators.json`, native SQLite packs, or the European SQLite bundle.
- The iOS baseline data resources and Android bundle manager were removed.
- The old iPhone bundle sync/update scripts were removed.
- Repository-level bounded caches were added:
  - iOS catalog search cache: 24 entries, 5 minute fresh TTL, 24 hour stale fallback.
  - iOS station detail cache: 240 entries, 24 hour fresh TTL, 7 day stale fallback.
  - Android catalog search cache: 48 LRU entries.
  - Android station summary cache: 600 LRU entries.
  - Android station detail cache: 180 LRU entries.
- Native i18n resources are generated from the web language catalog for all 23 web languages.
- Android emits `values`, `values-en`, and `values-*` resources with the same key set.
- iOS emits `Localizable.xcstrings` with all 23 localizations.
- iOS supports iPad target family, regular-width sidebar layout, and landscape orientations.
- Android switches to navigation rail and list/map split layout on wide screens.
- Favorite star marker/card parity and Info legend parity were included.

Validation completed:

- `node scripts/generate_native_i18n.mjs`
- `cd iphone && xcodegen generate`
- `cd android && ./gradlew testDebugUnitTest lintDebug`
- `cd iphone && xcodebuild test -project Woladen.xcodeproj -scheme Woladen -destination 'platform=iOS Simulator,name=iPhone 16,OS=18.6'`
- `cd iphone && xcodebuild build -project Woladen.xcodeproj -scheme Woladen -destination 'platform=iOS Simulator,name=iPad Pro 13-inch (M4),OS=18.6'`

Remaining follow-ups after this slice:

- Add emulator/manual smoke tests for Android expanded width.
- Add manual iPad/iPhone smoke tests against live catalog data.
- Decide whether repository caches should become file-backed persistence; current implementation is bounded in-memory repository caching.
- Add API-backed place search/geocoding parity.
- Add optional occupancy/history only after the `live-eu` API contract is confirmed.

## Non-Goals

- Do not bundle `open_static.sqlite3` or regional SQLite packs in iOS or Android.
- Do not keep the old native `chargers_fast.geojson` baseline as the long-term product path.
- Do not port web SEO, Open Graph, install-banner, or purely web metadata behavior.
- Do not make cached live availability appear current when it is stale.
- Do not add a branch or pull request unless explicitly requested.

## Backend And Data Contract

The native apps should use `live-eu` as the single catalog and live-data gateway.

Primary API usage:

- `GET /v1/catalog/search`
  - Nearby and filtered station discovery.
  - Query parameters should include location, radius, limit, mode, country filter when relevant, min power, connector/current filters, operator, and source identifiers as the UI grows.
  - For travel/default fast-charger behavior, preserve the product default of `>= 50 kW` unless the user lowers the power filter.
- `GET /v1/catalog/stations/{station_id}`
  - Static station detail, charger rows, and amenity detail.
  - Use as the detail source instead of local GeoJSON.
- `POST /v1/stations/lookup`
  - Live summary overlay for visible or listed stations.
  - Batch at no more than 20 station IDs per request.
- `GET /v1/stations/{station_id}`
  - Live station detail.
- `GET /v1/geocode/autocomplete`
  - Place/address autocomplete.

`live-eu` currently proxies German `DE:` station lookup/detail traffic to the German backend during the transition. Native code should not special-case Germany beyond keeping station IDs intact and respecting backend response contracts.

Occupancy/history endpoints should be treated as a separate contract until confirmed on `live-eu`. If they remain on the commercial mobile API, configure them as a separate optional API base rather than mixing assumptions into the catalog client.

## Caching Model

Use explicit repository-level caches in both apps. Cache entries must include fetched-at timestamps, request keys, response schema version when available, and enough metadata to distinguish fresh from stale UI.

Recommended cache classes:

| Data | Key | Fresh TTL | Stale Fallback | Notes |
| --- | --- | ---: | ---: | --- |
| Catalog search | rounded lat/lon, radius, filters, language | 5 min | 24 h | Use stale results only with visible stale/error state. |
| Station static detail | station ID, language | 24 h | 7 d | Static catalog detail changes less often. |
| Live lookup summary | sorted station IDs | 60 s | 5 min | Never show as fresh past TTL. |
| Live station detail | station ID | 60 s | 5 min | Preserve backend timestamps in UI. |
| Geocode autocomplete | query, language, optional country | 24 h | 7 d | Cache normalized query text. |
| Occupancy/history | station ID, chart profile, language | 6 h | 7 d | Only after API contract is confirmed. |

Implementation notes:

- iOS can use a small file-backed cache under Application Support plus in-memory request coalescing.
- Android can use a small SQLite or file-backed cache under app storage plus in-memory request coalescing.
- Cache size should be bounded and evicted by age and least-recently-used access.
- Failed live refresh must not erase fresher cached static catalog data.
- Availability UI must show backend timestamps and stale/error state when data is outside the fresh TTL.

## Internationalization

Native apps must support the same language set as `web/i18n/`:

`cs`, `da`, `de`, `el`, `en`, `es`, `fi`, `fr`, `hu`, `it`, `lb`, `lt`, `lv`, `mt`, `nb`, `nl`, `nn`, `pl`, `pt`, `rm`, `sl`, `sv`, `tr`.

Required behavior:

- Default fallback language is English.
- Resolve initial language from explicit app setting, then OS language, then English.
- Preserve web aliases where relevant, especially `no -> nb`.
- Send `Accept-Language` on API requests.
- If `live-eu` accepts an explicit language parameter for catalog/geocoder endpoints, pass it consistently.
- Generated native resources should be derived from the web catalog where practical.

Implementation approach:

- Add a canonical native string-key map aligned with `web/i18n.mjs`.
- Generate iOS `.xcstrings` or `.strings`/`.stringsdict` from `web/i18n/*.json`.
- Generate Android `res/values-*/strings.xml` from `web/i18n/*.json`.
- Replace hardcoded German SwiftUI and Compose strings incrementally, starting with shared navigation, filters, list/map/favorites, detail, info/help, errors, and data freshness labels.
- Add tests that fail when required native keys are missing for German, English, and at least one non-German European language.

## Feature Parity Work

After API and i18n foundations are in place, migrate the web behavior in this order.

### 1. API-Backed App Startup

- Remove startup dependency on loading all local features.
- Start from last known location, selected map/search location, or a conservative default viewport.
- Load catalog results from `/v1/catalog/search`.
- Populate live summaries through `/v1/stations/lookup`.
- Keep favorites visible by fetching/caching their station details individually when they are outside the current search area.

### 2. Filter Parity

- Add `availableOnly`, default enabled.
- Add `currentlyOpenOnly`.
- Allow min power to start at `0`.
- Keep product default fast-charger behavior at `>= 50 kW` until the user changes the power filter.
- Match web active-filter counting and active-label summaries.
- Ensure filters are represented in catalog-search cache keys.

### 3. Detail Parity

- Use `/v1/catalog/stations/{station_id}` for static detail and amenities.
- Use `/v1/stations/{station_id}` for live detail.
- Preserve web section order:
  - mini map
  - highlights
  - amenities
  - live status
  - personal note
  - rating
  - navigation/help actions
  - typical occupancy when API-backed
  - static details/source
- Add amenity drill-in/detail sheet where native is missing it.
- Keep personal notes, ratings, and favorites local unless a backend persistence contract is added later.

### 4. Favorite Visibility Parity

- Show a filled star as the leading station-card symbol for favorite stations.
- Do not add a nested favorite button inside station cards; the card remains the primary tap/click target.
- Use a favorite star marker on the map and detail mini-map for favorite stations.
- Favorite marker should take precedence over live out-of-order and fully occupied marker overlays, matching web behavior.
- Toggling favorite state from detail must immediately refresh:
  - detail favorite button state
  - visible list/favorites cards
  - map markers
  - detail mini-map marker
- Add the favorite marker to Info/legend copy through i18n.

### 5. Tablet And Widescreen Layouts

- iOS:
  - Enable iPad support in the Xcode project settings.
  - Use adaptive SwiftUI layouts for compact vs regular horizontal size classes.
  - Prefer a side navigation rail/sidebar on wide screens.
  - Present details as a split view beside list/map where practical instead of a phone-style full modal.
- Android:
  - Use window-size classes.
  - Switch from bottom navigation to navigation rail on medium/expanded widths.
  - Use list/map/detail panes on expanded screens.
  - Keep phone bottom-sheet behavior intact for compact screens.

### 6. Search And Geocoding

- Add API-backed place search using `/v1/geocode/autocomplete`.
- Cache autocomplete results by normalized query/language.
- Use selected place coordinates to drive catalog search.
- Keep current map-position search behavior where it exists.

## Platform-Specific Starting Points

Current repo files to update tomorrow:

- iOS live client: `/Users/raphaelvolz/Github/woladen.de/iphone/Woladen/Services/LiveAPIClient.swift`
- iOS repository/data path: `/Users/raphaelvolz/Github/woladen.de/iphone/Woladen/Services/ChargerRepository.swift`
- iOS app state: `/Users/raphaelvolz/Github/woladen.de/iphone/Woladen/ViewModels/AppViewModel.swift`
- iOS project settings: `/Users/raphaelvolz/Github/woladen.de/iphone/project.yml`
- Android live client: `/Users/raphaelvolz/Github/woladen.de/android/app/src/main/java/de/woladen/android/service/LiveApiClient.kt`
- Android repository/data path: `/Users/raphaelvolz/Github/woladen.de/android/app/src/main/java/de/woladen/android/service/ChargerRepository.kt`
- Android app state: `/Users/raphaelvolz/Github/woladen.de/android/app/src/main/java/de/woladen/android/viewmodel/AppViewModel.kt`
- Android strings: `/Users/raphaelvolz/Github/woladen.de/android/app/src/main/res/values/strings.xml`

Useful sibling-repo references, as backend/data contract examples only:

- `/Users/raphaelvolz/Github/woladen.de-analytics/iphone/Woladen/Services/OpenStaticSQLiteStore.swift`
- `/Users/raphaelvolz/Github/woladen.de-analytics/iphone/Woladen/Services/DataBundleManager.swift`
- `/Users/raphaelvolz/Github/woladen.de-analytics/iphone/Woladen/Models/CountryPackCatalog.swift`

Do not port the sibling repo's native SQLite-pack install flow into this repo under the API-backed decision.

## Implementation Sequence

1. Add shared native API client support for `live-eu`, `Accept-Language`, request timeouts, and response errors.
2. Add cache infrastructure and cached repository methods.
3. Replace native catalog loading with `/v1/catalog/search`.
4. Replace native static detail loading with `/v1/catalog/stations/{station_id}`.
5. Preserve existing live summary/detail behavior on the new `live-eu` default.
6. Add i18n resource generation and language selection/fallback.
7. Replace hardcoded native strings in the main app flows.
8. Add favorite visibility parity from the recent web commits.
9. Add filter parity.
10. Add detail parity.
11. Add tablet/widescreen adaptive layouts.
12. Add search/geocoder parity.
13. Add optional occupancy/history only after the API target is confirmed.

## Validation Gates

Minimum checks before considering the migration complete:

- iOS unit tests for API request construction, cache freshness, filter parity, language fallback, and batch sizing.
- Android unit tests for API request construction, cache freshness, filter parity, language fallback, and batch sizing.
- Germany smoke test through `live-eu` with `DE:` station IDs.
- Non-Germany EU smoke test through catalog search and detail.
- Fresh-cache and stale-cache UI checks with network disabled.
- Language smoke checks for English, German, French, Dutch, Polish, and one Nordic language.
- Tablet checks on iPad simulator and Android expanded-width emulator.
- Phone checks to confirm compact layout did not regress.

## Open Questions

- Confirm whether `live-eu` accepts explicit language parameters in addition to `Accept-Language`.
- Confirm the public/native target for occupancy/history chart endpoints.
- Decide exact cache storage implementation per platform before coding.
- Decide whether language selection lives in the Info screen, app settings, or both.
- Decide whether ratings stay purely local or need a future backend contract.
