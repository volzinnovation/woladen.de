# Improved Favorites Plan

Status: planning
Scope: web, Android, and iPhone. The feature should behave consistently across all three clients.

## Goal

Let users organize saved charging stations into personal categories such as `Home`, `Work`, or `Route to Paris`, then filter and browse favorites by those categories.

The favorites feature should support:

- category entry when saving or editing a favorite
- autocomplete from categories already used on the device
- multiple categories per favorite
- grouped favorite display by category
- category filters in the favorites view
- bulk-saving route result stops into a category, defaulting to `Route to <destination name>`

## Synchronization Semantics

For this plan, "in sync across web, Android, and iPhone" means feature parity and a shared local metadata contract:

- same favorite metadata shape
- same category normalization rules
- same migration behavior from existing favorite ID lists
- same grouped and filtered favorite behavior
- same route-to-category behavior once route planning ships on native clients

True cross-device cloud sync is not part of the current app architecture. Today favorites are local-only:

- web stores favorite IDs in `localStorage["woladen_favs"]`
- Android stores favorite IDs in `SharedPreferences` key `woladen_favorites`
- iPhone stores favorite IDs in `UserDefaults` key `woladen_favorites`

If account-backed cross-device sync is required later, add a separate backend-backed favorites API. Do not overload this local-first work with implicit account or identity assumptions.

## Current State

Favorites are currently station-ID sets with separate platform storage:

- Web keeps `state.favorites` as `Set<station_id>` and persists it as a JSON array in `localStorage`.
- Android exposes `FavoritesStore.favorites` as `Set<String>` and persists it as a `SharedPreferences` string set.
- iPhone exposes `FavoritesStore.favorites` as `Set<String>` and persists it as a `UserDefaults` string array.
- Android and iPhone hydrate favorite details through the catalog detail path, so favorites outside the current list/search area can still be shown once fetched.
- Web favorites can already show personal notes and sort by distance or rating.

The current model cannot represent category membership, category autocomplete, route-import provenance, or grouped favorite display without replacing the plain set with metadata.

## Data Model

Introduce a versioned local favorite metadata object on every platform.

Recommended logical shape:

```json
{
  "version": 2,
  "items": {
    "DE:example": {
      "station_id": "DE:example",
      "categories": ["Home", "Route to Paris"],
      "created_at": "2026-06-22T12:00:00Z",
      "updated_at": "2026-06-22T12:05:00Z",
      "source": "manual"
    }
  }
}
```

Recommended platform storage:

- Web: new `localStorage` key `woladen_favorites_v2`
- Android: JSON string in `SharedPreferences` key `woladen_favorites_v2`
- iPhone: JSON `Data` or encoded string in `UserDefaults` key `woladen_favorites_v2`

Keep a derived favorite station ID set for existing code paths:

- marker favorite state
- station detail favorite button state
- favorite detail hydration
- live summary refreshes
- route bulk-add merge checks

Do not expose raw storage details to UI components. Each client should have a small store/service API that returns both metadata and station IDs.

## Favorite Item Fields

Required:

- `station_id`: stable catalog station ID
- `categories`: ordered unique list of user category labels

Recommended:

- `created_at`: first time this station was saved as a favorite
- `updated_at`: last metadata update
- `source`: `manual`, `route`, or `migration`

Optional later:

- `route_labels`: route category provenance if a favorite is saved from multiple routes
- `route_hashes`: backend route result identifiers if route replay or route cleanup is added
- `last_seen_station_name`: cached display fallback if station hydration is unavailable

Do not duplicate station catalog data in favorites storage except for optional display fallbacks. Station details should continue to come from the static catalog and live API.

## Migration

All clients must migrate existing favorites without user action.

Migration rules:

1. If v2 metadata exists and parses, use it.
2. Otherwise read the legacy favorite ID list.
3. Normalize and deduplicate station IDs.
4. Create v2 items with empty `categories`, `source: "migration"`, and timestamps set to the migration time.
5. Keep the legacy key for rollback during the first release.
6. Save v2 metadata after successful migration.

Favorites with no categories should appear under an `Uncategorized` group in UI. Do not write `Uncategorized` as a real category unless the user explicitly chooses that label.

Handle invalid local data defensively:

- Ignore malformed station IDs.
- Ignore malformed item records.
- Normalize category arrays into a clean list.
- Never delete legacy storage just because v2 parsing fails.

## Category Rules

Category labels are user-facing free text.

Normalization:

- trim leading and trailing whitespace
- collapse internal whitespace to a single space
- reject empty labels
- enforce case-insensitive uniqueness per favorite
- preserve display casing from the first existing category match

Recommended limits:

- maximum category label length: 48 characters
- maximum categories per favorite: 12
- maximum autocomplete suggestions shown: 6

Sorting:

- `Home`, `Work`, and route categories are not hard-coded special cases.
- Sort categories alphabetically by localized display name.
- Put `Uncategorized` last unless it is the active filter.
- Within a category group, reuse the platform's existing favorite sort behavior where available.

Autocomplete source:

- all categories currently used by local favorite metadata
- case-insensitive prefix match first
- then case-insensitive substring match
- no network calls

## Web UX

Station detail:

- Existing star button keeps toggling favorite state.
- When a station is a favorite, show a compact category editor near the existing favorite/note controls.
- Category editor includes chips for assigned categories, an input, autocomplete suggestions, and remove controls.
- Adding a category to a non-favorite station should implicitly save it as a favorite.

Favorites view:

- Add category filter chips above the list: `All`, user categories, `Uncategorized`.
- Default `All` view groups cards by category.
- Show a group heading with category name and count.
- A station with multiple categories appears in each relevant group in `All`.
- In a single-category filter, show each station once.
- Keep existing sort select. Sorting applies inside each group.
- Keep notes visible where they are visible today.

Generated site:

- Edit `web/` first.
- Run `python3 scripts/build_site.py` so `site/` stays in sync.

## Android UX

Store:

- Replace `FavoritesStore.favorites: Set<String>` with metadata-backed state.
- Keep a derived `favoriteStationIds: Set<String>` property for callers that only need membership.
- Add functions such as `setCategories(stationId, categories)`, `addCategory(stationId, category)`, `removeCategory(stationId, category)`, and `categorySuggestions(query)`.

Station detail and list/map:

- Existing favorite star behavior remains.
- Favorite metadata should update marker/list state through the derived station-ID set.
- Add category editing to the detail sheet after a station is favorited.

Favorites tab:

- Group `LazyColumn` rows by category with sticky or regular section headers.
- Add horizontal filter chips at the top.
- Preserve catalog detail hydration and live summary refresh for all favorite station IDs.
- Deleting a favorite removes the whole station favorite, not only the active category. Removing one category should be a separate chip action.

## iPhone UX

Store:

- Replace `FavoritesStore.favorites: Set<String>` with metadata-backed state.
- Keep a derived `favoriteStationIDs: Set<String>` property for existing view model calls.
- Preserve screenshot seeding support through `WOLADEN_SCREENSHOT_FAVORITES`; seeded IDs should become in-memory metadata items with empty categories.

Station detail and list/map:

- Existing favorite star behavior remains.
- Add category editing to station detail when a station is favorited.
- Keep map/list favorite marker behavior based on derived station IDs.

Favorites tab:

- Group `List` sections by category.
- Add category filter chips or a compact menu at the top, depending on available layout.
- Preserve catalog detail hydration and live summary refresh for all favorite station IDs.
- Keep row delete behavior as whole-favorite removal.

## Route Integration

`routing.md` currently scopes route planning to Android and iPhone only. This favorites plan should respect that.

Native route results should offer:

- action: `Save route stops to favorites`
- default category: `Route to <destination name>`
- editable category name before saving
- station count preview
- merge behavior for stations already saved as favorites

Bulk-save behavior:

1. Start from the current route result station list.
2. Use the destination label from the route query or geocoder result.
3. Build default category label as `Route to <destination name>`.
4. Let the user edit the label before committing.
5. For each route station:
   - create a favorite item if missing
   - add the route category if missing
   - keep existing categories
   - set `source` to `route` only for newly created favorites
   - update `updated_at`
6. Show a completion message with added and updated counts.

Do not add route UI to web unless web route planning is explicitly requested later. Web should still understand and display route categories if they exist in local metadata.

## API And Backend

No backend changes are required for the local-first version.

Potential future backend-backed sync would need:

- user or device identity
- authenticated favorites API
- conflict resolution
- data export/delete handling
- privacy copy updates
- native and web account UX

That is a separate feature set. Keep this version independent from account infrastructure.

## Privacy

Favorites and categories can reveal sensitive patterns such as home, workplace, and travel routes. Keep the first version local-only.

Privacy requirements:

- Do not send categories to live API endpoints.
- Do not include categories in telemetry.
- Keep privacy/help copy aligned with the existing local-only favorite wording.
- Treat route category names as user-generated personal data.

## Testing

Shared behavior to test on all clients:

- legacy favorites migrate to v2 metadata
- invalid stored metadata does not crash startup
- favorite membership remains correct after migration
- category normalization deduplicates labels case-insensitively
- category autocomplete uses existing local categories
- favorites group by category
- `Uncategorized` is shown for favorites without categories
- category filtering shows correct stations
- multi-category stations appear in each group in `All`
- deleting a favorite removes it from all categories
- removing one category keeps the favorite when other categories remain

Web validation:

- add focused unit tests for favorite metadata parsing and category normalization
- update or add Playwright coverage for category editing and grouped favorites
- run `node --test web/filtering.test.mjs web/location.test.mjs`
- run `python3 scripts/build_site.py`
- if generated site changes, smoke-test `site/` locally

Android validation:

- add store migration and category normalization tests
- add view model or Compose tests for grouped favorites if the existing test setup supports it
- manually smoke-test add/edit/filter/group/delete on emulator
- route bulk-save tests once route planning exists

iPhone validation:

- add store migration and category normalization tests
- add SwiftUI or view model coverage for grouped favorites where practical
- manually smoke-test add/edit/filter/group/delete on simulator
- route bulk-save tests once route planning exists

## Rollout Order

1. Add shared favorite metadata contract and platform migration helpers.
2. Web: implement storage migration, detail category editor, grouped favorites, category filters, and generated `site/`.
3. Android: implement metadata store, category editor, grouped favorites, and category filters.
4. iPhone: implement metadata store, category editor, grouped favorites, and category filters.
5. Add route-result bulk-save action on Android and iPhone when the route feature is implemented.
6. Update German user-facing strings and generated localization outputs.
7. Run platform-specific tests and smoke checks.

## Open Decisions

- Whether to add explicit category rename/delete management in v1 or defer it until category editing proves useful.
- Whether `Save route stops to favorites` should save all route result stations or only user-selected route stops if route planning introduces a curated stop list.
- Whether route category labels should be localized as `Route to Paris` or German-first in German UI, while keeping the stored label as user-editable text.
- Whether cross-device cloud sync is desired later. If yes, treat it as account-backed product work, not a storage-only enhancement.
