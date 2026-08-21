# Woladen iPhone App

The native SwiftUI app is an API-backed client of
`https://live-eu.woladen.de`. It does not package or download catalog bundles.

## Data flow

- Nearby search: `GET /v1/catalog/search`
- Catalog station detail: `GET /v1/catalog/stations/{station_id}`
- Live summaries: `POST /v1/stations/lookup`
- Live station detail: `GET /v1/stations/{station_id}`
- Bundle statistics: `GET /data/open_static_summary.json`

The app keeps bounded in-memory response caches and user-local preferences such
as favorites, notes, and ratings. Those caches are not an offline catalog.

## Build

```bash
cd iphone
xcodegen generate
open Woladen.xcodeproj
```

For command-line builds and App Store Connect uploads, use
`iphone/scripts/build_iphone_app.sh`.

The source includes CarPlay scaffolding. Distribution on actual CarPlay hardware
still requires the appropriate Apple entitlement and signing capability.
