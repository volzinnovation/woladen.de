# Woladen iPhone App (Native SwiftUI)

This folder contains a fresh native iPhone implementation of the Woladen web app.

## Goals Implemented

- Native iOS app (SwiftUI + MapKit)
- Offline-first operation (no network required for normal usage)
- Packaged baseline data inside app bundle:
  - `chargers_fast.geojson`
  - `operators.json`
- GPS location access (`NSLocationWhenInUseUsageDescription`)
- iOS 17 map API migration (`Map(position:)` + `Annotation`, no deprecated Map initializers)
- CarPlay scaffolding:
  - `AppDelegate` scene configuration for CarPlay role
  - `CarPlaySceneDelegate` with offline charger list template
  - `UISupportsCarPlay` enabled in Info.plist
- Core user flows:
  - List view
  - Map view
  - Favorites
  - Info
  - Filters (operator, min power, amenities)
  - Charger detail with mini-map + nearby amenities + navigation links

## Project Structure

- `project.yml`: XcodeGen spec for generating the Xcode project
- `Woladen/App`: app entry + root tabs
- `Woladen/Models`: GeoJSON/operator/filter models
- `Woladen/Services`: data loading, location, favorites, data bundle management
- `Woladen/ViewModels`: app state and filtering/sorting logic
- `Woladen/Views`: UI tabs, filter sheet, station detail
- `Woladen/Resources/Data/baseline`: bundled offline baseline dataset
- `Woladen/Resources/ReleaseAccess`: optional Hugging Face stable-channel
  discovery config for analytics-published regional SQLite packages
- `scripts/sync_data_bundle.sh`: prepare the small bootstrap manifest, or sync a
  local SQLite baseline for debug builds
- `scripts/build_iphone_app.sh`: command-line build/archive/upload helper
- `scripts/write_github_release_access_config.sh`: generate the ignored HF
  release access config (the script/file retain their legacy names for build
  compatibility)

## Build

1. Install XcodeGen if needed:

```bash
brew install xcodegen
```

2. Generate Xcode project:

```bash
cd iphone
xcodegen generate
```

3. Open and run:

```bash
open Woladen.xcodeproj
```

## CarPlay Notes

- The codebase includes CarPlay scene scaffolding and compiles as-is.
- For real CarPlay distribution you still need:
  - Apple CarPlay entitlement approval for your app category
  - enabling CarPlay capability in the Xcode target/signing profile
- Without entitlement, the iPhone app still works normally; CarPlay scene just will not be available on actual CarPlay head units.

## Offline Data + Separate Data/Code Update Strategy

The app resolves data in this order:

1. Installed data bundle in app support directory (`WoladenDataBundle/current`)
2. Baseline bundled data in app resources

This allows code and data to be updated separately:

- Code update: app binary update from App Store/TestFlight
- Data update: import a new data bundle folder from Files app (Info tab)

### Expected Imported Bundle Folder Contents

- `chargers_fast.geojson`
- `operators.json`
- optional `data_manifest.json`

If manifest is omitted, the app creates one during import.

## Refresh Baseline Bundle From Repository Data

From repo root:

```bash
./iphone/scripts/sync_data_bundle.sh
```

The regional open-static SQLite packages are built and published from
`Woladen.de-analytics`. This repository contains the iPhone client code and
app-facing release access assets, not the backend bundle generation pipeline.

### Immutable data-only release discovery

The analytics publisher promotes
`AFIR/open-static/releases/open-static-ios-regional-latest` as an atomic HF
stable-channel directory. It is a discovery pointer, not an artifact revision.
The downloader therefore:

1. pins the HF `main` commit used to read the stable-channel mirror manifest;
2. requires its revision-scoped tag to equal
   `open-static-ios-regional-latest-<source-commit>`;
3. resolves that HF tag to an exact immutable repository commit and verifies
   the matching `commits/<source-commit>` manifest;
4. verifies the release manifest, exact asset inventory, byte counts, and
   SHA-256 for both compressed and expanded SQLite before replacing output.

It never falls back to GitHub Latest or the obsolete mutable GitHub tag.
GitHub downloads are available only when an explicit revision-scoped immutable
tag is supplied together with `--no-hf-mirror`.

Generate the optional app/tooling access config with:

```bash
./iphone/scripts/write_github_release_access_config.sh
```

Public HF reads need no token. If the mirror requires authentication, place a
read-only token in `secret/hf_iphone_download_token.txt` or set
`WOLADEN_HF_RELEASE_TOKEN_FILE`. Set `WOLADEN_REQUIRE_HF_RELEASE_TOKEN=1` (or
invoke the config helper with `--require`) to fail closed when that token is
absent.

The current iPhone app uses the live catalog API and has no active on-device
regional SQLite downloader. Reintroducing that runtime data layer is a separate
product/architecture decision; the config and offline bundle tooling here do
not silently enable it.
