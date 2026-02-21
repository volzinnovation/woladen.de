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
- `scripts/sync_data_bundle.sh`: sync latest generated data into iPhone baseline bundle

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
