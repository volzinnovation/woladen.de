# iOS App Playbook (Transferred from Woladen)

Last updated: 2026-02-22
Scope: Reusable implementation patterns from `/Users/raphaelvolz/Github/woladen.de/iphone`.

## 1) Default Stack

- Language/UI: Swift 5 + SwiftUI
- Maps: MapKit (iOS 17 API: `Map(position:)`, `Annotation`, `onMapCameraChange`)
- Architecture: lightweight MVVM
- Project generation: XcodeGen (`project.yml`)
- Data strategy: offline-first, bundle + optional local update bundle import
- Persistence: `UserDefaults` for simple user state (favorites)
- Optional platform extension: CarPlay scaffold via scene delegate

## 2) Baseline Project Layout

Use this structure for new apps:

- `App/`: `@main` app entry, app delegate, scene delegates
- `Models/`: wire format + domain structs
- `Services/`: repositories, location, storage, bundle manager
- `ViewModels/`: screen-independent state and filtering/sorting logic
- `Views/`: tabs, lists, map, detail sheets, filter sheets
- `Resources/Data/baseline/`: offline seed data
- `scripts/`: data sync and update-bundle creation helpers
- `project.yml`: build target definition

## 3) Offline Data Pattern (Keep This)

Use a two-level data source lookup:

1. Installed bundle in Application Support (for data-only updates).
2. Baseline bundle in app resources (always available fallback).

Required bundle files:

- `chargers_fast.geojson`
- `operators.json`
- optional `data_manifest.json` with `version`, `generatedAt`, `schema`

Implementation notes:

- Validate bundle contents before install.
- If manifest is missing, generate one during import.
- Keep source metadata accessible in UI (active source + version + generatedAt).
- On import/remove, trigger view model reload immediately.

## 4) App Startup Flow

Recommended launch sequence:

1. Initialize shared objects once in app root (`@StateObject`).
2. Request location authorization and one-shot location.
3. Load data on background queue; publish results on main queue.
4. Seed nearby/discovered list from first known location.
5. Keep list/map views bound to the same view model state.

## 5) State + Filtering Pattern

Follow this shape:

- One `AppViewModel` as source of truth for loaded features, active filters, selected item, loading/error state.
- Filter once into a pool, then derive visible/discovered items from map center or user location.
- Use a movement threshold before recomputing nearby items (Woladen uses 250m).
- Cap rendered map/list items to protect performance (Woladen uses 20 nearest).

## 6) UI Patterns Worth Reusing

- Custom tab shell with stable safe-area handling (do not depend on default tab bar behavior).
- Shared detail sheet item binding (`selectedFeature`) from both list and map.
- Filter sheet uses draft state and applies on confirmation.
- Detail view should include a mini map focused on station + nearby points.
- Detail view should include a favorite toggle.
- Detail view should include route handoff buttons (Apple Maps + Google Maps).
- Detail view should include amenity rows with icon + distance/opening metadata.
- Info screen should include location status and manual refresh.
- Info screen should include data-bundle import/remove actions.
- Info screen should include source/version transparency.

## 7) Location + Permissions

Info.plist keys used:

- `NSLocationWhenInUseUsageDescription`
- `NSLocationAlwaysAndWhenInUseUsageDescription`
- `NSLocationAlwaysUsageDescription`

Runtime behavior:

- Request when-in-use first.
- Optionally attempt always-upgrade only after when-in-use granted.
- Start updates only when authorized.
- Keep explicit user action to refresh location.

## 8) CarPlay Scaffold Pattern

- Use `AppDelegate.configurationForConnecting` and attach `CarPlaySceneDelegate` for `.carTemplateApplication`.
- Keep CarPlay template fully offline-capable by reading the same local repository data.
- Treat CarPlay as optional path; app must function normally without entitlement.

## 9) Build + Ops Commands

From repo root:

```bash
./iphone/scripts/sync_data_bundle.sh
```

Create standalone importable update bundle:

```bash
./iphone/scripts/create_update_bundle.sh
```

Generate/open iOS project:

```bash
cd iphone
xcodegen generate
open Woladen.xcodeproj
```

Fast compile sanity (no simulator boot required):

```bash
xcrun --sdk iphonesimulator swiftc \
  -target arm64-apple-ios18.0-simulator \
  -typecheck \
  iphone/Woladen/App/*.swift \
  iphone/Woladen/Models/*.swift \
  iphone/Woladen/Services/*.swift \
  iphone/Woladen/ViewModels/*.swift \
  iphone/Woladen/Views/*.swift
```

## 10) Quality Gates for the Next App

Apply these before release:

- Verify map/list/favorites share the same selected/filter state.
- Verify safe-area behavior on devices with and without home indicator.
- Verify first-open detail map camera fit is local (not world zoom).
- Verify location recenter action reliably triggers camera update.
- Verify offline startup with airplane mode.
- Verify data bundle import/remove updates UI and source metadata.
- Verify power/number formatting has no artifacts.

## 11) Known Pitfalls and Fixes

- Pitfall: long iterative safe-area tweaks.
- Fix: inspect runtime hierarchy and container behavior early.

- Pitfall: map inside transient containers starts with wrong size/region.
- Fix: update camera after layout/appearance and use explicit region fit logic.

- Pitfall: UX regressions caused by data artifacts, not code.
- Fix: add artifact-level sanity checks and treat generated data as versioned product input.

## 12) Kickoff Prompt for a New Thread

Use this text to start the next app quickly:

```text
Build the new iPhone app by following /Users/raphaelvolz/Github/woladen.de/docs/ios-app-playbook.md.
Keep the same architecture (SwiftUI + MapKit + MVVM + offline-first data bundle strategy),
then adapt domain models and screens for the new product.
```
