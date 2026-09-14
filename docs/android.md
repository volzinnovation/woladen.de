# Woladen Android App

This folder contains an Android port of the iPhone app in `iphone/Woladen`.

## Stack

- Kotlin + Jetpack Compose
- MapLibre for the phone map and AndroidX Car App templates for Android Auto
- API-backed catalog and live data from `https://live-eu.woladen.de`

## Behavior Parity With iPhone

- Same tabs: `Liste`, `Karte`, `Route`, `Favoriten`, `Info`
- Same filter model: operator, min power, amenities
- Same nearest-discovery map/list logic:
  - visible pool is seeded from current filter
  - map-center updates are thresholded at `250m`
  - each center update merges nearest `20` stations into discovered order
- Same marker color semantics:
  - `gold` `>10` amenities
  - `silver` `>5`
  - `bronze` `>0`
  - otherwise gray
- Same detail flow:
  - map/list tap opens detail sheet
  - detail mini-map shows station + amenity overlays
  - favorite toggle and navigation handoff actions

## Android Auto

The app registers as a point-of-interest app for Android Auto. The car screen
shows up to six nearby fast chargers (20 km search radius), live availability,
distance, power, price, and favorite chargers. Selecting a charger opens a
compact detail screen with a favorite action and hands navigation to the car's
navigation app through the standard `geo:` intent. Location permission is
requested on the phone, and favorites use the same process-shared store as the
phone UI. The car reads the same persisted phone filter preferences, including
operator, minimum power, amenity, availability, and opening-hours filters.

The Android Auto experience is intentionally a distraction-safe POI flow. It
does not render turn-by-turn navigation; the navigation handoff remains the
platform behavior used by the phone app as well.
## Data Source

Catalog search, station detail, live summaries, live station detail, and bundle
statistics use the same `live-eu` contracts as the iPhone app. The Android app
does not package or import a local catalog bundle.

## Open In Android Studio

Open the `android/` folder as a standalone project.

If you want a local wrapper, generate one from inside `android/`:

```bash
gradle wrapper
```

Then build with:

```bash
./gradlew :app:assembleDebug
```

## Release Signing

Release builds read signing credentials from either:

- `android/keystore.properties`
- environment variables:
  - `ANDROID_KEYSTORE_FILE`
  - `ANDROID_KEYSTORE_PASSWORD`
  - `ANDROID_KEY_ALIAS`
  - `ANDROID_KEY_PASSWORD`

Use `keystore.properties.example` as the template for a local `keystore.properties` file.
If you generate the keystore with `keytool`'s default `PKCS12` format, use the same value for
`storePassword` and `keyPassword`.

Build a signed app bundle with:

```bash
./gradlew :app:bundleRelease
```

## Automated Pre-User-Testing Checks

Run instrumentation smoke/regression tests on a connected device:

```bash
./gradlew :app:connectedDebugAndroidTest
```

Covered flows (`WoladenSmokeTest`):

- tab navigation (`Liste`, `Karte`, `Favoriten`, `Info`)
- map filter open/apply
- list -> detail open/close
- favorite toggle + favorites tab presence
- info tab + location refresh action
- map double-tap responsiveness regression guard
