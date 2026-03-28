# Woladen Android App (OSMDroid)

This folder contains an Android port of the iPhone app in `iphone/Woladen`.

## Stack

- Kotlin + Jetpack Compose
- OSMDroid (`org.osmdroid:osmdroid-android`)
- Offline-first data bundle strategy identical to iOS

## Behavior Parity With iPhone

- Same tabs: `Liste`, `Karte`, `Favoriten`, `Info`
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
- Same data bundle flow:
  - installed bundle (`files/WoladenDataBundle/current`) takes precedence
  - baseline fallback from packaged assets
  - import/remove actions in Info tab

## Baseline Data Source

The Android app reuses the iPhone baseline files directly via Gradle `sourceSets`:

- `../../iphone/Woladen/Resources/Data/baseline/chargers_fast.geojson`
- `../../iphone/Woladen/Resources/Data/baseline/operators.json`
- `../../iphone/Woladen/Resources/Data/baseline/data_manifest.json`

This avoids duplicate large data files and keeps iOS/Android baselines aligned.

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
