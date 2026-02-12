# iPhone App Port (SwiftUI)

This directory contains a native iPhone port of the current web app.

## What Is Included
- SwiftUI app structure and screens.
- Bundled offline loading of:
- `chargers_fast.geojson`
- `operators.json`
- Bundled amenity icon images from `web/img` (copied to `Woladen/Resources/img`).
- Map view, list view, favorites view, info view.
- Filters (operator, min power, amenity categories).
- Charger detail with amenity examples (`name`, `opening_hours`, distance).
- Apple Maps routing handoff.
- Favorites persistence in `UserDefaults`.

## Folder Layout
- `Woladen/` app source files.
- `Woladen/Resources/` bundled app data files.
- `Woladen/Resources/img/` bundled amenity icons.

## Create Xcode App Target
1. Open Xcode and create a new iOS App project, name it `Woladen`.
2. Delete the template Swift files and add all files from `iphone/Woladen/`.
3. Ensure all files in `iphone/Woladen/Resources/` are added to target membership:
  - `chargers_fast.geojson`
  - `operators.json`
  - all files in `img/`
4. Set deployment target to iOS 16+ (iOS 17 recommended).
5. In target settings, add `Privacy - Location When In Use Usage Description`.
  Example value: `Used to sort chargers by distance and center the map on your location.`
6. Build and run on iPhone or simulator.
