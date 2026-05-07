#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
IPHONE_DIR="$ROOT_DIR/iphone"
DERIVED_DATA_DIR="$ROOT_DIR/tmp/ios-app-store-screenshots-derived-data"
OUTPUT_DIR="$ROOT_DIR/output/app-store/ios/6.5-inch"
APP_BUNDLE_ID="de.woladen.ios"
APP_SCHEME="Woladen"
SIMULATOR_NAME="${SIMULATOR_NAME:-}"
SCREENSHOT_LOCATION="${SCREENSHOT_LOCATION:-53.554808,10.009998}"
SCREENSHOT_STATION_ID="${SCREENSHOT_STATION_ID:-DE:fb23ac5910c5e002}"

mkdir -p "$OUTPUT_DIR"

if [[ -n "${SIMULATOR_UDID:-}" ]]; then
  DEVICE_UDID="$SIMULATOR_UDID"
else
  devices="$(xcrun simctl list devices available)"

  if [[ -n "$SIMULATOR_NAME" ]]; then
    DEVICE_UDID="$(printf '%s\n' "$devices" | grep -F "$SIMULATOR_NAME" | head -1 | sed -E 's/.*\(([A-F0-9-]+)\).*/\1/')"
  fi

  if [[ -z "${DEVICE_UDID:-}" ]]; then
    while read -r candidate; do
      [[ -n "$candidate" ]] || continue
      DEVICE_UDID="$(printf '%s\n' "$devices" | grep -F "$candidate" | head -1 | sed -E 's/.*\(([A-F0-9-]+)\).*/\1/')"
      if [[ -n "$DEVICE_UDID" ]]; then
        SIMULATOR_NAME="$candidate"
        break
      fi
    done <<'EOF'
6,5" Device
6,5" Decive
iPhone 16 Pro Max
iPhone 16 Plus
iPhone 15 Pro Max
iPhone 15 Plus
iPhone 14 Pro Max
iPhone 14 Plus
iPhone 13 Pro Max
iPhone 12 Pro Max
iPhone 11 Pro Max
iPhone Xs Max
EOF
  fi
fi

if [[ -z "${DEVICE_UDID:-}" ]]; then
  echo "Unable to find the 6.5-inch simulator. Set SIMULATOR_UDID or SIMULATOR_NAME." >&2
  exit 1
fi

open -a Simulator --args -CurrentDeviceUDID "$DEVICE_UDID" >/dev/null 2>&1 || true
xcrun simctl boot "$DEVICE_UDID" >/dev/null 2>&1 || true
xcrun simctl bootstatus "$DEVICE_UDID" -b
xcrun simctl ui "$DEVICE_UDID" appearance light
xcrun simctl status_bar "$DEVICE_UDID" clear || true
xcrun simctl status_bar "$DEVICE_UDID" override \
  --time 9:41 \
  --dataNetwork wifi \
  --wifiMode active \
  --wifiBars 3 \
  --batteryState charged \
  --batteryLevel 100 \
  --operatorName "" || true

xcodebuild \
  -project "$IPHONE_DIR/Woladen.xcodeproj" \
  -scheme "$APP_SCHEME" \
  -configuration Debug \
  -sdk iphonesimulator \
  -destination "id=$DEVICE_UDID" \
  -derivedDataPath "$DERIVED_DATA_DIR" \
  build

APP_PATH="$DERIVED_DATA_DIR/Build/Products/Debug-iphonesimulator/Woladen.app"

if [[ ! -d "$APP_PATH" ]]; then
  echo "Built app not found at $APP_PATH" >&2
  exit 1
fi

xcrun simctl terminate "$DEVICE_UDID" "$APP_BUNDLE_ID" >/dev/null 2>&1 || true
xcrun simctl uninstall "$DEVICE_UDID" "$APP_BUNDLE_ID" >/dev/null 2>&1 || true
xcrun simctl install "$DEVICE_UDID" "$APP_PATH"

DATA_CONTAINER="$(xcrun simctl get_app_container "$DEVICE_UDID" "$APP_BUNDLE_ID" data)"
READY_DIR="$DATA_CONTAINER/Documents/app-store-screenshots"
mkdir -p "$READY_DIR"

declare -a SCREENS=(
  "01-list:list:"
  "02-detail:detail:"
  "03-map:map:"
  "04-favorites:favorites:$SCREENSHOT_STATION_ID"
  "05-info:info:"
)

wait_for_ready_marker() {
  local marker_path="$1"
  local deadline=$((SECONDS + 60))

  while [[ ! -f "$marker_path" ]]; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for $marker_path" >&2
      return 1
    fi
    sleep 1
  done
}

for entry in "${SCREENS[@]}"; do
  IFS=":" read -r name scene favorites <<<"$entry"
  marker_path="$READY_DIR/$name.ready"
  rm -f "$marker_path" "$OUTPUT_DIR/$name.png"

  xcrun simctl terminate "$DEVICE_UDID" "$APP_BUNDLE_ID" >/dev/null 2>&1 || true

  env \
    SIMCTL_CHILD_WOLADEN_SCREENSHOT_MODE=1 \
    SIMCTL_CHILD_WOLADEN_SCREENSHOT_SCENE="$scene" \
    SIMCTL_CHILD_WOLADEN_SCREENSHOT_NAME="$name" \
    SIMCTL_CHILD_WOLADEN_SCREENSHOT_LOCATION="$SCREENSHOT_LOCATION" \
    SIMCTL_CHILD_WOLADEN_SCREENSHOT_STATION_ID="$SCREENSHOT_STATION_ID" \
    SIMCTL_CHILD_WOLADEN_SCREENSHOT_FAVORITES="$favorites" \
    SIMCTL_CHILD_WOLADEN_LIVE_API_BASE_URL="" \
    xcrun simctl launch --terminate-running-process "$DEVICE_UDID" "$APP_BUNDLE_ID" >/dev/null

  wait_for_ready_marker "$marker_path"
  sleep 1
  xcrun simctl io "$DEVICE_UDID" screenshot --type=png "$OUTPUT_DIR/$name.png" >/dev/null
done

xcrun simctl terminate "$DEVICE_UDID" "$APP_BUNDLE_ID" >/dev/null 2>&1 || true
xcrun simctl status_bar "$DEVICE_UDID" clear || true

echo "Saved iPhone App Store screenshots under $OUTPUT_DIR"
