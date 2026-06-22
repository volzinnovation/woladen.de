#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
IPHONE_DIR="$ROOT_DIR/iphone"
DERIVED_DATA_DIR="$ROOT_DIR/tmp/ios-app-store-screenshots-derived-data"
OUTPUT_ROOT="$ROOT_DIR/output/app-store/ios"
APP_BUNDLE_ID="de.woladen.ios"
APP_SCHEME="Woladen"
SCREENSHOT_LOCATION="${SCREENSHOT_LOCATION:-53.554808,10.009998}"
SCREENSHOT_STATION_ID="${SCREENSHOT_STATION_ID:-DE:fb23ac5910c5e002}"
DEFAULT_SCREENSHOT_FAVORITES="$SCREENSHOT_STATION_ID,DE:f7b16ecb8c2cd608,DE:f694335d99a838f6,DE:dedcfd246d7ad4ac,DE:fadd10a6403a2cbe,DE:ba60752547e033b4"
SCREENSHOT_FAVORITES="${SCREENSHOT_FAVORITES:-$DEFAULT_SCREENSHOT_FAVORITES}"
SCREENSHOT_PROFILES="${SCREENSHOT_PROFILES:-iphone-6.9,ipad-13}"

declare -a SCREENS=(
  "01-list:list:"
  "02-detail:detail:"
  "03-map:map:"
  "04-favorites:favorites:$SCREENSHOT_FAVORITES"
  "05-info:info:"
)

find_device_udid() {
  local explicit_udid="$1"
  local explicit_name="$2"
  shift 2
  local devices
  local candidate
  local udid

  if [[ -n "$explicit_udid" ]]; then
    printf '%s\n' "$explicit_udid"
    return 0
  fi

  devices="$(xcrun simctl list devices available)"

  if [[ -n "$explicit_name" ]]; then
    udid="$(printf '%s\n' "$devices" | grep -F "$explicit_name" | head -1 | sed -E 's/.*\(([A-F0-9-]+)\).*/\1/')"
    if [[ -n "$udid" ]]; then
      printf '%s\n' "$udid"
      return 0
    fi
  fi

  for candidate in "$@"; do
    [[ -n "$candidate" ]] || continue
    udid="$(printf '%s\n' "$devices" | grep -F "$candidate" | head -1 | sed -E 's/.*\(([A-F0-9-]+)\).*/\1/')"
    if [[ -n "$udid" ]]; then
      printf '%s\n' "$udid"
      return 0
    fi
  done

  return 1
}

wait_for_ready_marker() {
  local marker_path="$1"
  local deadline=$((SECONDS + 90))

  while [[ ! -f "$marker_path" ]]; do
    if (( SECONDS >= deadline )); then
      echo "Timed out waiting for $marker_path" >&2
      return 1
    fi
    sleep 1
  done
}

build_app() {
  local device_udid="$1"

  xcodebuild \
    -quiet \
    -project "$IPHONE_DIR/Woladen.xcodeproj" \
    -scheme "$APP_SCHEME" \
    -configuration Debug \
    -sdk iphonesimulator \
    -destination "id=$device_udid" \
    -derivedDataPath "$DERIVED_DATA_DIR" \
    build
}

prepare_simulator() {
  local device_udid="$1"

  open -a Simulator --args -CurrentDeviceUDID "$device_udid" >/dev/null 2>&1 || true
  xcrun simctl boot "$device_udid" >/dev/null 2>&1 || true
  xcrun simctl bootstatus "$device_udid" -b
  xcrun simctl ui "$device_udid" appearance light
  xcrun simctl ui "$device_udid" content_size medium || true
  xcrun simctl status_bar "$device_udid" clear || true
  xcrun simctl status_bar "$device_udid" override \
    --time 9:41 \
    --dataNetwork wifi \
    --wifiMode active \
    --wifiBars 3 \
    --batteryState charged \
    --batteryLevel 100 \
    --operatorName "" || true
}

install_app() {
  local device_udid="$1"
  local app_path="$DERIVED_DATA_DIR/Build/Products/Debug-iphonesimulator/Woladen.app"

  if [[ ! -d "$app_path" ]]; then
    echo "Built app not found at $app_path" >&2
    exit 1
  fi

  xcrun simctl terminate "$device_udid" "$APP_BUNDLE_ID" >/dev/null 2>&1 || true
  xcrun simctl uninstall "$device_udid" "$APP_BUNDLE_ID" >/dev/null 2>&1 || true
  xcrun simctl install "$device_udid" "$app_path"
}

capture_profile() {
  local profile="$1"
  local output_dir="$2"
  local explicit_udid="$3"
  local explicit_name="$4"
  shift 4
  local device_udid
  local data_container
  local ready_dir
  local entry
  local name
  local scene
  local favorites
  local marker_path

  mkdir -p "$output_dir"

  if ! device_udid="$(find_device_udid "$explicit_udid" "$explicit_name" "$@")"; then
    echo "Unable to find simulator for $profile. Set SIMULATOR_UDID/SIMULATOR_NAME or the profile-specific IPHONE_SIMULATOR_* / IPAD_SIMULATOR_* override." >&2
    exit 1
  fi

  echo "Generating $profile screenshots on simulator $device_udid"
  prepare_simulator "$device_udid"
  build_app "$device_udid"
  install_app "$device_udid"

  data_container="$(xcrun simctl get_app_container "$device_udid" "$APP_BUNDLE_ID" data)"
  ready_dir="$data_container/Documents/app-store-screenshots"
  mkdir -p "$ready_dir"

  for entry in "${SCREENS[@]}"; do
    IFS=":" read -r name scene favorites <<<"$entry"
    marker_path="$ready_dir/$name.ready"
    rm -f "$marker_path" "$output_dir/$name.png"

    xcrun simctl terminate "$device_udid" "$APP_BUNDLE_ID" >/dev/null 2>&1 || true

    env \
      SIMCTL_CHILD_WOLADEN_SCREENSHOT_MODE=1 \
      SIMCTL_CHILD_WOLADEN_SCREENSHOT_SCENE="$scene" \
      SIMCTL_CHILD_WOLADEN_SCREENSHOT_NAME="$name" \
      SIMCTL_CHILD_WOLADEN_SCREENSHOT_LOCATION="$SCREENSHOT_LOCATION" \
      SIMCTL_CHILD_WOLADEN_SCREENSHOT_STATION_ID="$SCREENSHOT_STATION_ID" \
      SIMCTL_CHILD_WOLADEN_SCREENSHOT_FAVORITES="$favorites" \
      xcrun simctl launch --terminate-running-process "$device_udid" "$APP_BUNDLE_ID" >/dev/null

    wait_for_ready_marker "$marker_path"
    sleep 1
    xcrun simctl io "$device_udid" screenshot --type=png "$output_dir/$name.png" >/dev/null
  done

  xcrun simctl terminate "$device_udid" "$APP_BUNDLE_ID" >/dev/null 2>&1 || true
  xcrun simctl status_bar "$device_udid" clear || true
}

run_profile() {
  local profile="$1"

  case "$profile" in
    iphone-6.9)
      capture_profile \
        "$profile" \
        "$OUTPUT_ROOT/6.9-inch" \
        "${IPHONE_SIMULATOR_UDID:-${SIMULATOR_UDID:-}}" \
        "${IPHONE_SIMULATOR_NAME:-${SIMULATOR_NAME:-}}" \
        "iPhone 16 Pro Max" \
        "iPhone 17 Pro Max"
      ;;
    iphone-6.5)
      capture_profile \
        "$profile" \
        "$OUTPUT_ROOT/6.5-inch" \
        "${IPHONE_SIMULATOR_UDID:-${SIMULATOR_UDID:-}}" \
        "${IPHONE_SIMULATOR_NAME:-${SIMULATOR_NAME:-}}" \
        "6,5\" Device" \
        "6,5\" Decive" \
        "iPhone 16 Plus" \
        "iPhone 15 Plus" \
        "iPhone 14 Plus" \
        "iPhone 13 Pro Max" \
        "iPhone 12 Pro Max" \
        "iPhone 11 Pro Max" \
        "iPhone Xs Max"
      ;;
    ipad-13)
      capture_profile \
        "$profile" \
        "$OUTPUT_ROOT/13-inch-ipad" \
        "${IPAD_SIMULATOR_UDID:-${SIMULATOR_UDID:-}}" \
        "${IPAD_SIMULATOR_NAME:-${SIMULATOR_NAME:-}}" \
        "iPad Pro 13-inch (M4)" \
        "iPad Pro 13-inch (M5)" \
        "iPad Air 13-inch (M4)" \
        "iPad Air 13-inch (M3)"
      ;;
    *)
      echo "Unknown screenshot profile: $profile" >&2
      echo "Supported profiles: iphone-6.9, iphone-6.5, ipad-13" >&2
      exit 1
      ;;
  esac
}

IFS="," read -r -a requested_profiles <<<"$SCREENSHOT_PROFILES"
for profile in "${requested_profiles[@]}"; do
  profile="${profile#"${profile%%[![:space:]]*}"}"
  profile="${profile%"${profile##*[![:space:]]}"}"
  [[ -n "$profile" ]] || continue
  run_profile "$profile"
done

echo "Saved Apple App Store screenshots under $OUTPUT_ROOT"
