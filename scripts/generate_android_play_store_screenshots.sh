#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ANDROID_DIR="$ROOT_DIR/android"
OUTPUT_DIR="$ROOT_DIR/output/play-store/android"
TEST_CLASS="de.woladen.android.PlayStoreScreenshotTest"
RUNNER="de.woladen.android.test/androidx.test.runner.AndroidJUnitRunner"

mkdir -p "$OUTPUT_DIR"

pushd "$ANDROID_DIR" >/dev/null
./gradlew :app:installDebug :app:installDebugAndroidTest
popd >/dev/null

PHONE_SERIAL=""
TABLET_SERIAL=""

detect_connected_emulators() {
  PHONE_SERIAL=""
  TABLET_SERIAL=""

  while read -r serial state _; do
    [[ "$serial" =~ ^emulator- ]] || continue
    [[ "$state" == "device" ]] || continue

    avd_name="$(adb -s "$serial" emu avd name | sed -n '1p' | tr -d '\r')"
    smallest_edge="$(adb -s "$serial" shell wm size | tr -d '\r' | sed -E 's/.*: ([0-9]+)x([0-9]+)/\1 \2/' | awk '{print ($1 < $2) ? $1 : $2}')"

    if [[ "$avd_name" == *Tablet* ]] || [[ "${smallest_edge:-0}" -ge 1400 ]]; then
      TABLET_SERIAL="$serial"
    else
      PHONE_SERIAL="$serial"
    fi
  done <<< "$(adb devices)"
}

for _ in $(seq 1 30); do
  detect_connected_emulators
  if [[ -n "$PHONE_SERIAL" && -n "$TABLET_SERIAL" ]]; then
    break
  fi
  sleep 2
done

declare -a PROFILES=(
  "phone-portrait:0"
  "tablet-landscape:3"
)

for entry in "${PROFILES[@]}"; do
  IFS=":" read -r profile rotation <<<"$entry"
  if [[ "$profile" == "phone-portrait" ]]; then
    serial="$PHONE_SERIAL"
  else
    serial="$TABLET_SERIAL"
  fi
  if [[ -z "$serial" ]]; then
    echo "Missing connected emulator for profile $profile" >&2
    exit 1
  fi
  target_dir="$OUTPUT_DIR/$profile"

  mkdir -p "$target_dir"

  adb -s "$serial" shell settings put system accelerometer_rotation 0
  adb -s "$serial" shell settings put system user_rotation "$rotation"
  sleep 2
  adb -s "$serial" emu geo fix 13.4050 52.5200
  adb -s "$serial" shell pm clear de.woladen.android >/dev/null || true
  adb -s "$serial" shell am instrument -w -e class "$TEST_CLASS" "$RUNNER"

  for name in 01-list 02-detail 03-map 04-favorites 05-info; do
    adb -s "$serial" pull \
      "/sdcard/Download/play-store-screenshots/$profile/$name.png" "$target_dir/$name.png" >/dev/null
  done
done

echo "Saved Android Play Store screenshots under $OUTPUT_DIR"
