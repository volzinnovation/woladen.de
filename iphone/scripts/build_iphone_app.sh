#!/usr/bin/env bash
set -euo pipefail

IPHONE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ROOT_DIR="$(cd "$IPHONE_DIR/.." && pwd)"
MODE="${1:-archive}"
if [[ $# -gt 0 ]]; then
  shift
fi

SCHEME="${WOLADEN_IOS_SCHEME:-Woladen}"
PROJECT="${WOLADEN_IOS_PROJECT:-Woladen.xcodeproj}"
CONFIGURATION="${WOLADEN_IOS_CONFIGURATION:-Release}"
SIM_DESTINATION="${WOLADEN_IOS_SIM_DESTINATION:-platform=iOS Simulator,name=iPhone 16,OS=18.6}"
ARCHIVE_DIR="${WOLADEN_IOS_ARCHIVE_DIR:-$IPHONE_DIR/build/archive}"
EXPORT_DIR="${WOLADEN_IOS_EXPORT_DIR:-$IPHONE_DIR/build/export}"
ARCHIVE_PATH="${WOLADEN_IOS_ARCHIVE_PATH:-$ARCHIVE_DIR/Woladen-$(date -u +%Y%m%dT%H%M%SZ).xcarchive}"
EXPORT_OPTIONS_PLIST="${WOLADEN_IOS_EXPORT_OPTIONS_PLIST:-$IPHONE_DIR/build/ExportOptions.plist}"
EXPORT_METHOD="${WOLADEN_IOS_EXPORT_METHOD:-app-store-connect}"

usage() {
  cat <<USAGE
Usage: $0 [test|build|archive|export|upload] [extra xcodebuild args...]

Modes:
  test     Generate project/config and run simulator tests.
  build    Build the app for a generic iOS device.
  archive  Archive the app for App Store Connect distribution.
  export   Archive and export an App Store Connect IPA locally.
  upload   Archive and upload to App Store Connect.

Environment:
  DEVELOPMENT_TEAM                    optional Apple Developer Team ID
  WOLADEN_IOS_CONFIGURATION           default: Release
  WOLADEN_IOS_ARCHIVE_PATH            optional explicit .xcarchive path
  APP_STORE_CONNECT_API_KEY_PATH      optional App Store Connect API key path
  APP_STORE_CONNECT_API_KEY_ID        optional App Store Connect API key ID
  APP_STORE_CONNECT_API_ISSUER_ID     optional App Store Connect issuer ID

If no App Store Connect API key variables are set, upload/export relies on the
Apple account configured in Xcode.
USAGE
}

if [[ "$MODE" == "-h" || "$MODE" == "--help" ]]; then
  usage
  exit 0
fi

case "$MODE" in
  test|build|archive|export|upload)
    ;;
  *)
    echo "unknown mode: $MODE" >&2
    usage >&2
    exit 2
    ;;
esac

cd "$IPHONE_DIR"

xcodegen generate

xcode_settings=()
if [[ -n "${DEVELOPMENT_TEAM:-}" ]]; then
  xcode_settings+=("DEVELOPMENT_TEAM=$DEVELOPMENT_TEAM")
fi
xcode_settings+=("CODE_SIGN_STYLE=Automatic")

provisioning_args=()
if [[ "${WOLADEN_ALLOW_PROVISIONING_UPDATES:-1}" == "1" ]]; then
  provisioning_args+=("-allowProvisioningUpdates")
fi

app_store_auth_args=()
api_key_id="${APP_STORE_CONNECT_API_KEY_ID:-${ASC_KEY_ID:-}}"
api_issuer_id="${APP_STORE_CONNECT_API_ISSUER_ID:-${ASC_ISSUER_ID:-}}"
api_key_path="${APP_STORE_CONNECT_API_KEY_PATH:-${ASC_KEY_PATH:-}}"
if [[ -n "$api_key_id" && -z "$api_key_path" && -f "$ROOT_DIR/secret/AuthKey_${api_key_id}.p8" ]]; then
  api_key_path="$ROOT_DIR/secret/AuthKey_${api_key_id}.p8"
fi
if [[ -n "$api_key_id" || -n "$api_issuer_id" || -n "$api_key_path" ]]; then
  if [[ -z "$api_key_id" || -z "$api_issuer_id" || -z "$api_key_path" ]]; then
    echo "APP_STORE_CONNECT_API_KEY_PATH, APP_STORE_CONNECT_API_KEY_ID, and APP_STORE_CONNECT_API_ISSUER_ID must be set together." >&2
    exit 1
  fi
  app_store_auth_args+=(
    "-authenticationKeyPath" "$api_key_path"
    "-authenticationKeyID" "$api_key_id"
    "-authenticationKeyIssuerID" "$api_issuer_id"
  )
fi

write_export_options() {
  local destination="$1"
  mkdir -p "$(dirname "$EXPORT_OPTIONS_PLIST")"
  cat > "$EXPORT_OPTIONS_PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>destination</key>
  <string>$destination</string>
  <key>method</key>
  <string>$EXPORT_METHOD</string>
  <key>signingStyle</key>
  <string>automatic</string>
  <key>stripSwiftSymbols</key>
  <true/>
  <key>uploadSymbols</key>
  <true/>
</dict>
</plist>
PLIST
}

archive_app() {
  mkdir -p "$ARCHIVE_DIR"
  xcodebuild archive \
    -project "$PROJECT" \
    -scheme "$SCHEME" \
    -configuration "$CONFIGURATION" \
    -destination "generic/platform=iOS" \
    -archivePath "$ARCHIVE_PATH" \
    "${provisioning_args[@]}" \
    "${xcode_settings[@]}" \
    "$@"
}

case "$MODE" in
  test)
    xcodebuild test \
      -project "$PROJECT" \
      -scheme "$SCHEME" \
      -destination "$SIM_DESTINATION" \
      CODE_SIGNING_ALLOWED=NO \
      "$@"
    ;;
  build)
    xcodebuild build \
      -project "$PROJECT" \
      -scheme "$SCHEME" \
      -configuration "$CONFIGURATION" \
      -destination "generic/platform=iOS" \
      "${provisioning_args[@]}" \
      "${xcode_settings[@]}" \
      "$@"
    ;;
  archive)
    archive_app "$@"
    echo "Archive written to $ARCHIVE_PATH"
    ;;
  export)
    archive_app "$@"
    mkdir -p "$EXPORT_DIR"
    write_export_options "export"
    xcodebuild -exportArchive \
      -archivePath "$ARCHIVE_PATH" \
      -exportPath "$EXPORT_DIR" \
      -exportOptionsPlist "$EXPORT_OPTIONS_PLIST" \
      "${provisioning_args[@]}" \
      "${app_store_auth_args[@]}"
    echo "Export written to $EXPORT_DIR"
    ;;
  upload)
    archive_app "$@"
    mkdir -p "$EXPORT_DIR"
    write_export_options "upload"
    xcodebuild -exportArchive \
      -archivePath "$ARCHIVE_PATH" \
      -exportPath "$EXPORT_DIR" \
      -exportOptionsPlist "$EXPORT_OPTIONS_PLIST" \
      "${provisioning_args[@]}" \
      "${app_store_auth_args[@]}"
    echo "Uploaded archive $ARCHIVE_PATH to App Store Connect"
    ;;
esac
