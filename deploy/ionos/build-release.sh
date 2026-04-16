#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
OUT_DIR=${1:-"$ROOT_DIR/tmp/ionos-release"}
STAMP=$(date -u +"%Y%m%dT%H%M%SZ")
GIT_SHA=$(git -C "$ROOT_DIR" rev-parse --short HEAD 2>/dev/null || echo "working")
BUNDLE_NAME="woladen-live-backend-${STAMP}-${GIT_SHA}"
BUNDLE_DIR="$OUT_DIR/$BUNDLE_NAME"
ARCHIVE_PATH="$OUT_DIR/${BUNDLE_NAME}.tar.gz"

rm -rf "$BUNDLE_DIR"
mkdir -p \
  "$BUNDLE_DIR/backend" \
  "$BUNDLE_DIR/data" \
  "$BUNDLE_DIR/deploy/ionos" \
  "$BUNDLE_DIR/docs" \
  "$BUNDLE_DIR/scripts"

rsync -a --exclude "__pycache__/" "$ROOT_DIR/backend/" "$BUNDLE_DIR/backend/"
cp "$ROOT_DIR/data/chargers_fast.csv" "$BUNDLE_DIR/data/"
cp "$ROOT_DIR/data/chargers_fast.geojson" "$BUNDLE_DIR/data/"
cp "$ROOT_DIR/data/mobilithek_afir_provider_configs.json" "$BUNDLE_DIR/data/"
cp "$ROOT_DIR/data/mobilithek_afir_static_matches.csv" "$BUNDLE_DIR/data/"
if [[ -f "$ROOT_DIR/data/live_provider_overrides.json" ]]; then
  cp "$ROOT_DIR/data/live_provider_overrides.json" "$BUNDLE_DIR/data/"
fi
cp "$ROOT_DIR/deploy/ionos/"* "$BUNDLE_DIR/deploy/ionos/"
cp "$ROOT_DIR/docs/live-api-mvp.md" "$BUNDLE_DIR/docs/"
cp "$ROOT_DIR/requirements-live.txt" "$BUNDLE_DIR/"
cp "$ROOT_DIR/scripts/live_api.py" "$BUNDLE_DIR/scripts/"
cp "$ROOT_DIR/scripts/live_deploy_plan.py" "$BUNDLE_DIR/scripts/"
cp "$ROOT_DIR/scripts/live_archive_logs.py" "$BUNDLE_DIR/scripts/"
cp "$ROOT_DIR/scripts/live_ingester.py" "$BUNDLE_DIR/scripts/"
cp "$ROOT_DIR/scripts/live_subscription_registry.py" "$BUNDLE_DIR/scripts/"
cp "$ROOT_DIR/scripts/sync_mobilithek_subscriptions.py" "$BUNDLE_DIR/scripts/"
cp "$ROOT_DIR/LICENSE" "$BUNDLE_DIR/"

cat >"$BUNDLE_DIR/release.json" <<EOF
{
  "built_at_utc": "$STAMP",
  "git_sha": "$GIT_SHA",
  "bundle_name": "$BUNDLE_NAME",
  "live_domain": "live.woladen.de"
}
EOF

mkdir -p "$OUT_DIR"
tar -czf "$ARCHIVE_PATH" -C "$OUT_DIR" "$BUNDLE_NAME"

echo "$ARCHIVE_PATH"
