#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${1:-$ROOT_DIR/iphone/dist/data-bundle}"
DEFAULT_SQLITE="$ROOT_DIR/data/eu27_ch_static/open_static.sqlite3"
DEFAULT_COMPRESSED="$ROOT_DIR/data/eu27_ch_static/open_static.sqlite3.zst"
SQLITE_SOURCE="${WOLADEN_OPEN_STATIC_SQLITE_PATH:-$DEFAULT_SQLITE}"
COMPRESSED_SOURCE="${WOLADEN_OPEN_STATIC_SQLITE_ZST_PATH:-$DEFAULT_COMPRESSED}"
DEST_SQLITE="$OUT_DIR/open_static.sqlite3"

mkdir -p "$OUT_DIR"

if [[ -f "$SQLITE_SOURCE" ]]; then
  cp "$SQLITE_SOURCE" "$DEST_SQLITE"
  if [[ -f "$COMPRESSED_SOURCE" ]]; then
    cp "$COMPRESSED_SOURCE" "$OUT_DIR/open_static.sqlite3.zst"
  fi
elif [[ -f "$COMPRESSED_SOURCE" ]]; then
  if ! command -v zstd >/dev/null 2>&1; then
    echo "zstd is required to expand $COMPRESSED_SOURCE" >&2
    exit 1
  fi
  zstd -d -f -c "$COMPRESSED_SOURCE" > "$DEST_SQLITE"
  cp "$COMPRESSED_SOURCE" "$OUT_DIR/open_static.sqlite3.zst"
else
  python3 "$ROOT_DIR/scripts/download_latest_open_static_release.py" \
    --output-path "$DEST_SQLITE" \
    --keep-compressed \
    --require-checksum
fi

GENERATED_AT="$(sqlite3 "$DEST_SQLITE" "select json_extract(json_value, '$.generated_at') from bundle_metadata where key='build' limit 1;" 2>/dev/null || true)"
GENERATED_AT="${GENERATED_AT:-unknown}"
STATION_COUNT="$(sqlite3 "$DEST_SQLITE" "select count(*) from stations;" 2>/dev/null || echo 0)"
CHARGER_COUNT="$(sqlite3 "$DEST_SQLITE" "select count(*) from chargers;" 2>/dev/null || echo 0)"
COUNTRIES_JSON="$(sqlite3 -json "$DEST_SQLITE" "select distinct country_code from stations order by country_code;" | jq '[.[].country_code]')"
VERSION="open-static-update-$(date -u +%Y%m%dT%H%M%SZ)"

jq -n \
  --arg version "$VERSION" \
  --arg generatedAt "$GENERATED_AT" \
  --arg schema "open_static.sqlite3" \
  --argjson stationCount "$STATION_COUNT" \
  --argjson chargerCount "$CHARGER_COUNT" \
  --argjson countries "$COUNTRIES_JSON" \
  '{version:$version,generatedAt:$generatedAt,schema:$schema,stationCount:$stationCount,chargerCount:$chargerCount,countries:$countries}' \
  > "$OUT_DIR/data_manifest.json"

echo "Created open-static SQLite update bundle in: $OUT_DIR"
