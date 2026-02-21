#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${1:-$ROOT_DIR/iphone/dist/data-bundle}"

mkdir -p "$OUT_DIR"
cp "$ROOT_DIR/data/chargers_fast.geojson" "$OUT_DIR/chargers_fast.geojson"
cp "$ROOT_DIR/data/operators.json" "$OUT_DIR/operators.json"

GENERATED_AT="$(jq -r '.run.finished_at // .source.fetched_at // "unknown"' "$ROOT_DIR/data/summary.json")"
VERSION="update-$(date -u +%Y%m%dT%H%M%SZ)"

jq -n \
  --arg version "$VERSION" \
  --arg generatedAt "$GENERATED_AT" \
  --arg schema "chargers_fast.geojson+operators.json" \
  '{version:$version,generatedAt:$generatedAt,schema:$schema}' \
  > "$OUT_DIR/data_manifest.json"

echo "Created update bundle in: $OUT_DIR"
