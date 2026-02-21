#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BASELINE_DIR="$ROOT_DIR/iphone/Woladen/Resources/Data/baseline"

mkdir -p "$BASELINE_DIR"
cp "$ROOT_DIR/data/chargers_fast.geojson" "$BASELINE_DIR/chargers_fast.geojson"
cp "$ROOT_DIR/data/operators.json" "$BASELINE_DIR/operators.json"

GENERATED_AT="$(jq -r '.run.finished_at // .source.fetched_at // "unknown"' "$ROOT_DIR/data/summary.json")"
VERSION="baseline-$(date -u +%Y%m%dT%H%M%SZ)"

jq -n \
  --arg version "$VERSION" \
  --arg generatedAt "$GENERATED_AT" \
  --arg schema "chargers_fast.geojson+operators.json" \
  '{version:$version,generatedAt:$generatedAt,schema:$schema}' \
  > "$BASELINE_DIR/data_manifest.json"

echo "Synced iPhone baseline data bundle to: $BASELINE_DIR"
