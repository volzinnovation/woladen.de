#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUT_DIR="${1:-$ROOT_DIR/iphone/dist/data-bundle}"
DEFAULT_SQLITE="$ROOT_DIR/data/eu27_ch_static/open_static.sqlite3"
DEFAULT_COMPRESSED="$ROOT_DIR/data/eu27_ch_static/open_static.sqlite3.zst"
SQLITE_SOURCE="${WOLADEN_OPEN_STATIC_SQLITE_PATH:-$DEFAULT_SQLITE}"
COMPRESSED_SOURCE="${WOLADEN_OPEN_STATIC_SQLITE_ZST_PATH:-$DEFAULT_COMPRESSED}"
DEST_SQLITE="$OUT_DIR/open_static.sqlite3"
PUBLICATION_RECEIPT="$OUT_DIR/open_static_publication_receipt.json"

mkdir -p "$OUT_DIR"
rm -f "$PUBLICATION_RECEIPT"

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
  RAW_PUBLICATION_RECEIPT="$(mktemp "$OUT_DIR/.open-static-publication-receipt.XXXXXX")"
  trap 'rm -f "$RAW_PUBLICATION_RECEIPT"' EXIT
  python3 "$ROOT_DIR/scripts/download_latest_open_static_release.py" \
    --output-path "$DEST_SQLITE" \
    --keep-compressed \
    --require-checksum \
    > "$RAW_PUBLICATION_RECEIPT"
  jq -e '
    . as $receipt
    | if $receipt.source == "hf_immutable_release"
        and $receipt.checksum_verified == true
        and $receipt.download_checksum_verified == true
        and $receipt.decompressed_checksum_verified == true
        and $receipt.manifest_verified == true
        and ($receipt.source_commit | test("^[0-9a-f]{40}$"))
        and ($receipt.hf_stable_revision | test("^[0-9a-f]{40}([0-9a-f]{24})?$"))
        and ($receipt.hf_immutable_revision | test("^[0-9a-f]{40}([0-9a-f]{24})?$"))
        and ($receipt.release_tag == ($receipt.hf_stable_alias + "-" + $receipt.source_commit))
        and ($receipt.asset_name == "open_static.sqlite3.zst")
        and ($receipt.size_bytes | type == "number" and . > 0 and floor == .)
      then {
        schema_version: "woladen-open-static-client-publication-receipt-v1",
        source: $receipt.source,
        source_repository: $receipt.repo,
        release_tag: $receipt.release_tag,
        source_commit: $receipt.source_commit,
        hf_repo: $receipt.hf_repo,
        hf_prefix: $receipt.hf_prefix,
        hf_stable_alias: $receipt.hf_stable_alias,
        hf_stable_revision: $receipt.hf_stable_revision,
        hf_immutable_revision: $receipt.hf_immutable_revision,
        asset_name: $receipt.asset_name,
        size_bytes: $receipt.size_bytes,
        checksum_verified: $receipt.checksum_verified,
        download_checksum_verified: $receipt.download_checksum_verified,
        decompressed_checksum_verified: $receipt.decompressed_checksum_verified,
        manifest_verified: $receipt.manifest_verified
      }
      else error("open_static_publication_receipt_invalid")
      end
  ' "$RAW_PUBLICATION_RECEIPT" > "$PUBLICATION_RECEIPT"
  rm -f "$RAW_PUBLICATION_RECEIPT"
  trap - EXIT
fi

GENERATED_AT="$(sqlite3 "$DEST_SQLITE" "select json_extract(json_value, '$.generated_at') from bundle_metadata where key='build' limit 1;" 2>/dev/null || true)"
GENERATED_AT="${GENERATED_AT:-unknown}"
STATION_COUNT="$(sqlite3 "$DEST_SQLITE" "select count(*) from stations;" 2>/dev/null || echo 0)"
CHARGER_COUNT="$(sqlite3 "$DEST_SQLITE" "select count(*) from chargers;" 2>/dev/null || echo 0)"
COUNTRIES_JSON="$(sqlite3 -json "$DEST_SQLITE" "select distinct country_code from stations order by country_code;" | jq '[.[].country_code]')"
VERSION="open-static-update-$(date -u +%Y%m%dT%H%M%SZ)"
PUBLICATION_JSON="null"
if [[ -f "$PUBLICATION_RECEIPT" ]]; then
  VERSION="$(jq -er '.release_tag' "$PUBLICATION_RECEIPT")"
  PUBLICATION_JSON="$(jq -c '.' "$PUBLICATION_RECEIPT")"
fi

jq -n \
  --arg version "$VERSION" \
  --arg generatedAt "$GENERATED_AT" \
  --arg schema "open_static.sqlite3" \
  --argjson stationCount "$STATION_COUNT" \
  --argjson chargerCount "$CHARGER_COUNT" \
  --argjson countries "$COUNTRIES_JSON" \
  --argjson publication "$PUBLICATION_JSON" \
  '{version:$version,generatedAt:$generatedAt,schema:$schema,stationCount:$stationCount,chargerCount:$chargerCount,countries:$countries,publication:$publication}' \
  > "$OUT_DIR/data_manifest.json"

echo "Created open-static SQLite update bundle in: $OUT_DIR"
