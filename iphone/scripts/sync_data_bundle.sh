#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
BASELINE_DIR="$ROOT_DIR/iphone/Woladen/Resources/Data/baseline"
DEFAULT_SQLITE="$ROOT_DIR/data/eu27_ch_static/open_static.sqlite3"
DEFAULT_COMPRESSED="$ROOT_DIR/data/eu27_ch_static/open_static.sqlite3.zst"
SQLITE_SOURCE="${WOLADEN_OPEN_STATIC_SQLITE_PATH:-$DEFAULT_SQLITE}"
COMPRESSED_SOURCE="${WOLADEN_OPEN_STATIC_SQLITE_ZST_PATH:-$DEFAULT_COMPRESSED}"
DEST_SQLITE="$BASELINE_DIR/open_static.sqlite3"
PUBLICATION_RECEIPT="$BASELINE_DIR/open_static_publication_receipt.json"
INCLUDE_BASELINE_SQLITE="${WOLADEN_INCLUDE_BASELINE_SQLITE:-0}"

mkdir -p "$BASELINE_DIR"

if [[ "$INCLUDE_BASELINE_SQLITE" != "1" ]]; then
  rm -f "$DEST_SQLITE" "$BASELINE_DIR/open_static.sqlite3.zst" \
    "$BASELINE_DIR/open_static.sqlite3.sha256" \
    "$BASELINE_DIR/open_static.sqlite3.zst.sha256" \
    "$PUBLICATION_RECEIPT"

  VERSION="github-release-regional-packs-$(date -u +%Y%m%dT%H%M%SZ)"
  jq -n \
    --arg version "$VERSION" \
    --arg generatedAt "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg schema "open_static.regional-packs.sqlite3" \
    --argjson countries "$(printf '%s\n' AT BE CH CY CZ DE ES FI FR GR HU LT LU LV MT NL NO PL PT SE SI | jq -R . | jq -s .)" \
    '{version:$version,generatedAt:$generatedAt,schema:$schema,stationCount:null,chargerCount:null,countries:$countries}' \
    > "$BASELINE_DIR/data_manifest.json"

  echo "Prepared small iPhone bootstrap manifest in: $BASELINE_DIR"
  echo "Set WOLADEN_INCLUDE_BASELINE_SQLITE=1 only for local debug builds that intentionally bundle the full SQLite."
  exit 0
fi

rm -f "$PUBLICATION_RECEIPT"

if [[ -f "$SQLITE_SOURCE" ]]; then
  cp "$SQLITE_SOURCE" "$DEST_SQLITE"
  if [[ "${WOLADEN_INCLUDE_COMPRESSED_BASELINE:-0}" == "1" && -f "$COMPRESSED_SOURCE" ]]; then
    cp "$COMPRESSED_SOURCE" "$BASELINE_DIR/open_static.sqlite3.zst"
  fi
elif [[ -f "$COMPRESSED_SOURCE" ]]; then
  if ! command -v zstd >/dev/null 2>&1; then
    echo "zstd is required to expand $COMPRESSED_SOURCE" >&2
    exit 1
  fi
  zstd -d -f -c "$COMPRESSED_SOURCE" > "$DEST_SQLITE"
  if [[ "${WOLADEN_INCLUDE_COMPRESSED_BASELINE:-0}" == "1" ]]; then
    cp "$COMPRESSED_SOURCE" "$BASELINE_DIR/open_static.sqlite3.zst"
  fi
else
  RAW_PUBLICATION_RECEIPT="$(mktemp "$BASELINE_DIR/.open-static-publication-receipt.XXXXXX")"
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
VERSION="baseline-open-static-$(date -u +%Y%m%dT%H%M%SZ)"
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
  > "$BASELINE_DIR/data_manifest.json"

echo "Synced iPhone open-static SQLite baseline to: $BASELINE_DIR"
