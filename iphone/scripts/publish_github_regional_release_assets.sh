#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
ASSETS_DIR="${1:-$ROOT_DIR/iphone/dist/github-regional-packs}"
FULL_BUNDLE_DIR="${2:-${WOLADEN_OPEN_STATIC_BUNDLE_DIR:-}}"
REPO="${WOLADEN_GITHUB_RELEASE_REPO:-${GITHUB_REPOSITORY:-volzinnovation/Woladen.de-analytics}}"
TAG="${WOLADEN_GITHUB_REGIONAL_RELEASE_TAG:-open-static-ios-regional-latest}"
TITLE="${WOLADEN_GITHUB_REGIONAL_RELEASE_TITLE:-Woladen open-static SQLite bundle and regional iPhone packages}"

usage() {
  cat <<USAGE
Usage: $0 [regional-assets-dir] [full-bundle-dir]

Uploads Woladen open-static release assets to a GitHub Release. The release must
contain both the full SQLite bundle used by analytics/API consumers and the
regional compressed iPhone packages.

Build the regional assets first from the analytics repository, for example:

  cd ../Woladen.de-analytics
  python3 scripts/build_open_static_regional_release_assets.py \\
    --parts-dir data/eu27_ch_static/sqlite_parts \\
    --output-dir ../woladen.de/iphone/dist/github-regional-packs \\
    --github-owner volzinnovation \\
    --github-repo Woladen.de-analytics \\
    --github-release-tag open-static-ios-regional-latest

The full-bundle directory must contain:

  open_static.sqlite3.zst
  open_static.sqlite3.zst.sha256
  open_static.sqlite3.sha256

Environment:
  WOLADEN_GITHUB_RELEASE_REPO            owner/repo, default: ${REPO}
  WOLADEN_GITHUB_REGIONAL_RELEASE_TAG    release tag, default: ${TAG}
  WOLADEN_GITHUB_REGIONAL_RELEASE_TITLE  release title
  WOLADEN_OPEN_STATIC_BUNDLE_DIR         directory with full bundle assets
  GH_TOKEN or GITHUB_TOKEN               GitHub token for gh
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "gh CLI is required" >&2
  exit 1
fi

if [[ ! -d "$ASSETS_DIR" ]]; then
  echo "regional assets directory not found: $ASSETS_DIR" >&2
  exit 1
fi

if [[ -z "$FULL_BUNDLE_DIR" ]]; then
  echo "full bundle directory is required; pass it as the second argument or set WOLADEN_OPEN_STATIC_BUNDLE_DIR" >&2
  exit 1
fi

if [[ ! -d "$FULL_BUNDLE_DIR" ]]; then
  echo "full bundle directory not found: $FULL_BUNDLE_DIR" >&2
  exit 1
fi

assets=()
while IFS= read -r asset; do
  assets+=("$asset")
done < <(
  find "$ASSETS_DIR" -maxdepth 1 -type f \( \
    -name 'open-static-*.sqlite3.zlib' -o \
    -name 'open-static-*.sqlite3.zlib.sha256' -o \
    -name 'open-static-*.sqlite3.sha256' -o \
    -name 'open-static-*.manifest.json' -o \
    -name 'regional_pack_index.json' \
  \) | sort
)

if [[ "${#assets[@]}" -eq 0 ]]; then
  echo "no regional release assets found in $ASSETS_DIR" >&2
  exit 1
fi

full_bundle_assets=(
  "$FULL_BUNDLE_DIR/open_static.sqlite3.zst"
  "$FULL_BUNDLE_DIR/open_static.sqlite3.zst.sha256"
  "$FULL_BUNDLE_DIR/open_static.sqlite3.sha256"
)
for asset in "${full_bundle_assets[@]}"; do
  if [[ ! -f "$asset" ]]; then
    echo "required full bundle release asset not found: $asset" >&2
    exit 1
  fi
  assets+=("$asset")
done

notes_file="$(mktemp)"
trap 'rm -f "$notes_file"' EXIT
{
  echo "Woladen open-static SQLite bundle and regional iPhone packages."
  echo
  echo "- Assets include the full compressed open_static.sqlite3 bundle with checksums."
  echo "- Assets include zlib-compressed regional package SQLite files, manifests, checksums, and regional_pack_index.json."
  echo "- Dynamic availability, derived estimates, and paid insight artifacts are not included."
} > "$notes_file"

if gh release view "$TAG" --repo "$REPO" >/dev/null 2>&1; then
  gh release edit "$TAG" --repo "$REPO" --title "$TITLE" --notes-file "$notes_file" --latest
else
  gh release create "$TAG" --repo "$REPO" --title "$TITLE" --notes-file "$notes_file" --latest
fi

while IFS= read -r stale_asset; do
  if [[ -n "$stale_asset" ]]; then
    gh release delete-asset "$TAG" "$stale_asset" --repo "$REPO" --yes >/dev/null 2>&1 || true
  fi
done < <(
  gh release view "$TAG" --repo "$REPO" --json assets --jq '.assets[].name' \
    | grep -E '^open-static-[A-Z-]+\.sqlite3$' || true
)

gh release upload "$TAG" "${assets[@]}" --repo "$REPO" --clobber
echo "Published ${#assets[@]} regional release assets to $REPO tag=$TAG"
