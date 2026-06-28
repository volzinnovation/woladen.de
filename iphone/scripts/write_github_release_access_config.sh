#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUTPUT_PATH="$ROOT_DIR/iphone/Woladen/Resources/ReleaseAccess/GitHubReleaseAccess.json"
TOKEN_FILE="${WOLADEN_GITHUB_RELEASE_TOKEN_FILE:-$ROOT_DIR/secret/gh_iphone_download_token.txt}"
GITHUB_OWNER="${WOLADEN_GITHUB_OWNER:-volzinnovation}"
GITHUB_REPO="${WOLADEN_GITHUB_REPO:-Woladen.de-analytics}"
RELEASE_TAG="${WOLADEN_GITHUB_RELEASE_TAG:-open-static-ios-regional-latest}"
REQUIRE_TOKEN="${WOLADEN_REQUIRE_GITHUB_RELEASE_TOKEN:-0}"

usage() {
  cat <<USAGE
Usage: $0 [--require] [--token-file PATH] [--output PATH]

Generates the ignored bundled GitHub release access config used by the iPhone
app. The token value is read from secret/gh_iphone_download_token.txt by default.

Environment:
  WOLADEN_GITHUB_RELEASE_TOKEN_FILE   token file path
  WOLADEN_GITHUB_OWNER                GitHub owner, default: $GITHUB_OWNER
  WOLADEN_GITHUB_REPO                 GitHub repo, default: $GITHUB_REPO
  WOLADEN_GITHUB_RELEASE_TAG          release tag, default: $RELEASE_TAG
  WOLADEN_REQUIRE_GITHUB_RELEASE_TOKEN=1  fail if token file is missing
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --require)
      REQUIRE_TOKEN=1
      shift
      ;;
    --token-file)
      TOKEN_FILE="$2"
      shift 2
      ;;
    --output)
      OUTPUT_PATH="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

release_like_build=0
if [[ "${CONFIGURATION:-}" == "Release" || "${ACTION:-}" == "install" ]]; then
  release_like_build=1
fi

if [[ ! -f "$TOKEN_FILE" ]]; then
  if [[ "$REQUIRE_TOKEN" == "1" || "$release_like_build" == "1" ]]; then
    echo "GitHub release download token file missing: $TOKEN_FILE" >&2
    exit 1
  fi
  rm -f "$OUTPUT_PATH"
  echo "GitHub release download token file missing; continuing without bundled release access config for this build." >&2
  exit 0
fi

token="$(LC_ALL=C tr -d '[:space:]' < "$TOKEN_FILE")"
if [[ -z "$token" ]]; then
  echo "GitHub release download token file is empty: $TOKEN_FILE" >&2
  exit 1
fi

json_escape() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

mkdir -p "$(dirname "$OUTPUT_PATH")"
tmp="$(mktemp "$(dirname "$OUTPUT_PATH")/GitHubReleaseAccess.json.XXXXXX")"
chmod 600 "$tmp"

{
  printf '{\n'
  printf '  "github_owner": "%s",\n' "$(json_escape "$GITHUB_OWNER")"
  printf '  "github_repo": "%s",\n' "$(json_escape "$GITHUB_REPO")"
  printf '  "release_tag": "%s",\n' "$(json_escape "$RELEASE_TAG")"
  printf '  "release_read_token": "%s"\n' "$(json_escape "$token")"
  printf '}\n'
} > "$tmp"

mv "$tmp" "$OUTPUT_PATH"
chmod 600 "$OUTPUT_PATH"
echo "Generated GitHub release access config at $OUTPUT_PATH from $TOKEN_FILE"
