#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
OUTPUT_PATH="$ROOT_DIR/iphone/Woladen/Resources/ReleaseAccess/GitHubReleaseAccess.json"
TOKEN_FILE="${WOLADEN_HF_RELEASE_TOKEN_FILE:-$ROOT_DIR/secret/hf_iphone_download_token.txt}"
HF_REPO_ID="${WOLADEN_OPEN_STATIC_HF_REPO:-loffenauer/AFIR}"
HF_PREFIX="${WOLADEN_OPEN_STATIC_HF_PREFIX:-AFIR/open-static/releases}"
HF_STABLE_ALIAS="${WOLADEN_OPEN_STATIC_HF_STABLE_ALIAS:-open-static-ios-regional-latest}"
SOURCE_REPOSITORY="${WOLADEN_OPEN_STATIC_RELEASE_REPO:-volzinnovation/Woladen.de-analytics}"
REQUIRE_TOKEN="${WOLADEN_REQUIRE_HF_RELEASE_TOKEN:-0}"

usage() {
  cat <<USAGE
Usage: $0 [--require] [--token-file PATH] [--output PATH]

Generates the ignored open-static release access config used by iPhone data
tooling. The legacy file name is retained for build compatibility, but this
config discovers only the analytics-published Hugging Face stable channel.

Environment:
  WOLADEN_HF_RELEASE_TOKEN_FILE        optional HF read-token file
  WOLADEN_OPEN_STATIC_HF_REPO          HF dataset repo, default: $HF_REPO_ID
  WOLADEN_OPEN_STATIC_HF_PREFIX        mirror prefix, default: $HF_PREFIX
  WOLADEN_OPEN_STATIC_HF_STABLE_ALIAS  stable channel, default: $HF_STABLE_ALIAS
  WOLADEN_OPEN_STATIC_RELEASE_REPO     expected source repo, default: $SOURCE_REPOSITORY
  WOLADEN_REQUIRE_HF_RELEASE_TOKEN=1   fail if token file is missing
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

if ! [[ "$HF_REPO_ID" =~ ^[^/[:space:]]+/[^/[:space:]]+$ ]]; then
  echo "Hugging Face repo must use owner/name form: $HF_REPO_ID" >&2
  exit 1
fi
if ! [[ "$SOURCE_REPOSITORY" =~ ^[^/[:space:]]+/[^/[:space:]]+$ ]]; then
  echo "Source repository must use owner/name form: $SOURCE_REPOSITORY" >&2
  exit 1
fi
if [[ -z "$HF_PREFIX" || "$HF_PREFIX" == /* || "$HF_PREFIX" == */ || "$HF_PREFIX" == *".."* ]]; then
  echo "Invalid Hugging Face release prefix: $HF_PREFIX" >&2
  exit 1
fi
if ! [[ "$HF_STABLE_ALIAS" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || [[ "$HF_STABLE_ALIAS" == "latest" ]]; then
  echo "Invalid Hugging Face stable alias: $HF_STABLE_ALIAS" >&2
  exit 1
fi

token=""
if [[ ! -f "$TOKEN_FILE" ]]; then
  if [[ "$REQUIRE_TOKEN" == "1" ]]; then
    echo "Hugging Face release download token file missing: $TOKEN_FILE" >&2
    exit 1
  fi
  echo "Hugging Face token file missing; generating public-read release config." >&2
else
  token="$(LC_ALL=C tr -d '[:space:]' < "$TOKEN_FILE")"
  if [[ -z "$token" ]]; then
    echo "Hugging Face release download token file is empty: $TOKEN_FILE" >&2
    exit 1
  fi
fi

json_escape() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

mkdir -p "$(dirname "$OUTPUT_PATH")"
tmp="$(mktemp "$(dirname "$OUTPUT_PATH")/GitHubReleaseAccess.json.XXXXXX")"
chmod 600 "$tmp"

{
  printf '{\n'
  printf '  "schema_version": "woladen-open-static-release-access-v1",\n'
  printf '  "hf_repo_id": "%s",\n' "$(json_escape "$HF_REPO_ID")"
  printf '  "hf_repo_type": "dataset",\n'
  printf '  "hf_prefix": "%s",\n' "$(json_escape "$HF_PREFIX")"
  printf '  "hf_stable_alias": "%s",\n' "$(json_escape "$HF_STABLE_ALIAS")"
  printf '  "source_repository": "%s"' "$(json_escape "$SOURCE_REPOSITORY")"
  if [[ -n "$token" ]]; then
    printf ',\n  "hf_read_token": "%s"\n' "$(json_escape "$token")"
  else
    printf '\n'
  fi
  printf '}\n'
} > "$tmp"

mv "$tmp" "$OUTPUT_PATH"
chmod 600 "$OUTPUT_PATH"
echo "Generated immutable HF discovery config at $OUTPUT_PATH"
