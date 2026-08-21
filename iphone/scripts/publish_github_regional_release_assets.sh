#!/usr/bin/env bash
set -euo pipefail

readonly ANALYTICS_WORKFLOW_NAME="Build open static SQLite bundle"
readonly ANALYTICS_WORKFLOW_URL="https://github.com/volzinnovation/Woladen.de-analytics/actions/workflows/build-open-static-sqlite-bundle.yml"

usage() {
  cat <<USAGE
Usage: $0 [--help]

This frontend-owned release publisher is retired. It cannot create, edit,
delete, upload, or overwrite GitHub Release assets.

Build, validate, and publish open-static assets with the analytics-owned
"${ANALYTICS_WORKFLOW_NAME}" workflow:

  ${ANALYTICS_WORKFLOW_URL}

That workflow owns the release manifest, live-seed archive, SQLite/PBF
consistency checks, checksums, and final asset-inventory gate.
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

usage >&2
exit 1
