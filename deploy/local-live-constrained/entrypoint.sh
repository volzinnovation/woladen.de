#!/bin/sh
set -eu

secret_dir="/run/secrets/woladen"
mkdir -p "$secret_dir"

if [ -n "${WOLADEN_MACHINE_CERT_P12_B64:-}" ]; then
  python - <<'PY'
import base64
import os
from pathlib import Path

target = Path(os.environ.get("WOLADEN_MACHINE_CERT_P12", "/run/secrets/woladen/certificate.p12"))
target.parent.mkdir(parents=True, exist_ok=True)
target.write_bytes(base64.b64decode(os.environ["WOLADEN_MACHINE_CERT_P12_B64"]))
target.chmod(0o600)
PY
fi

if [ -n "${WOLADEN_MACHINE_CERT_PASSWORD:-}" ]; then
  password_file="${WOLADEN_MACHINE_CERT_PASSWORD_FILE:-/run/secrets/woladen/pwd.txt}"
  printf '%s\n' "$WOLADEN_MACHINE_CERT_PASSWORD" > "$password_file"
  chmod 0600 "$password_file"
fi

if [ -n "${WOLADEN_LIVE_SUBSCRIPTION_REGISTRY_JSON:-}" ]; then
  registry_file="${WOLADEN_LIVE_SUBSCRIPTION_REGISTRY_PATH:-/run/secrets/woladen/mobilithek_subscriptions.json}"
  printf '%s\n' "$WOLADEN_LIVE_SUBSCRIPTION_REGISTRY_JSON" > "$registry_file"
  chmod 0600 "$registry_file"

  if [ "${WOLADEN_LIVE_WRITE_REPO_SECRET_COMPAT:-}" = "1" ]; then
    mkdir -p /app/secret
    printf '%s\n' "$WOLADEN_LIVE_SUBSCRIPTION_REGISTRY_JSON" > /app/secret/mobilithek_subscriptions.json
    chmod 0600 /app/secret/mobilithek_subscriptions.json
  fi
fi

unset WOLADEN_MACHINE_CERT_P12_B64
unset WOLADEN_MACHINE_CERT_PASSWORD
unset WOLADEN_LIVE_SUBSCRIPTION_REGISTRY_JSON

exec "$@"
