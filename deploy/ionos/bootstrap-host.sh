#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID:-0} -ne 0 ]]; then
  echo "bootstrap-host.sh must run as root" >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

packages=(
  ca-certificates
  cron
  curl
  git
  python3
  python3-pip
  python3-venv
  rsync
  sqlite3
)

apt-get update
if apt-cache show caddy >/dev/null 2>&1; then
  packages+=(caddy)
fi
apt-get install -y --no-install-recommends "${packages[@]}"

echo "Installed packages:"
printf ' - %s\n' "${packages[@]}"
