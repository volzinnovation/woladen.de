#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: install-on-vps.sh [options]

Options:
  --bundle-dir PATH      Source tree or extracted release bundle
  --install-root PATH    Install root (default: /srv/woladen-live)
  --config-dir PATH      Config and secret directory (default: /etc/woladen)
  --state-dir PATH       Writable state directory (default: /var/lib/woladen)
  --app-user USER        Service user (default: woladen)
  --app-group GROUP      Service group (default: woladen)
  --live-domain HOST     Public API hostname (default: live.woladen.de)
  --keep-releases N      Number of staged releases to retain (default: 5)
  --start                Restart services after installing
  --no-enable            Do not enable services
  --help                 Show this help text
EOF
}

if [[ ${EUID:-0} -ne 0 ]]; then
  echo "install-on-vps.sh must run as root" >&2
  exit 1
fi

BUNDLE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
INSTALL_ROOT=/srv/woladen-live
CONFIG_DIR=/etc/woladen
STATE_DIR=/var/lib/woladen
APP_USER=woladen
APP_GROUP=woladen
LIVE_DOMAIN=live.woladen.de
KEEP_RELEASES=5
ENABLE_SERVICES=1
START_SERVICES=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bundle-dir)
      BUNDLE_DIR=$2
      shift 2
      ;;
    --install-root)
      INSTALL_ROOT=$2
      shift 2
      ;;
    --config-dir)
      CONFIG_DIR=$2
      shift 2
      ;;
    --state-dir)
      STATE_DIR=$2
      shift 2
      ;;
    --app-user)
      APP_USER=$2
      shift 2
      ;;
    --app-group)
      APP_GROUP=$2
      shift 2
      ;;
    --live-domain)
      LIVE_DOMAIN=$2
      shift 2
      ;;
    --keep-releases)
      KEEP_RELEASES=$2
      shift 2
      ;;
    --start)
      START_SERVICES=1
      shift
      ;;
    --no-enable)
      ENABLE_SERVICES=0
      shift
      ;;
    --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! [[ "$KEEP_RELEASES" =~ ^[0-9]+$ ]] || [[ "$KEEP_RELEASES" -lt 1 ]]; then
  echo "--keep-releases must be a positive integer" >&2
  exit 1
fi

BUNDLE_DIR=$(cd "$BUNDLE_DIR" && pwd)
BUNDLE_NAME=$(basename "$BUNDLE_DIR")
CURRENT_LINK="$INSTALL_ROOT/current"
RELEASES_DIR="$INSTALL_ROOT/releases"
RELEASE_DIR="$RELEASES_DIR/$BUNDLE_NAME"
VENV_DIR="$INSTALL_ROOT/venv"
ENV_FILE="$CONFIG_DIR/woladen-live.env"
RENDERED_CADDYFILE="$CONFIG_DIR/${LIVE_DOMAIN}.Caddyfile"
CRON_FILE=/etc/cron.d/woladen-live-log-archive
CURRENT_RELEASE_DIR=""

require_path() {
  local path=$1
  if [[ ! -e "$path" ]]; then
    echo "Missing required path: $path" >&2
    exit 1
  fi
}

sync_dir() {
  local source=$1
  local target=$2
  require_path "$source"
  install -d "$target"
  rsync -a --delete "$source/" "$target/"
}

sync_file() {
  local source=$1
  local target=$2
  require_path "$source"
  install -D -m 0644 "$source" "$target"
}

render_template() {
  local template=$1
  local target=$2
  sed \
    -e "s|__APP_USER__|$APP_USER|g" \
    -e "s|__APP_GROUP__|$APP_GROUP|g" \
    -e "s|__APP_DIR__|$CURRENT_LINK|g" \
    -e "s|__VENV_DIR__|$VENV_DIR|g" \
    -e "s|__ENV_FILE__|$ENV_FILE|g" \
    -e "s|__STATE_DIR__|$STATE_DIR|g" \
    -e "s|live.woladen.de|$LIVE_DOMAIN|g" \
    "$template" >"$target"
}

resolve_current_release_dir() {
  readlink -f "$CURRENT_LINK" 2>/dev/null || true
}

migrate_legacy_current_dir() {
  local legacy_release_base
  local legacy_release
  local suffix

  if [[ ! -d "$CURRENT_LINK" || -L "$CURRENT_LINK" ]]; then
    return
  fi

  legacy_release_base="$RELEASES_DIR/legacy-$(date -u +%Y%m%dT%H%M%SZ)"
  legacy_release="$legacy_release_base"
  suffix=1
  while [[ -e "$legacy_release" ]]; do
    legacy_release="${legacy_release_base}-${suffix}"
    suffix=$(( suffix + 1 ))
  done

  echo "Migrating legacy install layout: $CURRENT_LINK -> $legacy_release"
  mv "$CURRENT_LINK" "$legacy_release"
  ln -s "$legacy_release" "$CURRENT_LINK"
}

prune_releases() {
  local current_release
  local -a release_dirs
  local removable_count

  current_release=$(readlink -f "$CURRENT_LINK" 2>/dev/null || true)
  mapfile -t release_dirs < <(find "$RELEASES_DIR" -mindepth 1 -maxdepth 1 -type d | sort)
  removable_count=$(( ${#release_dirs[@]} - KEEP_RELEASES ))
  if [[ $removable_count -le 0 ]]; then
    return
  fi

  for release_path in "${release_dirs[@]}"; do
    if [[ "$release_path" == "$current_release" ]]; then
      continue
    fi
    rm -rf "$release_path"
    removable_count=$(( removable_count - 1 ))
    if [[ $removable_count -le 0 ]]; then
      break
    fi
  done
}

if ! getent group "$APP_GROUP" >/dev/null 2>&1; then
  groupadd --system "$APP_GROUP"
fi

if ! id -u "$APP_USER" >/dev/null 2>&1; then
  useradd \
    --system \
    --gid "$APP_GROUP" \
    --home-dir "$INSTALL_ROOT" \
    --create-home \
    --shell /usr/sbin/nologin \
    "$APP_USER"
fi

install -d -m 0755 "$INSTALL_ROOT" "$CONFIG_DIR" "$RELEASES_DIR"
install -d -m 0750 -o "$APP_USER" -g "$APP_GROUP" \
  "$STATE_DIR" \
  "$STATE_DIR/live_raw" \
  "$STATE_DIR/live_archives"
migrate_legacy_current_dir
CURRENT_RELEASE_DIR=$(resolve_current_release_dir)

rm -rf "$RELEASE_DIR"
mkdir -p \
  "$RELEASE_DIR/backend" \
  "$RELEASE_DIR/data" \
  "$RELEASE_DIR/deploy/ionos" \
  "$RELEASE_DIR/docs" \
  "$RELEASE_DIR/scripts"

sync_dir "$BUNDLE_DIR/backend" "$RELEASE_DIR/backend"
sync_dir "$BUNDLE_DIR/deploy/ionos" "$RELEASE_DIR/deploy/ionos"
sync_dir "$BUNDLE_DIR/scripts" "$RELEASE_DIR/scripts"
sync_file "$BUNDLE_DIR/data/chargers_fast.csv" "$RELEASE_DIR/data/chargers_fast.csv"
sync_file "$BUNDLE_DIR/data/chargers_fast.geojson" "$RELEASE_DIR/data/chargers_fast.geojson"
sync_file "$BUNDLE_DIR/data/mobilithek_afir_provider_configs.json" "$RELEASE_DIR/data/mobilithek_afir_provider_configs.json"
sync_file "$BUNDLE_DIR/data/mobilithek_afir_static_matches.csv" "$RELEASE_DIR/data/mobilithek_afir_static_matches.csv"
if [[ -f "$BUNDLE_DIR/data/live_provider_overrides.json" ]]; then
  sync_file "$BUNDLE_DIR/data/live_provider_overrides.json" "$RELEASE_DIR/data/live_provider_overrides.json"
fi
sync_file "$BUNDLE_DIR/docs/live-api-mvp.md" "$RELEASE_DIR/docs/live-api-mvp.md"
sync_file "$BUNDLE_DIR/requirements-live.txt" "$RELEASE_DIR/requirements-live.txt"
sync_file "$BUNDLE_DIR/LICENSE" "$RELEASE_DIR/LICENSE"
if [[ -f "$BUNDLE_DIR/release.json" ]]; then
  sync_file "$BUNDLE_DIR/release.json" "$RELEASE_DIR/release.json"
fi

REQUIRES_VENV_REFRESH=1
if [[ -d "$VENV_DIR" ]] && [[ -n "$CURRENT_RELEASE_DIR" ]] && [[ -f "$CURRENT_RELEASE_DIR/requirements-live.txt" ]]; then
  if cmp -s "$CURRENT_RELEASE_DIR/requirements-live.txt" "$RELEASE_DIR/requirements-live.txt"; then
    REQUIRES_VENV_REFRESH=0
  fi
fi

if [[ $REQUIRES_VENV_REFRESH -eq 1 ]]; then
  python3 -m venv "$VENV_DIR"
  "$VENV_DIR/bin/pip" install --upgrade pip
  "$VENV_DIR/bin/pip" install -r "$RELEASE_DIR/requirements-live.txt"
fi

chown -R root:root "$RELEASE_DIR"
chmod -R a+rX "$RELEASE_DIR"
chown -R "$APP_USER:$APP_GROUP" "$STATE_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  install -D -m 0640 -o root -g "$APP_GROUP" \
    "$RELEASE_DIR/deploy/ionos/woladen-live.env.example" \
    "$ENV_FILE"
fi

install -d /etc/systemd/system
render_template \
  "$RELEASE_DIR/deploy/ionos/woladen-live-api.service" \
  /etc/systemd/system/woladen-live-api.service
render_template \
  "$RELEASE_DIR/deploy/ionos/woladen-live-ingester.service" \
  /etc/systemd/system/woladen-live-ingester.service
render_template \
  "$RELEASE_DIR/deploy/ionos/Caddyfile" \
  "$RENDERED_CADDYFILE"
render_template \
  "$RELEASE_DIR/deploy/ionos/woladen-live-log-archive.cron" \
  "$CRON_FILE"
chmod 0644 "$CRON_FILE"

ln -sfn "$RELEASE_DIR" "$CURRENT_LINK.next"
mv -Tf "$CURRENT_LINK.next" "$CURRENT_LINK"
prune_releases

systemctl daemon-reload

if systemctl list-unit-files cron.service >/dev/null 2>&1; then
  systemctl enable cron.service >/dev/null 2>&1 || true
  systemctl start cron.service >/dev/null 2>&1 || true
fi

if [[ $ENABLE_SERVICES -eq 1 ]]; then
  systemctl enable woladen-live-api.service woladen-live-ingester.service
fi

if [[ $START_SERVICES -eq 1 ]]; then
  systemctl restart woladen-live-api.service woladen-live-ingester.service
fi

cat <<EOF
Install root: $INSTALL_ROOT
Current release: $(readlink -f "$CURRENT_LINK")
Current symlink: $CURRENT_LINK
Virtualenv: $VENV_DIR
Virtualenv refreshed: $REQUIRES_VENV_REFRESH
State dir: $STATE_DIR
Env file: $ENV_FILE
Rendered Caddyfile: $RENDERED_CADDYFILE

Next steps:
1. Copy certificate.p12, pwd.txt, and mobilithek_subscriptions.json into $CONFIG_DIR
2. Add a Hugging Face token at $CONFIG_DIR/huggingface.token; the default env file targets loffenauer/AFIR
3. Edit $ENV_FILE if any paths differ
4. Copy or import $RENDERED_CADDYFILE into your Caddy config
5. Run: systemctl restart woladen-live-api.service woladen-live-ingester.service
6. Check: journalctl -u woladen-live-api.service -u woladen-live-ingester.service -n 100 --no-pager
7. Check archive job output in: $STATE_DIR/live-log-archive.log
EOF
