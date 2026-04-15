#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: install-on-vps.sh [options]

Options:
  --bundle-dir PATH     Source tree or extracted release bundle
  --install-root PATH   Install root (default: /srv/woladen-live)
  --config-dir PATH     Config and secret directory (default: /etc/woladen)
  --state-dir PATH      Writable state directory (default: /var/lib/woladen)
  --app-user USER       Service user (default: woladen)
  --app-group GROUP     Service group (default: woladen)
  --live-domain HOST    Public API hostname (default: live.woladen.de)
  --start               Restart services after installing
  --no-enable           Do not enable services
  --help                Show this help text
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

APP_DIR="$INSTALL_ROOT/current"
VENV_DIR="$INSTALL_ROOT/venv"
ENV_FILE="$CONFIG_DIR/woladen-live.env"
RENDERED_CADDYFILE="$CONFIG_DIR/${LIVE_DOMAIN}.Caddyfile"
CRON_FILE=/etc/cron.d/woladen-live-log-archive

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
    -e "s|__APP_DIR__|$APP_DIR|g" \
    -e "s|__VENV_DIR__|$VENV_DIR|g" \
    -e "s|__ENV_FILE__|$ENV_FILE|g" \
    -e "s|__STATE_DIR__|$STATE_DIR|g" \
    -e "s|live.woladen.de|$LIVE_DOMAIN|g" \
    "$template" >"$target"
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

install -d -m 0755 "$APP_DIR" "$CONFIG_DIR" "$INSTALL_ROOT"
install -d -m 0750 -o "$APP_USER" -g "$APP_GROUP" \
  "$STATE_DIR" \
  "$STATE_DIR/live_raw" \
  "$STATE_DIR/live_archives"

sync_dir "$BUNDLE_DIR/backend" "$APP_DIR/backend"
sync_dir "$BUNDLE_DIR/deploy/ionos" "$APP_DIR/deploy/ionos"
sync_dir "$BUNDLE_DIR/scripts" "$APP_DIR/scripts"
sync_file "$BUNDLE_DIR/data/chargers_fast.csv" "$APP_DIR/data/chargers_fast.csv"
sync_file "$BUNDLE_DIR/data/chargers_fast.geojson" "$APP_DIR/data/chargers_fast.geojson"
sync_file "$BUNDLE_DIR/data/mobilithek_afir_provider_configs.json" "$APP_DIR/data/mobilithek_afir_provider_configs.json"
sync_file "$BUNDLE_DIR/data/mobilithek_afir_static_matches.csv" "$APP_DIR/data/mobilithek_afir_static_matches.csv"
if [[ -f "$BUNDLE_DIR/data/live_provider_overrides.json" ]]; then
  sync_file "$BUNDLE_DIR/data/live_provider_overrides.json" "$APP_DIR/data/live_provider_overrides.json"
fi
sync_file "$BUNDLE_DIR/docs/live-api-mvp.md" "$APP_DIR/docs/live-api-mvp.md"
sync_file "$BUNDLE_DIR/requirements-live.txt" "$APP_DIR/requirements-live.txt"
sync_file "$BUNDLE_DIR/LICENSE" "$APP_DIR/LICENSE"
if [[ -f "$BUNDLE_DIR/release.json" ]]; then
  sync_file "$BUNDLE_DIR/release.json" "$APP_DIR/release.json"
fi

python3 -m venv "$VENV_DIR"
"$VENV_DIR/bin/pip" install --upgrade pip
"$VENV_DIR/bin/pip" install -r "$APP_DIR/requirements-live.txt"

chown -R root:root "$APP_DIR"
chmod -R a+rX "$APP_DIR"
chown -R "$APP_USER:$APP_GROUP" "$STATE_DIR"

if [[ ! -f "$ENV_FILE" ]]; then
  install -D -m 0640 -o root -g "$APP_GROUP" \
    "$APP_DIR/deploy/ionos/woladen-live.env.example" \
    "$ENV_FILE"
fi

install -d /etc/systemd/system
render_template \
  "$APP_DIR/deploy/ionos/woladen-live-api.service" \
  /etc/systemd/system/woladen-live-api.service
render_template \
  "$APP_DIR/deploy/ionos/woladen-live-ingester.service" \
  /etc/systemd/system/woladen-live-ingester.service
render_template \
  "$APP_DIR/deploy/ionos/Caddyfile" \
  "$RENDERED_CADDYFILE"
render_template \
  "$APP_DIR/deploy/ionos/woladen-live-log-archive.cron" \
  "$CRON_FILE"
chmod 0644 "$CRON_FILE"

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
Application tree: $APP_DIR
Virtualenv: $VENV_DIR
State dir: $STATE_DIR
Env file: $ENV_FILE
Rendered Caddyfile: $RENDERED_CADDYFILE

Next steps:
1. Copy certificate.p12, pwd.txt, and mobilithek_subscriptions.json into $CONFIG_DIR
2. Add a Hugging Face token at $CONFIG_DIR/huggingface.token and set WOLADEN_LIVE_HF_ARCHIVE_REPO_ID in $ENV_FILE
3. Edit $ENV_FILE if any paths differ
4. Copy or import $RENDERED_CADDYFILE into your Caddy config
5. Run: systemctl restart woladen-live-api.service woladen-live-ingester.service
6. Check: journalctl -u woladen-live-api.service -u woladen-live-ingester.service -n 100 --no-pager
7. Check archive job output in: $STATE_DIR/live-log-archive.log
EOF
