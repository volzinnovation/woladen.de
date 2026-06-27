#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: deploy-release.sh --host HOST --archive PATH --cert PATH --password-file PATH --subscriptions PATH [options]

Required:
  --host HOST              Remote VPS hostname or IP
  --archive PATH           Local path to the release tarball created by build-release.sh
  --cert PATH              Local path to Mobilithek certificate.p12
  --password-file PATH     Local path to pwd.txt
  --subscriptions PATH     Local path to mobilithek_subscriptions.json

Optional:
  --ssh-user USER          SSH user on the VPS (default: current local user)
  --port PORT              SSH port (default: 22)
  --identity PATH          SSH private key
  --hf-token PATH          Local Hugging Face token file to upload as /etc/woladen/huggingface.token
  --hf-repo-id REPO        Hugging Face dataset repo id to write into the remote env file
  --env-file PATH          Local env file to upload as /etc/woladen/woladen-live.env
  --install-root PATH      Remote install root (default: /srv/woladen-live)
  --config-dir PATH        Remote config dir (default: /etc/woladen)
  --state-dir PATH         Remote state dir (default: /var/lib/woladen)
  --app-user USER          Remote service user (default: woladen)
  --app-group GROUP        Remote service group (default: woladen)
  --live-domain HOST       Public API hostname (default: live.woladen.de)
  --keep-releases N        Number of releases to keep on the server after verification (default: 1)
  --help                   Show this help text

Environment:
  WOLADEN_DEPLOY_SUDO_PASSWORD must be set so the deploy user can run sudo non-interactively.
EOF
}

HOST=""
ARCHIVE_PATH=""
SSH_USER="${USER:-}"
SSH_PORT="22"
SSH_IDENTITY=""
LOCAL_CERT=""
LOCAL_PASSWORD_FILE=""
LOCAL_SUBSCRIPTIONS=""
LOCAL_HF_TOKEN=""
LOCAL_ENV_FILE=""
HF_REPO_ID=""
INSTALL_ROOT="/srv/woladen-live"
CONFIG_DIR="/etc/woladen"
STATE_DIR="/var/lib/woladen"
APP_USER="woladen"
APP_GROUP="woladen"
LIVE_DOMAIN="live.woladen.de"
KEEP_RELEASES="1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST=$2
      shift 2
      ;;
    --archive)
      ARCHIVE_PATH=$2
      shift 2
      ;;
    --ssh-user)
      SSH_USER=$2
      shift 2
      ;;
    --port)
      SSH_PORT=$2
      shift 2
      ;;
    --identity)
      SSH_IDENTITY=$2
      shift 2
      ;;
    --cert)
      LOCAL_CERT=$2
      shift 2
      ;;
    --password-file)
      LOCAL_PASSWORD_FILE=$2
      shift 2
      ;;
    --subscriptions)
      LOCAL_SUBSCRIPTIONS=$2
      shift 2
      ;;
    --hf-token)
      LOCAL_HF_TOKEN=$2
      shift 2
      ;;
    --hf-repo-id)
      HF_REPO_ID=$2
      shift 2
      ;;
    --env-file)
      LOCAL_ENV_FILE=$2
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

require_file() {
  local path=$1
  if [[ -z "$path" || ! -f "$path" ]]; then
    echo "Missing file: $path" >&2
    exit 1
  fi
}

if [[ -z "$HOST" || -z "$ARCHIVE_PATH" ]]; then
  echo "--host and --archive are required" >&2
  usage >&2
  exit 1
fi
if [[ -z "${WOLADEN_DEPLOY_SUDO_PASSWORD:-}" ]]; then
  echo "WOLADEN_DEPLOY_SUDO_PASSWORD must be set" >&2
  exit 1
fi

require_file "$ARCHIVE_PATH"
require_file "$LOCAL_CERT"
require_file "$LOCAL_PASSWORD_FILE"
require_file "$LOCAL_SUBSCRIPTIONS"
if [[ -n "$LOCAL_ENV_FILE" ]]; then
  require_file "$LOCAL_ENV_FILE"
fi
if [[ -n "$LOCAL_HF_TOKEN" ]]; then
  require_file "$LOCAL_HF_TOKEN"
fi

SSH_TARGET="$HOST"
if [[ -n "$SSH_USER" ]]; then
  SSH_TARGET="$SSH_USER@$HOST"
fi

ssh_cmd=(ssh -p "$SSH_PORT")
scp_cmd=(scp -P "$SSH_PORT")
if [[ -n "$SSH_IDENTITY" ]]; then
  ssh_cmd+=(-i "$SSH_IDENTITY")
  scp_cmd+=(-i "$SSH_IDENTITY")
fi

REMOTE_TMP_DIR=$("${ssh_cmd[@]}" "$SSH_TARGET" "mktemp -d -t woladen-release.XXXXXX")
cleanup_remote_tmp() {
  if [[ -n "${REMOTE_TMP_DIR:-}" ]]; then
    "${ssh_cmd[@]}" "$SSH_TARGET" "rm -rf '$REMOTE_TMP_DIR'" >/dev/null 2>&1 || true
  fi
}
trap cleanup_remote_tmp EXIT

"${scp_cmd[@]}" "$ARCHIVE_PATH" "$SSH_TARGET:$REMOTE_TMP_DIR/release.tar.gz"
"${scp_cmd[@]}" "$LOCAL_CERT" "$SSH_TARGET:$REMOTE_TMP_DIR/certificate.p12"
"${scp_cmd[@]}" "$LOCAL_PASSWORD_FILE" "$SSH_TARGET:$REMOTE_TMP_DIR/pwd.txt"
"${scp_cmd[@]}" "$LOCAL_SUBSCRIPTIONS" "$SSH_TARGET:$REMOTE_TMP_DIR/mobilithek_subscriptions.json"
if [[ -n "$LOCAL_HF_TOKEN" ]]; then
  "${scp_cmd[@]}" "$LOCAL_HF_TOKEN" "$SSH_TARGET:$REMOTE_TMP_DIR/huggingface.token"
fi
if [[ -n "$LOCAL_ENV_FILE" ]]; then
  "${scp_cmd[@]}" "$LOCAL_ENV_FILE" "$SSH_TARGET:$REMOTE_TMP_DIR/woladen-live.env"
fi

SUDO_PASSWORD_B64=$(printf '%s' "${WOLADEN_DEPLOY_SUDO_PASSWORD}" | base64 | tr -d '\n')
HF_REPO_ID_B64=$(printf '%s' "$HF_REPO_ID" | base64 | tr -d '\n')

"${ssh_cmd[@]}" "$SSH_TARGET" \
  "env SUDO_PASSWORD_B64='$SUDO_PASSWORD_B64' HF_REPO_ID_B64='$HF_REPO_ID_B64' bash -s -- '$REMOTE_TMP_DIR' '$INSTALL_ROOT' '$CONFIG_DIR' '$STATE_DIR' '$APP_USER' '$APP_GROUP' '$LIVE_DOMAIN' '$KEEP_RELEASES'" <<'EOF'
set -euo pipefail

remote_tmp_dir=$1
install_root=$2
config_dir=$3
state_dir=$4
app_user=$5
app_group=$6
live_domain=$7
keep_releases=$8

sudo_password=$(printf '%s' "${SUDO_PASSWORD_B64:-}" | base64 --decode)
hf_repo_id=$(printf '%s' "${HF_REPO_ID_B64:-}" | base64 --decode)
bundle_extract_dir="$remote_tmp_dir/extracted"
bundle_dir=""
current_release_dir=""
env_file="$config_dir/woladen-live.env"
hf_token_target="$config_dir/huggingface.token"
cert_target="$config_dir/certificate.p12"
pwd_target="$config_dir/pwd.txt"
subscriptions_target="$config_dir/mobilithek_subscriptions.json"
venv_dir="$install_root/venv"
current_link="$install_root/current"

sudo_cmd() {
  printf '%s\n' "$sudo_password" | sudo -S -p '' "$@"
}

remote_needs_bootstrap() {
  if ! command -v python3 >/dev/null 2>&1; then
    return 0
  fi
  if ! command -v rsync >/dev/null 2>&1; then
    return 0
  fi
  if ! command -v curl >/dev/null 2>&1; then
    return 0
  fi
  return 1
}

install_optional_file() {
  local source_path=$1
  local target_path=$2
  local change_var=$3
  if [[ ! -f "$source_path" ]]; then
    return 0
  fi
  if ! sudo_cmd test -f "$target_path" || ! sudo_cmd cmp -s "$source_path" "$target_path"; then
    printf -v "$change_var" '1'
  fi
  sudo_cmd install -D -m 0640 -o root -g "$app_group" "$source_path" "$target_path"
}

upsert_env_value() {
  local key=$1
  local value=$2
  local temp_path

  temp_path=$(mktemp)
  sudo_cmd cat "$env_file" >"$temp_path"
  awk -v key="$key" -v value="$value" '
    BEGIN { updated = 0 }
    $0 ~ "^#?[[:space:]]*" key "=" {
      if (!updated) {
        print key "=" value
        updated = 1
      }
      next
    }
    { print }
    END {
      if (!updated) {
        print key "=" value
      }
    }
  ' "$temp_path" >"${temp_path}.next"

  if ! sudo_cmd cmp -s "${temp_path}.next" "$env_file"; then
    env_changed=1
  fi
  sudo_cmd install -m 0640 -o root -g "$app_group" "${temp_path}.next" "$env_file"
  rm -f "$temp_path" "${temp_path}.next"
}

wait_for_api_health() {
  local health_url="http://127.0.0.1:8001/healthz"
  local attempt

  for attempt in $(seq 1 15); do
    if curl -fsS "$health_url" >/dev/null; then
      return 0
    fi
    sleep 2
  done

  echo "Live API health check failed after waiting for startup: $health_url" >&2
  sudo_cmd systemctl --no-pager --full status woladen-live-api.service || true
  sudo_cmd journalctl -u woladen-live-api.service -u woladen-live-ingester.service -u woladen-live-queue-worker.service -n 100 --no-pager || true
  return 1
}

extract_release_bundle() {
  mkdir -p "$bundle_extract_dir"
  tar -xzf "$remote_tmp_dir/release.tar.gz" -C "$bundle_extract_dir"
  rm -f "$remote_tmp_dir/release.tar.gz"
  bundle_dir=$(find "$bundle_extract_dir" -mindepth 1 -maxdepth 1 -type d | head -n 1)
  if [[ -z "$bundle_dir" ]]; then
    echo "Release archive did not unpack into a bundle directory" >&2
    exit 1
  fi
}

prune_releases() {
  local current_release
  local -a release_dirs
  local removable_count

  current_release=$(readlink -f "$current_link" 2>/dev/null || true)
  mapfile -t release_dirs < <(sudo_cmd find "$install_root/releases" -mindepth 1 -maxdepth 1 -type d | sort)
  removable_count=$(( ${#release_dirs[@]} - keep_releases ))
  if [[ $removable_count -le 0 ]]; then
    return
  fi

  for release_path in "${release_dirs[@]}"; do
    if [[ "$release_path" == "$current_release" ]]; then
      continue
    fi
    sudo_cmd rm -rf "$release_path"
    removable_count=$(( removable_count - 1 ))
    if [[ $removable_count -le 0 ]]; then
      break
    fi
  done
}

if remote_needs_bootstrap; then
  extract_release_bundle
  sudo_cmd bash "$bundle_dir/deploy/ionos/bootstrap-host.sh"
else
  extract_release_bundle
fi

if [[ -e "$current_link" ]]; then
  current_release_dir=$(readlink -f "$current_link")
fi

plan_cmd=(python3 "$bundle_dir/scripts/live_deploy_plan.py" --shell --candidate-root "$bundle_dir")
if [[ -n "$current_release_dir" ]]; then
  plan_cmd+=(--current-root "$current_release_dir")
fi
plan_output=$("${plan_cmd[@]}")
eval "$plan_output"

echo "Deploy plan: ${DEPLOY_PLAN_SUMMARY:-none} (changed files: ${DEPLOY_PLAN_CHANGED_COUNT:-0})"

sudo_cmd bash "$bundle_dir/deploy/ionos/install-on-vps.sh" \
  --bundle-dir "$bundle_dir" \
  --install-root "$install_root" \
  --config-dir "$config_dir" \
  --state-dir "$state_dir" \
  --app-user "$app_user" \
  --app-group "$app_group" \
  --live-domain "$live_domain" \
  --keep-releases "$keep_releases" \
  --skip-prune

env_changed=0
subscriptions_changed=0

install_optional_file "$remote_tmp_dir/certificate.p12" "$cert_target" cert_changed
install_optional_file "$remote_tmp_dir/pwd.txt" "$pwd_target" password_changed
install_optional_file "$remote_tmp_dir/mobilithek_subscriptions.json" "$subscriptions_target" subscriptions_changed
install_optional_file "$remote_tmp_dir/huggingface.token" "$hf_token_target" hf_token_changed
install_optional_file "$remote_tmp_dir/woladen-live.env" "$env_file" env_changed

echo "Runtime config sync: cert=${cert_changed:-0} pwd=${password_changed:-0} subscriptions=${subscriptions_changed:-0} hf_token=${hf_token_changed:-0} env=${env_changed:-0}"

if [[ -n "$hf_repo_id" ]]; then
  upsert_env_value "WOLADEN_LIVE_HF_ARCHIVE_REPO_ID" "$hf_repo_id"
fi
if [[ -f "$remote_tmp_dir/huggingface.token" ]]; then
  upsert_env_value "WOLADEN_LIVE_HF_ARCHIVE_TOKEN_FILE" "$hf_token_target"
fi
upsert_env_value "WOLADEN_LIVE_QUEUE_DIR" "$state_dir/live_queue"

should_restart=$DEPLOY_PLAN_RESTART_SERVICES
should_bootstrap=$DEPLOY_PLAN_BOOTSTRAP_RUNTIME
if [[ ${env_changed:-0} -eq 1 ]]; then
  should_restart=1
fi
if [[ ${subscriptions_changed:-0} -eq 1 ]]; then
  should_bootstrap=1
fi
if [[ $should_restart -eq 1 ]]; then
  should_bootstrap=0
fi

if [[ $should_restart -eq 1 ]]; then
  sudo_cmd systemctl restart woladen-live-api.service woladen-live-ingester.service woladen-live-queue-worker.service
elif [[ $should_bootstrap -eq 1 ]]; then
  sudo_cmd -u "$app_user" "$venv_dir/bin/python" "$current_link/scripts/live_ingester.py" --bootstrap-only >/dev/null
fi

if [[ $DEPLOY_PLAN_RELOAD_CADDY -eq 1 ]] && sudo_cmd systemctl list-unit-files caddy.service >/dev/null 2>&1; then
  sudo_cmd systemctl reload caddy
fi

wait_for_api_health
prune_releases
EOF

echo "Release deployed to $SSH_TARGET"
