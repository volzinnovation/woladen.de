#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: push-secrets-and-start.sh --host HOST --cert PATH --password-file PATH --subscriptions PATH [options]

Required:
  --host HOST              Remote VPS hostname or IP
  --cert PATH              Local path to Mobilithek certificate.p12
  --password-file PATH     Local path to pwd.txt
  --subscriptions PATH     Local path to mobilithek_subscriptions.json

Optional:
  --ssh-user USER          SSH user on the VPS (default: current local user)
  --port PORT              SSH port (default: 22)
  --identity PATH          SSH private key
  --hf-token PATH          Local Hugging Face token file to upload as /etc/woladen/huggingface.token
  --env-file PATH          Local env file to upload as /etc/woladen/woladen-live.env
  --remote-config-dir DIR  Remote config dir (default: /etc/woladen)
  --app-group GROUP        Remote service group (default: woladen)
  --no-start               Upload only, do not restart services
  --help                   Show this help text

Example:
  ./deploy/ionos/push-secrets-and-start.sh \
    --host 203.0.113.10 \
    --ssh-user deploy \
    --cert secret/certificate.p12 \
    --password-file secret/pwd.txt \
    --subscriptions secret/mobilithek_subscriptions.json
EOF
}

HOST=""
SSH_USER="${USER:-}"
SSH_PORT="22"
SSH_IDENTITY=""
LOCAL_CERT=""
LOCAL_PASSWORD_FILE=""
LOCAL_SUBSCRIPTIONS=""
LOCAL_HF_TOKEN=""
LOCAL_ENV_FILE=""
REMOTE_CONFIG_DIR="/etc/woladen"
APP_GROUP="woladen"
START_SERVICES=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --host)
      HOST=$2
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
    --env-file)
      LOCAL_ENV_FILE=$2
      shift 2
      ;;
    --hf-token)
      LOCAL_HF_TOKEN=$2
      shift 2
      ;;
    --remote-config-dir)
      REMOTE_CONFIG_DIR=$2
      shift 2
      ;;
    --app-group)
      APP_GROUP=$2
      shift 2
      ;;
    --no-start)
      START_SERVICES=0
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

require_file() {
  local path=$1
  if [[ -z "$path" || ! -f "$path" ]]; then
    echo "Missing file: $path" >&2
    exit 1
  fi
}

if [[ -z "$HOST" ]]; then
  echo "--host is required" >&2
  usage >&2
  exit 1
fi

require_file "$LOCAL_CERT"
require_file "$LOCAL_PASSWORD_FILE"
require_file "$LOCAL_SUBSCRIPTIONS"
if [[ -n "$LOCAL_ENV_FILE" ]]; then
  require_file "$LOCAL_ENV_FILE"
fi
if [[ -n "$LOCAL_HF_TOKEN" ]]; then
  require_file "$LOCAL_HF_TOKEN"
fi

SSH_TARGET="$SSH_USER@$HOST"
ssh_cmd=(ssh -p "$SSH_PORT")
scp_cmd=(scp -P "$SSH_PORT")
if [[ -n "$SSH_IDENTITY" ]]; then
  ssh_cmd+=(-i "$SSH_IDENTITY")
  scp_cmd+=(-i "$SSH_IDENTITY")
fi

REMOTE_TMP_DIR=$("${ssh_cmd[@]}" "$SSH_TARGET" "mktemp -d -t woladen-secrets.XXXXXX")
cleanup_remote_tmp() {
  "${ssh_cmd[@]}" "$SSH_TARGET" "rm -rf '$REMOTE_TMP_DIR'" >/dev/null 2>&1 || true
}
trap cleanup_remote_tmp EXIT

"${scp_cmd[@]}" "$LOCAL_CERT" "$SSH_TARGET:$REMOTE_TMP_DIR/certificate.p12"
"${scp_cmd[@]}" "$LOCAL_PASSWORD_FILE" "$SSH_TARGET:$REMOTE_TMP_DIR/pwd.txt"
"${scp_cmd[@]}" "$LOCAL_SUBSCRIPTIONS" "$SSH_TARGET:$REMOTE_TMP_DIR/mobilithek_subscriptions.json"
if [[ -n "$LOCAL_HF_TOKEN" ]]; then
  "${scp_cmd[@]}" "$LOCAL_HF_TOKEN" "$SSH_TARGET:$REMOTE_TMP_DIR/huggingface.token"
fi
if [[ -n "$LOCAL_ENV_FILE" ]]; then
  "${scp_cmd[@]}" "$LOCAL_ENV_FILE" "$SSH_TARGET:$REMOTE_TMP_DIR/woladen-live.env"
fi

REMOTE_HAS_ENV=0
if [[ -n "$LOCAL_ENV_FILE" ]]; then
  REMOTE_HAS_ENV=1
fi
REMOTE_HAS_HF_TOKEN=0
if [[ -n "$LOCAL_HF_TOKEN" ]]; then
  REMOTE_HAS_HF_TOKEN=1
fi

"${ssh_cmd[@]}" "$SSH_TARGET" "sudo bash -s -- '$REMOTE_TMP_DIR' '$REMOTE_CONFIG_DIR' '$APP_GROUP' '$REMOTE_HAS_ENV' '$REMOTE_HAS_HF_TOKEN' '$START_SERVICES'" <<'EOF'
set -euo pipefail

remote_tmp_dir=$1
remote_config_dir=$2
app_group=$3
has_env=$4
has_hf_token=$5
start_services=$6

if ! getent group "$app_group" >/dev/null 2>&1; then
  echo "Remote group does not exist: $app_group" >&2
  exit 1
fi

install -d -m 0750 -o root -g "$app_group" "$remote_config_dir"
install -m 0640 -o root -g "$app_group" "$remote_tmp_dir/certificate.p12" "$remote_config_dir/certificate.p12"
install -m 0640 -o root -g "$app_group" "$remote_tmp_dir/pwd.txt" "$remote_config_dir/pwd.txt"
install -m 0640 -o root -g "$app_group" \
  "$remote_tmp_dir/mobilithek_subscriptions.json" \
  "$remote_config_dir/mobilithek_subscriptions.json"

if [[ "$has_hf_token" == "1" ]]; then
  install -m 0640 -o root -g "$app_group" \
    "$remote_tmp_dir/huggingface.token" \
    "$remote_config_dir/huggingface.token"
fi

if [[ "$has_env" == "1" ]]; then
  install -m 0640 -o root -g "$app_group" \
    "$remote_tmp_dir/woladen-live.env" \
    "$remote_config_dir/woladen-live.env"
fi

systemctl daemon-reload

if [[ "$start_services" == "1" ]]; then
  systemctl restart woladen-live-api.service woladen-live-ingester.service
  systemctl --no-pager --lines=5 status woladen-live-api.service woladen-live-ingester.service
  curl -fsS http://127.0.0.1:8001/healthz
fi
EOF

echo "Secrets uploaded to $SSH_TARGET:$REMOTE_CONFIG_DIR"
if [[ "$START_SERVICES" == "1" ]]; then
  echo "Services restarted and local health check passed."
else
  echo "Services were not restarted because --no-start was used."
fi
