# IONOS VPS Packaging

This package targets a small IONOS VPS where:

- the static frontend stays on `https://woladen.de`
- the live API is published on `https://live.woladen.de`
- the live ingester and API run as native `systemd` services
- a daily cron job archives the previous day of provider response logs
- the archive job can upload to Hugging Face Hub with a token file
- releases are staged under `/srv/woladen-live/releases/` and activated via the `current` symlink

## Files

- `build-release.sh`: creates a backend-only tarball in `tmp/ionos-release/`
- `deploy-release.sh`: uploads a tarball plus secrets, installs it remotely, and applies the right runtime action
- `bootstrap-host.sh`: installs the base OS packages on Debian/Ubuntu
- `install-on-vps.sh`: stages a release under `/srv/woladen-live/releases/`, updates `current`, and renders runtime config
- `push-secrets-and-start.sh`: manual secret-only upload helper
- `woladen-live.env.example`: runtime env file for `/etc/woladen/woladen-live.env`
- `woladen-live-api.service`: `systemd` unit template for FastAPI
- `woladen-live-ingester.service`: `systemd` unit template for the polling loop
- `woladen-live-log-archive.cron`: daily archive/upload job for provider response logs
- `Caddyfile`: reverse-proxy config for `live.woladen.de`

## Runtime Uptake

Deployments now distinguish between runtime change types:

- Python/runtime code changes: restart `woladen-live-api.service` and `woladen-live-ingester.service`
- Provider/config data changes: keep the services running and refresh the live DB with `live_ingester.py --bootstrap-only`
- Caddy config changes: reload `caddy`
- Archive-only or documentation-only changes: install the new release without bouncing the live services

The API keeps reading `data/chargers_fast.geojson` through `/srv/woladen-live/current`, so GeoJSON-only updates become visible immediately after the symlink switch.

## Manual Fallback

Build the release bundle locally:

```bash
./deploy/ionos/build-release.sh
```

Run the one-time host bootstrap if this is a fresh VPS:

```bash
sudo ./deploy/ionos/bootstrap-host.sh
```

Deploy a release from your workstation with the same path that GitHub Actions uses:

```bash
export WOLADEN_DEPLOY_SUDO_PASSWORD='your-deploy-user-sudo-password'
./deploy/ionos/deploy-release.sh \
  --host your-vps-hostname-or-ip \
  --ssh-user deploy \
  --identity ~/.ssh/woladen-live \
  --archive tmp/ionos-release/woladen-live-backend-*.tar.gz \
  --cert secret/certificate.p12 \
  --password-file secret/pwd.txt \
  --subscriptions secret/mobilithek_subscriptions.json \
  --hf-token secret/hf_private \
  --hf-repo-id loffenauer/AFIR
```

If you only need to replace the remote secrets without deploying a new release, keep using:

```bash
./deploy/ionos/push-secrets-and-start.sh \
  --host your-vps-hostname-or-ip \
  --ssh-user deploy \
  --cert secret/certificate.p12 \
  --password-file secret/pwd.txt \
  --subscriptions secret/mobilithek_subscriptions.json \
  --hf-token secret/hf_private
```

## GitHub Automation

`.github/workflows/live-deploy.yml` now deploys the live backend in two cases:

- direct pushes to `main` that touch the live backend, live data, or deployment files
- successful completion of `Daily Data Generation`

The second trigger is necessary because commits created by a workflow with `GITHUB_TOKEN` do not fan out into downstream `push` workflows.

Configure these GitHub secrets:

- `LIVE_DEPLOY_HOST`
- `LIVE_DEPLOY_USER`
- `LIVE_DEPLOY_PORT`
- `LIVE_DEPLOY_SSH_PRIVATE_KEY`
- `LIVE_DEPLOY_SUDO_PASSWORD`
- `MOBILITHEK_USERNAME`
- `MOBILITHEK_PASSWORD`
- `MOBILITHEK_MACHINE_CERT_P12_BASE64`
- `MOBILITHEK_MACHINE_CERT_PASSWORD`
- `HF_PRIVATE`

Important:

- the VPS only needs the deploy user's public key in `authorized_keys`
- GitHub must store the matching private key in `LIVE_DEPLOY_SSH_PRIVATE_KEY`
- the deploy user still needs `sudo` rights on the VPS because installation writes into `/srv`, `/etc/systemd/system`, and `/etc/cron.d`

## Required Remote Secrets

The default runtime layout expects these files in `/etc/woladen/`:

- `certificate.p12`
- `pwd.txt`
- `mobilithek_subscriptions.json`
- `huggingface.token`

The env file also needs `WOLADEN_LIVE_HF_ARCHIVE_REPO_ID` for archive uploads. The deploy helper now writes `loffenauer/AFIR` automatically unless you override it manually.

## Reverse Proxy

The installer renders a Caddy config snippet to:

```bash
/etc/woladen/live.woladen.de.Caddyfile
```

If this server is dedicated to the live API, you can replace `/etc/caddy/Caddyfile`
with that file. If Caddy already serves other apps, import the rendered snippet
into your existing config instead.

## Verification

Validate the Mobilithek certificate:

```bash
sudo -u woladen /srv/woladen-live/venv/bin/python \
  /srv/woladen-live/current/scripts/live_subscription_registry.py \
  --probe-certificate
```

Check the API locally:

```bash
curl http://127.0.0.1:8001/healthz
```

Check the push endpoint locally:

```bash
curl -I http://127.0.0.1:8001/v1/push/qwello
```

Run the archive job manually:

```bash
sudo -u woladen /srv/woladen-live/venv/bin/python \
  /srv/woladen-live/current/scripts/live_archive_logs.py
```

Check the public endpoint after Caddy is reloaded:

```bash
curl https://live.woladen.de/healthz
```

Configure Mobilithek subscriber push URLs against the same public service, for example:

```text
https://live.woladen.de/v1/push/enbwmobility
https://live.woladen.de/v1/push/wirelane
```
