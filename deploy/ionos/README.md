# IONOS VPS Packaging

This package targets a small IONOS VPS where:

- the static frontend stays on `https://woladen.de`
- the live API is published on `https://live.woladen.de`
- the live ingester and API run as native `systemd` services
- a daily cron job archives the previous day of provider response logs
- SQLite lives on the VPS filesystem

## Files

- `build-release.sh`: creates a backend-only tarball in `tmp/ionos-release/`
- `bootstrap-host.sh`: installs the base OS packages on Debian/Ubuntu
- `install-on-vps.sh`: installs the bundle into `/srv/woladen-live`
- `woladen-live.env.example`: runtime env file for `/etc/woladen/woladen-live.env`
- `woladen-live-api.service`: `systemd` unit template for FastAPI
- `woladen-live-ingester.service`: `systemd` unit template for the polling loop
- `woladen-live-log-archive.cron`: daily archive/upload job for provider response logs
- `Caddyfile`: reverse-proxy config for `live.woladen.de`

## Recommended Flow

Build the deployment bundle locally:

```bash
./deploy/ionos/build-release.sh
```

Upload the resulting tarball to the VPS, then extract it there:

```bash
tar -xzf woladen-live-backend-*.tar.gz
cd woladen-live-backend-*
```

Bootstrap the host once:

```bash
sudo ./deploy/ionos/bootstrap-host.sh
```

Install the app:

```bash
sudo ./deploy/ionos/install-on-vps.sh
```

Upload secrets and restart the services from your local machine:

```bash
./deploy/ionos/push-secrets-and-start.sh \
  --host your-vps-hostname-or-ip \
  --ssh-user your-ssh-user \
  --cert secret/certificate.p12 \
  --password-file secret/pwd.txt \
  --subscriptions secret/mobilithek_subscriptions.json \
  --hf-token secret/huggingface.token
```

If you also want to replace the remote env file in the same step:

```bash
./deploy/ionos/push-secrets-and-start.sh \
  --host your-vps-hostname-or-ip \
  --ssh-user your-ssh-user \
  --cert secret/certificate.p12 \
  --password-file secret/pwd.txt \
  --subscriptions secret/mobilithek_subscriptions.json \
  --hf-token secret/huggingface.token \
  --env-file deploy/ionos/woladen-live.env.example
```

## Required Secrets

Place these files in `/etc/woladen/`:

- `certificate.p12`
- `pwd.txt`
- `mobilithek_subscriptions.json`
- `huggingface.token` for the dataset upload job if you enable daily archive uploads

The default env file references exactly those paths. The helper script
`push-secrets-and-start.sh` uploads them with `root:<app-group>` ownership
and `0640` permissions.

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
