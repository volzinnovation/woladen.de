# Live EU Routing Deploy Notes

Date: 2026-06-26

These notes capture the deployment context observed while preparing the backend
routing API rollout on `live-eu.woladen.de`.

## Host Snapshot

- Public hostname: `live-eu.woladen.de`
- Resolved host during inspection: `141.47.91.39`
- SSH user used by Codex: `raphael`
- SSH identity: `~/.ssh/woladen_live_deploy`
- Operating system: Ubuntu 26.04 LTS
- Docker binary: `/usr/bin/docker`
- Docker Compose: `Docker Compose version v5.1.3`
- `raphael` is in the `docker` group and can run Docker commands without sudo.
- Passwordless sudo is not available. `sudo -n true` fails with interactive
  authentication required.

## Running Stack

The live EU API is not the older IONOS/systemd deployment documented for
`live.woladen.de`. It is a Docker Compose stack under:

```text
/srv/woladen/Woladen.de-analytics/deploy/onboarded-ingest/compose.yml
```

The relevant source tree is:

```text
/srv/woladen/Woladen.de-analytics
```

The stack is not a normal Git checkout on the host. It has a
`.deployed-source-revision` marker, but no `.git` directory was found during
inspection. Treat it as deployed source, not as a repository working tree.

Observed API container:

```text
woladen-commercial-onboarded-api-1
```

It runs:

```text
python scripts/live_api.py
```

and exposes the API on:

```text
127.0.0.1:8001
```

The public `https://live-eu.woladen.de` frontend proxy routes to that service.

## Health Checks Observed

These public endpoints were healthy before routing deployment work continued:

```bash
curl -fsS https://live-eu.woladen.de/healthz
curl -fsS 'https://live-eu.woladen.de/v1/catalog/search?lat=52.52&lon=13.405&radius_m=1000&limit=1'
```

`/healthz` returned `ok: true`; the catalog search returned station data.

## Deployment Implications

- Do not use `deploy/ionos/*` for `live-eu.woladen.de`; that path targets the
  older `live.woladen.de` systemd deployment.
- Do not blindly copy local `backend/api.py` over the remote file. The remote
  `backend/api.py` includes live-EU-specific geocoding and German proxy logic.
- Remote `backend/open_catalog.py` is close to the local open catalog module and
  needs the route corridor query additions.
- Remote `backend/config.py` already has OpenRouteService secret discovery for
  geocoding. Routing should reuse those credentials or add compatible route
  config fields.
- The Compose file already mounts secrets under `/run/secrets/woladen-local`,
  `/run/secrets/woladen-de`, and `/app/secret`.
- Public API exposure is controlled by a Caddy path allowlist. Adding a backend
  route is not enough; Caddy must also proxy the public path to `127.0.0.1:8001`.

## Caddy Proxy Notes

The system service reads:

```text
/etc/caddy/Caddyfile
```

which imports:

```text
/etc/woladen/*.Caddyfile
```

During the routing deployment, the active Caddy config was newer than the
root-owned `/etc/woladen/onboarded-ingest.Caddyfile`. The writable deployed
source Caddyfile was:

```text
/srv/woladen/Woladen.de-analytics/deploy/onboarded-ingest/Caddyfile
```

The public routing endpoint initially returned Caddy's own `404` even though
`POST http://127.0.0.1:8001/v1/routes/chargers` returned `200` on the host. The
fix was to add `/v1/routes/*` to the `@live_api` path matcher and reload Caddy:

```bash
cd /srv/woladen/Woladen.de-analytics
caddy validate --config deploy/onboarded-ingest/Caddyfile
caddy reload --config deploy/onboarded-ingest/Caddyfile
```

For persistence across Caddy service restarts, copy the deployed Caddyfile into
the imported `/etc/woladen` path with sudo after reviewing the diff:

```bash
sudo cp /srv/woladen/Woladen.de-analytics/deploy/onboarded-ingest/Caddyfile \
  /etc/woladen/onboarded-ingest.Caddyfile
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy
```

## GitHub Actions Deploy Blocker

The local commit was pushed to `origin/main`, but the GitHub Actions deploy run
failed in its SSH preparation step. The failed run showed these required secrets
as empty:

```text
LIVE_DEPLOY_SSH_KNOWN_HOSTS
LIVE_API_PUSH_TOKEN
```

That is not a server package problem. Fixing the Actions deploy path requires
setting repository/environment secrets, not sudo on the host.

Suggested secret setup:

```bash
ssh-keyscan -H live-eu.woladen.de
```

Store the resulting known-hosts line as `LIVE_DEPLOY_SSH_KNOWN_HOSTS`.

Set `LIVE_API_PUSH_TOKEN` to the production push token expected by the live API.
Do not print the token in logs or commit it to the repository.

## Sudo Bootstrap Instructions

No sudo installation was required for the current host state: Docker, Compose,
SSH access, and write access to the deployed source directory already work.

Use the following only if rebuilding the host or repairing a broken Docker
installation on Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg git rsync openssh-server

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

. /etc/os-release
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null

sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker raphael
```

After changing Docker group membership, log out and back in before running:

```bash
docker ps
docker compose version
```

If the deployed source directory needs to be restored for the `raphael` user:

```bash
sudo mkdir -p /srv/woladen
sudo chown -R raphael:raphael /srv/woladen
```

Only run the `chown` command if ownership is actually wrong. The directory was
already owned by `raphael` during inspection.

## Manual API Deploy Shape

Until the GitHub Actions secrets are fixed, the practical deployment path is:

```bash
ssh -i ~/.ssh/woladen_live_deploy raphael@live-eu.woladen.de
cd /srv/woladen/Woladen.de-analytics

# Patch backend files carefully. Preserve live-EU-specific api.py behavior.
python3 -m py_compile backend/api.py backend/config.py backend/open_catalog.py backend/routing.py

cd deploy/onboarded-ingest
docker compose build api
docker compose up -d api
docker compose ps
```

Post-deploy smoke tests:

```bash
curl -fsS https://live-eu.woladen.de/healthz
curl -fsS 'https://live-eu.woladen.de/v1/catalog/search?lat=52.52&lon=13.405&radius_m=1000&limit=1'
curl -fsS -X POST 'https://live-eu.woladen.de/v1/routes/chargers' \
  -H 'content-type: application/json' \
  --data '{"origin":{"lat":52.52,"lon":13.405},"destination":{"lat":52.3906,"lon":13.0645},"filters":{"min_power_kw":50},"filter_mode":"route_calculation"}'
```
