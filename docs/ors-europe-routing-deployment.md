# Self-Hosted ORS Europe Deployment

Date: 2026-06-27

This runbook documents the self-hosted OpenRouteService Europe deployment used
for full-Europe `driving-car` routing. It is separate from the
`live-eu.woladen.de` Docker Compose API stack documented in
`docs/live-eu-routing-deploy-notes.md`.

## Hosts

### ORS builder/runtime host

- SSH alias: `wingki01`
- Public IP: `141.47.5.55`
- SSH user: `raphael`
- SSH key: `~/.ssh/woladen_live_deploy`
- ORS home: `/home/raphael/ors`
- Java: system OpenJDK 21
- Docker: installed, but `raphael` cannot access `/var/run/docker.sock`
- Sudo: not available for `raphael`

ORS therefore runs directly from the ORS release JAR, not Docker.

### Live EU host

- Hostname: `live-eu.woladen.de`
- IP during deployment: `141.47.91.39`
- SSH alias in local config: `woladen-dev-container`
- SSH user: `raphael`
- Direct traffic from `live-eu` to `141.47.5.55` was blocked during deployment,
  even for SSH port `22`.
- Traffic from `wingki01` to `live-eu` works. The live host exposure therefore
  uses a reverse SSH tunnel initiated from `wingki01`.

## Runtime Architecture

```text
External clients
  |
  | http://141.47.5.55:8188/ors/...
  v
wingki01:0.0.0.0:8188
  socat public proxy
  |
  v
wingki01:127.0.0.1:8082
  ORS Java process

External clients
  |
  | http://live-eu.woladen.de:8188/ors/...
  v
live-eu:0.0.0.0:8188
  Python TCP proxy
  |
  v
live-eu:127.0.0.1:18188
  reverse SSH tunnel endpoint
  |
  v
wingki01:127.0.0.1:8082
  ORS Java process
```

The ORS Java process itself stays bound to localhost on `wingki01`. Public
access is provided by lightweight TCP proxies.

## ORS Files On `wingki01`

```text
/home/raphael/ors/
  ors.jar
  ors-config.yml
  data/europe-latest.osm.pbf
  data/europe-latest.osm.pbf.md5.remote
  data/europe-latest.resolved-url
  graphs/driving-car/driving-car/
  logs/ors.log
  logs/setup-run.log
  logs/public-8188-proxy.log
  logs/live-reverse-tunnel.log
  bin/start-ors.sh
  bin/download-and-run.sh
  bin/start-public-8188-proxy.sh
  bin/start-live-reverse-tunnel.sh
```

## Graph Input

The Europe graph was built from the Geofabrik Europe PBF resolved from:

```text
https://download.geofabrik.de/europe-latest.osm.pbf
```

During deployment, `latest` resolved to:

```text
https://download.geofabrik.de/europe-260626.osm.pbf
```

with MD5:

```text
624965d7999e7266d89b171c31f1e1f0  europe-260626.osm.pbf
```

Important: do not validate `europe-latest.osm.pbf` against
`europe-latest.osm.pbf.md5` after the redirect target has changed. The setup
script resolves `latest` to the dated PBF first and then fetches the matching
dated `.md5` file to avoid a stale-checksum race.

## ORS Configuration

Current ORS process:

```bash
java \
  -Djava.awt.headless=true \
  -server \
  -XX:+UseG1GC \
  -XX:ParallelGCThreads=16 \
  -Xms8g \
  -Xmx96g \
  -jar /home/raphael/ors/ors.jar
```

Current ORS bind:

```yaml
server:
  address: 127.0.0.1
  port: 8082
  servlet:
    context-path: /ors
```

Only `driving-car` is enabled. The Europe graph build completed successfully:

```text
Finished at: 2026-06-27 11:49:26 UTC
Total time: 45012.82s
Edges: 116327988
Nodes: 97992560
Graph size: about 48G
```

## Process Supervision

No systemd user service is installed. Long-running processes are managed with
`tmux` or `nohup` because the user does not have sudo.

On `wingki01`:

```bash
tmux list-sessions
```

Expected sessions:

```text
ors
ors-public-8188
ors-live-reverse-tunnel
```

On `live-eu`, `tmux` is not installed. The public proxy is a detached Python
process started with `nohup`:

```bash
ps -eo pid,args | awk '/[o]rs-public-8188-proxy.py/'
```

## Public Ports

### `wingki01` direct public endpoint

`wingki01` listens publicly on port `8188`:

```text
0.0.0.0:8188 -> 127.0.0.1:8082
```

Started by:

```bash
/home/raphael/ors/bin/start-public-8188-proxy.sh
```

Implementation:

```bash
socat -d -d TCP4-LISTEN:8188,bind=0.0.0.0,reuseaddr,fork TCP4:127.0.0.1:8082
```

### `live-eu.woladen.de` public endpoint

Because `live-eu` cannot directly reach `141.47.5.55`, access through
`live-eu.woladen.de:8188` uses:

1. Reverse SSH tunnel from `wingki01` to `live-eu`:

   ```text
   live-eu:127.0.0.1:18188 -> wingki01:127.0.0.1:8082
   ```

2. Python TCP proxy on `live-eu`:

   ```text
   live-eu:0.0.0.0:8188 -> live-eu:127.0.0.1:18188
   ```

Dedicated tunnel key on `wingki01`:

```text
/home/raphael/.ssh/ors_live_tunnel_ed25519
```

The corresponding public key is installed in `live-eu`'s
`/home/raphael/.ssh/authorized_keys` with a source restriction:

```text
from="141.47.5.55",no-pty,no-X11-forwarding,no-agent-forwarding ...
```

Reverse tunnel command:

```bash
ssh -i /home/raphael/.ssh/ors_live_tunnel_ed25519 \
  -N -T \
  -o BatchMode=yes \
  -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=30 \
  -o ServerAliveCountMax=3 \
  -R 127.0.0.1:18188:127.0.0.1:8082 \
  raphael@141.47.91.39
```

Live proxy files:

```text
/home/raphael/bin/ors-public-8188-proxy.py
/home/raphael/bin/start-ors-public-8188-proxy.sh
/home/raphael/logs/ors-public-8188-proxy.log
/home/raphael/logs/ors-public-8188-nohup.log
```

## Health Checks

From any external machine that can reach the public ports:

```bash
curl -fsS http://141.47.5.55:8188/ors/v2/health
curl -fsS http://live-eu.woladen.de:8188/ors/v2/health
```

Expected response:

```json
{"status":"ready"}
```

Route smoke test:

```bash
curl -fsS -X POST \
  http://live-eu.woladen.de:8188/ors/v2/directions/driving-car/json \
  -H 'Content-Type: application/json' \
  --data '{"coordinates":[[13.38886,52.51704],[13.39763,52.52941]]}'
```

The response should contain ORS routing metadata and one route. During deployment
this returned approximately:

```text
distance_m 1883.9
duration_s 418.4
```

## Operational Checks

Check ORS on `wingki01`:

```bash
ssh wingki01 '
  pgrep -af "[o]rs.jar"
  ss -ltnp | awk "NR==1 || /:8082|:8188/"
  curl -sS http://127.0.0.1:8082/ors/v2/health
  du -sh /home/raphael/ors/graphs
'
```

Check the reverse tunnel from `wingki01`:

```bash
ssh wingki01 '
  tmux list-sessions
  pgrep -af "ssh .*18188.*8082"
  tail -n 50 /home/raphael/ors/logs/live-reverse-tunnel.log
'
```

Check the live proxy:

```bash
ssh woladen-dev-container '
  ss -ltnp | awk "NR==1 || /:18188|:8188/"
  ps -eo pid,args | awk "/[o]rs-public-8188-proxy.py/"
  curl -sS http://127.0.0.1:8188/ors/v2/health
  tail -n 50 /home/raphael/logs/ors-public-8188-proxy.log
'
```

## Restart Procedures

Restart only the direct `wingki01:8188` proxy:

```bash
ssh wingki01 '
  tmux kill-session -t ors-public-8188 2>/dev/null || true
  tmux new-session -d -s ors-public-8188 /home/raphael/ors/bin/start-public-8188-proxy.sh
'
```

Restart only the reverse SSH tunnel:

```bash
ssh wingki01 '
  tmux kill-session -t ors-live-reverse-tunnel 2>/dev/null || true
  tmux new-session -d -s ors-live-reverse-tunnel /home/raphael/ors/bin/start-live-reverse-tunnel.sh
'
```

Restart only the live-side public proxy:

```bash
ssh woladen-dev-container '
  ps -eo pid,args | awk "/[o]rs-public-8188-proxy.py/ {print \$1}" | xargs -r kill
  nohup /home/raphael/bin/start-ors-public-8188-proxy.sh \
    >/home/raphael/logs/ors-public-8188-nohup.log 2>&1 &
'
```

Restart ORS itself only when necessary. The Europe graph build is expensive; a
clean rebuild took about 12.5 hours. If ORS must be restarted and the graph is
already present:

```bash
ssh wingki01 '
  tmux kill-session -t ors 2>/dev/null || true
  tmux new-session -d -s ors /home/raphael/ors/bin/start-ors.sh
'
```

## Known Caveats

- `141.47.5.55:8188` is directly public.
- `live-eu.woladen.de:8188` depends on a reverse SSH tunnel from `wingki01`.
- `live-eu` could not reach `141.47.5.55` directly during deployment, even on
  port `22`. If the rack ACL/routing issue is fixed later, the live-side reverse
  tunnel can be replaced with a direct proxy.
- None of the ORS helper processes are systemd services. They will not survive a
  host reboot unless restarted manually or converted to user/system services.
- The ORS graph uses the full Geofabrik Europe extract. Avoid country-only
  clipped graphs for cross-border routing.
