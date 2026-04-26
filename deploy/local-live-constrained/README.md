# Local constrained live-ingestion environment

This setup runs the live backend in Docker or OrbStack with the live state stored
on a deliberately small tmpfs volume. It is meant to catch disk and inode failure
modes before a VPS rollout without touching production state.

## Safety properties

- State lives in `/var/lib/woladen` inside the containers, backed by the
  `live_state` tmpfs volume from `compose.yml`.
- The API binds to `127.0.0.1` only, and `WOLADEN_LIVE_API_PUSH_ENABLED=0`
  disables `/v1/push` routes for this environment.
- Hugging Face archive upload variables are empty, so archive commands build
  local `.tgz` files only.
- Mobilithek secrets are bind-mounted read-only from local paths. They are not
  copied into the image and are excluded from Docker build context by
  `.dockerignore`.

If OrbStack is installed but `docker` is not on `PATH`, use:

```bash
export PATH="/Applications/OrbStack.app/Contents/MacOS/xbin:$PATH"
```

## Mobilithek secrets

No secrets are copied into the image or written into this repository. Pass the
local files from `secret/` as environment variables before starting containers:

```bash
export WOLADEN_MACHINE_CERT_P12_B64="$(base64 < secret/certificate.p12 | tr -d '\n')"
export WOLADEN_MACHINE_CERT_PASSWORD="$(cat secret/pwd.txt)"
export WOLADEN_LIVE_SUBSCRIPTION_REGISTRY_JSON="$(cat secret/mobilithek_subscriptions.json)"
```

The container entrypoint writes those values to `/run/secrets/woladen/*` inside
the ephemeral container filesystem and unsets the raw environment values before
starting the app process. Docker still receives the variables at container
creation time, so do not paste command output from `docker inspect` into logs or
issues.

## Build

Run commands from the repository root:

```bash
docker compose -f deploy/local-live-constrained/compose.yml build
```

## VPS resource envelope

The Compose services default to the current VPS-sized runtime envelope:

```bash
export WOLADEN_CONSTRAINED_CPUS=2
export WOLADEN_CONSTRAINED_MEMORY=2g
export WOLADEN_CONSTRAINED_MEMORY_SWAP=6g
```

`WOLADEN_CONSTRAINED_MEMORY_SWAP=6g` is Docker's memory-plus-swap limit:
2 GB RAM plus 4 GB swap. Docker containers do not get a normal managed swap
file unless run with extra privileges; this cgroup limit is the closest local
OrbStack/Docker equivalent without weakening the container isolation.

The state volume defaults to an 80 GB VPS-sized filesystem with an inode count
matching the common ext4 ratio of one inode per 16 KiB:

```bash
export WOLADEN_CONSTRAINED_STATE_SIZE=80g
export WOLADEN_CONSTRAINED_STATE_INODES=5242880
```

If you change either value after the volume exists, recreate it with the cleanup
command below.

Verify the effective container limits:

```bash
docker compose -f deploy/local-live-constrained/compose.yml run --rm live-tools \
  sh -lc 'cat /sys/fs/cgroup/cpu.max; cat /sys/fs/cgroup/memory.max; cat /sys/fs/cgroup/memory.swap.max'
```

For the defaults, `cpu.max` should read `200000 100000`, which is a 2-CPU
quota. `memory.max` should read `2147483648` and `memory.swap.max` should read
`4294967296`.

## Bounded pull/archive cycle

Bootstrap the SQLite state:

```bash
docker compose -f deploy/local-live-constrained/compose.yml run --rm live-tools \
  python scripts/live_ingester.py --bootstrap-only
```

Run one bounded pull pass. This uses noauth providers by default, and uses the
mounted mTLS certificate/subscription registry when those host paths are set:

```bash
docker compose -f deploy/local-live-constrained/compose.yml run --rm live-tools \
  python scripts/live_ingester.py --max-providers 1
```

Drain a bounded number of queued receipts:

```bash
docker compose -f deploy/local-live-constrained/compose.yml run --rm live-tools \
  python scripts/live_queue_worker.py --max-items 25
```

Archive today's local raw response journals without uploading them:

```bash
TARGET_DATE="$(date +%F)"
docker compose -f deploy/local-live-constrained/compose.yml run --rm live-tools \
  python scripts/live_archive_logs.py --date "$TARGET_DATE"
```

Run the local API for inspection:

```bash
docker compose -f deploy/local-live-constrained/compose.yml up live-api
curl -fsS http://127.0.0.1:8001/healthz
curl -fsS http://127.0.0.1:8001/v1/push || true
```

The second `curl` should return a disabled/404 response in this environment.

## Inode stress test

The stress command writes many synthetic raw-response journal records and SQLite
queue rows, then builds a local archive. It also creates a legacy-style tiny-file
probe to demonstrate the low inode/file budget without millions of messages:

```bash
export WOLADEN_CONSTRAINED_STATE_SIZE=256m
export WOLADEN_CONSTRAINED_STATE_INODES=4096
docker compose -f deploy/local-live-constrained/compose.yml down --volumes --remove-orphans
docker compose -f deploy/local-live-constrained/compose.yml run --rm live-tools \
  python scripts/live_inode_stress.py \
    --records 2000 \
    --legacy-file-probe \
    --legacy-file-limit 10000
```

Expected signal:

- `journal_queue.records_written` equals the requested record count.
- `journal_queue.queue_stats.pending_count` increases by that count.
- `file_tree_counts.raw.file_count` stays small because records are appended to
  `records.jsonl`.
- `legacy_file_probe.result` is `inode_or_space_exhausted` when the override
  inode budget is lower than the probe limit. With the default 80 GB / 5,242,880
  inode VPS-sized volume, keep the legacy probe disabled or raise the probe
  limit only for deliberate long-running checks.

## Long-running local stack

To run the API, ingester loop, and queue worker together against the same
constrained state volume:

```bash
docker compose -f deploy/local-live-constrained/compose.yml --profile workers up
```

## Cleanup

Remove containers and the constrained tmpfs state volume:

```bash
docker compose -f deploy/local-live-constrained/compose.yml down --volumes --remove-orphans
```
