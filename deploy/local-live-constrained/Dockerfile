FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements-live.txt /app/requirements-live.txt
RUN python -m pip install --no-cache-dir -r /app/requirements-live.txt

COPY backend /app/backend
COPY scripts /app/scripts
COPY data/chargers_fast.csv /app/data/chargers_fast.csv
COPY data/chargers_full.csv /app/data/chargers_full.csv
COPY data/chargers_fast.geojson /app/data/chargers_fast.geojson
COPY data/live_provider_overrides.json /app/data/live_provider_overrides.json
COPY data/mobilithek_afir_provider_configs.json /app/data/mobilithek_afir_provider_configs.json
COPY data/mobilithek_afir_static_matches.csv /app/data/mobilithek_afir_static_matches.csv
COPY deploy/local-live-constrained/entrypoint.sh /usr/local/bin/woladen-live-entrypoint

RUN groupadd --system --gid 10001 woladen \
    && useradd --system --uid 10001 --gid 10001 --home-dir /nonexistent --shell /usr/sbin/nologin woladen \
    && mkdir -p /var/lib/woladen /run/secrets/woladen \
    && chown -R 10001:10001 /var/lib/woladen /run/secrets/woladen \
    && chmod 0755 /usr/local/bin/woladen-live-entrypoint

USER 10001:10001

ENTRYPOINT ["woladen-live-entrypoint"]
CMD ["python", "scripts/live_ingester.py", "--bootstrap-only"]
