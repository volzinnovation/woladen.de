CREATE TABLE IF NOT EXISTS ingest_sources (
    source_uid TEXT PRIMARY KEY,
    country_code TEXT NOT NULL,
    source_kind TEXT NOT NULL DEFAULT 'afir_dynamic',
    display_name TEXT NOT NULL DEFAULT '',
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    accepts_push BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ingest_sources_country
    ON ingest_sources (country_code);

CREATE TABLE IF NOT EXISTS raw_payloads (
    payload_sha256 TEXT PRIMARY KEY,
    first_received_at TIMESTAMPTZ NOT NULL,
    country_code TEXT NOT NULL,
    source_uid TEXT NOT NULL,
    storage_uri TEXT NOT NULL,
    byte_length BIGINT NOT NULL,
    content_type TEXT NOT NULL DEFAULT '',
    content_encoding TEXT NOT NULL DEFAULT '',
    inline_payload BYTEA
);

CREATE INDEX IF NOT EXISTS idx_raw_payloads_source_received
    ON raw_payloads (country_code, source_uid, first_received_at DESC);

CREATE TABLE IF NOT EXISTS push_receipts (
    receipt_id UUID PRIMARY KEY,
    received_at TIMESTAMPTZ NOT NULL,
    country_code TEXT NOT NULL,
    source_uid TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL REFERENCES raw_payloads(payload_sha256),
    byte_length BIGINT NOT NULL,
    duplicate_payload BOOLEAN NOT NULL DEFAULT FALSE,
    result TEXT NOT NULL DEFAULT 'queued',
    content_type TEXT NOT NULL DEFAULT '',
    content_encoding TEXT NOT NULL DEFAULT '',
    request_path TEXT NOT NULL DEFAULT '',
    request_query TEXT NOT NULL DEFAULT '',
    request_headers JSONB NOT NULL DEFAULT '{}'::jsonb,
    remote_addr TEXT NOT NULL DEFAULT '',
    error_text TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_push_receipts_received
    ON push_receipts (received_at DESC);

CREATE INDEX IF NOT EXISTS idx_push_receipts_source_received
    ON push_receipts (country_code, source_uid, received_at DESC);

CREATE TABLE IF NOT EXISTS ingest_tasks (
    task_id UUID PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL,
    country_code TEXT NOT NULL,
    source_uid TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL REFERENCES raw_payloads(payload_sha256),
    receipt_id UUID NOT NULL REFERENCES push_receipts(receipt_id),
    task_kind TEXT NOT NULL DEFAULT 'parse_dynamic_payload',
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    locked_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    error_text TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_ingest_tasks_status_created
    ON ingest_tasks (status, created_at);

