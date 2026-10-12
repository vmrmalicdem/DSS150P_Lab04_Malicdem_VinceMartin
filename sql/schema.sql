-- Idempotent schema: safe to run on every pipeline start.
-- record_hash is the stable business key (hash of all source columns); source_row_key is the
-- row position in the source snapshot. Both are unique, so a changed source fails loudly.

CREATE TABLE IF NOT EXISTS staging_traffic (
    record_hash       TEXT             PRIMARY KEY,
    source_row_key    BIGINT           NOT NULL UNIQUE,
    batch_id          INTEGER          NOT NULL,
    highest_layer     TEXT             NOT NULL,
    transport_layer   TEXT             NOT NULL,
    source_ip         INTEGER          NOT NULL,
    dest_ip           TEXT             NOT NULL,
    source_port       INTEGER          NOT NULL,
    dest_port         INTEGER          NOT NULL,
    packet_length     INTEGER          NOT NULL,
    packets_per_time  DOUBLE PRECISION NOT NULL,
    target            SMALLINT         NOT NULL CHECK (target IN (0, 1)),
    pipeline_run_id   TEXT             NOT NULL,
    processed_at_utc  TIMESTAMPTZ      NOT NULL
);

CREATE TABLE IF NOT EXISTS curated_traffic (
    record_hash       TEXT             PRIMARY KEY,
    source_row_key    BIGINT           NOT NULL UNIQUE,
    highest_layer     TEXT             NOT NULL,
    transport_layer   TEXT             NOT NULL,
    dest_ip           TEXT             NOT NULL,
    source_port       INTEGER          NOT NULL,
    dest_port         INTEGER          NOT NULL,
    packet_length     INTEGER          NOT NULL,
    packets_per_time  DOUBLE PRECISION NOT NULL,
    target            SMALLINT         NOT NULL CHECK (target IN (0, 1)),
    pipeline_run_id   TEXT             NOT NULL,
    loaded_at_utc     TIMESTAMPTZ      NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS quarantine_traffic (
    source_row_key    BIGINT           PRIMARY KEY,
    reject_reason     TEXT             NOT NULL,
    row_payload       JSONB            NOT NULL,
    pipeline_run_id   TEXT             NOT NULL,
    processed_at_utc  TIMESTAMPTZ      NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_curated_transport_layer ON curated_traffic (transport_layer);
CREATE INDEX IF NOT EXISTS ix_curated_target          ON curated_traffic (target);
