-- Operations traffic only; no new write on the normal delivery path.
CREATE TABLE dlt_recovery_operation (
    operation_id uuid PRIMARY KEY,
    cluster_alias varchar(100) NOT NULL,
    source_topic varchar(249) NOT NULL,
    source_partition integer NOT NULL CHECK (source_partition >= 0),
    source_offset bigint NOT NULL CHECK (source_offset >= 0),
    value_sha256 char(64) NOT NULL,
    execution_id uuid NOT NULL,
    target_topic varchar(249) NOT NULL,
    command_json text NOT NULL,
    state varchar(16) NOT NULL CHECK (state IN ('PENDING', 'ACKNOWLEDGED', 'HELD')),
    last_attempt uuid,
    target_partition integer,
    target_offset bigint,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (cluster_alias, source_topic, source_partition, source_offset),
    CHECK (state <> 'ACKNOWLEDGED' OR (target_partition IS NOT NULL AND target_offset IS NOT NULL))
);
CREATE TABLE dlt_recovery_attempt (
    attempt_id uuid PRIMARY KEY,
    operation_id uuid NOT NULL REFERENCES dlt_recovery_operation(operation_id),
    actor varchar(100) NOT NULL,
    reason varchar(500) NOT NULL,
    outcome varchar(32) NOT NULL CHECK (outcome IN ('STARTED', 'ACKNOWLEDGED', 'UNCONFIRMED', 'STATE_CHANGED')),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz
);
CREATE INDEX dlt_recovery_pending ON dlt_recovery_operation(updated_at) WHERE state <> 'ACKNOWLEDGED';
CREATE INDEX delivery_history_request_key ON delivery_history
    (tenant_id, (COALESCE(result_json->>'requestKey', delivery_id::text)));
