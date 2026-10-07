CREATE TABLE tbl_webhook_lane (
    client_id bigint PRIMARY KEY CHECK (client_id > 0)
);

CREATE TABLE tbl_webhook_batch (
    batch_id uuid PRIMARY KEY,
    client_id bigint NOT NULL REFERENCES tbl_webhook_lane(client_id),
    destination_url text NOT NULL,
    request_body text NOT NULL,
    item_count integer NOT NULL CHECK (item_count BETWEEN 1 AND 100),
    status varchar(16) NOT NULL CHECK (status IN ('PENDING', 'IN_FLIGHT', 'DELIVERED', 'EXHAUSTED')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 21),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_token uuid,
    lease_until timestamptz,
    last_error varchar(64),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((status = 'IN_FLIGHT') = (lease_token IS NOT NULL AND lease_until IS NOT NULL))
);

CREATE UNIQUE INDEX tbl_webhook_active_lane ON tbl_webhook_batch (client_id)
    WHERE status = 'IN_FLIGHT';
CREATE INDEX tbl_webhook_due ON tbl_webhook_batch (client_id, next_attempt_at, batch_id)
    WHERE status = 'PENDING';

CREATE TABLE tbl_webhook_outbox (
    webhook_id uuid PRIMARY KEY,
    client_msg_id varchar(40) NOT NULL UNIQUE,
    client_id bigint NOT NULL REFERENCES tbl_webhook_lane(client_id),
    command_json jsonb NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'DELIVERED', 'EXHAUSTED')),
    batch_id uuid REFERENCES tbl_webhook_batch(batch_id),
    captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX tbl_webhook_unbatched ON tbl_webhook_outbox (client_id, captured_at, webhook_id)
    WHERE status = 'PENDING' AND batch_id IS NULL;

CREATE TABLE tbl_webhook_hist (
    batch_id uuid NOT NULL REFERENCES tbl_webhook_batch(batch_id),
    attempt_count integer NOT NULL CHECK (attempt_count BETWEEN 1 AND 21),
    client_id bigint NOT NULL,
    destination_url text NOT NULL,
    item_count integer NOT NULL CHECK (item_count BETWEEN 1 AND 100),
    sent_at timestamptz NOT NULL,
    http_status integer,
    acknowledged boolean NOT NULL,
    error varchar(64),
    PRIMARY KEY (batch_id, attempt_count)
);
