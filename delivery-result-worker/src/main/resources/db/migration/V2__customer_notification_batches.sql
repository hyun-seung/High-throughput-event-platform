CREATE TABLE customer_notification_lane (
    tenant_id bigint PRIMARY KEY CHECK (tenant_id > 0)
);

CREATE TABLE customer_notification_batch (
    batch_id uuid PRIMARY KEY,
    tenant_id bigint NOT NULL REFERENCES customer_notification_lane(tenant_id),
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

-- One customer's HTTP can occupy at most one lane across worker replicas.
CREATE UNIQUE INDEX customer_notification_active_lane ON customer_notification_batch (tenant_id)
    WHERE status = 'IN_FLIGHT';
CREATE INDEX customer_notification_retry_due ON customer_notification_batch (tenant_id, next_attempt_at, batch_id)
    WHERE status = 'PENDING';

ALTER TABLE customer_notification_outbox
    ADD COLUMN batch_id uuid REFERENCES customer_notification_batch(batch_id);
CREATE INDEX customer_notification_batch_items ON customer_notification_outbox (batch_id);
CREATE INDEX customer_notification_unbatched ON customer_notification_outbox (created_at, result_event_id)
    WHERE status = 'PENDING' AND batch_id IS NULL;
