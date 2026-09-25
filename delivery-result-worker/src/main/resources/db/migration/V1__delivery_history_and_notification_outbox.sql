CREATE TABLE delivery_history (
    delivery_id uuid PRIMARY KEY,
    result_event_id uuid NOT NULL UNIQUE,
    tenant_id bigint NOT NULL CHECK (tenant_id > 0),
    delivery_type varchar(100) NOT NULL,
    outcome varchar(16) NOT NULL CHECK (outcome IN ('DELIVERED', 'FAILED', 'EXPIRED')),
    reason varchar(200) NOT NULL,
    route_order smallint NOT NULL CHECK (route_order IN (1, 2)),
    attempt_id uuid NOT NULL,
    provider varchar(100) NOT NULL,
    occurred_at timestamptz NOT NULL,
    result_at timestamptz NOT NULL,
    finalized_at timestamptz NOT NULL,
    deadline timestamptz NOT NULL,
    result_json jsonb NOT NULL,
    stored_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX delivery_history_tenant_time ON delivery_history (tenant_id, finalized_at, delivery_id);

-- Reservation only. The next implementation owns HTTP batching, leases and retry transitions.
CREATE TABLE customer_notification_outbox (
    result_event_id uuid PRIMARY KEY REFERENCES delivery_history(result_event_id),
    status varchar(16) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'DELIVERED', 'EXHAUSTED')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND 21),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX customer_notification_due ON customer_notification_outbox (next_attempt_at, result_event_id)
    WHERE status = 'PENDING';
