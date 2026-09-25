CREATE TABLE delivery_cleanup_outbox (
    result_event_id uuid PRIMARY KEY REFERENCES delivery_history(result_event_id),
    status varchar(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'DONE')),
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_token uuid,
    lease_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz
);
CREATE INDEX delivery_cleanup_due ON delivery_cleanup_outbox (next_attempt_at, result_event_id) WHERE status = 'PENDING';

-- SQL evidence is required even for pre-existing results. Legacy DynamoDB is retained until fenced/tracked.
INSERT INTO delivery_cleanup_outbox(result_event_id)
SELECT h.result_event_id FROM delivery_history h
JOIN customer_notification_outbox n ON n.result_event_id = h.result_event_id;
