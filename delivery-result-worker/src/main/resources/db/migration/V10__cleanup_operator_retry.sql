ALTER TABLE delivery_cleanup_outbox ADD COLUMN version bigint NOT NULL DEFAULT 0 CHECK (version >= 0);

CREATE TABLE delivery_cleanup_action (
    action_id uuid PRIMARY KEY,
    tenant_id bigint NOT NULL,
    result_event_id uuid NOT NULL REFERENCES delivery_cleanup_outbox(result_event_id),
    expected_version bigint NOT NULL CHECK (expected_version >= 0),
    actor varchar(100) NOT NULL,
    reason varchar(500) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
