CREATE TABLE delivery_resolution_action (
    action_id uuid PRIMARY KEY,
    tenant_id bigint NOT NULL CHECK (tenant_id > 0),
    request_key uuid NOT NULL,
    delivery_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    expected_version bigint NOT NULL CHECK (expected_version >= 0),
    decision varchar(16) NOT NULL CHECK (decision IN ('SUCCEEDED','FAILED')),
    actor varchar(100) NOT NULL,
    reason varchar(500) NOT NULL,
    status varchar(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','APPLIED','REJECTED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz
);
CREATE INDEX delivery_resolution_pending ON delivery_resolution_action(delivery_id) WHERE status='PENDING';
CREATE INDEX delivery_resolution_pending_tenant ON delivery_resolution_action(tenant_id,action_id) WHERE status='PENDING';
