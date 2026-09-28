ALTER TABLE delivery_resolution_action
    ADD COLUMN next_check_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ADD COLUMN check_count integer NOT NULL DEFAULT 0 CHECK (check_count >= 0);
CREATE INDEX delivery_resolution_next_check ON delivery_resolution_action(next_check_at, action_id)
    WHERE status = 'PENDING';
