-- Operator intent is committed atomically with HELD -> NEW; normal delivery path unchanged.
CREATE TABLE dlt_intake_action (
    action_id uuid PRIMARY KEY,
    intake_id uuid NOT NULL REFERENCES dlt_intake_record(intake_id),
    expected_updated_at timestamptz NOT NULL,
    actor varchar(100) NOT NULL,
    reason varchar(500) NOT NULL,
    queued_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX dlt_intake_action_history ON dlt_intake_action(intake_id,queued_at DESC,action_id);
CREATE INDEX dlt_recovery_attempt_history ON dlt_recovery_attempt(operation_id,started_at DESC,attempt_id);
CREATE INDEX dlt_intake_held_page ON dlt_intake_record(cluster_alias,topic,partition_id,record_offset)
    WHERE state='HELD';
