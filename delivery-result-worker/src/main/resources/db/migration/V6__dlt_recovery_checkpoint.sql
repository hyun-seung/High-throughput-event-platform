-- Only explicitly submitted recovery operations receive a replay checkpoint.
-- Existing rows stay manual; never infer the original input from a dispatch command.
ALTER TABLE dlt_recovery_operation ADD COLUMN checkpoint_json text;
CREATE INDEX dlt_recovery_auto_pending ON dlt_recovery_operation
    (cluster_alias, target_topic, updated_at, operation_id)
    WHERE state = 'PENDING' AND checkpoint_json IS NOT NULL;
