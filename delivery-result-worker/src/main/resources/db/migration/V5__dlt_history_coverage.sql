-- Recovery may interpret absent history only within an explicitly enabled, retained interval.
-- Enable only after every ingress/dispatch writer understands dlt_recovery_hold.
CREATE TABLE dlt_history_coverage (
    singleton boolean PRIMARY KEY CHECK (singleton),
    complete_since timestamptz NOT NULL,
    origin_restore_enabled boolean NOT NULL DEFAULT false
);
INSERT INTO dlt_history_coverage(singleton, complete_since) VALUES (true, clock_timestamp());
