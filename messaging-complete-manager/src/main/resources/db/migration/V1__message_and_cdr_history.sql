CREATE TABLE tbl_msg_hist (
    client_msg_id varchar(40) PRIMARY KEY,
    decision_id varchar(128) NOT NULL,
    client_id bigint NOT NULL,
    message_id varchar(40) NOT NULL,
    recipient_number varchar(64) NOT NULL,
    message_category varchar(32) NOT NULL,
    final_stage varchar(16) NOT NULL CHECK (final_stage IN ('PRIMARY', 'SECONDARY')),
    outcome varchar(16) NOT NULL CHECK (outcome IN ('SUCCESS', 'FAILURE')),
    result_source varchar(32) NOT NULL,
    carrier varchar(8),
    invocation integer,
    error_code integer,
    reason varchar(64),
    received_at timestamptz NOT NULL,
    decided_at timestamptz NOT NULL,
    result_json jsonb NOT NULL,
    submission_json jsonb NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX tbl_msg_hist_client_received_idx ON tbl_msg_hist (client_id, received_at DESC);

CREATE TABLE tbl_cdr_hist (
    client_msg_id varchar(40) PRIMARY KEY REFERENCES tbl_msg_hist (client_msg_id),
    decision_id varchar(128) NOT NULL,
    client_id bigint NOT NULL,
    message_id varchar(40) NOT NULL,
    carrier varchar(8) NOT NULL,
    billable_at timestamptz NOT NULL,
    billing_status varchar(24) NOT NULL DEFAULT 'BILLABLE' CHECK (billing_status = 'BILLABLE'),
    recorded_at timestamptz NOT NULL DEFAULT now()
);
