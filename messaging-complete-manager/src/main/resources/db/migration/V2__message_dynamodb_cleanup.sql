ALTER TABLE tbl_msg_hist
    ADD COLUMN cleanup_status varchar(16) NOT NULL DEFAULT 'PENDING'
        CHECK (cleanup_status IN ('PENDING', 'DONE')),
    ADD COLUMN cleanup_next_at timestamptz NOT NULL DEFAULT now(),
    ADD COLUMN cleanup_attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN cleanup_last_error varchar(256),
    ADD COLUMN cleaned_at timestamptz;

CREATE INDEX tbl_msg_hist_cleanup_due_idx
    ON tbl_msg_hist (cleanup_next_at)
    WHERE cleanup_status = 'PENDING';
