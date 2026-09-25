-- Operations only: archive before moving the independent DLT scan cursor.
CREATE TABLE dlt_intake_cursor (
    cluster_alias varchar(100) NOT NULL,
    topic varchar(249) NOT NULL,
    partition_id integer NOT NULL CHECK (partition_id >= 0),
    source_topic varchar(249) NOT NULL,
    target_topic varchar(249) NOT NULL,
    next_offset bigint NOT NULL CHECK (next_offset >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cluster_alias, topic, partition_id)
);
CREATE TABLE dlt_intake_record (
    intake_id uuid PRIMARY KEY,
    cluster_alias varchar(100) NOT NULL,
    topic varchar(249) NOT NULL,
    partition_id integer NOT NULL,
    record_offset bigint NOT NULL,
    record_json text NOT NULL,
    state varchar(16) NOT NULL DEFAULT 'NEW' CHECK (state IN ('NEW','REGISTERED','HELD')),
    decision varchar(64),
    operation_id uuid REFERENCES dlt_recovery_operation(operation_id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (cluster_alias, topic, partition_id, record_offset),
    CHECK (state <> 'REGISTERED' OR operation_id IS NOT NULL)
);
CREATE INDEX dlt_intake_unprocessed ON dlt_intake_record(cluster_alias,topic,partition_id,updated_at)
    WHERE state='NEW';
CREATE INDEX dlt_intake_held ON dlt_intake_record(cluster_alias,topic,partition_id,created_at)
    WHERE state='HELD';
