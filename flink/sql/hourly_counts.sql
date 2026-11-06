-- Part 3: live submission counts per form, per hour.
--
--   Postgres WAL --(CDC)--> Flink GROUP BY (form_id, hour) --(upsert)--> form_hourly_counts
--
-- Crash safety (exactly-once results):
--   * Every 10s Flink takes a checkpoint holding BOTH the running counts AND the
--     Postgres WAL position (LSN) the source has read up to. They are saved
--     together, so they always agree with each other.
--   * The replication slot is only advanced after a checkpoint completes, so
--     Postgres keeps any WAL Flink has not durably processed.
--   * On a crash Flink restores the last checkpoint and re-reads the WAL from
--     that LSN: nothing is lost, and the restored counts don't yet include the
--     replayed rows, so nothing is counted twice.
--   * The sink writes the full count (an upsert keyed on form_id + hour), not
--     an increment, so rewriting a row during the replay cannot inflate it.

SET 'pipeline.name' = 'form-hourly-submission-counts';
SET 'execution.runtime-mode' = 'streaming';
SET 'table.local-time-zone' = 'UTC';

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';

-- Batch updates for up to 1s so a burst (e.g. the load test) produces one
-- count update per group instead of one per row.
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '1s';
SET 'table.exec.mini-batch.size' = '5000';

-- Source: every insert/update/delete on submissions, read from the Postgres
-- write-ahead log. 'initial' first reads existing rows, then follows changes.
CREATE TABLE submissions_cdc (
    id          BIGINT,
    form_id     STRING,
    created_at  TIMESTAMP_LTZ(6),
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'forms',
    'schema-name' = 'public',
    'table-name' = 'submissions',
    'slot.name' = 'flink_hourly_counts',
    'decoding.plugin.name' = 'pgoutput',
    'debezium.publication.name' = 'flink_submissions',
    'debezium.publication.autocreate.mode' = 'disabled',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.startup.mode' = 'initial'
);

-- Sink: upsert into form_hourly_counts on its primary key.
CREATE TABLE form_hourly_counts_sink (
    form_id          STRING,
    hour_start       TIMESTAMP(3),
    submission_count BIGINT,
    PRIMARY KEY (form_id, hour_start) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/forms',
    'table-name' = 'form_hourly_counts',
    'username' = 'postgres',
    'password' = 'postgres',
    'sink.buffer-flush.interval' = '1s',
    'sink.buffer-flush.max-rows' = '500'
);

-- A continuously updating aggregate (not a window that only fires when the
-- hour ends), so the current hour's count changes live.
INSERT INTO form_hourly_counts_sink
SELECT
    form_id,
    FLOOR(CAST(created_at AS TIMESTAMP(3)) TO HOUR) AS hour_start,
    COUNT(*) AS submission_count
FROM submissions_cdc
GROUP BY form_id, FLOOR(CAST(created_at AS TIMESTAMP(3)) TO HOUR);
