-- Compares Flink's form_hourly_counts with a count computed directly from
-- submissions. mismatched_groups = 0 means Flink lost nothing and double-counted nothing.
WITH truth AS (
    SELECT form_id, date_trunc('hour', created_at) AS hour_start, count(*) AS c
    FROM submissions
    GROUP BY 1, 2
),
flink AS (
    SELECT form_id, hour_start, submission_count AS c FROM form_hourly_counts
)
SELECT
    (SELECT count(*) FROM submissions)               AS rows_in_submissions,
    (SELECT coalesce(sum(c), 0) FROM flink)          AS total_counted_by_flink,
    (SELECT count(*) FROM truth)                     AS form_hour_groups,
    (SELECT count(*)
       FROM truth FULL JOIN flink USING (form_id, hour_start)
      WHERE truth.c IS DISTINCT FROM flink.c)        AS mismatched_groups;
