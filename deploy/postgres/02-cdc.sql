-- Runs once when the Docker Postgres volume is first created (after 01-schema.sql).
-- Prepares the submissions table for change data capture by Flink.

-- Include every column in the change log for updates and deletes, so Flink
-- can retract a row from the right (form_id, hour) group, not just by id.
ALTER TABLE submissions REPLICA IDENTITY FULL;

-- Publish only the table Flink reads. The Flink job refers to this by name.
CREATE PUBLICATION flink_submissions FOR TABLE submissions;
