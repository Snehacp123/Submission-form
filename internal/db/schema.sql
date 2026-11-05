-- Idempotent schema; applied on API startup.

CREATE TABLE IF NOT EXISTS submissions (
    id              BIGSERIAL    PRIMARY KEY,
    form_id         TEXT         NOT NULL,
    idempotency_key TEXT         NOT NULL,
    email           TEXT         NOT NULL,
    data            JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- The database, not the application, is the source of truth for
    -- duplicate protection. This holds under any amount of concurrency.
    CONSTRAINT submissions_idempotency_key_uq UNIQUE (idempotency_key)
);

-- Serves GET /forms/{form_id}/submissions: filter by form, ordered by id.
CREATE INDEX IF NOT EXISTS submissions_form_id_id_idx ON submissions (form_id, id);
