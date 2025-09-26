-- Migrate legacy job table entries into the unified job_queue table.

-- Rename old columns if present from earlier schema versions.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'job_queue' AND column_name = 'payload_json'
    ) THEN
        EXECUTE 'ALTER TABLE job_queue RENAME COLUMN payload_json TO payload';
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'job_queue' AND column_name = 'error'
    ) THEN
        EXECUTE 'ALTER TABLE job_queue RENAME COLUMN error TO last_error';
    END IF;
END
$$;

-- Ensure required columns exist with appropriate defaults.
ALTER TABLE job_queue
    ADD COLUMN IF NOT EXISTS payload JSONB DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS run_at TIMESTAMPTZ DEFAULT now(),
    ADD COLUMN IF NOT EXISTS next_run_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS max_attempts INT NOT NULL DEFAULT 3,
    ADD COLUMN IF NOT EXISTS result JSONB,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT now(),
    ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

ALTER TABLE job_queue
    ALTER COLUMN job_type SET NOT NULL,
    ALTER COLUMN status SET NOT NULL,
    ALTER COLUMN status SET DEFAULT 'queued',
    ALTER COLUMN priority SET NOT NULL,
    ALTER COLUMN priority SET DEFAULT 0,
    ALTER COLUMN attempts SET NOT NULL,
    ALTER COLUMN attempts SET DEFAULT 0,
    ALTER COLUMN payload SET DEFAULT '{}'::jsonb,
    ALTER COLUMN payload SET NOT NULL,
    ALTER COLUMN run_at SET DEFAULT now(),
    ALTER COLUMN run_at SET NOT NULL,
    ALTER COLUMN created_at SET DEFAULT now(),
    ALTER COLUMN created_at SET NOT NULL,
    ALTER COLUMN updated_at SET DEFAULT now(),
    ALTER COLUMN updated_at SET NOT NULL;

-- Backfill missing values for existing rows.
UPDATE job_queue
SET
    payload = COALESCE(payload, '{}'::jsonb),
    priority = COALESCE(priority, 0),
    attempts = COALESCE(attempts, 0),
    max_attempts = COALESCE(max_attempts, 3),
    status = COALESCE(status, 'queued'),
    run_at = COALESCE(run_at, created_at, now()),
    next_run_at = COALESCE(next_run_at, run_at),
    created_at = COALESCE(created_at, now()),
    updated_at = COALESCE(updated_at, created_at, now());

-- Copy data from the legacy job table if it exists.
INSERT INTO job_queue (id, job_type, payload, status, priority, run_at, attempts, max_attempts, last_error, result, created_at, started_at, finished_at, updated_at)
SELECT
    id,
    job_type,
    payload,
    status,
    COALESCE(priority, 0),
    COALESCE(updated_at, created_at, now()),
    0,
    3,
    error,
    result,
    created_at,
    NULL,
    NULL,
    COALESCE(updated_at, created_at, now())
FROM job
ON CONFLICT (id) DO NOTHING;

-- Align the sequence with the highest identifier in the queue.
SELECT setval('job_queue_id_seq', GREATEST((SELECT COALESCE(MAX(id), 0) FROM job_queue), 1));

-- Drop the legacy job table now that data has been migrated.
DROP TABLE IF EXISTS job;
