-- Ensure job priority column exists for admin API features
ALTER TABLE job
    ADD COLUMN IF NOT EXISTS priority INT NOT NULL DEFAULT 0;
