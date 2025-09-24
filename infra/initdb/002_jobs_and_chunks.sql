-- Additional tables for job processing and structured transcript data

-- Job queue for background work
CREATE TABLE IF NOT EXISTS job_queue (
  id SERIAL PRIMARY KEY,
  job_type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'queued',
  priority INT NOT NULL DEFAULT 0,
  run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 3,
  last_error TEXT,
  result JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_job_queue_status_priority_run_at
  ON job_queue (status, priority, run_at);

-- Transcript chunks allow storing smaller units with embeddings
CREATE TABLE IF NOT EXISTS transcript_chunk (
  id SERIAL PRIMARY KEY,
  transcript_id INT REFERENCES transcript(id) ON DELETE CASCADE,
  start_ms INT,
  end_ms INT,
  text TEXT,
  tokens INT,
  embedding vector(768)
);

CREATE INDEX IF NOT EXISTS idx_transcript_chunk_transcript_id_start_ms
  ON transcript_chunk (transcript_id, start_ms);

-- Episode outlines allow summarising sections of an episode
CREATE TABLE IF NOT EXISTS episode_outline (
  id SERIAL PRIMARY KEY,
  episode_id INT REFERENCES episode(id) ON DELETE CASCADE,
  start_ms INT,
  end_ms INT,
  heading TEXT,
  bullet_points TEXT
);
