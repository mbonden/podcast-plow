-- Additional tables for job processing and structured transcript data

-- Job queue for background work
CREATE TABLE IF NOT EXISTS job_queue (
  id SERIAL PRIMARY KEY,
  job_type TEXT,
  payload_json JSONB,
  status TEXT,
  priority INT DEFAULT 5,
  attempts INT DEFAULT 0,
  error TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_job_queue_status_priority_created_at
  ON job_queue (status, priority, created_at);

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
