-- Enable useful extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS vector;  -- from pgvector image

-- 1) Catalog tables
CREATE TABLE podcast (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  rss_url TEXT,
  official_site TEXT,
  description TEXT,
  owner_org TEXT,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE person (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  role TEXT,
  website TEXT,
  twitter TEXT,
  notes TEXT
);

CREATE TABLE episode (
  id SERIAL PRIMARY KEY,
  podcast_id INT NOT NULL REFERENCES podcast(id) ON DELETE CASCADE,
  title TEXT NOT NULL,
  description TEXT,
  published_at TIMESTAMPTZ,
  duration_sec INT,
  spotify_id TEXT,
  youtube_url TEXT,
  rss_guid TEXT,
  audio_url TEXT,
  show_notes_url TEXT,
  legal_status TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE episode_participant (
  episode_id INT REFERENCES episode(id) ON DELETE CASCADE,
  person_id INT REFERENCES person(id) ON DELETE CASCADE,
  role TEXT,
  PRIMARY KEY (episode_id, person_id)
);

-- 2) Ingestion artifacts
CREATE TABLE transcript (
  id SERIAL PRIMARY KEY,
  episode_id INT NOT NULL REFERENCES episode(id) ON DELETE CASCADE,
  source TEXT,
  lang TEXT,
  text TEXT,
  word_count INT,
  has_verbatim_ok BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE transcript_segment (
  id SERIAL PRIMARY KEY,
  transcript_id INT NOT NULL REFERENCES transcript(id) ON DELETE CASCADE,
  start_ms INT,
  end_ms INT,
  speaker TEXT,
  text TEXT
);

CREATE TABLE transcript_chunk (
  id SERIAL PRIMARY KEY,
  transcript_id INT NOT NULL REFERENCES transcript(id) ON DELETE CASCADE,
  chunk_index INT NOT NULL,
  token_start INT NOT NULL,
  token_end INT NOT NULL,
  token_count INT NOT NULL,
  text TEXT NOT NULL,
  key_points TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE (transcript_id, chunk_index)
);

-- 3) Claims and grading
CREATE TABLE claim (
  id SERIAL PRIMARY KEY,
  episode_id INT NOT NULL REFERENCES episode(id) ON DELETE CASCADE,
  start_ms INT,
  end_ms INT,
  raw_text TEXT,
  normalized_text TEXT,
  topic TEXT,
  domain TEXT,
  risk_level TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE evidence_source (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  year INT,
  doi TEXT,
  pubmed_id TEXT,
  url TEXT,
  type TEXT,
  journal TEXT
);

CREATE TABLE claim_evidence (
  claim_id INT REFERENCES claim(id) ON DELETE CASCADE,
  evidence_id INT REFERENCES evidence_source(id) ON DELETE CASCADE,
  stance TEXT NOT NULL,
  notes TEXT,
  PRIMARY KEY (claim_id, evidence_id)
);

CREATE TABLE claim_grade (
  id SERIAL PRIMARY KEY,
  claim_id INT NOT NULL REFERENCES claim(id) ON DELETE CASCADE,
  grade TEXT NOT NULL,
  rationale TEXT,
  rubric_version TEXT DEFAULT 'v1',
  graded_by TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE episode_summary (
  id SERIAL PRIMARY KEY,
  episode_id INT NOT NULL REFERENCES episode(id) ON DELETE CASCADE,
  tl_dr TEXT,
  narrative TEXT,
  created_by TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE job_queue (
  id SERIAL PRIMARY KEY,
  job_type TEXT NOT NULL,
  payload JSONB,
  status TEXT NOT NULL DEFAULT 'queued',
  priority INT NOT NULL DEFAULT 0,
  run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 3,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  last_error TEXT,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Indexes
CREATE INDEX idx_episode_title_trgm ON episode USING gin (title gin_trgm_ops);
CREATE INDEX idx_claim_topic ON claim(topic);
CREATE INDEX idx_claim_norm_trgm ON claim USING gin (normalized_text gin_trgm_ops);
CREATE INDEX idx_evidence_ids ON evidence_source(pubmed_id, doi);
CREATE INDEX idx_transcript_chunk_transcript ON transcript_chunk(transcript_id, chunk_index);
CREATE INDEX idx_job_queue_status_priority ON job_queue(status, priority DESC, run_at, id);
