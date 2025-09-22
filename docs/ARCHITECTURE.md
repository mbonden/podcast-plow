# Architecture (high level)

- **db (Postgres + pgvector)**: stores podcasts, episodes, transcripts (private), claims, evidence, grades, summaries.
- **server (FastAPI)**: read-only API exposing safe data (no transcript text).
- **worker**: future background tasks (ingest feeds, fetch transcripts, summarize, extract claims, link evidence, grade).

## Data flow (planned)

discover -> fetch transcript/captions or STT -> summarize -> extract claims -> link evidence -> grade -> publish safe views.

## Public vs Private

- Private: `transcript` and `transcript_segment`
- Public: `episode_summary`, `claim`, `claim_grade`, `evidence_source`

## Rubric v1

- Strong / Moderate / Weak / Unsupported based on evidence type and consistency.
