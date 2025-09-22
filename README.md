# podcast-plow (bootstrap)

A minimal starter for your podcast analysis backend: Postgres (+pgvector), FastAPI API, and schema for episodes, transcripts, claims, evidence, and grades.

## Quick start

Requirements:
- Docker and Docker Compose

```bash
cd infra
docker compose up -d
# API at http://localhost:8000, DB at localhost:5432
```

Check health:
```
curl http://localhost:8000/healthz
```

Open API docs:
- http://localhost:8000/docs
- http://localhost:8000/redoc

## What you got

- **Database schema** auto-applied on first run (see `infra/initdb/001_init.sql`)
- **FastAPI** endpoints:
  - `GET /healthz`
  - `GET /episodes/{id}`
  - `GET /topics/{topic}/claims`
  - `GET /claims/{id}`
  - `GET /search?q=...`

## Next steps

1. Seed some rows (episodes, claims) to see the endpoints in action.
2. Add ingestion code (RSS/YouTube/Transcripts) under `/worker` and `/server/services` (create folder).
3. Implement summarization + claim extraction and persist to DB.
4. Add public UI later under `/web`.

## Safety & Legal

- Keep **full transcripts** private; do not expose via API.
- Public surfaces should only show summaries, paraphrased claims, grades, and citations.
