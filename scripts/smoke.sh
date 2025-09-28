#!/usr/bin/env bash
set -euo pipefail
docker compose -f infra/docker-compose.yml exec ingest bash -lc "python /app/manage.py jobs enqueue summarize-latest --limit 3"
docker compose -f infra/docker-compose.yml exec ingest bash -lc "python /app/manage.py jobs work --loop --max-jobs 3 --poll-interval 1"
docker exec -i podcast_plow_db psql -U postgres -d podcast_plow -Atc \
  "SELECT count(*) FROM episode_summary WHERE tl_dr IS NOT NULL;" \
  | awk '{print "episode_summary rows: " $0}'
