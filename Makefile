.PHONY: up down logs psql test db-shell ingest-shell enqueue-latest work

up:
docker compose -f infra/docker-compose.yml up -d

down:
docker compose -f infra/docker-compose.yml down -v

logs:
docker compose -f infra/docker-compose.yml logs -f

psql:
docker exec -it podcast_plow_db psql -U postgres -d podcast_plow

test:
docker compose -f infra/docker-compose.yml run --rm ingest pytest -q

db-shell:
docker exec -it podcast_plow_db psql -U postgres -d podcast_plow

ingest-shell:
docker compose -f infra/docker-compose.yml exec ingest bash

enqueue-latest:
docker compose -f infra/docker-compose.yml exec ingest bash -lc "python /app/manage.py jobs enqueue summarize-latest --limit $${N:-5}"

work:
docker compose -f infra/docker-compose.yml exec ingest bash -lc "python /app/manage.py jobs work --loop --poll-interval 2 --max-jobs $${MAX:-10}"
