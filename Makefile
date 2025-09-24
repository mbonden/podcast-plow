.PHONY: up down logs psql test

up:
	cd infra && docker compose up -d

down:
	cd infra && docker compose down -v

logs:
	cd infra && docker compose logs -f

psql:
	docker exec -it podcast_plow_db psql -U postgres -d podcast_plow

test:
	cd infra && docker compose run --rm ingest pytest -q
