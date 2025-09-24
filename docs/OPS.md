# Operations runbook

This guide explains how to enqueue background work, run processors, and
inspect the publicly exposed results. All commands assume the repository root
as the working directory and that `DATABASE_URL` points at your Postgres
instance (e.g. `postgresql://postgres:postgres@localhost:5432/podcast_plow`).

## Enqueue background jobs

Use the Typer CLI in `server/manage.py` to queue work. Each command accepts a
comma-separated list of identifiers and an optional priority (higher numbers
run first).

```bash
# Summaries for specific episodes
python -m server.manage jobs enqueue summarize --episode-ids 101,102 --priority 5

# Extract claims for an episode (regenerating transcript chunks first)
python -m server.manage jobs enqueue extract-claims --episode-ids 101 --refresh

# Re-grade claims that already have evidence
python -m server.manage jobs enqueue auto-grade --episode-ids 101
```

## Work the queue

Process queued items with the worker loop. The `--type` flag restricts the job
kinds a worker will handle. Use `--once` for ad-hoc runs or `--loop` to poll
continuously.

```bash
# Process a single summarize job and exit
python -m server.manage jobs work --once --type summarize

# Long-running worker that handles summarize and extract_claims jobs
python -m server.manage jobs work --loop --type summarize --type extract_claims --poll-interval 5
```

## Inspect queue state

List queued, running, failed, or completed jobs:

```bash
# Show the 10 most recent queued jobs
python -m server.manage jobs list --status queued --limit 10
```

When no jobs match, the command prints `No jobs match the provided filters.`

## View public API results

The FastAPI server exposes read-only endpoints once data has been produced:

* `GET /episodes/{id}` – episode metadata, summaries, and graded claims.
* `GET /episodes/{id}/outline` – ordered outline sections when an outline has
  been generated for the episode.
* `GET /topics/{topic}/claims` – all claims tagged with a topic.
* `GET /claims/{id}` – claim detail with linked evidence and latest grade.
* `GET /search?q={term}` – lightweight search over episode titles and claims.

Example request:

```bash
curl http://localhost:8000/episodes/101/outline | jq
```

Endpoints intentionally omit raw transcript text to keep private uploads out of
public surfaces.
