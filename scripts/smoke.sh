#!/usr/bin/env bash
set -euo pipefail

# --- Windows Git Bash guard: prevent MSYS path mangling of /app/... args ---
if [[ -n "$MSYSTEM" || "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
  export MSYS_NO_PATHCONV=1
  export MSYS2_ARG_CONV_EXCL="*"
fi


# Optional: verbose mode
if [[ "${SMOKE_DEBUG:-}" == "1" ]]; then
  set -x
fi

# Allow forced skip
if [[ "${SKIP_SMOKE:-}" == "1" ]]; then
  echo "[smoke] SKIP_SMOKE=1 set; skipping."
  exit 0
fi

# 1) Require docker cli
if ! command -v docker >/dev/null 2>&1; then
  echo "[smoke] docker CLI not found; skipping."
  exit 0
fi

# 2) Require running docker daemon
if ! docker info >/dev/null 2>&1; then
  echo "[smoke] docker daemon not reachable; skipping."
  exit 0
fi

# 3) Compose shim: prefer `docker compose`, fallback to legacy `docker-compose`
if docker compose version >/dev/null 2>&1; then
  dc() { docker compose "$@"; }
elif command -v docker-compose >/dev/null 2>&1; then
  dc() { docker-compose "$@"; }
else
  echo "[smoke] docker compose not available; skipping."
  exit 0
fi

echo "[smoke] Docker & Compose available; running minimal smoke."

wait_for_postgres() {
  local -r max_attempts=30
  local attempt=1

  echo "[smoke] Waiting for Postgres to accept connections..."
  while (( attempt <= max_attempts )); do
    if dc exec -T db pg_isready -U postgres -d podcast_plow >/dev/null 2>&1; then
      echo "[smoke] Postgres is ready."
      return 0
    fi

    sleep 1
    (( attempt++ ))
  done

  echo "[smoke] Postgres did not become ready after $max_attempts attempts."
  return 1
}

# --- Minimal but meaningful checks ---
# Keep this light so it runs fast in CI.

# Validate compose file
pushd infra >/dev/null
dc config -q

# Bring up only the pieces needed for a quick check
dc up -d --build db ingest

wait_for_postgres

echo "[smoke] Ensuring job_queue has required columns…"
dc exec -T db psql -U postgres -d podcast_plow <<'SQL'
ALTER TABLE job_queue
  ADD COLUMN IF NOT EXISTS payload       jsonb,
  ADD COLUMN IF NOT EXISTS run_at        timestamptz DEFAULT now(),
  ADD COLUMN IF NOT EXISTS next_run_at   timestamptz,
  ADD COLUMN IF NOT EXISTS max_attempts  integer DEFAULT 3,
  ADD COLUMN IF NOT EXISTS last_error    text,
  ADD COLUMN IF NOT EXISTS result        text,
  ADD COLUMN IF NOT EXISTS updated_at    timestamptz,
  ADD COLUMN IF NOT EXISTS progress      integer DEFAULT 0;

UPDATE job_queue SET payload = payload_json
 WHERE payload IS NULL AND payload_json IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_job_queue_status_run_at
    ON job_queue (status, run_at);

CREATE OR REPLACE FUNCTION touch_job_queue_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_job_queue_updated_at ON job_queue;
CREATE TRIGGER trg_job_queue_updated_at
BEFORE UPDATE ON job_queue
FOR EACH ROW EXECUTE FUNCTION touch_job_queue_updated_at();
SQL

# Quick health checks
dc ps
dc exec -T db psql -U postgres -d podcast_plow -c "SELECT 1;"
dc exec -T ingest bash -lc "PYTHONPATH=/app:/workspace:/workspace/server:/workspace/worker python /app/manage.py --help >/dev/null"

# Optionally enqueue one no-op-ish command just to prove command path is OK
# (Comment out if you prefer not to touch data)
# dc exec -T ingest bash -lc "python /app/manage.py jobs list || true"

echo "[smoke] Enqueuing one summarize job for the newest episode…"
EP_ID=$(dc exec -T db psql -U postgres -d podcast_plow -Atc \
  "SELECT id FROM episode ORDER BY published_at DESC NULLS LAST LIMIT 1;")

if [ -n "$EP_ID" ]; then
  dc exec -T ingest bash -lc '
    PYTHONPATH=/app:/workspace:/workspace/server:/workspace/worker \
    python /app/manage.py jobs enqueue summarize --episode-ids '"$EP_ID"'
  '

  echo "[smoke] Working exactly one job…"
  dc exec -T ingest bash -lc '
    PYTHONPATH=/app:/workspace:/workspace/server:/workspace/worker \
    python /app/manage.py jobs work --once
  '

  echo "[smoke] Verifying a summary row exists…"
  dc exec -T db psql -U postgres -d podcast_plow -c \
    "SELECT episode_id, LEFT(tl_dr, 60) FROM episode_summary ORDER BY id DESC LIMIT 3;"
else
  echo "[smoke] No episodes found to summarize; skipping E2E."
fi


popd >/dev/null

# Optional teardown: opt-in to cleanups
if [[ "${SMOKE_TEARDOWN:-}" == "1" ]]; then
  pushd infra >/dev/null
  dc down -v
  popd >/dev/null
fi

echo "[smoke] OK"
