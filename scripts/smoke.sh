#!/usr/bin/env bash
set -euo pipefail

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

# --- Minimal but meaningful checks ---
# Keep this light so it runs fast in CI.

# Validate compose file
pushd infra >/dev/null
dc config -q

# Bring up only the pieces needed for a quick check
dc up -d --build db ingest

# Quick health checks
dc ps
dc exec -T db psql -U postgres -d podcast_plow -c "SELECT 1;"
dc exec -T ingest bash -lc "python /app/manage.py --help"

# Optionally enqueue one no-op-ish command just to prove command path is OK
# (Comment out if you prefer not to touch data)
# dc exec -T ingest bash -lc "python /app/manage.py jobs list || true"

popd >/dev/null

# Optional teardown: opt-in to cleanups
if [[ "${SMOKE_TEARDOWN:-}" == "1" ]]; then
  pushd infra >/dev/null
  dc down -v
  popd >/dev/null
fi

echo "[smoke] OK"
