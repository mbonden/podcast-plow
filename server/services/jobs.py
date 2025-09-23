"""Simple database-backed job queue helpers."""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

UTC = dt.timezone.utc


@dataclass
class Job:
    """Representation of a job queued in the database."""

    id: int
    job_type: str
    payload: Dict[str, Any]
    status: str
    priority: int
    run_at: dt.datetime
    attempts: int
    max_attempts: int
    last_error: Optional[str] = None


def _ensure_datetime(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value, tz=UTC)
    return dt.datetime.now(tz=UTC)


def _parse_payload(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            logger.debug("Unable to decode payload JSON; falling back to empty dict")
        return {}
    if isinstance(value, dict):
        return dict(value)
    return {}


def _row_to_job(row: Any) -> Job:
    job_id, job_type, payload, status, priority, run_at, attempts, max_attempts, last_error = row
    return Job(
        id=int(job_id),
        job_type=str(job_type),
        payload=_parse_payload(payload),
        status=str(status),
        priority=int(priority),
        run_at=_ensure_datetime(run_at),
        attempts=int(attempts),
        max_attempts=int(max_attempts),
        last_error=last_error,
    )


def enqueue_job(
    conn,
    job_type: str,
    payload: Dict[str, Any] | None = None,
    *,
    priority: int = 0,
    run_at: dt.datetime | None = None,
    max_attempts: int = 3,
) -> Job:
    payload = payload or {}
    run_at = run_at or dt.datetime.now(tz=UTC)
    serialized_payload = json.dumps(payload)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO job_queue (job_type, payload, priority, run_at, max_attempts)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, job_type, payload, status, priority, run_at, attempts, max_attempts, last_error
            """,
            (job_type, serialized_payload, priority, run_at, max_attempts),
        )
        row = cur.fetchone()
    job = _row_to_job(row)
    logger.info("Enqueued job %s (type=%s) with priority %s", job.id, job.job_type, job.priority)
    return job


def dequeue_job(conn) -> Job | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, job_type, payload, status, priority, run_at, attempts, max_attempts, last_error
            FROM job_queue
            WHERE status = %s AND run_at <= now()
            ORDER BY priority DESC, run_at, id
            LIMIT 1
            """,
            ("queued",),
        )
        row = cur.fetchone()
    if not row:
        return None

    job = _row_to_job(row)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE job_queue
            SET status = %s,
                attempts = attempts + 1,
                started_at = now(),
                updated_at = now()
            WHERE id = %s
            """,
            ("running", job.id),
        )
    job.status = "running"
    job.attempts += 1
    return job


def mark_job_done(conn, job_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE job_queue
            SET status = %s,
                finished_at = now(),
                last_error = NULL,
                updated_at = now()
            WHERE id = %s
            """,
            ("done", job_id),
        )
    logger.info("Job %s completed", job_id)


def mark_job_failed(
    conn,
    job: Job,
    error: str,
    *,
    backoff_seconds: int | None = None,
) -> None:
    message = (error or "").strip()
    if len(message) > 2000:
        message = message[:2000]

    if job.attempts >= job.max_attempts:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE job_queue
                SET status = %s,
                    finished_at = now(),
                    last_error = %s,
                    updated_at = now()
                WHERE id = %s
                """,
                ("failed", message or None, job.id),
            )
        logger.warning("Job %s permanently failed after %s attempts", job.id, job.attempts)
        return

    if backoff_seconds is None:
        backoff_seconds = min(3600, max(30, job.attempts * 60))
    next_run = dt.datetime.now(tz=UTC) + dt.timedelta(seconds=backoff_seconds)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE job_queue
            SET status = %s,
                run_at = %s,
                last_error = %s,
                started_at = NULL,
                finished_at = NULL,
                updated_at = now()
            WHERE id = %s
            """,
            ("queued", next_run, message or None, job.id),
        )
    job.status = "queued"
    job.run_at = next_run
    logger.info(
        "Job %s requeued after failure (attempt %s/%s)",
        job.id,
        job.attempts,
        job.max_attempts,
    )


__all__ = [
    "Job",
    "enqueue_job",
    "dequeue_job",
    "mark_job_done",
    "mark_job_failed",
]
