"""Simple database-backed job queue helpers."""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence

logger = logging.getLogger(__name__)

UTC = dt.timezone.utc

_JOB_COLUMNS = (
    "id",
    "job_type",
    "payload",
    "status",
    "priority",
    "run_at",
    "attempts",
    "max_attempts",
    "last_error",
    "result",
    "created_at",
    "updated_at",
    "started_at",
    "finished_at",
)

_JOB_SELECT = ", ".join(_JOB_COLUMNS)


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
    result: Any = None
    created_at: dt.datetime | None = None
    updated_at: dt.datetime | None = None
    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None


def _ensure_datetime(value: Any, *, default: dt.datetime | None = None) -> dt.datetime:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value, tz=UTC)
    if default is not None:
        return default
    return dt.datetime.now(tz=UTC)


def _ensure_optional_datetime(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    return _ensure_datetime(value)


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


def _parse_result(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            logger.debug("Unable to decode result JSON; returning raw value")
    return value


def _serialize_payload(payload: Dict[str, Any]) -> str:
    try:
        return json.dumps(payload, sort_keys=True)
    except TypeError:
        logger.debug("Payload not JSON serializable; storing empty object")
        return json.dumps({}, sort_keys=True)


def _row_to_job(row: Any) -> Job:
    if isinstance(row, dict):
        data = row
    else:
        data = dict(zip(_JOB_COLUMNS, row, strict=False))

    created_at = _ensure_optional_datetime(data.get("created_at"))
    updated_at = _ensure_optional_datetime(data.get("updated_at"))
    run_at = _ensure_datetime(data.get("run_at"), default=created_at)

    return Job(
        id=int(data.get("id")),
        job_type=str(data.get("job_type")),
        payload=_parse_payload(data.get("payload")),
        status=str(data.get("status")),
        priority=int(data.get("priority") or 0),
        run_at=run_at,
        attempts=int(data.get("attempts") or 0),
        max_attempts=int(data.get("max_attempts") or 0),
        last_error=data.get("last_error"),
        result=_parse_result(data.get("result")),
        created_at=created_at,
        updated_at=updated_at,
        started_at=_ensure_optional_datetime(data.get("started_at")),
        finished_at=_ensure_optional_datetime(data.get("finished_at")),
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
    run_at = _ensure_datetime(run_at, default=dt.datetime.now(tz=UTC))
    serialized_payload = _serialize_payload(payload)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO job_queue (job_type, payload, priority, run_at, max_attempts)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING
                id,
                job_type,
                payload,
                status,
                priority,
                run_at,
                attempts,
                max_attempts,
                last_error,
                result,
                created_at,
                updated_at,
                started_at,
                finished_at
            """,
            (job_type, serialized_payload, priority, run_at, max_attempts),
        )
        row = cur.fetchone()
    job = _row_to_job(row)
    logger.info("Enqueued job %s (type=%s) with priority %s", job.id, job.job_type, job.priority)
    return job


def dequeue_job(conn, job_types: Sequence[str] | None = None) -> Job | None:
    filters = ["status = %s", "run_at <= now()"]
    params: list[Any] = ["queued"]

    normalized_types: list[str] = []
    if job_types:
        for job_type in job_types:
            if job_type is None:
                continue
            cleaned = str(job_type).strip()
            if cleaned:
                normalized_types.append(cleaned)

    if normalized_types:
        placeholders = ", ".join(["%s"] * len(normalized_types))
        filters.append(f"job_type IN ({placeholders})")
        params.extend(normalized_types)

    sql = f"""
        SELECT {_JOB_SELECT}
        FROM job_queue
    """
    if filters:
        sql += " WHERE " + " AND ".join(filters)
    sql += " ORDER BY priority DESC, run_at, id LIMIT 1"

    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
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


def list_jobs(conn, *, status: str | None = None, limit: int | None = None) -> list[Job]:
    """Return queued jobs ordered by priority and run time."""

    params: list[Any] = []
    sql = f"SELECT {_JOB_SELECT} FROM job_queue"
    if status is not None:
        sql += " WHERE status = %s"
        params.append(status)

    sql += " ORDER BY priority DESC, run_at, id"

    if limit is not None:
        sql += " LIMIT %s"
        params.append(int(limit))

    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

    return [_row_to_job(row) for row in rows]


def get_job(conn, job_id: int) -> Job | None:
    """Return a single job by identifier."""

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {_JOB_SELECT} FROM job_queue WHERE id = %s",
            (job_id,),
        )
        row = cur.fetchone()
    return _row_to_job(row) if row else None


def find_job_by_payload(
    conn,
    *,
    job_type: str,
    payload: Dict[str, Any],
) -> Job | None:
    """Return the newest job matching the provided type and payload."""

    serialized_payload = _serialize_payload(payload)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT {_JOB_SELECT}
            FROM job_queue
            WHERE job_type = %s AND payload::jsonb = %s::jsonb
            ORDER BY id DESC
            LIMIT 1
            """,
            (job_type, serialized_payload),
        )
        row = cur.fetchone()

    return _row_to_job(row) if row else None


def list_jobs_admin(
    conn,
    *,
    status: str | None = None,
    job_type: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> list[Job]:
    """Return jobs ordered by priority and id for administrative views."""

    filters: list[str] = []
    params: list[Any] = []

    if status is not None:
        filters.append("status = %s")
        params.append(status)

    if job_type is not None:
        filters.append("job_type = %s")
        params.append(job_type)

    sql = f"SELECT {_JOB_SELECT} FROM job_queue"
    if filters:
        sql += " WHERE " + " AND ".join(filters)
    sql += " ORDER BY priority DESC, id DESC"

    if limit is not None:
        sql += " LIMIT %s"
        params.append(int(limit))
    if offset:
        sql += " OFFSET %s"
        params.append(int(offset))

    with conn.cursor() as cur:
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

    return [_row_to_job(row) for row in rows]


__all__ = [
    "Job",
    "enqueue_job",
    "dequeue_job",
    "mark_job_done",
    "mark_job_failed",
    "list_jobs",
    "get_job",
    "find_job_by_payload",
    "list_jobs_admin",
]
