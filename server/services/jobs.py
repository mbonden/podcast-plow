"""Simple database-backed job queue helpers."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
import random
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)


def _load_default_max_attempts() -> int:
    value = os.getenv("JOB_QUEUE_MAX_ATTEMPTS", "3")
    try:
        parsed = int(value)
    except ValueError:
        logger.debug("Invalid JOB_QUEUE_MAX_ATTEMPTS value %s; defaulting to 3", value)
        return 3
    return max(1, parsed)


DEFAULT_MAX_ATTEMPTS = _load_default_max_attempts()

UTC = dt.timezone.utc

_JOB_COLUMNS = (
    "id",
    "job_type",
    "payload_json",
    "status",
    "priority",
    "run_at",
    "next_run_at",
    "attempts",
    "max_attempts",
    "error",
    "created_at",
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
    next_run_at: dt.datetime | None
    attempts: int
    max_attempts: int
    error: Optional[str] = None
    created_at: dt.datetime | None = None
    started_at: dt.datetime | None = None
    finished_at: dt.datetime | None = None

    @property
    def updated_at(self) -> dt.datetime | None:
        timestamps = [self.created_at, self.started_at, self.finished_at]
        values = [value for value in timestamps if value is not None]
        if not values:
            return None
        return max(values)


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


def _serialize_payload(payload: Dict[str, Any]) -> str:
    try:
        return json.dumps(payload, sort_keys=True)
    except TypeError:
        logger.debug("Payload not JSON serializable; storing empty object")
        return json.dumps({}, sort_keys=True)


def _compute_backoff_delay(job: Job, backoff_seconds: int | None = None) -> int:
    if backoff_seconds is not None:
        try:
            base = int(backoff_seconds)
        except (TypeError, ValueError):
            base = 0
    else:
        base = job.attempts * 60
    base = max(30, min(base, 3600))
    jitter_low = base * 0.8
    jitter_high = base * 1.2
    delay = random.uniform(jitter_low, jitter_high)
    clamped = max(30, min(int(round(delay)), 3600))
    return clamped


def _row_to_job(row: Any) -> Job:
    if isinstance(row, dict):
        data = row
    else:
        data = dict(zip(_JOB_COLUMNS, row, strict=False))

    created_at = _ensure_optional_datetime(data.get("created_at"))
    run_at = _ensure_datetime(data.get("run_at"), default=created_at)
    next_run_at = _ensure_optional_datetime(data.get("next_run_at"))
    started_at = _ensure_optional_datetime(data.get("started_at"))
    finished_at = _ensure_optional_datetime(data.get("finished_at"))

    return Job(
        id=int(data.get("id")),
        job_type=str(data.get("job_type")),
        payload=_parse_payload(data.get("payload_json")),
        status=str(data.get("status")),
        priority=int(data.get("priority") or 0),
        run_at=run_at,
        next_run_at=next_run_at,
        attempts=int(data.get("attempts") or 0),
        max_attempts=int(data.get("max_attempts") or 0),
        error=data.get("error"),
        created_at=created_at,
        started_at=started_at,
        finished_at=finished_at,
    )


def compute_job_fingerprint(job_type: str, payload: Mapping[str, Any] | None = None) -> str:
    """Return a stable fingerprint for a job type/payload pair."""

    normalized_type = (job_type or "").strip()
    if payload is None:
        normalized_payload: Mapping[str, Any] = {}
    else:
        normalized_payload = payload

    serialized = json.dumps(
        normalized_payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    message = f"{normalized_type}:{serialized}".encode("utf-8")
    return hashlib.sha256(message).hexdigest()


def enqueue_job(
    conn,
    job_type: str,
    payload: Dict[str, Any] | None = None,
    *,
    priority: int = 0,
    run_at: dt.datetime | None = None,
    max_attempts: int | None = None,
) -> Job:
    payload = payload or {}
    run_at = _ensure_datetime(run_at, default=dt.datetime.now(tz=UTC))
    effective_max_attempts = max_attempts if max_attempts is not None else DEFAULT_MAX_ATTEMPTS
    if effective_max_attempts <= 0:
        effective_max_attempts = DEFAULT_MAX_ATTEMPTS
    serialized_payload = _serialize_payload(payload)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO job_queue (job_type, payload_json, priority, run_at, next_run_at, max_attempts)
            VALUES (%s, %s::jsonb, %s, %s, %s, %s)
            RETURNING
                id,
                job_type,
                payload_json,
                status,
                priority,
                run_at,
                next_run_at,
                attempts,
                max_attempts,
                error,
                created_at,
                started_at,
                finished_at
            """,
            (
                job_type,
                serialized_payload,
                priority,
                run_at,
                run_at,
                effective_max_attempts,
            ),
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
            "UPDATE job_queue SET status = %s, started_at = now() WHERE id = %s",
            ("running", job.id),
        )
    job.status = "running"
    job.next_run_at = None
    job.started_at = dt.datetime.now(tz=UTC)
    return job


def mark_job_done(conn, job_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE job_queue SET status = %s, finished_at = now(), error = NULL WHERE id = %s",
            ("finished", job_id),
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

    next_attempt = job.attempts + 1
    if next_attempt >= job.max_attempts:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE job_queue SET status = %s, finished_at = now(), error = %s WHERE id = %s",
                ("failed", message or None, job.id),
            )
        logger.warning("Job %s permanently failed after %s attempts", job.id, job.attempts)
        job.status = "failed"
        job.finished_at = dt.datetime.now(tz=UTC)
        job.next_run_at = None
        job.error = message or None
        return

    delay_seconds = _compute_backoff_delay(job, backoff_seconds)
    next_run = dt.datetime.now(tz=UTC) + dt.timedelta(seconds=delay_seconds)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE job_queue
            SET status = %s,
                run_at = %s,
                next_run_at = %s,
                error = %s,
                started_at = NULL,
                finished_at = NULL,
                attempts = attempts + 1
            WHERE id = %s
            """,
            ("queued", next_run, next_run, message or None, job.id),
        )
    job.status = "queued"
    job.run_at = next_run
    job.next_run_at = next_run
    job.attempts = next_attempt
    job.error = message or None
    logger.info(
        "Job %s requeued after failure (attempt %s/%s)",
        job.id,
        job.attempts,
        job.max_attempts,
    )


def update_job_progress(
    conn,
    job_id: int,
    *,
    total_chunks: int,
    completed_chunks: int,
    current_chunk: int | None = None,
    message: str | None = None,
) -> None:
    total = max(int(total_chunks), 0)
    completed = int(completed_chunks)
    if total > 0:
        completed = max(0, min(completed, total))
    else:
        completed = max(0, completed)

    current_value: int | None = None
    if current_chunk is not None:
        try:
            current_value = int(current_chunk)
        except (TypeError, ValueError):
            current_value = None

    percent = 0.0
    if total > 0:
        percent = min(1.0, completed / total)

    progress_payload: Dict[str, Any] = {
        "total_chunks": total,
        "completed_chunks": completed,
        "current_chunk": current_value,
        "percent_complete": round(percent, 4),
        "updated_at": dt.datetime.now(tz=UTC).isoformat(),
    }
    cleaned_message = (message or "").strip()
    if cleaned_message:
        progress_payload["message"] = cleaned_message

    serialized = _serialize_payload({"progress": progress_payload})
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE job_queue
            SET payload_json = COALESCE(payload_json, '{}'::jsonb) || %s::jsonb
            WHERE id = %s
            """,
            (serialized, job_id),
        )


def list_jobs(
    conn,
    *,
    status: str | None = None,
    job_type: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
) -> list[Job]:
    """Return queued jobs ordered by priority and run time."""

    params: list[Any] = []

    sql = f"SELECT {_JOB_SELECT} FROM job_queue"
    filters: list[str] = []

    if status is not None:
        filters.append("status = %s")
        params.append(status)
    if job_type is not None:
        filters.append("job_type = %s")
        params.append(job_type)

    if filters:
        sql += " WHERE " + " AND ".join(filters)

    sql += " ORDER BY priority DESC, run_at, id"

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
            WHERE job_type = %s AND payload_json = %s::jsonb
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
    "compute_job_fingerprint",
    "enqueue_job",
    "dequeue_job",
    "mark_job_done",
    "mark_job_failed",
    "update_job_progress",
    "list_jobs",
    "get_job",
    "find_job_by_payload",
    "list_jobs_admin",
]
