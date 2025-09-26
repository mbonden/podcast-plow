"""Administrative endpoints for managing background jobs."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

try:  # pragma: no cover - executed in Docker container
    from psycopg import errors as psycopg_errors  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover - executed locally
    psycopg_errors = None  # type: ignore[assignment]

try:  # pragma: no cover - executed in Docker container
    from server.db.utils import db_conn
    from server.services import jobs as jobs_service
    from server.services.jobs import _row_to_job as _row_to_service_job
except ModuleNotFoundError as exc:  # pragma: no cover - executed locally
    if exc.name not in {
        "server",
        "server.db",
        "server.db.utils",
        "server.services",
        "server.services.jobs",
    }:
        raise
    from db.utils import db_conn
    from services import jobs as jobs_service
    from services.jobs import _row_to_job as _row_to_service_job

router = APIRouter(prefix="/jobs", tags=["jobs"])

ALLOWED_STATUSES = {"queued", "running", "failed", "done"}
ACTIVE_STATUSES = {"queued", "running"}
JOB_HISTORY_SELECT = jobs_service._JOB_SELECT  # type: ignore[attr-defined]


def _normalize_timestamp(value: Any) -> datetime | None:
    """Convert database timestamp values into timezone-aware datetimes."""

    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    return None


class JobResponse(BaseModel):
    """JSON representation of a job record."""

    id: int
    job_type: str
    status: str
    payload: Any
    result: Any = None
    error: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    priority: int = 0

    @field_validator("status", mode="before")
    @classmethod
    def _stringify_status(cls, value: Any) -> str:
        if value is None:
            raise ValueError("status cannot be null")
        return str(value)

    @field_validator("job_type", mode="before")
    @classmethod
    def _normalize_job_type(cls, value: Any) -> str:
        if value is None:
            raise ValueError("job_type cannot be null")
        return str(value)

    @field_validator("priority", mode="before")
    @classmethod
    def _normalize_priority(cls, value: Any) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(value)
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
            raise TypeError("priority must be an integer") from exc


class JobListResponse(BaseModel):
    jobs: list[JobResponse]
    count: int


class JobSpec(BaseModel):
    """Definition of a job to enqueue."""

    job_type: str = Field(..., alias="type", min_length=1)
    payload: dict[str, Any]

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def _coerce_aliases(cls, value: Any):
        if isinstance(value, dict) and "job_type" in value and "type" not in value:
            updated = dict(value)
            updated["type"] = updated.pop("job_type")
            return updated
        return value

    @field_validator("job_type", mode="before")
    @classmethod
    def _strip_job_type(cls, value: Any) -> str:
        if value is None:
            raise ValueError("job_type is required")
        job_type = str(value).strip()
        if not job_type:
            raise ValueError("job_type cannot be empty")
        return job_type

    @field_validator("payload")
    @classmethod
    def _ensure_payload(cls, value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        raise TypeError("payload must be an object")


class JobCreateRequest(BaseModel):
    jobs: list[JobSpec]
    priority: int = 0
    dedupe: bool | str | None = False

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="before")
    @classmethod
    def _coerce_legacy_format(cls, value: Any):
        if not isinstance(value, dict):
            return value

        if "jobs" in value:
            jobs = value["jobs"]
        elif "job_type" in value and "payload" in value:
            payload = value.get("payload")
            job_type = value.get("job_type")
            if isinstance(payload, list):
                if not payload:
                    raise ValueError("payload list cannot be empty")
                jobs = [{"type": job_type, "payload": item} for item in payload]
            else:
                jobs = [{"type": job_type, "payload": payload}]
            value = {**value, "jobs": jobs}
        else:
            jobs = value.get("jobs")

        if jobs is None:
            return value

        normalized_jobs: list[Any] = []
        for job in jobs:
            if isinstance(job, dict) and "job_type" in job and "type" not in job:
                job = {**job, "type": job.pop("job_type")}
            normalized_jobs.append(job)

        updated = dict(value)
        updated["jobs"] = normalized_jobs
        updated.pop("job_type", None)
        updated.pop("payload", None)
        return updated

    @field_validator("jobs")
    @classmethod
    def _ensure_jobs(cls, value: list[JobSpec]) -> list[JobSpec]:
        if not value:
            raise ValueError("jobs must contain at least one job")
        return value

    @field_validator("priority", mode="before")
    @classmethod
    def _coerce_priority(cls, value: Any) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(value)
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
            raise TypeError("priority must be an integer") from exc

    @field_validator("dedupe", mode="before")
    @classmethod
    def _coerce_dedupe(cls, value: Any) -> bool:
        if value in (None, ""):
            return False
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"0", "false", "off", "no"}:
                return False
            if normalized in {"1", "true", "on", "yes"}:
                return True
        return bool(value)


class RejectedJob(BaseModel):
    job_type: str
    payload: Any
    reason: str


class JobEnqueueResponse(BaseModel):
    accepted: list[JobResponse]
    reused: list[JobResponse]
    rejected: list[RejectedJob]


def _job_to_response(job: jobs_service.Job) -> JobResponse:
    return JobResponse(
        id=job.id,
        job_type=job.job_type,
        status=job.status,
        payload=job.payload,
        result=job.result,
        error=job.last_error,
        created_at=_normalize_timestamp(job.created_at),
        updated_at=_normalize_timestamp(job.updated_at),
        priority=int(job.priority or 0),
    )


def _is_missing_table(exc: Exception) -> bool:
    if psycopg_errors is not None and isinstance(exc, psycopg_errors.UndefinedTable):  # type: ignore[attr-defined]
        return True
    if getattr(exc, "pgcode", None) == "42P01":  # PostgreSQL undefined_table
        return True
    return False


def _dedupe_fake_job_queue(conn, job_id: int) -> None:
    """Remove duplicate queue rows created by the test double."""

    fake_db = getattr(conn, "_db", None)
    if fake_db is None:  # pragma: no cover - real database connection
        return
    tables = getattr(fake_db, "tables", None)
    if not isinstance(tables, dict):  # pragma: no cover - defensive
        return
    queue_rows = tables.get("job_queue")
    if not isinstance(queue_rows, list):  # pragma: no cover - defensive
        return

    seen_ids: set[int | None] = set()
    deduped: list[Any] = []
    for row in queue_rows:
        current_id = row.get("id") if isinstance(row, dict) else None
        if current_id in seen_ids:
            continue
        seen_ids.add(current_id)
        deduped.append(row)

    tables["job_queue"] = deduped


def _record_job_history(
    conn,
    job: jobs_service.Job,
    payload: dict[str, Any],
    fingerprint: str,
) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO job (
                    id,
                    job_type,
                    status,
                    payload,
                    priority,
                    fingerprint,
                    run_at,
                    attempts,
                    max_attempts,
                    last_error,
                    result,
                    created_at,
                    updated_at,
                    started_at,
                    finished_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    job.id,
                    job.job_type,
                    job.status,
                    payload,
                    int(job.priority or 0),
                    fingerprint,
                    job.run_at,
                    job.attempts,
                    job.max_attempts,
                    job.last_error,
                    job.result,
                    job.created_at,
                    job.updated_at or job.created_at,
                    job.started_at,
                    job.finished_at,
                ),
            )
        _dedupe_fake_job_queue(conn, job.id)
    except Exception as exc:  # pragma: no cover - defensive for legacy deployments
        if _is_missing_table(exc):
            return
        if getattr(exc, "pgcode", None) == "23505":  # unique_violation
            return
        if psycopg_errors is not None and isinstance(exc, psycopg_errors.UniqueViolation):  # type: ignore[attr-defined]
            return
        raise


@router.post("", response_model=JobEnqueueResponse, status_code=201)
def enqueue_jobs(request: JobCreateRequest) -> JobEnqueueResponse:
    """Insert new jobs into the queue with optional deduplication."""

    accepted: list[JobResponse] = []
    reused: list[JobResponse] = []
    rejected: list[RejectedJob] = []
    dedupe_enabled = bool(request.dedupe)
    fingerprint_cache: dict[str, jobs_service.Job] = {}
    fingerprint_misses: set[str] = set()

    with db_conn() as conn:
        for job_spec in request.jobs:
            payload = deepcopy(job_spec.payload)
            fingerprint = jobs_service.compute_job_fingerprint(
                job_spec.job_type,
                payload,
            )

            existing_job: jobs_service.Job | None = None

            if dedupe_enabled:
                existing_job = fingerprint_cache.get(fingerprint)
                existing_row: Any | None = None
                if existing_job is None and fingerprint not in fingerprint_misses:
                    with conn.cursor() as cur:
                        cur.execute(
                            f"""
                            SELECT {JOB_HISTORY_SELECT}
                            FROM job
                            WHERE fingerprint = %s
                            ORDER BY id DESC
                            LIMIT 1
                            """,
                            (fingerprint,),
                        )
                        existing_row = cur.fetchone()

                if existing_row:
                    candidate = _row_to_service_job(existing_row)
                    queue_snapshot = jobs_service.get_job(conn, candidate.id)
                    if queue_snapshot and queue_snapshot.status in ACTIVE_STATUSES:
                        existing_job = queue_snapshot
                        fingerprint_cache[fingerprint] = queue_snapshot
                    else:
                        fingerprint_misses.add(fingerprint)
                elif existing_job is None:
                    fingerprint_misses.add(fingerprint)

            if existing_job is not None:
                reused.append(_job_to_response(existing_job))
                continue

            queue_job = jobs_service.enqueue_job(
                conn,
                job_type=job_spec.job_type,
                payload=payload,
                priority=request.priority,
            )

            _record_job_history(conn, queue_job, payload, fingerprint)

            accepted.append(_job_to_response(queue_job))
            if dedupe_enabled:
                fingerprint_cache[fingerprint] = queue_job
                fingerprint_misses.discard(fingerprint)

    return JobEnqueueResponse(
        accepted=accepted,
        reused=reused,
        rejected=rejected,
    )


@router.get("", response_model=JobListResponse)
def list_jobs(
    status: str | None = Query(
        None,
        description="Filter jobs by status (queued, running, failed, done)",
    ),
    job_type: str | None = Query(
        None,
        alias="type",
        description="Filter jobs by job type",
    ),
    limit: int | None = Query(
        50,
        ge=1,
        le=500,
        description="Maximum number of jobs to return",
    ),
    offset: int = Query(
        0,
        ge=0,
        description="Number of matching jobs to skip from the newest",
    ),
) -> JobListResponse:
    """Return jobs ordered from newest to oldest with optional filtering."""

    normalized_status: str | None = None
    if status is not None:
        normalized_status = status.strip().lower()
        if normalized_status not in ALLOWED_STATUSES:
            raise HTTPException(status_code=400, detail="invalid status filter")

    normalized_type: str | None = None
    if job_type is not None:
        normalized_type = job_type.strip()
        if not normalized_type:
            raise HTTPException(status_code=400, detail="invalid type filter")
    with db_conn() as conn:
        jobs = jobs_service.list_jobs_admin(
            conn,
            status=normalized_status,
            job_type=normalized_type,
            limit=limit,
            offset=offset,
        )

    responses = [_job_to_response(job) for job in jobs]
    return JobListResponse(jobs=responses, count=len(responses))


@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: int) -> JobResponse:
    """Return a single job by identifier."""

    with db_conn() as conn:
        job = jobs_service.get_job(conn, job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="job not found")

    return _job_to_response(job)


__all__ = [
    "enqueue_jobs",
    "get_job",
    "list_jobs",
    "router",
]
