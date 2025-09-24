"""Administrative endpoints for managing background jobs."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Sequence

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

try:  # pragma: no cover - executed in Docker container
    from server.db.utils import db_conn
except ModuleNotFoundError as exc:  # pragma: no cover - executed locally
    if exc.name not in {"server", "server.db", "server.db.utils"}:
        raise
    from db.utils import db_conn

try:  # pragma: no cover - executed in Docker container
    from server.services import jobs as jobs_service
except ModuleNotFoundError as exc:  # pragma: no cover - executed locally
    if exc.name not in {"server", "server.services", "server.services.jobs"}:
        raise
    from services import jobs as jobs_service

router = APIRouter(prefix="/jobs", tags=["jobs"])

JOB_RETURNING_COLUMNS = (
    "id, job_type, status, payload, result, error, created_at, updated_at, priority"
)
ALLOWED_STATUSES = {"queued", "running", "failed", "done"}
ACTIVE_STATUSES = {"queued", "running"}


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


def _row_to_job(row: Sequence[Any] | None) -> JobResponse:
    if row is None:
        raise ValueError("Expected database row but received None")
    if isinstance(row, dict):
        data = row
    else:
        keys = (
            "id",
            "job_type",
            "status",
            "payload",
            "result",
            "error",
            "created_at",
            "updated_at",
            "priority",
        )
        data = dict(zip(keys, row, strict=False))
    return JobResponse(
        id=data.get("id"),
        job_type=data.get("job_type"),
        status=data.get("status"),
        payload=data.get("payload"),
        result=data.get("result"),
        error=data.get("error"),
        created_at=_normalize_timestamp(data.get("created_at")),
        updated_at=_normalize_timestamp(data.get("updated_at")),
        priority=int(data.get("priority") or 0),
    )


@router.post("", response_model=JobEnqueueResponse, status_code=201)
def enqueue_jobs(request: JobCreateRequest) -> JobEnqueueResponse:
    """Insert new jobs into the queue with optional deduplication."""

    accepted: list[JobResponse] = []
    reused: list[JobResponse] = []
    rejected: list[RejectedJob] = []
    dedupe_enabled = bool(request.dedupe)
    fingerprint_cache: dict[str, JobResponse] = {}
    fingerprint_misses: set[str] = set()

    with db_conn() as conn:
        with conn.cursor() as cur:
            for job_spec in request.jobs:
                payload = deepcopy(job_spec.payload)
                fingerprint = jobs_service.compute_job_fingerprint(
                    job_spec.job_type,
                    payload,
                )

                if dedupe_enabled:
                    existing_job = fingerprint_cache.get(fingerprint)
                    if existing_job is None and fingerprint not in fingerprint_misses:
                        cur.execute(
                            f"""
                            SELECT {JOB_RETURNING_COLUMNS}
                            FROM job
                            WHERE fingerprint = %s
                            ORDER BY id DESC
                            LIMIT 1
                            """,
                            (fingerprint,),
                        )
                        existing_row = cur.fetchone()
                        if existing_row:
                            candidate = _row_to_job(existing_row)
                            if candidate.status in ACTIVE_STATUSES:
                                existing_job = candidate
                                fingerprint_cache[fingerprint] = candidate
                            else:
                                fingerprint_misses.add(fingerprint)
                        else:
                            fingerprint_misses.add(fingerprint)

                    if existing_job is not None:
                        reused.append(existing_job)
                        continue

                cur.execute(
                    f"""
                    INSERT INTO job (job_type, status, payload, priority, fingerprint)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING {JOB_RETURNING_COLUMNS}
                    """,
                    (
                        job_spec.job_type,
                        "queued",
                        payload,
                        request.priority,
                        fingerprint,
                    ),
                )
                row = cur.fetchone()
                created = _row_to_job(row)
                accepted.append(created)
                if dedupe_enabled:
                    fingerprint_cache[fingerprint] = created
                    fingerprint_misses.discard(fingerprint)

    return JobEnqueueResponse(accepted=accepted, reused=reused, rejected=rejected)


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

    params: list[Any] = []
    filters: list[str] = []
    if status is not None:
        normalized_status = status.strip().lower()
        if normalized_status not in ALLOWED_STATUSES:
            raise HTTPException(status_code=400, detail="invalid status filter")
        filters.append("status = %s")
        params.append(normalized_status)

    if job_type is not None:
        normalized_type = job_type.strip()
        if not normalized_type:
            raise HTTPException(status_code=400, detail="invalid type filter")
        filters.append("job_type = %s")
        params.append(normalized_type)

    sql = f"SELECT {JOB_RETURNING_COLUMNS} FROM job"
    if filters:
        sql += " WHERE " + " AND ".join(filters)
    sql += " ORDER BY priority DESC, id DESC"
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)
    if offset:
        sql += " OFFSET %s"
        params.append(offset)

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

    jobs = [_row_to_job(row) for row in rows]
    return JobListResponse(jobs=jobs, count=len(jobs))


@router.get("/{job_id}", response_model=JobResponse)
def get_job(job_id: int) -> JobResponse:
    """Return a single job by identifier."""

    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {JOB_RETURNING_COLUMNS} FROM job WHERE id = %s",
                (job_id,),
            )
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="job not found")

    return _row_to_job(row)


__all__ = [
    "enqueue_jobs",
    "get_job",
    "list_jobs",
    "router",
]
