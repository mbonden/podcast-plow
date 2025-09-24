"""Administrative endpoints for managing background jobs."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Iterable

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

from server.services import jobs as jobs_service

try:  # pragma: no cover - executed in Docker container
    from server.db.utils import db_conn
except ModuleNotFoundError as exc:  # pragma: no cover - executed locally
    if exc.name not in {"server", "server.db", "server.db.utils"}:
        raise
    from db.utils import db_conn

router = APIRouter(prefix="/jobs", tags=["jobs"])
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


class JobCreateRequest(BaseModel):
    job_type: str = Field(..., min_length=1)
    payload: dict[str, Any] | list[dict[str, Any]]
    priority: int = 0
    dedupe: bool | str | None = False

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
    def _ensure_payload(cls, value: Any) -> dict[str, Any] | list[dict[str, Any]]:
        if isinstance(value, dict):
            return value
        if isinstance(value, list):
            if not value:
                raise ValueError("payload list cannot be empty")
            for item in value:
                if not isinstance(item, dict):
                    raise TypeError("payload list entries must be objects")
            return value
        raise TypeError("payload must be an object or list of objects")

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

    def iter_payloads(self) -> Iterable[dict[str, Any]]:
        payload = self.payload
        if isinstance(payload, list):
            for item in payload:
                yield deepcopy(item)
        else:
            yield deepcopy(payload)


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


@router.post("", response_model=JobListResponse, status_code=201)
def enqueue_jobs(request: JobCreateRequest) -> JobListResponse:
    """Insert new jobs into the queue with an initial queued status."""

    payloads = list(request.iter_payloads())
    jobs: list[JobResponse] = []
    seen_job_ids: set[int] = set()
    dedupe_enabled = bool(request.dedupe)
    with db_conn() as conn:
        for payload in payloads:
            if dedupe_enabled:
                existing = jobs_service.find_job_by_payload(
                    conn,
                    job_type=request.job_type,
                    payload=payload,
                )
                if existing and existing.status in ACTIVE_STATUSES:
                    if existing.id not in seen_job_ids:
                        jobs.append(_job_to_response(existing))
                        seen_job_ids.add(existing.id)
                    continue
            job = jobs_service.enqueue_job(
                conn,
                job_type=request.job_type,
                payload=payload,
                priority=request.priority,
            )
            if job.id not in seen_job_ids:
                jobs.append(_job_to_response(job))
                seen_job_ids.add(job.id)
    return JobListResponse(jobs=jobs, count=len(jobs))


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
