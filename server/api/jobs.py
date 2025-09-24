"""Administrative endpoints for managing background jobs."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, field_validator

try:  # pragma: no cover - executed in Docker container
    from server.db.utils import db_conn
except ModuleNotFoundError as exc:  # pragma: no cover - executed locally
    if exc.name not in {"server", "server.db", "server.db.utils"}:
        raise
    from db.utils import db_conn

router = APIRouter(prefix="/jobs", tags=["jobs"])

JOB_RETURNING_COLUMNS = (
    "id, job_type, status, payload, result, error, created_at, updated_at"
)
ALLOWED_STATUSES = {"queued", "running", "failed", "done"}


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


class JobListResponse(BaseModel):
    jobs: list[JobResponse]
    count: int


class JobCreateRequest(BaseModel):
    job_type: str = Field(..., min_length=1)
    payload: dict[str, Any] | list[dict[str, Any]]

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

    def iter_payloads(self) -> Iterable[dict[str, Any]]:
        payload = self.payload
        if isinstance(payload, list):
            for item in payload:
                yield deepcopy(item)
        else:
            yield deepcopy(payload)


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
    )


@router.post("", response_model=JobListResponse, status_code=201)
def enqueue_jobs(request: JobCreateRequest) -> JobListResponse:
    """Insert new jobs into the queue with an initial queued status."""

    payloads = list(request.iter_payloads())
    jobs: list[JobResponse] = []
    with db_conn() as conn:
        with conn.cursor() as cur:
            for payload in payloads:
                cur.execute(
                    f"""
                    INSERT INTO job (job_type, status, payload)
                    VALUES (%s, %s, %s)
                    RETURNING {JOB_RETURNING_COLUMNS}
                    """,
                    (request.job_type, "queued", payload),
                )
                row = cur.fetchone()
                jobs.append(_row_to_job(row))
    return JobListResponse(jobs=jobs, count=len(jobs))


@router.get("", response_model=JobListResponse)
def list_jobs(
    status: str | None = Query(
        None,
        description="Filter jobs by status (queued, running, failed, done)",
    ),
    limit: int | None = Query(
        50,
        ge=1,
        le=500,
        description="Maximum number of jobs to return",
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

    sql = f"SELECT {JOB_RETURNING_COLUMNS} FROM job"
    if filters:
        sql += " WHERE " + " AND ".join(filters)
    sql += " ORDER BY id DESC"
    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

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
