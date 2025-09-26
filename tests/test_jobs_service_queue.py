from __future__ import annotations

import datetime as dt

import pytest

from server.services import jobs as jobs_service

from .fake_db import FakeConnection, FakeDatabase


def test_list_jobs_orders_by_priority_and_filters_status() -> None:
    db = FakeDatabase()

    with FakeConnection(db) as conn:
        low = jobs_service.enqueue_job(
            conn,
            job_type="summarize",
            payload={"episode_id": 1},
            priority=0,
        )
        high = jobs_service.enqueue_job(
            conn,
            job_type="summarize",
            payload={"episode_id": 2},
            priority=5,
        )
        mid = jobs_service.enqueue_job(
            conn,
            job_type="summarize",
            payload={"episode_id": 3},
            priority=2,
        )

    with FakeConnection(db) as conn:
        first = jobs_service.dequeue_job(conn)
        assert first is not None
        jobs_service.mark_job_done(conn, first.id)

    with FakeConnection(db) as conn:
        second = jobs_service.dequeue_job(conn)
        assert second is not None
        second.attempts = second.max_attempts
        jobs_service.mark_job_failed(conn, second, "boom")

    with FakeConnection(db) as conn:
        all_jobs = jobs_service.list_jobs(conn)

    assert [job.id for job in all_jobs] == [high.id, mid.id, low.id]

    with FakeConnection(db) as conn:
        queued_jobs = jobs_service.list_jobs(conn, status="queued")

    assert [job.id for job in queued_jobs] == [low.id]


def test_list_jobs_respects_limit() -> None:
    db = FakeDatabase()

    with FakeConnection(db) as conn:
        jobs_service.enqueue_job(
            conn,
            job_type="summarize",
            payload={"episode_id": 10},
            priority=0,
        )
        jobs_service.enqueue_job(
            conn,
            job_type="summarize",
            payload={"episode_id": 11},
            priority=1,
        )
        jobs_service.enqueue_job(
            conn,
            job_type="summarize",
            payload={"episode_id": 12},
            priority=2,
        )

    with FakeConnection(db) as conn:
        limited = jobs_service.list_jobs(conn, limit=2)

    assert len(limited) == 2
    assert [job.priority for job in limited] == [2, 1]


def test_enqueue_job_respects_configured_default(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeDatabase()
    monkeypatch.setattr(jobs_service, "DEFAULT_MAX_ATTEMPTS", 5)

    with FakeConnection(db) as conn:
        job = jobs_service.enqueue_job(conn, job_type="alpha")

    assert job.max_attempts == 5

    with FakeConnection(db) as conn:
        stored = jobs_service.get_job(conn, job.id)

    assert stored is not None
    assert stored.max_attempts == 5


def test_mark_job_failed_applies_jittered_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    db = FakeDatabase()

    with FakeConnection(db) as conn:
        jobs_service.enqueue_job(conn, job_type="beta")

    with FakeConnection(db) as conn:
        job = jobs_service.dequeue_job(conn)
        assert job is not None
        monkeypatch.setattr(jobs_service.random, "uniform", lambda low, high: low)
        before = dt.datetime.now(tz=jobs_service.UTC)
        jobs_service.mark_job_failed(conn, job, "boom")

    with FakeConnection(db) as conn:
        refreshed = jobs_service.get_job(conn, job.id)

    assert refreshed is not None
    assert refreshed.status == "queued"
    assert refreshed.next_run_at == refreshed.run_at
    assert refreshed.run_at is not None
    delay = (refreshed.run_at - before).total_seconds()
    assert 30 <= delay <= 60


def test_update_job_progress_records_payload() -> None:
    db = FakeDatabase()

    with FakeConnection(db) as conn:
        job = jobs_service.enqueue_job(conn, job_type="gamma")
        jobs_service.update_job_progress(
            conn,
            job.id,
            total_chunks=5,
            completed_chunks=2,
            current_chunk=1,
            message="Processing",
        )

    with FakeConnection(db) as conn:
        stored = jobs_service.get_job(conn, job.id)

    assert stored is not None
    assert isinstance(stored.result, dict)
    assert stored.result["total_chunks"] == 5
    assert stored.result["completed_chunks"] == 2
    assert stored.result["current_chunk"] == 1
    assert stored.result["percent_complete"] == pytest.approx(0.4)
    assert stored.result.get("message") == "Processing"
