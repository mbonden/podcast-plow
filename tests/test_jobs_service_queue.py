from __future__ import annotations

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
