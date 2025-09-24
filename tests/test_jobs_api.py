from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SERVER_ROOT = ROOT / "server"
if str(SERVER_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVER_ROOT))

TESTS_ROOT = ROOT / "tests"
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

import server.app as app_module
import server.api.jobs as jobs_module
from server.services import jobs as jobs_service
from fake_db import FakeConnection, FakeDatabase


@pytest.fixture
def fake_db(monkeypatch: pytest.MonkeyPatch) -> FakeDatabase:
    database = FakeDatabase()

    def _db_conn() -> FakeConnection:
        return FakeConnection(database)

    monkeypatch.setattr(app_module, "db_conn", _db_conn)
    monkeypatch.setattr(jobs_module, "db_conn", _db_conn)
    return database


@pytest.fixture
def client(fake_db: FakeDatabase) -> Iterable[TestClient]:
    with TestClient(app_module.app) as client:
        yield client


def test_enqueue_single_job_with_priority(
    client: TestClient, fake_db: FakeDatabase
) -> None:
    response = client.post(
        "/jobs",
        json={
            "job_type": "summarize_episode",
            "payload": {"episode_id": 42},
            "priority": 7,
        },
    )
    assert response.status_code == 201

    payload = response.json()
    assert payload["count"] == 1
    job = payload["jobs"][0]

    assert job["job_type"] == "summarize_episode"
    assert job["status"] == "queued"
    assert job["payload"] == {"episode_id": 42}
    assert job["priority"] == 7

    stored_job = fake_db.tables["job_queue"][0]
    assert stored_job["job_type"] == "summarize_episode"
    assert stored_job["priority"] == 7


def test_enqueue_multiple_jobs(client: TestClient, fake_db: FakeDatabase) -> None:
    response = client.post(
        "/jobs",
        json={
            "job_type": "extract",
            "payload": [
                {"episode_id": 1},
                {"episode_id": 2},
            ],
        },
    )
    assert response.status_code == 201

    payload = response.json()
    assert payload["count"] == 2
    job_ids = [job["id"] for job in payload["jobs"]]
    assert len(set(job_ids)) == 2
    assert all(job["status"] == "queued" for job in payload["jobs"])
    assert {job["payload"]["episode_id"] for job in payload["jobs"]} == {1, 2}

    stored_ids = {row["payload"].get("episode_id") for row in fake_db.tables["job_queue"]}
    assert stored_ids == {1, 2}
    assert all(row["priority"] == 0 for row in fake_db.tables["job_queue"])


def test_enqueue_with_dedupe_returns_existing_job(
    client: TestClient, fake_db: FakeDatabase
) -> None:
    first = client.post(
        "/jobs",
        json={
            "job_type": "summarize_episode",
            "payload": {"episode_id": 99},
            "priority": 5,
            "dedupe": True,
        },
    )
    first_id = first.json()["jobs"][0]["id"]

    with app_module.db_conn() as conn:
        dequeued = jobs_service.dequeue_job(conn, job_types=["summarize_episode"])
        assert dequeued is not None

    second = client.post(
        "/jobs",
        json={
            "job_type": "summarize_episode",
            "payload": {"episode_id": 99},
            "priority": 1,
            "dedupe": True,
        },
    )
    assert second.status_code == 201
    second_body = second.json()

    assert second_body["count"] == 1
    assert second_body["jobs"][0]["id"] == first_id
    assert second_body["jobs"][0]["status"] == "running"
    assert len(fake_db.tables["job_queue"]) == 1


def test_enqueue_with_string_false_does_not_dedupe(
    client: TestClient, fake_db: FakeDatabase
) -> None:
    first = client.post(
        "/jobs",
        json={
            "job_type": "summarize_episode",
            "payload": {"episode_id": 101},
            "priority": 1,
            "dedupe": True,
        },
    )
    first_id = first.json()["jobs"][0]["id"]

    second = client.post(
        "/jobs",
        json={
            "job_type": "summarize_episode",
            "payload": {"episode_id": 101},
            "priority": 3,
            "dedupe": "false",
        },
    )
    assert second.status_code == 201
    payload = second.json()

    assert payload["count"] == 1
    assert payload["jobs"][0]["id"] != first_id
    assert len(fake_db.tables["job_queue"]) == 2


def test_get_job_returns_latest_status(client: TestClient) -> None:
    create_resp = client.post(
        "/jobs",
        json={"job_type": "evidence", "payload": {"claim_id": 9}},
    )
    job_id = create_resp.json()["jobs"][0]["id"]

    with app_module.db_conn() as conn:
        dequeued = jobs_service.dequeue_job(conn, job_types=["evidence"])
        assert dequeued is not None

    detail = client.get(f"/jobs/{job_id}")
    assert detail.status_code == 200
    data = detail.json()
    assert data["id"] == job_id
    assert data["status"] == "running"
    assert data["payload"] == {"claim_id": 9}

    with app_module.db_conn() as conn:
        jobs_service.mark_job_done(conn, job_id)

    done_detail = client.get(f"/jobs/{job_id}")
    assert done_detail.status_code == 200
    assert done_detail.json()["status"] == "done"


def test_list_jobs_supports_filters_and_limit(
    client: TestClient, fake_db: FakeDatabase
) -> None:
    client.post(
        "/jobs",
        json={"job_type": "summarize", "payload": {"episode_id": 1}},
    )
    second = client.post(
        "/jobs",
        json={"job_type": "grade", "payload": {"claim_id": 2}},
    ).json()["jobs"][0]

    with app_module.db_conn() as conn:
        jobs_service.mark_job_done(conn, second["id"])

    list_resp = client.get("/jobs?status=done&type=grade&limit=1")
    assert list_resp.status_code == 200
    data = list_resp.json()
    assert data["count"] == 1
    assert all(job["status"] == "done" for job in data["jobs"])
    assert data["jobs"][0]["id"] == second["id"]
    assert data["jobs"][0]["job_type"] == "grade"


def test_list_jobs_orders_by_priority_then_id(client: TestClient) -> None:
    low = client.post(
        "/jobs",
        json={"job_type": "summarize", "payload": {"episode_id": 1}, "priority": 1},
    ).json()["jobs"][0]
    mid = client.post(
        "/jobs",
        json={"job_type": "summarize", "payload": {"episode_id": 2}, "priority": 5},
    ).json()["jobs"][0]
    high = client.post(
        "/jobs",
        json={"job_type": "summarize", "payload": {"episode_id": 3}, "priority": 9},
    ).json()["jobs"][0]
    later_mid = client.post(
        "/jobs",
        json={"job_type": "summarize", "payload": {"episode_id": 4}, "priority": 5},
    ).json()["jobs"][0]

    listing = client.get("/jobs?limit=4").json()
    ordered_ids = [job["id"] for job in listing["jobs"]]

    assert ordered_ids == [high["id"], later_mid["id"], mid["id"], low["id"]]


def test_list_jobs_supports_offset(client: TestClient) -> None:
    first = client.post(
        "/jobs",
        json={"job_type": "alpha", "payload": {"value": 1}, "priority": 10},
    ).json()["jobs"][0]
    second = client.post(
        "/jobs",
        json={"job_type": "alpha", "payload": {"value": 2}, "priority": 8},
    ).json()["jobs"][0]
    third = client.post(
        "/jobs",
        json={"job_type": "alpha", "payload": {"value": 3}, "priority": 6},
    ).json()["jobs"][0]

    response = client.get("/jobs?limit=1&offset=1")
    assert response.status_code == 200
    data = response.json()

    assert data["count"] == 1
    assert data["jobs"][0]["id"] == second["id"]
    assert data["jobs"][0]["priority"] == second["priority"]


def test_invalid_status_filter_returns_error(client: TestClient) -> None:
    response = client.get("/jobs?status=unknown")
    assert response.status_code == 400
    assert response.json()["detail"] == "invalid status filter"


def test_invalid_type_filter_returns_error(client: TestClient) -> None:
    response = client.get("/jobs?type=")
    assert response.status_code == 400
    assert response.json()["detail"] == "invalid type filter"
