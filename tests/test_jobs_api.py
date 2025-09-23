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

import server.app as app_module
import server.api.jobs as jobs_module
from tests.fake_db import FakeConnection, FakeDatabase


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


def test_enqueue_single_job(client: TestClient, fake_db: FakeDatabase) -> None:
    response = client.post(
        "/jobs",
        json={"job_type": "summarize", "payload": {"episode_id": 42}},
    )
    assert response.status_code == 201

    payload = response.json()
    assert payload["count"] == 1
    job = payload["jobs"][0]

    assert job["job_type"] == "summarize"
    assert job["status"] == "queued"
    assert job["payload"] == {"episode_id": 42}
    assert fake_db.tables["job"][0]["job_type"] == "summarize"


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

    stored_ids = {row["payload"].get("episode_id") for row in fake_db.tables["job"]}
    assert stored_ids == {1, 2}


def test_get_job_returns_latest_status(client: TestClient) -> None:
    create_resp = client.post(
        "/jobs",
        json={"job_type": "evidence", "payload": {"claim_id": 9}},
    )
    job_id = create_resp.json()["jobs"][0]["id"]

    with app_module.db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE job SET status = %s, error = %s, updated_at = now() WHERE id = %s RETURNING id",
            ("running", None, job_id),
        )

    detail = client.get(f"/jobs/{job_id}")
    assert detail.status_code == 200
    data = detail.json()
    assert data["id"] == job_id
    assert data["status"] == "running"
    assert data["payload"] == {"claim_id": 9}


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
        cur = conn.cursor()
        cur.execute(
            "UPDATE job SET status = %s, updated_at = now() WHERE id = %s",
            ("done", second["id"]),
        )

    list_resp = client.get("/jobs?status=done&limit=1")
    assert list_resp.status_code == 200
    data = list_resp.json()
    assert data["count"] == 1
    assert all(job["status"] == "done" for job in data["jobs"])
    assert data["jobs"][0]["id"] == second["id"]


def test_invalid_status_filter_returns_error(client: TestClient) -> None:
    response = client.get("/jobs?status=unknown")
    assert response.status_code == 400
    assert response.json()["detail"] == "invalid status filter"
