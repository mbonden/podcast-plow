from __future__ import annotations

from pathlib import Path
import sys
from typing import Iterable, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
from fastapi.testclient import TestClient

import server.app as app_module
from tests.fake_db import FakeConnection, FakeDatabase


def load_statements() -> List[str]:
    text = Path("docs/SEED.sql").read_text()
    statements: List[str] = []
    current: List[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            statements.append("\n".join(current))
            current = []
    if current:
        statements.append("\n".join(current))
    return statements


@pytest.fixture
def fake_db(monkeypatch: pytest.MonkeyPatch) -> FakeDatabase:
    database = FakeDatabase()

    def _db_conn() -> FakeConnection:
        return FakeConnection(database)

    monkeypatch.setattr(app_module, "db_conn", _db_conn)
    return database


@pytest.fixture
def seeded_client(fake_db: FakeDatabase) -> Iterable[TestClient]:
    statements = load_statements()
    transcript_sql = "INSERT INTO transcript (episode_id, source, lang, text) VALUES (1, 'upload', 'en', 'Full transcript text');"

    with TestClient(app_module.app) as client:
        with app_module.db_conn() as conn:
            cur = conn.cursor()
            for stmt in statements:
                cur.execute(stmt)
            cur.execute(transcript_sql)
        yield client


def test_episode_endpoint_hides_transcript(seeded_client: TestClient) -> None:
    response = seeded_client.get("/episodes/1")
    assert response.status_code == 200
    data = response.json()

    assert data["id"] == 1
    assert "transcript" not in data
    assert data.get("summary", {}).get("tl_dr") == "Ketones, creatine, pragmatic levers."

    grades = {item["id"]: item["grade"] for item in data.get("claims", [])}
    assert grades.get(1) == "moderate"


def test_claim_endpoints_return_seed_data(seeded_client: TestClient) -> None:
    claim_resp = seeded_client.get("/claims/1")
    assert claim_resp.status_code == 200
    claim = claim_resp.json()

    assert claim["grade"] == "moderate"
    assert any(evidence["stance"] == "supports" for evidence in claim["evidence"])

    topic_resp = seeded_client.get("/topics/ketones/claims")
    assert topic_resp.status_code == 200
    topic = topic_resp.json()

    assert topic["topic"] == "ketones"
    assert any(item["claim_id"] == 1 for item in topic["claims"])

    topic_grades = {item["claim_id"]: item["grade"] for item in topic["claims"]}
    assert topic_grades.get(1) == "moderate"
