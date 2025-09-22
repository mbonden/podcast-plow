from __future__ import annotations

from pathlib import Path
import sys
from typing import Iterable, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SERVER_ROOT = ROOT / "server"
if str(SERVER_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVER_ROOT))

import pytest
from fastapi.testclient import TestClient

import server.app as app_module
from tests.fake_db import FakeConnection, FakeDatabase


TRANSCRIPT_TEXT_BY_EPISODE = {
    1: "Full transcript text",
    2: "Episode 2 transcript contains SECRET_SAUNA",
    3: "Episode 3 transcript mentions SECRET_MAGNESIUM",
}

ADDITIONAL_EPISODE_SQL = [
    "INSERT INTO episode (id, podcast_id, title) VALUES (2, 1, 'Metabolic Morning Show 002');",
    "INSERT INTO episode (id, podcast_id, title) VALUES (3, 1, 'Brain and Body Chat 015');",
]


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

    with TestClient(app_module.app) as client:
        with app_module.db_conn() as conn:
            cur = conn.cursor()
            for stmt in statements:
                cur.execute(stmt)
            for stmt in ADDITIONAL_EPISODE_SQL:
                cur.execute(stmt)
            for episode_id, transcript_text in TRANSCRIPT_TEXT_BY_EPISODE.items():
                cur.execute(
                    "INSERT INTO transcript (episode_id, source, lang, text) "
                    f"VALUES ({episode_id}, 'upload', 'en', '{transcript_text}')"
                )
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


@pytest.mark.parametrize(
    ("episode_id", "transcript_text"),
    sorted(TRANSCRIPT_TEXT_BY_EPISODE.items()),
)
def test_episode_endpoint_omits_transcript_text(
    seeded_client: TestClient, episode_id: int, transcript_text: str
) -> None:
    response = seeded_client.get(f"/episodes/{episode_id}")
    assert response.status_code == 200

    payload = response.json()
    assert payload["id"] == episode_id
    assert "transcript" not in payload
    assert transcript_text not in response.text


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
