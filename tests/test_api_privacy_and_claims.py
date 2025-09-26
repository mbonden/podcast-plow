from __future__ import annotations

from pathlib import Path
import sys
from typing import Any, Iterable, List

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


def _iter_keys(payload: Any) -> Iterable[str]:
    if isinstance(payload, dict):
        for key, value in payload.items():
            yield key
            yield from _iter_keys(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from _iter_keys(item)


def _iter_strings(payload: Any) -> Iterable[str]:
    if isinstance(payload, str):
        yield payload
    elif isinstance(payload, dict):
        for value in payload.values():
            yield from _iter_strings(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from _iter_strings(item)


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
                    "INSERT INTO transcript (episode_id, source, lang, text) VALUES (%s, %s, %s, %s)",
                    (episode_id, "upload", "en", transcript_text),
                )
            cur.execute(
                "INSERT INTO episode_outline (episode_id, start_ms, end_ms, heading, bullet_points) VALUES (%s, %s, %s, %s, %s)",
                (
                    1,
                    0,
                    90000,
                    "Foundations",
                    "- Safe protocols\n- Benefits overview",
                ),
            )
            cur.execute(
                "INSERT INTO episode_outline (episode_id, start_ms, end_ms, heading, bullet_points) VALUES (%s, %s, %s, %s, %s)",
                (
                    1,
                    90000,
                    180000,
                    "Protocol deep dive",
                    "• Scheduling cold exposure\n• Contrast showers",
                ),
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


def test_episode_outline_endpoint_returns_outline(seeded_client: TestClient) -> None:
    response = seeded_client.get("/episodes/1/outline")
    assert response.status_code == 200

    payload = response.json()
    assert payload["episode_id"] == 1
    assert payload["title"] == "Ep. 825 - Dominic D’Agostino"

    outline = payload["outline"]
    assert [item["heading"] for item in outline] == ["Foundations", "Protocol deep dive"]
    assert outline[0]["start_ms"] == 0
    assert outline[1]["start_ms"] == 90000

    assert outline[0]["bullet_points"] == ["Safe protocols", "Benefits overview"]
    assert outline[1]["bullet_points"] == ["Scheduling cold exposure", "Contrast showers"]

    for item in outline:
        for bullet in item.get("bullet_points", []):
            assert "SECRET" not in bullet


def test_episode_outline_endpoint_missing_returns_404(seeded_client: TestClient) -> None:
    response = seeded_client.get("/episodes/2/outline")
    assert response.status_code == 404

    payload = response.json()
    assert payload["error"] == "outline not available"


def test_claim_endpoints_return_seed_data(seeded_client: TestClient) -> None:
    claim_resp = seeded_client.get("/claims/1")
    assert claim_resp.status_code == 200
    claim = claim_resp.json()

    assert claim["grade"] == "moderate"
    assert any(evidence["stance"] == "supports" for evidence in claim["evidence"])
    assert claim["evidence"]
    assert all("is_primary" in evidence for evidence in claim["evidence"])
    assert claim["evidence"][0]["is_primary"] is True

    topic_resp = seeded_client.get("/topics/ketones/claims")
    assert topic_resp.status_code == 200
    topic = topic_resp.json()

    assert topic["topic"] == "ketones"
    assert any(item["claim_id"] == 1 for item in topic["claims"])

    topic_grades = {item["claim_id"]: item["grade"] for item in topic["claims"]}
    assert topic_grades.get(1) == "moderate"


def test_search_endpoint_matches_claims(seeded_client: TestClient) -> None:
    response = seeded_client.get("/search", params={"q": "ketones"})
    assert response.status_code == 200

    payload = response.json()
    assert payload["q"] == "ketones"

    episodes = payload.get("episodes")
    assert isinstance(episodes, list)
    for episode in episodes:
        assert "id" in episode
        assert "title" in episode
        assert "published_at" in episode

    matching_claims = [item for item in payload["claims"] if item["id"] == 1]
    assert matching_claims
    claim = matching_claims[0]
    assert claim["grade"] == "moderate"
    assert claim["grade_rationale"]
    assert claim["rubric_version"]
    assert claim["graded_at"] is not None
    assert claim["episode_id"] == 1
    assert claim["episode_title"] == "Ep. 825 - Dominic D’Agostino"
    assert "episode_published_at" in claim
    assert claim["domain"] == "neuro"
    assert claim["risk_level"] == "low"


@pytest.mark.parametrize(
    ("path", "params"),
    [
        ("/episodes/1", None),
        ("/episodes/2", None),
        ("/episodes/3", None),
        ("/episodes/1/outline", None),
        ("/topics/ketones/claims", None),
        ("/claims/1", None),
        ("/search", {"q": "ketones"}),
    ],
)
def test_api_endpoints_do_not_expose_transcripts(
    seeded_client: TestClient, path: str, params: dict | None
) -> None:
    response = seeded_client.get(path, params=params)
    assert response.status_code == 200

    payload = response.json()
    assert all(key.lower() != "transcript" for key in _iter_keys(payload))

    for transcript_text in TRANSCRIPT_TEXT_BY_EPISODE.values():
        assert transcript_text not in response.text
        for text_value in _iter_strings(payload):
            assert transcript_text not in text_value


def test_search_endpoint_matches_episodes(seeded_client: TestClient) -> None:
    response = seeded_client.get("/search", params={"q": "brain"})
    assert response.status_code == 200

    payload = response.json()
    titles = {item["title"] for item in payload["episodes"]}
    assert "Brain and Body Chat 015" in titles
