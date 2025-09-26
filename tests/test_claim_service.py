import json
import sys
from pathlib import Path
from typing import Iterable, Tuple

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SERVER_ROOT = ROOT / "server"
if str(SERVER_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVER_ROOT))

from tests.fake_db import FakeConnection, FakeDatabase

import server.services.claims as claims_service
import server.services.chunker as chunker_service


@pytest.fixture(scope="module")
def sample_transcripts() -> Iterable[dict]:
    data_path = Path(__file__).parent / "data" / "sample_transcripts.json"
    with data_path.open() as fh:
        payload = json.load(fh)
    return payload["episodes"]


@pytest.fixture()
def seeded_conn(sample_transcripts: Iterable[dict]) -> Tuple[FakeConnection, FakeDatabase]:
    db = FakeDatabase()
    conn = FakeConnection(db)
    cur = conn.cursor()
    cur.execute("INSERT INTO podcast (id, title) VALUES (%s, %s)", (1, "Synthetic Podcast"))
    for episode in sample_transcripts:
        episode_id = episode["id"]
        cur.execute(
            "INSERT INTO episode (id, podcast_id, title) VALUES (%s, %s, %s)",
            (episode_id, 1, episode["title"]),
        )
        text = episode["transcript"]
        cur.execute(
            """
            INSERT INTO transcript (episode_id, source, lang, text, word_count, has_verbatim_ok)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (episode_id, "synthetic", "en", text, len(text.split()), True),
        )
    return conn, db


def test_extract_claims_for_each_episode(seeded_conn: Tuple[FakeConnection, FakeDatabase], sample_transcripts: Iterable[dict]) -> None:
    conn, db = seeded_conn
    for episode in sample_transcripts:
        episode_id = episode["id"]
        stored = claims_service.extract_episode_claims(conn, episode_id, refresh=True)
        assert len(stored) >= 5
        starts = [claim.start_ms for claim in stored]
        assert starts == sorted(starts)
        assert all(claim.start_ms < claim.end_ms for claim in stored)

    for episode in sample_transcripts:
        episode_id = episode["id"]
        rows = [row for row in db.tables["claim"] if row["episode_id"] == episode_id]
        assert len(rows) >= 5
        normalized = [row["normalized_text"] for row in rows]
        assert len(normalized) == len(set(normalized))

    topic_episodes: dict[str, set[int]] = {}
    for row in db.tables["claim"]:
        topic_episodes.setdefault(row["topic"], set()).add(row["episode_id"])
    assert any(len(episodes) >= 2 for episodes in topic_episodes.values())


def test_extract_claims_is_idempotent(seeded_conn: Tuple[FakeConnection, FakeDatabase]) -> None:
    conn, db = seeded_conn
    episode_id = 1
    claims_service.extract_episode_claims(conn, episode_id, refresh=True)
    before = [row for row in db.tables["claim"] if row["episode_id"] == episode_id]
    claims_service.extract_episode_claims(conn, episode_id, refresh=False)
    after = [row for row in db.tables["claim"] if row["episode_id"] == episode_id]

    assert len(after) == len(before)
    assert {row["id"] for row in after} == {row["id"] for row in before}
    assert len({row["normalized_text"] for row in after}) == len(after)


def test_chunker_reuses_chunks_when_transcript_unchanged(
    seeded_conn: Tuple[FakeConnection, FakeDatabase]
) -> None:
    conn, db = seeded_conn
    episode_id = 1

    first = chunker_service.ensure_chunks_for_episode(conn, episode_id, refresh=True)
    assert first is not None

    initial_rows = [row for row in db.tables["transcript_chunk"] if row["transcript_id"] == first.transcript.id]
    assert initial_rows, "expected chunks to be created"
    hashes = {row.get("source_hash") for row in initial_rows}
    assert len(hashes - {None}) == 1

    chunker_service.ensure_chunks_for_episode(conn, episode_id, refresh=False)
    second_rows = [row for row in db.tables["transcript_chunk"] if row["transcript_id"] == first.transcript.id]
    assert [row["id"] for row in second_rows] == [row["id"] for row in initial_rows]

    transcript_row = next(row for row in db.tables["transcript"] if row["episode_id"] == episode_id)
    transcript_row["text"] = (transcript_row["text"] or "") + " extra"
    transcript_row["word_count"] = len((transcript_row["text"] or "").split())

    chunker_service.ensure_chunks_for_episode(conn, episode_id, refresh=False)
    updated_rows = [row for row in db.tables["transcript_chunk"] if row["transcript_id"] == first.transcript.id]
    assert [row["id"] for row in updated_rows] != [row["id"] for row in initial_rows]
