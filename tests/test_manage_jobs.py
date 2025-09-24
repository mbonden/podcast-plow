from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
SERVER_ROOT = ROOT / "server"
if str(SERVER_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVER_ROOT))

import manage as manage_module

from tests.fake_db import FakeConnection, FakeDatabase

runner = CliRunner()


@pytest.fixture
def fake_db(monkeypatch: pytest.MonkeyPatch) -> FakeDatabase:
    database = FakeDatabase()

    def _db_conn() -> FakeConnection:
        return FakeConnection(database)

    monkeypatch.setattr(manage_module, "db_conn", _db_conn)
    return database


def test_jobs_enqueue_summarize_adds_queue_entries(fake_db: FakeDatabase) -> None:
    result = runner.invoke(
        manage_module.app,
        ["jobs", "enqueue", "summarize", "--episode-ids", "1,2"],
    )
    assert result.exit_code == 0

    rows = fake_db.tables["job_queue"]
    assert len(rows) == 2
    assert all(row["job_type"] == "summarize" for row in rows)

    payload_episode_ids = {
        json.loads(row["payload"]).get("episode_id") for row in rows
    }
    assert payload_episode_ids == {1, 2}


def test_jobs_work_once_processes_summarize_job(
    fake_db: FakeDatabase, monkeypatch: pytest.MonkeyPatch
) -> None:
    fake_db.tables["episode"].append({"id": 1, "podcast_id": 1, "title": "Episode 1"})
    fake_db.tables["transcript"].append(
        {
            "id": 1,
            "episode_id": 1,
            "text": "First chunk text. Second chunk text.",
            "word_count": 6,
        }
    )

    monkeypatch.setattr(
        manage_module.summarize_service,
        "_summarize_chunk_text",
        lambda text, desired: [f"Point {desired}"],
    )

    enqueue_result = runner.invoke(
        manage_module.app,
        ["jobs", "enqueue", "summarize", "--episode-ids", "1"],
    )
    assert enqueue_result.exit_code == 0

    work_result = runner.invoke(
        manage_module.app,
        ["jobs", "work", "--once", "--type", "summarize"],
    )
    assert work_result.exit_code == 0

    job_row = fake_db.tables["job_queue"][0]
    assert job_row["status"] == "done"

    summaries = [row for row in fake_db.tables["episode_summary"] if row["episode_id"] == 1]
    assert summaries, "Expected a summary to be stored for the episode"
    summary = summaries[-1]
    assert summary["created_by"] == "worker"
    assert "Point" in (summary.get("tl_dr") or "")

