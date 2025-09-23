from __future__ import annotations

import pathlib
import sys
from typing import Dict, List

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from server.services.grader import AutoGradeService, EvidenceItem, compute_grade
from tests.fake_db import FakeConnection, FakeDatabase
from worker.worker import run_auto_grade_job


EVIDENCE_PLAN: Dict[int, List[tuple[str, str]]] = {
    1: [("supports", "Meta-analysis of randomized trials")],
    2: [
        ("supports", "Randomized controlled trial"),
        ("supports", "Double-blind randomized clinical trial"),
    ],
    3: [("supports", "Randomized controlled trial")],
    4: [
        ("supports", "Prospective observational cohort"),
        ("supports", "Retrospective cohort study"),
    ],
    5: [("supports", "Mouse model mechanistic study")],
    7: [
        ("supports", "Randomized controlled trial"),
        ("refutes", "Randomized controlled trial"),
        ("refutes", "Observational study"),
    ],
    8: [("supports", "In vitro mechanistic experiment")],
    9: [("supports", "Pilot observational study")],
    10: [("supports", "Systematic review of randomized trials")],
    11: [("supports", "Observational cohort")],
    12: [("supports", "Case report")],
}


def _seed_claims(db: FakeDatabase) -> None:
    db.tables["podcast"].append({"id": 1, "title": "Testcast"})
    for episode_id in range(1, 4):
        db.tables["episode"].append(
            {
                "id": episode_id,
                "podcast_id": 1,
                "title": f"Episode {episode_id}",
                "created_at": episode_id,
            }
        )
    for claim_id in range(1, 13):
        db.tables["claim"].append(
            {
                "id": claim_id,
                "episode_id": ((claim_id - 1) % 3) + 1,
                "raw_text": f"Claim {claim_id}",
                "normalized_text": f"claim {claim_id}",
                "created_at": claim_id,
            }
        )

    evidence_id = 1
    for claim_id, entries in EVIDENCE_PLAN.items():
        for stance, ev_type in entries:
            db.tables["evidence_source"].append(
                {
                    "id": evidence_id,
                    "title": f"Study {evidence_id}",
                    "type": ev_type,
                    "year": 2020,
                    "journal": None,
                    "doi": None,
                    "pubmed_id": None,
                    "url": None,
                }
            )
            db.tables["claim_evidence"].append(
                {
                    "claim_id": claim_id,
                    "evidence_id": evidence_id,
                    "stance": stance,
                }
            )
            evidence_id += 1


@pytest.fixture
def seeded_db() -> FakeDatabase:
    db = FakeDatabase()
    _seed_claims(db)
    return db


def test_compute_grade_rules() -> None:
    grade, rationale = compute_grade(
        [EvidenceItem(stance="supports", type="Meta-analysis of randomized trials")]
    )
    assert grade == "strong"
    assert rationale.startswith("Auto-graded as strong")

    grade, rationale = compute_grade(
        [EvidenceItem(stance="supports", type="Randomized controlled trial")]
    )
    assert grade == "moderate"
    assert "randomized trial" in rationale

    grade, rationale = compute_grade(
        [EvidenceItem(stance="supports", type="Mouse model study")]
    )
    assert grade == "weak"
    assert "mechanistic/animal" in rationale

    grade, rationale = compute_grade(
        [
            EvidenceItem(stance="supports", type="Randomized controlled trial"),
            EvidenceItem(stance="refutes", type="Observational study"),
            EvidenceItem(stance="refutes", type="Randomized controlled trial"),
        ]
    )
    assert grade == "unsupported"
    assert rationale.count(".") in {1, 2}


def test_service_grades_many_claims(seeded_db: FakeDatabase) -> None:
    service = AutoGradeService(FakeConnection(seeded_db))
    results = service.grade_claims()

    assert len(results) == 12
    grades = {row["claim_id"]: row["grade"] for row in results}
    assert grades[1] == "strong"
    assert grades[2] == "strong"
    assert grades[3] == "moderate"
    assert grades[4] == "moderate"
    assert grades[5] == "weak"
    assert grades[6] == "unsupported"
    assert grades[7] == "unsupported"

    for row in results:
        assert row["rationale"].count(".") in {1, 2}

    assert len(seeded_db.tables["claim_grade"]) == 12
    assert {entry["rubric_version"] for entry in seeded_db.tables["claim_grade"]} == {"v1"}


def test_regrading_creates_history(seeded_db: FakeDatabase) -> None:
    service = AutoGradeService(FakeConnection(seeded_db))
    service.grade_claims(claim_ids=[1])
    first = [row for row in seeded_db.tables["claim_grade"] if row["claim_id"] == 1]
    assert len(first) == 1

    service.grade_claims(claim_ids=[1])
    history = [row for row in seeded_db.tables["claim_grade"] if row["claim_id"] == 1]
    assert len(history) == 2
    assert history[0]["grade"] == history[1]["grade"]
    assert history[0]["rationale"] == history[1]["rationale"]


def test_cli_auto_grade_filters(monkeypatch: pytest.MonkeyPatch, seeded_db: FakeDatabase) -> None:
    monkeypatch.setattr("worker.worker.db_conn", lambda: FakeConnection(seeded_db))
    total = run_auto_grade_job(claim_ids=[1, 2], episode_ids=None)
    assert total == 2
    graded_ids = {row["claim_id"] for row in seeded_db.tables["claim_grade"]}
    assert graded_ids == {1, 2}
