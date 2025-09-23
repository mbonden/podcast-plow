from __future__ import annotations

from dataclasses import dataclass
import pathlib
import sys

import pytest

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from server.core.grading import ClaimEvidence, EvidenceItem, compute_grade
from worker.auto_grade import AutoGrader


@dataclass
class FakeStore:
    rows: list[dict]

    def insert(self, claim_id: int, grade: str, rationale: str) -> None:
        self.rows.append({
            "claim_id": claim_id,
            "grade": grade,
            "rationale": rationale,
        })


def test_compute_grade_strong_support():
    evidence = [
        EvidenceItem(stance="supports", type="meta-analysis"),
        EvidenceItem(stance="supports", type="randomized controlled trial"),
    ]
    grade, rationale = compute_grade(evidence)
    assert grade == "strong"
    assert "supporting evidence" in rationale


@pytest.mark.parametrize(
    "evidence, expected",
    [
        (
            (
                EvidenceItem(stance="supports", type="randomized controlled trial"),
            ),
            "moderate",
        ),
        (
            (
                EvidenceItem(stance="supports", type="observational study"),
            ),
            "weak",
        ),
        ((), "unsupported"),
    ],
)
def test_compute_grade_expected_outcomes(evidence, expected):
    grade, rationale = compute_grade(evidence)
    assert grade == expected
    assert rationale.startswith("Auto-graded")


def test_compute_grade_conflict_reduces_confidence():
    evidence = [
        EvidenceItem(stance="supports", type="randomized controlled trial"),
        EvidenceItem(stance="supports", type="observational study"),
        EvidenceItem(stance="refutes", type="case report"),
    ]
    grade, rationale = compute_grade(evidence)
    assert grade == "moderate"
    assert "Conflicting evidence reduced confidence." in rationale


def test_auto_grader_handles_multiple_claims():
    claims = []
    for idx in range(12):
        if idx % 3 == 0:
            ev = [EvidenceItem(stance="supports", type="meta-analysis")]
        elif idx % 3 == 1:
            ev = [EvidenceItem(stance="supports", type="observational")]
        else:
            ev = [EvidenceItem(stance="refutes", type="systematic review")]
        claims.append(ClaimEvidence(claim_id=idx + 1, evidence=tuple(ev)))

    store = FakeStore(rows=[])
    grader = AutoGrader(source=claims, store=store)
    total = grader.grade_all()

    assert total == len(claims)
    assert len(store.rows) == len(claims)


def test_regrading_creates_new_row():
    claim = ClaimEvidence(
        claim_id=42,
        evidence=(EvidenceItem(stance="supports", type="meta-analysis"),),
    )
    store = FakeStore(rows=[])
    grader = AutoGrader(source=[claim], store=store)

    first_total = grader.grade_all()
    assert first_total == 1
    assert len(store.rows) == 1
    first_snapshot = list(store.rows)

    second_total = grader.grade_all()
    assert second_total == 1
    assert len(store.rows) == 2
    assert store.rows[0] == first_snapshot[0]
    assert store.rows[1]["claim_id"] == 42
    assert store.rows[1]["grade"] == store.rows[0]["grade"]
