"""Command line entry-point for auto grading claims based on evidence."""

from __future__ import annotations

import os
from collections import defaultdict
from typing import Iterable, Iterator

try:  # pragma: no cover - optional dependency during tests
    import psycopg
except ModuleNotFoundError:  # pragma: no cover - fallback for unit tests without psycopg installed
    psycopg = None  # type: ignore[assignment]

from server.core.grading import (
    AUTO_GRADED_BY,
    RUBRIC_VERSION,
    ClaimEvidence,
    EvidenceItem,
    compute_grade,
)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/podcast_plow",
)


class ClaimSource:
    """Iterable view of claims and their evidence from Postgres."""

    def __init__(self, conn):
        self.conn = conn

    def __iter__(self) -> Iterator[ClaimEvidence]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT c.id, ce.stance, es.type
            FROM claim c
            LEFT JOIN claim_evidence ce ON ce.claim_id = c.id
            LEFT JOIN evidence_source es ON es.id = ce.evidence_id
            ORDER BY c.id
            """
        )
        by_claim: dict[int, list[EvidenceItem]] = defaultdict(list)
        claim_order: list[int] = []
        for claim_id, stance, ev_type in cur.fetchall():
            if claim_id not in by_claim:
                claim_order.append(claim_id)
            by_claim[claim_id].append(EvidenceItem(stance=stance, type=ev_type))

        for claim_id in claim_order:
            yield ClaimEvidence(claim_id=claim_id, evidence=tuple(by_claim[claim_id]))


class GradeStore:
    """Persists computed grades back into Postgres."""

    def __init__(self, conn):
        self.conn = conn

    def insert(self, claim_id: int, grade: str, rationale: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO claim_grade (claim_id, grade, rationale, rubric_version, graded_by)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (claim_id, grade, rationale, RUBRIC_VERSION, AUTO_GRADED_BY),
        )


class AutoGrader:
    """Orchestrates fetching evidence, grading, and persisting results."""

    def __init__(self, source: Iterable[ClaimEvidence], store: GradeStore):
        self.source = source
        self.store = store

    def grade_all(self) -> int:
        graded = 0
        for claim in self.source:
            grade, rationale = compute_grade(claim.evidence)
            self.store.insert(claim.claim_id, grade, rationale)
            graded += 1
        return graded


def main() -> None:
    if psycopg is None:  # pragma: no cover - safety guard
        raise RuntimeError("psycopg is required to run the auto grader")
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        grader = AutoGrader(source=ClaimSource(conn), store=GradeStore(conn))
        total = grader.grade_all()
        print(f"Auto-graded {total} claims using rubric {RUBRIC_VERSION}.")


if __name__ == "__main__":
    main()

