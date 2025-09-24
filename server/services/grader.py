"""Rule-based auto-grader for claim evidence.

This module provides simple heuristics to translate linked evidence rows into
quality assessments.  The rules intentionally lean conservative: randomized and
synthesised human trials drive higher confidence, while animal/mechanistic
studies only warrant a *weak* grade.  Claims with no supporting evidence or
where refuting sources outweigh the support are marked as *unsupported*.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Sequence

RUBRIC_VERSION = "v1"
AUTO_GRADED_BY = "auto-grader"

META_KEYWORDS = {
    "meta-analysis",
    "meta analysis",
    "systematic review",
}
RCT_KEYWORDS = {
    "randomized",
    "randomised",
    "randomized controlled trial",
    "randomised controlled trial",
    "randomized clinical trial",
    "randomised clinical trial",
    "double-blind",
    "double blind",
    "rct",
}
OBSERVATIONAL_KEYWORDS = {
    "cohort",
    "case-control",
    "case control",
    "observational",
    "prospective",
    "retrospective",
    "cross-sectional",
    "cross sectional",
    "longitudinal",
    "registry",
    "population",
    "survey",
    "pilot",
    "feasibility",
    "open-label",
    "open label",
    "clinical study",
    "clinical trial",
}
WEAK_KEYWORDS = {
    "animal",
    "mouse",
    "rat",
    "mice",
    "in vivo",
    "in vitro",
    "ex vivo",
    "mechanistic",
    "cell",
    "cells",
    "case report",
    "case series",
    "expert opinion",
    "preclinical",
}


@dataclass(frozen=True)
class EvidenceItem:
    """Normalized representation of evidence used for grading."""

    stance: str | None
    type: str | None


@dataclass(frozen=True)
class ClaimEvidence:
    """Container for a claim and its evidence rows."""

    claim_id: int
    evidence: Sequence[EvidenceItem]


def _normalize(text: str | None) -> str:
    return (text or "").strip().lower()


def _classify_type(evidence_type: str | None) -> str:
    """Classify a textual evidence type into a strength bucket."""

    text = _normalize(evidence_type)
    if not text:
        return "weak"

    if any(keyword in text for keyword in META_KEYWORDS):
        return "meta"
    if any(keyword in text for keyword in RCT_KEYWORDS):
        return "rct"
    if any(keyword in text for keyword in WEAK_KEYWORDS):
        return "weak"
    if any(keyword in text for keyword in OBSERVATIONAL_KEYWORDS):
        return "observational"
    # When unsure, treat the study as an observational/small clinical study.
    return "observational"


def _plural(count: int, singular: str, plural: str | None = None) -> str:
    if count == 1:
        return f"1 {singular}"
    label = plural or f"{singular}s"
    return f"{count} {label}"


def _format_summary(meta: int, rct: int, observational: int, weak: int) -> str:
    parts: List[str] = []
    if meta:
        parts.append(_plural(meta, "meta-analysis", "meta-analyses"))
    if rct:
        parts.append(_plural(rct, "randomized trial"))
    if observational:
        parts.append(_plural(observational, "observational study"))
    if weak:
        parts.append(
            _plural(weak, "mechanistic/animal study", "mechanistic/animal studies")
        )
    if not parts:
        return "no supporting evidence"
    if len(parts) == 1:
        return parts[0]
    return ", ".join(parts[:-1]) + " and " + parts[-1]


def compute_grade(evidence_items: Iterable[EvidenceItem]) -> tuple[str, str]:
    """Compute a (grade, rationale) tuple for the provided evidence rows."""

    support = {"meta": 0, "rct": 0, "observational": 0, "weak": 0}
    refute = {"meta": 0, "rct": 0, "observational": 0, "weak": 0}

    for item in evidence_items:
        stance = _normalize(item.stance)
        if stance not in {"supports", "refutes"}:
            continue
        bucket = _classify_type(item.type)
        target = support if stance == "supports" else refute
        target[bucket] += 1

    total_support = sum(support.values())
    total_refute = sum(refute.values())

    if total_support == 0:
        return "unsupported", "Auto-graded as unsupported because no supporting evidence was linked."

    if total_refute > total_support:
        first_sentence = (
            "Auto-graded as unsupported because refuting evidence outweighs support."
        )
        second_sentence = ""
        if total_refute:
            ref_summary = _format_summary(
                refute["meta"], refute["rct"], refute["observational"], refute["weak"]
            )
            second_sentence = f" Refuting evidence noted ({ref_summary})."
        return "unsupported", first_sentence + second_sentence

    meta = support["meta"]
    rct = support["rct"]
    observational = support["observational"]
    weak = support["weak"]

    if meta >= 1 or rct >= 2:
        grade = "strong"
    elif rct >= 1:
        grade = "moderate"
    elif observational >= 2:
        grade = "moderate"
    elif observational >= 1 or weak >= 1:
        grade = "weak"
    else:
        grade = "unsupported"

    summary = _format_summary(meta, rct, observational, weak)
    rationale_parts = [
        f"Auto-graded as {grade} because supporting evidence includes {summary}."
    ]
    if total_refute:
        rationale_parts.append("Conflicting evidence reduced confidence.")
        ref_summary = _format_summary(
            refute["meta"], refute["rct"], refute["observational"], refute["weak"]
        )
        rationale_parts.append(f"Refuting evidence noted ({ref_summary}).")
    return grade, " ".join(part.strip() for part in rationale_parts if part)


class AutoGradeService:
    """High level helper for grading and persisting claim grades."""

    def __init__(self, conn):
        self.conn = conn

    def grade_claims(
        self,
        *,
        claim_ids: Sequence[int] | None = None,
        episode_ids: Sequence[int] | None = None,
    ) -> List[dict]:
        to_grade = self._resolve_claim_ids(claim_ids, episode_ids)
        results: List[dict] = []
        for claim_id in to_grade:
            evidence = self._fetch_evidence(claim_id)
            grade, rationale = compute_grade(evidence)
            self._store_grade(claim_id, grade, rationale)
            results.append({"claim_id": claim_id, "grade": grade, "rationale": rationale})
        return results

    # internal helpers -----------------------------------------------------

    def _resolve_claim_ids(
        self,
        claim_ids: Sequence[int] | None,
        episode_ids: Sequence[int] | None,
    ) -> List[int]:
        claim_filter = set(claim_ids) if claim_ids else None
        episode_filter = set(episode_ids) if episode_ids else None

        cur = self.conn.cursor()
        cur.execute("SELECT id, episode_id FROM claim ORDER BY id")
        rows = cur.fetchall()

        resolved: List[int] = []
        for claim_id, episode_id in rows:
            if claim_filter is not None and claim_id not in claim_filter:
                continue
            if episode_filter is not None and episode_id not in episode_filter:
                continue
            resolved.append(claim_id)

        if claim_filter:
            missing = sorted(claim_filter - set(resolved))
            if missing:
                # Allow missing ids silently but surface in logs for debugging.
                import logging

                logger = logging.getLogger(__name__)
                logger.warning("Claim ids not found: %s", ", ".join(str(m) for m in missing))
        return resolved

    def _fetch_evidence(self, claim_id: int) -> List[EvidenceItem]:
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT es.id, es.title, es.year, es.type, es.journal, es.doi, es.pubmed_id, es.url, ce.stance
            FROM claim_evidence ce
            JOIN evidence_source es ON es.id = ce.evidence_id
            WHERE ce.claim_id = %s
            ORDER BY es.year DESC NULLS LAST
            """,
            (claim_id,),
        )
        rows = cur.fetchall()
        return [EvidenceItem(stance=row[8], type=row[3]) for row in rows]

    def _store_grade(self, claim_id: int, grade: str, rationale: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO claim_grade (claim_id, grade, rationale, rubric_version, graded_by)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (claim_id, grade, rationale, RUBRIC_VERSION, AUTO_GRADED_BY),
        )


__all__ = [
    "AUTO_GRADED_BY",
    "RUBRIC_VERSION",
    "EvidenceItem",
    "ClaimEvidence",
    "compute_grade",
    "AutoGradeService",
]
