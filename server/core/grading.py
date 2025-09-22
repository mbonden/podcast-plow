"""Utilities for computing claim grades from linked evidence."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence


RUBRIC_VERSION = "auto-v1"
AUTO_GRADED_BY = "auto-grader"


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


HIGH_KEYWORDS = {
    "meta-analysis",
    "systematic review",
    "randomized controlled trial",
    "randomized",
    "double-blind",
    "rct",
}
MEDIUM_KEYWORDS = {
    "cohort",
    "case-control",
    "observational",
    "clinical trial",
    "pilot",
    "survey",
    "study",
}
LOW_KEYWORDS = {
    "case report",
    "case series",
    "animal",
    "mechanistic",
    "in vitro",
    "cell",
    "expert opinion",
}


def _classify_evidence_strength(evidence_type: str | None) -> str:
    """Return ``high``, ``medium``, or ``low`` for a textual type label."""

    if not evidence_type:
        return "low"

    etype = evidence_type.strip().lower()
    if not etype:
        return "low"
    if any(keyword in etype for keyword in HIGH_KEYWORDS):
        return "high"
    if any(keyword in etype for keyword in MEDIUM_KEYWORDS):
        return "medium"
    if any(keyword in etype for keyword in LOW_KEYWORDS):
        return "low"
    # default to medium quality when uncertain – better than unsupported but
    # not as strong as a randomized/meta analysis.
    return "medium"


def compute_grade(evidence_items: Iterable[EvidenceItem]) -> tuple[str, str]:
    """Compute a (grade, rationale) tuple for the provided evidence rows."""

    support_counts = {"high": 0, "medium": 0, "low": 0}
    refute_counts = {"high": 0, "medium": 0, "low": 0}

    for item in evidence_items:
        stance = (item.stance or "").strip().lower()
        strength = _classify_evidence_strength(item.type)
        if stance == "supports":
            support_counts[strength] += 1
        elif stance == "refutes":
            refute_counts[strength] += 1

    total_support = sum(support_counts.values())
    total_refute = sum(refute_counts.values())

    if total_support == 0:
        grade = "unsupported"
    else:
        high = support_counts["high"]
        medium = support_counts["medium"]
        low = support_counts["low"]

        if high >= 2 or (high >= 1 and (medium >= 1 or total_support >= 3)):
            grade = "strong"
        elif high >= 1 or medium >= 2:
            grade = "moderate"
        elif medium >= 1 or low >= 1:
            grade = "weak"
        else:
            grade = "unsupported"

    # conflicting evidence dampens confidence
    grade_order = ["unsupported", "weak", "moderate", "strong"]
    idx = grade_order.index(grade)
    if refute_counts["high"] > 0:
        idx = max(0, idx - 2)
    elif refute_counts["medium"] > 0 or refute_counts["low"] > 0:
        idx = max(0, idx - 1)
    grade = grade_order[idx]

    parts: list[str] = []
    if total_support:
        parts.append(
            f"supporting evidence – high:{support_counts['high']} medium:{support_counts['medium']} low:{support_counts['low']}"
        )
    if total_refute:
        parts.append(
            f"refuting evidence – high:{refute_counts['high']} medium:{refute_counts['medium']} low:{refute_counts['low']}"
        )
    if not parts:
        parts.append("no linked evidence")

    rationale = "Auto-graded using evidence counts (" + "; ".join(parts) + ")."
    if total_refute:
        rationale += " Conflicting evidence reduced confidence."

    return grade, rationale

