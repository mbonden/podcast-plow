"""Compatibility wrappers for auto-grading utilities.

The original project exposed helpers from ``server.core.grading``; the new
rule-based implementation lives in :mod:`server.services.grader`.  This module
re-exports the public API so existing imports keep working.
"""

from __future__ import annotations

from server.services.grader import (  # noqa: F401
    AUTO_GRADED_BY,
    RUBRIC_VERSION,
    AutoGradeService,
    ClaimEvidence,
    EvidenceItem,
    compute_grade,
)

__all__ = [
    "AUTO_GRADED_BY",
    "RUBRIC_VERSION",
    "EvidenceItem",
    "ClaimEvidence",
    "compute_grade",
    "AutoGradeService",
]
