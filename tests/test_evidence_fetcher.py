import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from server.services.evidence_fetcher import (
    EvidenceCandidate,
    build_query_terms,
    build_query_variants,
    classify_stance,
    rank_candidates,
)


def test_build_query_terms_adds_mesh_synonyms():
    terms = build_query_terms("ketones improve cognition")
    assert "ketone bodies" in terms
    assert "cognition" in terms


def test_build_query_variants_has_multiple_forms():
    queries, _ = build_query_variants("ketones improve cognition", "Ketones can support cognition")
    assert len(queries) >= 2
    assert any("ketone" in q.lower() for q in queries)


def test_classify_stance_supports_positive_signal():
    abstract = "This randomized controlled trial reported significant improvement and reduced risk of decline."
    stance = classify_stance("ketones improve cognition", abstract)
    assert stance == "supports"


def test_classify_stance_contradicts_negative_signal():
    abstract = "The intervention produced no significant improvement and failed to improve primary outcomes."
    stance = classify_stance("ketones improve cognition", abstract)
    assert stance == "contradicts"


def test_rank_candidates_prefers_high_quality_and_recency():
    c_meta = EvidenceCandidate(
        pubmed_id="1",
        title="Meta study",
        abstract="",
        year=2018,
        doi=None,
        journal=None,
        publication_types=["Meta-Analysis"],
        url="https://example.com/1",
    )
    c_rct = EvidenceCandidate(
        pubmed_id="2",
        title="RCT",
        abstract="",
        year=2021,
        doi=None,
        journal=None,
        publication_types=["Randomized Controlled Trial"],
        url="https://example.com/2",
    )
    c_case = EvidenceCandidate(
        pubmed_id="3",
        title="Case report",
        abstract="",
        year=2023,
        doi=None,
        journal=None,
        publication_types=["Case Reports"],
        url="https://example.com/3",
    )
    ranked = rank_candidates([c_case, c_rct, c_meta])
    assert [c.pubmed_id for c in ranked] == ["1", "2", "3"]
