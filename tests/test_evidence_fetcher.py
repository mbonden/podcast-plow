from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from server.services.evidence_fetcher import (
    AUTO_NOTE_PREFIX,
    EvidenceCandidate,
    EvidenceFetcher,
    build_query_terms,
    build_query_variants,
    classify_stance,
    rank_candidates,
)
from tests.fake_db import FakeConnection, FakeDatabase


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


def test_evidence_fetcher_persists_candidates(monkeypatch):
    database = FakeDatabase()
    conn = FakeConnection(database)

    candidate = EvidenceCandidate(
        pubmed_id="123456",
        title="Magnesium improves sleep quality",
        abstract="Participants reported significant improvement in sleep quality.",
        year=2022,
        doi="10.1000/example",
        journal="Sleep Research",
        publication_types=("Randomized Controlled Trial", "Journal Article"),
        url="https://example.test/magnesium",
    )

    captured_queries: list[str] = []

    def fake_fetch(query: str, retmax: int = 30):
        captured_queries.append(query)
        return [candidate]

    monkeypatch.setattr("server.services.evidence_fetcher.fetch_pubmed_articles", fake_fetch)
    monkeypatch.setattr(
        "server.services.evidence_fetcher.classify_stance", lambda *_args, **_kwargs: "supports"
    )

    fetcher = EvidenceFetcher(conn, min_results=1, max_results=3, sleep_between=0.0)
    selected = fetcher.process_claim(
        42,
        "Magnesium supports sleep quality",
        "Magnesium supports sleep quality",
    )

    assert selected and selected[0].pubmed_id == candidate.pubmed_id
    assert captured_queries, "expected fetcher to issue at least one PubMed query"

    assert len(database.tables["evidence_source"]) == 1
    stored = database.tables["evidence_source"][0]
    assert stored["title"] == candidate.title
    assert stored["pubmed_id"] == candidate.pubmed_id
    assert stored["doi"] == candidate.doi
    assert stored["type"] == candidate.primary_type()
    assert stored["url"] == candidate.url

    links = database.tables["claim_evidence"]
    assert len(links) == 1
    link = links[0]
    assert link["claim_id"] == 42
    assert link["evidence_id"] == stored["id"]
    assert link["stance"] == "supports"
    assert link["notes"].startswith(AUTO_NOTE_PREFIX)
    assert "query=" in link["notes"]

    # A second run should notice the existing evidence and skip work.
    skipped = fetcher.process_claim(
        42,
        "Magnesium supports sleep quality",
        "Magnesium supports sleep quality",
    )
    assert skipped == []
