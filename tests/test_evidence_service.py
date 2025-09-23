from __future__ import annotations

from typing import List

from server.services.evidence import (
    EvidenceService,
    PubMedArticle,
    build_pubmed_queries,
    classify_publication_type,
    classify_stance,
)
from tests.fake_db import FakeConnection, FakeDatabase


class StubPubMedClient:
    def __init__(self, articles: List[PubMedArticle]) -> None:
        self.articles = articles
        self.queries: List[str] = []

    def search(self, query: str, *, retmax: int = 30) -> List[str]:
        self.queries.append(query)
        return [article.pmid for article in self.articles][:retmax]

    def fetch_details(self, ids: List[str]) -> List[PubMedArticle]:
        return [article for article in self.articles if article.pmid in ids]


def test_build_pubmed_queries_includes_synonyms() -> None:
    queries, terms = build_pubmed_queries("ketones improve cognition", "Ketones support cognition")
    assert "ketone bodies" in terms
    assert "cognition" in terms
    assert queries  # at least one query produced


def test_classify_stance_supports_positive_signal() -> None:
    stance = classify_stance(
        "ketones improve cognition",
        "Randomized trial of ketones",
        "This randomized controlled trial reported significant improvement in cognition.",
    )
    assert stance == "supports"


def test_classify_stance_contradicts_negative_signal() -> None:
    stance = classify_stance(
        "ketones improve cognition",
        "Ketone trial",
        "The intervention showed no significant improvement and did not change outcomes.",
    )
    assert stance == "contradicts"


def test_classify_publication_type_maps_observational() -> None:
    assert classify_publication_type(["Meta-Analysis"]) == "meta-analysis"
    assert classify_publication_type(["Randomized Controlled Trial"]) == "RCT"
    assert classify_publication_type(["Comparative Study"]) == "observational"
    assert classify_publication_type([]) == "mechanistic"


def test_evidence_service_links_articles() -> None:
    database = FakeDatabase()
    conn = FakeConnection(database)

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO claim (id, episode_id, raw_text, normalized_text) VALUES (1, 1, 'Ketones help', 'ketones improve cognition')"
    )

    article = PubMedArticle(
        pmid="12345",
        title="Ketone supplementation improves cognition",
        abstract="Participants experienced significant improvement in cognitive performance.",
        journal="Journal of Cognitive Health",
        year=2022,
        doi="10.1000/example",
        publication_types=("Randomized Controlled Trial",),
    )

    service = EvidenceService(
        conn,
        min_results=0,
        max_results=5,
        pubmed=StubPubMedClient([article]),
    )

    service.process_claim(1, "ketones improve cognition", "Ketones improve cognition", force=True)

    evidence_rows = database.tables["evidence_source"]
    assert len(evidence_rows) == 1
    stored = evidence_rows[0]
    assert stored["pubmed_id"] == "12345"
    assert stored["type"] == "RCT"

    links = database.tables["claim_evidence"]
    assert len(links) == 1
    assert links[0]["claim_id"] == 1
    assert links[0]["stance"] == "supports"

