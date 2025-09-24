"""PubMed evidence linking helpers."""

from __future__ import annotations

import datetime as dt
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from xml.etree import ElementTree as ET

import requests

logger = logging.getLogger(__name__)

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
NCBI_TOOL = os.getenv("NCBI_TOOL", "podcast_plow")
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "research@podcastplow.local")
AUTO_NOTE_PREFIX = "auto:evidence"

STOPWORDS = {
    "a",
    "about",
    "after",
    "against",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "because",
    "been",
    "before",
    "being",
    "between",
    "both",
    "but",
    "by",
    "can",
    "could",
    "did",
    "do",
    "does",
    "doing",
    "during",
    "each",
    "either",
    "few",
    "for",
    "from",
    "had",
    "has",
    "have",
    "having",
    "he",
    "her",
    "here",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "itself",
    "just",
    "may",
    "me",
    "might",
    "more",
    "most",
    "my",
    "myself",
    "no",
    "nor",
    "not",
    "of",
    "off",
    "on",
    "once",
    "only",
    "or",
    "other",
    "our",
    "ours",
    "ourselves",
    "out",
    "over",
    "own",
    "same",
    "she",
    "should",
    "so",
    "some",
    "such",
    "than",
    "that",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "to",
    "too",
    "under",
    "until",
    "up",
    "very",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "will",
    "with",
    "within",
    "without",
    "would",
    "you",
    "your",
    "yours",
    "yourself",
    "yourselves",
}

PHRASE_SYNONYMS: Dict[str, Sequence[str]] = {
    "blood pressure": ("blood pressure",),
    "body weight": ("body weight",),
    "heart rate": ("heart rate",),
    "cognitive function": ("cognition", "cognitive function"),
    "gut microbiome": ("gastrointestinal microbiome", "microbiota"),
    "immune system": ("immune system",),
    "insulin sensitivity": ("insulin sensitivity", "insulin resistance"),
    "metabolic health": ("metabolic diseases", "metabolic health"),
    "weight loss": ("weight loss", "body weight"),
}

TOKEN_SYNONYMS: Dict[str, Sequence[str]] = {
    "aging": ("aging", "longevity"),
    "alzheimer": ("alzheimer disease",),
    "alzheimers": ("alzheimer disease",),
    "anxiety": ("anxiety", "anxiety disorders"),
    "autophagy": ("autophagy",),
    "blood": ("blood", "blood pressure"),
    "brain": ("brain", "brain diseases"),
    "cancer": ("neoplasms",),
    "cardio": ("cardiovascular diseases",),
    "cardiovascular": ("cardiovascular diseases",),
    "cholesterol": ("cholesterol", "hypercholesterolemia"),
    "cognition": ("cognition", "cognition disorders"),
    "cognitive": ("cognition", "cognitive function"),
    "creatine": ("creatine",),
    "depression": ("depressive disorder", "depression"),
    "diabetes": ("diabetes mellitus",),
    "diet": ("diet", "diet therapy"),
    "exercise": ("exercise", "physical exercise"),
    "fasting": ("fasting", "intermittent fasting"),
    "glucose": ("blood glucose",),
    "gut": ("gastrointestinal microbiome", "microbiota"),
    "heart": ("heart diseases", "cardiovascular diseases"),
    "immune": ("immune system", "immune response"),
    "immunity": ("immune system", "immune response"),
    "inflammation": ("inflammation", "anti-inflammatory agents"),
    "ketone": ("ketone bodies",),
    "ketones": ("ketone bodies",),
    "ketogenic": ("ketogenic diet",),
    "longevity": ("longevity", "aging"),
    "magnesium": ("magnesium",),
    "memory": ("memory", "cognition"),
    "microbiome": ("microbiota", "gastrointestinal microbiome"),
    "neurodegenerative": ("neurodegenerative diseases",),
    "obesity": ("obesity", "body mass index"),
    "performance": ("physical endurance", "exercise"),
    "protein": ("dietary proteins", "protein supplements"),
    "risk": ("risk", "risk factors"),
    "sleep": ("sleep", "sleep disorders"),
    "supplement": ("dietary supplements",),
    "supplements": ("dietary supplements",),
    "tumor": ("neoplasms",),
    "vitamin": ("vitamins",),
    "weight": ("body weight", "weight loss"),
}

EVIDENCE_TYPE_MAP: List[Tuple[str, Tuple[str, ...]]] = [
    ("meta-analysis", ("meta-analysis", "systematic review and meta-analysis")),
    ("systematic review", ("systematic review",)),
    (
        "RCT",
        (
            "randomized controlled trial",
            "randomised controlled trial",
            "clinical trial",
            "controlled clinical trial",
            "multicenter study",
            "pragmatic clinical trial",
        ),
    ),
    (
        "observational",
        (
            "observational study",
            "cohort studies",
            "case-control studies",
            "cross-sectional studies",
            "comparative study",
            "prospective studies",
            "retrospective studies",
        ),
    ),
]

POSITIVE_KEYWORDS = (
    "significant improvement",
    "significant increase",
    "significant reduction",
    "improved",
    "improvement",
    "effective",
    "efficacy",
    "benefit",
    "beneficial",
    "reduced risk",
    "reduction",
    "decreased",
    "lower",
    "enhanced",
    "supports",
    "support",
    "associated with",
    "increase",
    "increased",
    "improves",
    "improve",
)

NEGATIVE_KEYWORDS = (
    "no significant",
    "not significant",
    "not associated",
    "no effect",
    "does not",
    "did not",
    "failed to",
    "without effect",
    "increase in risk",
    "increased risk",
    "worsened",
    "adverse",
    "harm",
    "no change",
    "null",
)

MIXED_KEYWORDS = (
    "mixed results",
    "inconclusive",
    "limited evidence",
    "uncertain",
    "conflicting",
    "insufficient",
)

REQUEST_TIMEOUT = 30

NEGATING_PREFIXES = (
    "no ",
    "no significant ",
    "not ",
    "failed to ",
    "did not ",
    "does not ",
    "without ",
    "lack of ",
)


def singularize(token: str) -> str:
    if token.endswith("ies") and len(token) > 3:
        return token[:-3] + "y"
    if token.endswith("ses") and len(token) > 3:
        return token[:-2]
    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]
    return token


def build_query_terms(text: str) -> List[str]:
    """Return search tokens with synonyms derived from ``text``."""

    lowered = text.lower().replace("-", " ")
    terms: List[str] = []
    seen: set[str] = set()

    for phrase, synonyms in PHRASE_SYNONYMS.items():
        if phrase in lowered:
            for synonym in synonyms:
                if synonym not in seen:
                    terms.append(synonym)
                    seen.add(synonym)

    tokens = re.findall(r"[a-z0-9']+", lowered)
    for token in tokens:
        if token in STOPWORDS or not token:
            continue
        base = singularize(token)
        candidates = list(TOKEN_SYNONYMS.get(base, (base,)))
        if token != base:
            candidates.extend(TOKEN_SYNONYMS.get(token, ()))
        for candidate in candidates:
            cleaned = candidate.strip()
            if not cleaned or cleaned in seen:
                continue
            terms.append(cleaned)
            seen.add(cleaned)
    return terms


def mesh_query_from_terms(terms: Sequence[str], max_terms: int = 6) -> str:
    parts: List[str] = []
    for term in terms[:max_terms]:
        clean = term.replace('"', "").strip()
        if not clean:
            continue
        if " " in clean:
            parts.append(f'("{clean}"[MeSH Terms] OR "{clean}"[Title/Abstract])')
        else:
            parts.append(f'({clean}[MeSH Terms] OR {clean}[Title/Abstract])')
    return " AND ".join(parts)


def simple_query_from_terms(terms: Sequence[str], max_terms: int = 8) -> str:
    selected: List[str] = []
    for term in terms:
        if len(selected) >= max_terms:
            break
        clean = term.replace('"', "").strip()
        if not clean:
            continue
        if " " in clean:
            selected.append(f'"{clean}"')
        else:
            selected.append(clean)
    return " ".join(selected)


def build_pubmed_queries(normalized_text: Optional[str], raw_text: Optional[str]) -> Tuple[List[str], List[str]]:
    """Return ``(queries, terms)`` extracted from claim text."""

    base_text = normalized_text or raw_text or ""
    terms = build_query_terms(base_text)
    queries: List[str] = []

    mesh_query = mesh_query_from_terms(terms)
    if mesh_query:
        queries.append(mesh_query)
    simple_query = simple_query_from_terms(terms)
    if simple_query:
        queries.append(simple_query)
    if not queries and base_text:
        queries.append(base_text)
    return queries, terms


@dataclass
class PubMedArticle:
    pmid: str
    title: str
    abstract: str
    journal: Optional[str]
    year: Optional[int]
    doi: Optional[str]
    publication_types: Tuple[str, ...]

    @property
    def url(self) -> str:
        return f"https://pubmed.ncbi.nlm.nih.gov/{self.pmid}/"

    @property
    def evidence_type(self) -> str:
        return classify_publication_type(self.publication_types)


class PubMedClient:
    """Lightweight wrapper around NCBI's E-utilities."""

    def __init__(self, session: Optional[requests.Session] = None) -> None:
        self.session = session or requests.Session()
        self._min_interval = self._compute_min_interval()
        self._last_request: Optional[float] = None

    @staticmethod
    def _compute_min_interval() -> Optional[float]:
        """Return the minimum spacing between requests based on env config."""

        raw_qps = os.getenv("PLOW_PUBMED_QPS")
        qps: Optional[float] = None
        if raw_qps:
            try:
                qps = float(raw_qps)
            except ValueError:
                logger.warning("Invalid PLOW_PUBMED_QPS value '%s'; using default", raw_qps)
        if qps is None:
            qps = 3.0
        if qps <= 0:
            return None
        return 1.0 / qps

    def _throttle(self) -> None:
        if not self._min_interval:
            return
        now = time.monotonic()
        if self._last_request is not None:
            elapsed = now - self._last_request
            remaining = self._min_interval - elapsed
            if remaining > 0:
                time.sleep(remaining)
                now = time.monotonic()
        self._last_request = now

    def search(self, query: str, *, retmax: int = 30) -> List[str]:
        self._throttle()
        params = {
            "db": "pubmed",
            "term": query,
            "retmax": str(retmax),
            "sort": "relevance",
            "retmode": "json",
            "tool": NCBI_TOOL,
            "email": NCBI_EMAIL,
        }
        response = self.session.get(
            f"{EUTILS_BASE}/esearch.fcgi", params=params, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()
        idlist = data.get("esearchresult", {}).get("idlist", [])
        return list(idlist)

    def fetch_details(self, ids: Sequence[str]) -> List[PubMedArticle]:
        if not ids:
            return []
        self._throttle()
        params = {
            "db": "pubmed",
            "id": ",".join(ids),
            "retmode": "xml",
            "rettype": "abstract",
            "tool": NCBI_TOOL,
            "email": NCBI_EMAIL,
        }
        response = self.session.get(
            f"{EUTILS_BASE}/efetch.fcgi", params=params, timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        root = ET.fromstring(response.content)
        articles: List[PubMedArticle] = []
        for article in root.findall("PubmedArticle"):
            medline = article.find("MedlineCitation")
            if medline is None:
                continue
            pmid = (medline.findtext("PMID") or "").strip()
            article_el = medline.find("Article")
            if not pmid or article_el is None:
                continue
            title = (article_el.findtext("ArticleTitle") or "").strip()
            abstract_paras = [
                (para.text or "").strip()
                for para in article_el.findall("Abstract/AbstractText")
                if para.text
            ]
            abstract = "\n".join(abstract_paras)
            journal = (article_el.findtext("Journal/Title") or "").strip() or None

            year: Optional[int] = None
            pub_date = article_el.find("Journal/JournalIssue/PubDate")
            if pub_date is not None:
                year_text = pub_date.findtext("Year")
                if year_text and year_text.isdigit():
                    year = int(year_text)
                else:
                    medline_date = pub_date.findtext("MedlineDate")
                    if medline_date:
                        match = re.search(r"(19|20)\d{2}", medline_date)
                        if match:
                            year = int(match.group(0))
            if year is None:
                fallback_year = medline.findtext("DateCreated/Year")
                if fallback_year and fallback_year.isdigit():
                    year = int(fallback_year)

            doi: Optional[str] = None
            for eloc in article_el.findall("ELocationID"):
                if eloc.get("EIdType", "").lower() == "doi":
                    doi_text = (eloc.text or "").strip()
                    if doi_text:
                        doi = doi_text
                        break

            publication_types = tuple(
                pt.text.strip()
                for pt in article_el.findall("PublicationTypeList/PublicationType")
                if pt.text
            )

            articles.append(
                PubMedArticle(
                    pmid=pmid,
                    title=title,
                    abstract=abstract,
                    journal=journal,
                    year=year,
                    doi=doi,
                    publication_types=publication_types,
                )
            )
        return articles


def classify_publication_type(publication_types: Sequence[str]) -> str:
    lowered = [pt.lower() for pt in publication_types]
    for mapped, aliases in EVIDENCE_TYPE_MAP:
        for alias in aliases:
            if alias in lowered:
                return mapped
    if any("review" in pt for pt in lowered):
        return "systematic review"
    if any("trial" in pt for pt in lowered):
        return "RCT"
    return "mechanistic"


def _count_occurrences(text: str, phrase: str, *, ignore_negated: bool = False) -> int:
    if not phrase:
        return 0
    pattern = re.escape(phrase.lower())
    boundary = r"\b" if re.search(r"[a-z0-9]", phrase.lower()) else ""
    if ignore_negated:
        lookbehind = "".join(f"(?<!{re.escape(prefix)})" for prefix in NEGATING_PREFIXES)
        regex = re.compile(f"{lookbehind}{boundary}{pattern}{boundary}")
    else:
        regex = re.compile(f"{boundary}{pattern}{boundary}")
    return len(regex.findall(text))


def classify_stance(claim_text: str, title: str, abstract: str) -> str:
    """Very small heuristic stance classifier."""

    claim = (claim_text or "").lower()
    combined = f"{title or ''} {abstract or ''}".lower()

    positive = sum(
        _count_occurrences(combined, term, ignore_negated=True)
        for term in POSITIVE_KEYWORDS
    )
    negative = sum(_count_occurrences(combined, term) for term in NEGATIVE_KEYWORDS)
    mixed = sum(_count_occurrences(combined, term) for term in MIXED_KEYWORDS)

    if positive and negative:
        return "mixed"
    if mixed:
        return "mixed"
    if positive and not negative:
        return "supports"
    if negative and not positive:
        return "contradicts"

    if any(word in claim for word in {"increase", "improve", "boost", "support"}):
        if "no" in combined or "not" in combined:
            return "contradicts"
        if any(term in combined for term in {"increase", "improve", "improved", "increased", "supports"}):
            return "supports"
    if any(word in claim for word in {"reduce", "lower", "decrease", "prevent"}):
        if "no" in combined or "not" in combined:
            return "contradicts"
        if any(term in combined for term in {"reduction", "reduced", "decrease", "decreased", "lower"}):
            return "supports"
    if "risk" in claim:
        if "increased risk" in combined and "no" not in combined:
            return "supports"
        if "no" in combined and "risk" in combined:
            return "contradicts"

    return "mixed"


def _count_existing_evidence(conn, claim_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM claim_evidence WHERE claim_id = %s AND stance IS NOT NULL",
            (claim_id,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0


def _upsert_evidence(conn, article: PubMedArticle) -> int:
    evidence_type = article.evidence_type
    with conn.cursor() as cur:
        if article.pmid:
            cur.execute(
                "SELECT id FROM evidence_source WHERE pubmed_id = %s",
                (article.pmid,),
            )
            row = cur.fetchone()
            if row:
                evidence_id = int(row[0])
                cur.execute(
                    """
                    UPDATE evidence_source
                    SET title = %s, year = %s, doi = COALESCE(NULLIF(%s, ''), doi),
                        url = %s, type = %s, journal = %s
                    WHERE id = %s
                    """,
                    (
                        article.title,
                        article.year,
                        article.doi,
                        article.url,
                        evidence_type,
                        article.journal,
                        evidence_id,
                    ),
                )
                return evidence_id
        if article.doi:
            cur.execute(
                "SELECT id FROM evidence_source WHERE doi = %s",
                (article.doi,),
            )
            row = cur.fetchone()
            if row:
                evidence_id = int(row[0])
                cur.execute(
                    """
                    UPDATE evidence_source
                    SET title = %s, year = %s, pubmed_id = COALESCE(NULLIF(%s, ''), pubmed_id),
                        url = %s, type = %s, journal = %s
                    WHERE id = %s
                    """,
                    (
                        article.title,
                        article.year,
                        article.pmid,
                        article.url,
                        evidence_type,
                        article.journal,
                        evidence_id,
                    ),
                )
                return evidence_id
        cur.execute(
            """
            INSERT INTO evidence_source (title, year, doi, pubmed_id, url, type, journal)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                article.title,
                article.year,
                article.doi,
                article.pmid,
                article.url,
                evidence_type,
                article.journal,
            ),
        )
        return int(cur.fetchone()[0])


def _is_auto_generated(note: Optional[str]) -> bool:
    if not note:
        return True
    return note.lower().startswith(AUTO_NOTE_PREFIX)


def _link_claim_evidence(
    conn,
    claim_id: int,
    evidence_id: int,
    stance: str,
    *,
    context: Optional[str] = None,
) -> bool:
    timestamp = dt.datetime.utcnow().date().isoformat()
    note_parts = [AUTO_NOTE_PREFIX, timestamp]
    if context:
        note_parts.append(context)
    note = " ".join(note_parts)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT stance, notes FROM claim_evidence WHERE claim_id = %s AND evidence_id = %s",
            (claim_id, evidence_id),
        )
        row = cur.fetchone()
        if row:
            existing_note = row[1]
            if existing_note and not _is_auto_generated(existing_note):
                logger.info(
                    "Skipping manual evidence link for claim %s evidence %s",
                    claim_id,
                    evidence_id,
                )
                return False
            cur.execute(
                """
                UPDATE claim_evidence
                SET stance = %s, notes = %s
                WHERE claim_id = %s AND evidence_id = %s
                """,
                (stance, note, claim_id, evidence_id),
            )
            return True
        cur.execute(
            "INSERT INTO claim_evidence (claim_id, evidence_id, stance, notes) VALUES (%s, %s, %s, %s)",
            (claim_id, evidence_id, stance, note),
        )
        return True


class EvidenceService:
    """High level orchestrator for PubMed evidence collection."""

    def __init__(
        self,
        conn,
        *,
        min_results: int = 2,
        max_results: int = 10,
        pubmed: Optional[PubMedClient] = None,
    ) -> None:
        self.conn = conn
        self.min_results = min_results
        self.max_results = max_results
        self.pubmed = pubmed or PubMedClient()

    def process_claim(
        self,
        claim_id: int,
        normalized_text: Optional[str],
        raw_text: Optional[str],
        *,
        force: bool = False,
    ) -> List[PubMedArticle]:
        existing = _count_existing_evidence(self.conn, claim_id)
        if existing >= self.min_results and not force:
            logger.info(
                "Claim %s already has %s evidence items; skipping", claim_id, existing
            )
            return []

        queries, terms = build_pubmed_queries(normalized_text, raw_text)
        if not queries:
            logger.warning("Claim %s: unable to build PubMed query", claim_id)
            return []

        collected: Dict[str, PubMedArticle] = {}
        for query in queries:
            try:
                ids = self.pubmed.search(query, retmax=self.max_results * 3)
            except requests.RequestException as exc:  # pragma: no cover - network failure
                logger.warning("Claim %s: search failed (%s)", claim_id, exc)
                continue
            if not ids:
                continue
            try:
                articles = self.pubmed.fetch_details(ids)
            except requests.RequestException as exc:  # pragma: no cover - network failure
                logger.warning("Claim %s: fetch failed (%s)", claim_id, exc)
                continue
            for article in articles:
                if article.pmid and article.pmid not in collected:
                    collected[article.pmid] = article
            if len(collected) >= self.max_results:
                break

        selected = list(collected.values())[: self.max_results]
        if not selected:
            logger.warning("Claim %s: no evidence found", claim_id)
            return []

        query_context = " ".join(terms[:4]) if terms else None
        for article in selected:
            stance = classify_stance(
                normalized_text or raw_text or "",
                article.title,
                article.abstract,
            )
            evidence_id = _upsert_evidence(self.conn, article)
            _link_claim_evidence(
                self.conn,
                claim_id,
                evidence_id,
                stance,
                context=f"query={query_context}" if query_context else None,
            )
        return selected


def iter_claim_rows(
    conn,
    *,
    claim_ids: Optional[Sequence[int]] = None,
    episode_ids: Optional[Sequence[int]] = None,
) -> Iterable[Tuple[int, Optional[str], Optional[str]]]:
    """Yield ``(claim_id, normalized_text, raw_text)`` rows matching filters."""

    query = "SELECT id, normalized_text, raw_text FROM claim"
    clauses: List[str] = []
    params: List[object] = []

    if claim_ids:
        clauses.append("id = ANY(%s)")
        params.append(list(claim_ids))
    if episode_ids:
        clauses.append("episode_id = ANY(%s)")
        params.append(list(episode_ids))

    if clauses:
        query += " WHERE " + " AND ".join(clauses)
    query += " ORDER BY id"

    with conn.cursor() as cur:
        cur.execute(query, params)
        for row in cur.fetchall():
            yield int(row[0]), row[1], row[2]


__all__ = [
    "AUTO_NOTE_PREFIX",
    "EvidenceService",
    "PubMedArticle",
    "PubMedClient",
    "build_pubmed_queries",
    "build_query_terms",
    "classify_publication_type",
    "classify_stance",
    "iter_claim_rows",
]

