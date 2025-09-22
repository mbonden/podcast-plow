"""Heuristics to discover literature that can support or contradict claims."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from xml.etree import ElementTree as ET

logger = logging.getLogger(__name__)

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
NCBI_TOOL = os.getenv("NCBI_TOOL", "podcast_plow")
NCBI_EMAIL = os.getenv("NCBI_EMAIL", "research@podcastplow.local")
AUTO_NOTE_PREFIX = "auto: heuristics"

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

DIRECTIONAL_KEYWORDS = {
    "boost",
    "cause",
    "decrease",
    "enhance",
    "improve",
    "increase",
    "lower",
    "prevent",
    "promote",
    "protect",
    "reduce",
    "support",
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

MESH_SYNONYMS: Dict[str, Sequence[str]] = {
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

TYPE_RANK = {
    "systematic review": 0,
    "meta-analysis": 0,
    "randomized controlled trial": 1,
    "controlled clinical trial": 1,
    "clinical trial": 2,
    "multicenter study": 2,
    "pragmatic clinical trial": 2,
    "observational study": 3,
    "cohort studies": 3,
    "case-control studies": 3,
    "cross-sectional studies": 3,
    "comparative study": 3,
    "prospective studies": 3,
    "retrospective studies": 3,
    "review": 4,
    "systematic review and meta-analysis": 0,
}
DEFAULT_TYPE_RANK = 6

POSITIVE_INDICATORS = (
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
)

NEGATIVE_INDICATORS = (
    "no significant",
    "not significant",
    "not associated",
    "no effect",
    "did not",
    "failed to",
    "without effect",
    "increase in risk",
    "increased risk",
    "worsened",
    "adverse",
    "harm",
)

MIXED_INDICATORS = (
    "mixed results",
    "inconclusive",
    "limited evidence",
    "uncertain",
    "conflicting",
    "insufficient",
)

USER_AGENT = os.getenv("HTTP_USER_AGENT", "podcast-plow/0.1 (+https://github.com)")
NEGATING_PREFIXES = (
    "no ",
    "no significant ",
    "not ",
    "failed to ",
    "did not ",
    "without ",
    "lack of ",
)


@dataclass
class EvidenceCandidate:
    """Representation of a PubMed article we can attach to a claim."""

    pubmed_id: str
    title: str
    abstract: str
    year: Optional[int]
    doi: Optional[str]
    journal: Optional[str]
    publication_types: Sequence[str]
    url: str

    def primary_type(self) -> Optional[str]:
        """Return the highest-value publication type for storage."""

        if not self.publication_types:
            return None
        ranked = sorted(
            self.publication_types,
            key=lambda value: TYPE_RANK.get(value.lower(), DEFAULT_TYPE_RANK),
        )
        return ranked[0]

    def sort_key(self) -> tuple[int, int, str]:
        """Key used for ranking candidates (lower is better)."""

        rank = min(
            [TYPE_RANK.get(pt.lower(), DEFAULT_TYPE_RANK) for pt in self.publication_types]
            or [DEFAULT_TYPE_RANK]
        )
        year = self.year or 0
        return (rank, -year, self.pubmed_id)


def _http_get(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=30) as resp:  # type: ignore[call-arg]
        return resp.read()


def _get_json(endpoint: str, params: Dict[str, str]) -> dict:
    params = {**params, "tool": NCBI_TOOL, "email": NCBI_EMAIL}
    url = f"{EUTILS_BASE}/{endpoint}?{urlencode(params)}"
    data = _http_get(url)
    return json.loads(data.decode("utf-8"))


def _get_xml(endpoint: str, params: Dict[str, str]) -> ET.Element:
    params = {**params, "tool": NCBI_TOOL, "email": NCBI_EMAIL}
    url = f"{EUTILS_BASE}/{endpoint}?{urlencode(params)}"
    data = _http_get(url)
    return ET.fromstring(data)


def singularize(token: str) -> str:
    if token.endswith("ies") and len(token) > 3:
        return token[:-3] + "y"
    if token.endswith("ses") and len(token) > 3:
        return token[:-2]
    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]
    return token


def build_query_terms(text: str) -> List[str]:
    if not text:
        return []
    lowered = text.lower().replace("-", " ")
    terms: List[str] = []
    seen = set()
    directional: List[str] = []

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
        options: List[str] = []
        if base in MESH_SYNONYMS:
            options.extend(MESH_SYNONYMS[base])
        if token in MESH_SYNONYMS and token != base:
            options.extend(MESH_SYNONYMS[token])
        if base in DIRECTIONAL_KEYWORDS:
            directional.append(base)
        else:
            options.append(base)
        for opt in options:
            opt = opt.strip()
            if not opt or opt in seen:
                continue
            terms.append(opt)
            seen.add(opt)
    for opt in directional:
        if opt not in seen:
            terms.append(opt)
            seen.add(opt)
    return terms[:12]


def mesh_query_from_terms(terms: Sequence[str], max_terms: int = 6) -> str:
    parts: List[str] = []
    for term in terms[:max_terms]:
        clean = term.replace('"', "")
        if not clean:
            continue
        if " " in clean:
            parts.append(f'("{clean}"[MeSH Terms] OR "{clean}"[Title/Abstract])')
        else:
            parts.append(f'({clean}[MeSH Terms] OR {clean}[Title/Abstract])')
    return " AND ".join(parts)


def simple_query_from_terms(terms: Sequence[str], max_terms: int = 8) -> str:
    selected = []
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


def build_query_variants(normalized_text: Optional[str], raw_text: Optional[str]) -> tuple[List[str], List[str]]:
    base_text = normalized_text or raw_text or ""
    terms = build_query_terms(base_text)
    queries: List[str] = []
    mesh_query = mesh_query_from_terms(terms)
    if mesh_query:
        queries.append(mesh_query)
    simple_query = simple_query_from_terms(terms)
    if simple_query:
        queries.append(simple_query)
    if normalized_text:
        queries.append(f'"{normalized_text.strip()}"')
    if raw_text and raw_text.strip() and raw_text.strip() != normalized_text:
        queries.append(f'"{raw_text.strip()}"')
    # deduplicate while preserving order
    seen = set()
    unique_queries = []
    for q in queries:
        q = q.strip()
        if not q or q in seen:
            continue
        unique_queries.append(q)
        seen.add(q)
    return unique_queries, terms


def fetch_pubmed_ids(query: str, retmax: int = 30) -> List[str]:
    payload = {
        "db": "pubmed",
        "term": query,
        "retmode": "json",
        "sort": "relevance",
        "retmax": str(retmax),
    }
    try:
        data = _get_json("esearch.fcgi", payload)
    except (HTTPError, URLError, ValueError) as exc:
        logger.warning("PubMed search failed: %s", exc)
        return []
    id_list = data.get("esearchresult", {}).get("idlist", [])
    return [pmid for pmid in id_list if pmid]


def fetch_pubmed_details(pubmed_ids: Sequence[str]) -> List[EvidenceCandidate]:
    if not pubmed_ids:
        return []
    payload = {
        "db": "pubmed",
        "id": ",".join(pubmed_ids),
        "retmode": "xml",
    }
    try:
        root = _get_xml("efetch.fcgi", payload)
    except (HTTPError, URLError, ET.ParseError) as exc:
        logger.warning("PubMed fetch failed: %s", exc)
        return []
    candidates: List[EvidenceCandidate] = []
    for article in root.findall("PubmedArticle"):
        pmid = article.findtext("MedlineCitation/PMID")
        if not pmid:
            continue
        medline = article.find("MedlineCitation/Article")
        if medline is None:
            continue
        title_node = medline.find("ArticleTitle")
        if title_node is None:
            continue
        title = "".join(title_node.itertext()).strip()
        abstract_parts = []
        for ab in medline.findall("Abstract/AbstractText"):
            label = ab.get("Label")
            text = "".join(ab.itertext()).strip()
            if not text:
                continue
            if label:
                abstract_parts.append(f"{label}: {text}")
            else:
                abstract_parts.append(text)
        abstract = " ".join(abstract_parts)
        journal = medline.findtext("Journal/Title")
        year = None
        pub_date = medline.find("Journal/JournalIssue/PubDate")
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
            fallback_year = article.findtext("MedlineCitation/DateCreated/Year")
            if fallback_year and fallback_year.isdigit():
                year = int(fallback_year)
        doi = None
        for eloc in medline.findall("ELocationID"):
            if eloc.get("EIdType", "").lower() == "doi":
                doi_text = (eloc.text or "").strip()
                if doi_text:
                    doi = doi_text
                    break
        publication_types = [
            pt.text.strip()
            for pt in medline.findall("PublicationTypeList/PublicationType")
            if pt.text
        ]
        candidates.append(
            EvidenceCandidate(
                pubmed_id=pmid,
                title=title,
                abstract=abstract,
                year=year,
                doi=doi,
                journal=journal,
                publication_types=publication_types,
                url=f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
            )
        )
    return candidates


def fetch_pubmed_articles(query: str, retmax: int = 30) -> List[EvidenceCandidate]:
    ids = fetch_pubmed_ids(query, retmax=retmax)
    if not ids:
        return []
    return fetch_pubmed_details(ids)


def rank_candidates(candidates: Iterable[EvidenceCandidate]) -> List[EvidenceCandidate]:
    return sorted(candidates, key=lambda c: c.sort_key())


def _count_occurrences(text: str, phrase: str, *, ignore_negated: bool = False) -> int:
    if not phrase:
        return 0
    pattern = re.escape(phrase.lower())
    boundary = ""
    if re.search(r"[a-z0-9]", phrase.lower()):
        boundary = r"\b"
    if ignore_negated:
        lookbehind = "".join(f"(?<!{re.escape(prefix)})" for prefix in NEGATING_PREFIXES)
        regex = re.compile(f"{lookbehind}{boundary}{pattern}{boundary}")
    else:
        regex = re.compile(f"{boundary}{pattern}{boundary}")
    return len(regex.findall(text))


def classify_stance(claim_text: str, abstract: str) -> str:
    if not abstract:
        return "mixed"
    text = abstract.lower()
    claim = (claim_text or "").lower()

    positive_terms = set(POSITIVE_INDICATORS)
    negative_terms = set(NEGATIVE_INDICATORS)
    mixed_terms = set(MIXED_INDICATORS)

    if any(word in claim for word in {"increase", "increases", "improve", "improves", "boost", "enhance", "supports", "support"}):
        positive_terms.update({"increase", "increased", "improve", "improved", "enhance", "enhanced", "boost", "boosted", "greater"})
        negative_terms.update({"no increase", "no improvement", "decrease", "decreased", "reduction"})
    if any(word in claim for word in {"reduce", "reduces", "reduction", "lower", "lowers", "decrease", "decreases", "prevent", "prevents", "protect"}):
        positive_terms.update({"decrease", "decreased", "reduced", "reduction", "lower", "lowered", "prevent", "prevented"})
        negative_terms.update({"no decrease", "no reduction", "no change", "increase", "increased"})
    if "risk" in claim:
        positive_terms.update({"reduced risk", "lower risk", "decreased risk", "increased risk", "higher risk", "risk reduction"})
        negative_terms.update({"no change in risk", "no difference in risk"})
        if any(phrase in claim for phrase in {"reduce risk", "reduces risk", "lower risk", "decrease risk", "protect"}):
            negative_terms.update({"increased risk", "higher risk", "no reduction in risk"})
        if any(phrase in claim for phrase in {"increase risk", "increases risk", "raises risk", "higher risk", "cause", "causes"}):
            positive_terms.update({"increased risk", "higher risk", "greater risk"})
            negative_terms.update({"no increased risk", "not associated", "no association", "no evidence of increased risk"})

    positive = sum(
        _count_occurrences(text, term, ignore_negated=True) for term in positive_terms
    )
    negative = sum(_count_occurrences(text, term) for term in negative_terms)
    mixed = sum(_count_occurrences(text, term) for term in mixed_terms)

    if positive == 0 and negative == 0:
        return "mixed"
    if positive >= max(1, negative) * 1.3:
        return "supports"
    if negative >= max(1, positive) * 1.3:
        return "contradicts"
    if mixed or (positive > 0 and negative > 0):
        return "mixed"
    return "supports" if positive >= negative else "contradicts"


def is_auto_generated_note(note: Optional[str]) -> bool:
    if note is None:
        return True
    return note.lower().startswith(AUTO_NOTE_PREFIX)


def upsert_evidence(conn, candidate: EvidenceCandidate) -> int:
    with conn.cursor() as cur:
        if candidate.pubmed_id:
            cur.execute(
                "SELECT id FROM evidence_source WHERE pubmed_id = %s",
                (candidate.pubmed_id,),
            )
            row = cur.fetchone()
            if row:
                evidence_id = row[0]
                cur.execute(
                    "UPDATE evidence_source SET title = %s, year = %s, doi = COALESCE(NULLIF(%s, ''), doi), url = %s, type = %s, journal = %s WHERE id = %s",
                    (
                        candidate.title,
                        candidate.year,
                        candidate.doi,
                        candidate.url,
                        candidate.primary_type(),
                        candidate.journal,
                        evidence_id,
                    ),
                )
                return evidence_id
        if candidate.doi:
            cur.execute(
                "SELECT id FROM evidence_source WHERE doi = %s",
                (candidate.doi,),
            )
            row = cur.fetchone()
            if row:
                evidence_id = row[0]
                cur.execute(
                    "UPDATE evidence_source SET title = %s, year = %s, pubmed_id = COALESCE(NULLIF(%s, ''), pubmed_id), url = %s, type = %s, journal = %s WHERE id = %s",
                    (
                        candidate.title,
                        candidate.year,
                        candidate.pubmed_id,
                        candidate.url,
                        candidate.primary_type(),
                        candidate.journal,
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
                candidate.title,
                candidate.year,
                candidate.doi,
                candidate.pubmed_id,
                candidate.url,
                candidate.primary_type(),
                candidate.journal,
            ),
        )
        evidence_id = cur.fetchone()[0]
        return evidence_id


def attach_evidence_to_claim(
    conn,
    claim_id: int,
    evidence_id: int,
    stance: str,
    note_context: Optional[str] = None,
) -> bool:
    timestamp = dt.datetime.utcnow().date().isoformat()
    note_parts = [AUTO_NOTE_PREFIX, timestamp]
    if note_context:
        note_parts.append(note_context)
    note = " ".join(note_parts)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT stance, notes FROM claim_evidence WHERE claim_id = %s AND evidence_id = %s",
            (claim_id, evidence_id),
        )
        row = cur.fetchone()
        if row:
            existing_note = row[1]
            if existing_note and not is_auto_generated_note(existing_note):
                logger.info(
                    "Skipping manual evidence link for claim %s evidence %s", claim_id, evidence_id
                )
                return False
            cur.execute(
                "UPDATE claim_evidence SET stance = %s, notes = %s WHERE claim_id = %s AND evidence_id = %s",
                (stance, note, claim_id, evidence_id),
            )
            return True
        cur.execute(
            "INSERT INTO claim_evidence (claim_id, evidence_id, stance, notes) VALUES (%s, %s, %s, %s)",
            (claim_id, evidence_id, stance, note),
        )
        return True


class EvidenceFetcher:
    """Fetch and attach evidence for claims."""

    def __init__(
        self,
        conn,
        *,
        min_results: int = 3,
        max_results: int = 10,
        sleep_between: float = 0.34,
    ) -> None:
        self.conn = conn
        self.min_results = min_results
        self.max_results = max_results
        self.sleep_between = sleep_between

    def existing_evidence_count(self, claim_id: int) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM claim_evidence WHERE claim_id = %s AND stance IS NOT NULL",
                (claim_id,),
            )
            row = cur.fetchone()
            return int(row[0]) if row else 0

    def process_claim(
        self,
        claim_id: int,
        normalized_text: Optional[str],
        raw_text: Optional[str],
        *,
        force: bool = False,
    ) -> List[EvidenceCandidate]:
        existing = self.existing_evidence_count(claim_id)
        if existing >= self.min_results and not force:
            logger.info(
                "Claim %s already has %s evidence items; skipping", claim_id, existing
            )
            return []
        queries, terms = build_query_variants(normalized_text, raw_text)
        if not queries:
            logger.warning("Claim %s: no search query could be generated", claim_id)
            return []
        collected: Dict[str, EvidenceCandidate] = {}
        for idx, query in enumerate(queries):
            logger.info("Claim %s: searching PubMed (%s/%s)", claim_id, idx + 1, len(queries))
            candidates = fetch_pubmed_articles(query, retmax=self.max_results * 3)
            for candidate in candidates:
                if candidate.pubmed_id not in collected:
                    collected[candidate.pubmed_id] = candidate
            if len(collected) >= self.max_results:
                break
            if idx < len(queries) - 1 and self.sleep_between:
                time.sleep(self.sleep_between)
        ranked = rank_candidates(collected.values())
        selected = ranked[: self.max_results]
        if len(selected) < self.min_results:
            logger.warning(
                "Claim %s: only %s evidence candidates found (min=%s)",
                claim_id,
                len(selected),
                self.min_results,
            )
        for candidate in selected:
            stance = classify_stance(normalized_text or raw_text or "", candidate.abstract)
            evidence_id = upsert_evidence(self.conn, candidate)
            attach_evidence_to_claim(
                self.conn,
                claim_id,
                evidence_id,
                stance,
                note_context=f"query={'/'.join(terms[:4])}",
            )
        return selected


__all__ = [
    "EvidenceCandidate",
    "EvidenceFetcher",
    "attach_evidence_to_claim",
    "build_query_terms",
    "build_query_variants",
    "classify_stance",
    "fetch_pubmed_articles",
    "rank_candidates",
]
