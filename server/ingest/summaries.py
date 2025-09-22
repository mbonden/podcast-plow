from __future__ import annotations

import logging
import re
from collections import Counter
from dataclasses import dataclass
from typing import Iterable, List, Optional

from core.db import db_connection

logger = logging.getLogger(__name__)

WORD_RE = re.compile(r"[\w']+")


@dataclass
class TranscriptRecord:
    episode_id: int
    episode_title: str
    podcast_title: str
    text: str


def _sentence_split(text: str) -> List[str]:
    normalized = re.sub(r"\s+", " ", text.replace("\r", " "))
    # basic sentence splitting that keeps abbreviations reasonable
    parts = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9])", normalized)
    sentences = [p.strip() for p in parts if p.strip()]
    return sentences


def _rank_sentences(sentences: List[str]) -> List[tuple[float, int, str]]:
    freq = Counter()
    for sentence in sentences:
        freq.update(word for word in WORD_RE.findall(sentence.lower()) if len(word) > 3)
    ranked: List[tuple[float, int, str]] = []
    for idx, sentence in enumerate(sentences):
        words = [w for w in WORD_RE.findall(sentence.lower()) if len(w) > 3]
        if not words:
            continue
        score = sum(freq[w] for w in words) / len(words)
        freshness = 1 / (1 + idx / 10)
        ranked.append((score * freshness, idx, sentence))
    ranked.sort(reverse=True)
    return ranked


def _select_sentences(sentences: List[str], *, max_words: int, max_sentences: int) -> List[str]:
    ranked = _rank_sentences(sentences)
    selected: List[tuple[int, str]] = []
    used = set()
    total_words = 0
    for score, idx, sentence in ranked:
        if idx in used:
            continue
        word_count = len(sentence.split())
        if word_count < 6:
            continue
        selected.append((idx, sentence))
        used.add(idx)
        total_words += word_count
        if total_words >= max_words or len(selected) >= max_sentences:
            break
    if not selected:
        for idx, sentence in enumerate(sentences):
            word_count = len(sentence.split())
            if word_count < 6:
                continue
            selected.append((idx, sentence))
            total_words += word_count
            if total_words >= max_words or len(selected) >= max_sentences:
                break
    selected.sort()
    return [sentence for _, sentence in selected]


def _paragraphise(sentences: List[str]) -> str:
    if not sentences:
        return ""
    paragraphs: List[str] = []
    buffer: List[str] = []
    word_acc = 0
    for sentence in sentences:
        buffer.append(sentence)
        word_acc += len(sentence.split())
        if word_acc >= 80:
            paragraphs.append(" ".join(buffer))
            buffer = []
            word_acc = 0
    if buffer:
        paragraphs.append(" ".join(buffer))
    return "\n\n".join(paragraphs)


def _collect_candidates(limit: Optional[int], refresh: bool) -> Iterable[TranscriptRecord]:
    sql = """
        SELECT e.id, e.title, p.title AS podcast_title, t.text
        FROM episode e
        JOIN podcast p ON p.id = e.podcast_id
        JOIN transcript t ON t.episode_id = e.id
    """
    if not refresh:
        sql += " LEFT JOIN episode_summary s ON s.episode_id = e.id WHERE s.id IS NULL"
    else:
        sql += " LEFT JOIN episode_summary s ON s.episode_id = e.id"
    sql += " ORDER BY e.published_at DESC NULLS LAST, e.id DESC"
    params: tuple[object, ...] = ()
    if limit is not None:
        sql += " LIMIT %s"
        params = (limit,)
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    for row in rows:
        yield TranscriptRecord(*row)


def _store_summary(episode_id: int, *, tl_dr: str, narrative: str, refresh: bool) -> None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            if refresh:
                cur.execute("DELETE FROM episode_summary WHERE episode_id = %s", (episode_id,))
            cur.execute(
                """
                INSERT INTO episode_summary (episode_id, tl_dr, narrative, created_by)
                VALUES (%s, %s, %s, %s)
                """,
                (episode_id, tl_dr, narrative, "pipeline"),
            )
    logger.info("Stored summary for episode %s", episode_id)


def _build_tldr(podcast: str, title: str, sentences: List[str]) -> str:
    highlighted = _select_sentences(sentences, max_words=80, max_sentences=3)
    if highlighted:
        return " ".join(highlighted)
    return f"{podcast} — {title}: conversation highlights unavailable."


def _build_narrative(sentences: List[str]) -> str:
    highlighted = _select_sentences(sentences, max_words=260, max_sentences=12)
    return _paragraphise(highlighted)


def summarize(limit: Optional[int] = None, *, refresh: bool = False) -> int:
    updated = 0
    for record in _collect_candidates(limit, refresh):
        logger.info("Summarising %s — %s", record.podcast_title, record.episode_title)
        sentences = _sentence_split(record.text)
        if not sentences:
            logger.info("Skipping episode %s because transcript is empty", record.episode_id)
            continue
        tl_dr = _build_tldr(record.podcast_title, record.episode_title, sentences)
        narrative = _build_narrative(sentences)
        if not narrative:
            narrative = "\n\n".join(sentences[:6])
        _store_summary(record.episode_id, tl_dr=tl_dr, narrative=narrative, refresh=refresh)
        updated += 1
    return updated
