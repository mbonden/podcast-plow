"""Map/reduce style summarisation helpers."""

from __future__ import annotations

import logging
import math
import re
from dataclasses import dataclass
from typing import Iterable, List, Sequence

from sumy.nlp.tokenizers import Tokenizer
from sumy.parsers.plaintext import PlaintextParser
from sumy.summarizers.luhn import LuhnSummarizer

from . import chunker

logger = logging.getLogger(__name__)

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")


@dataclass
class SummaryResult:
    """Summary data returned by :func:`summarize_episode`."""

    tl_dr: str
    narrative: str
    key_points: List[str]


def _fallback_sentences(text: str, limit: int) -> List[str]:
    pieces = _SENTENCE_SPLIT.split(text)
    results: List[str] = []
    for piece in pieces:
        cleaned = re.sub(r"\s+", " ", piece).strip()
        if not cleaned:
            continue
        results.append(cleaned)
        if len(results) >= limit:
            break
    return results


def _summarize_chunk_text(text: str, desired_points: int) -> List[str]:
    if not text.strip():
        return []

    parser = PlaintextParser.from_string(text, Tokenizer("english"))
    summarizer = LuhnSummarizer()
    try:
        sentences = summarizer(parser.document, desired_points)
    except ValueError:
        sentences = []

    points: List[str] = []
    for sentence in sentences:
        cleaned = re.sub(r"\s+", " ", str(sentence)).strip()
        if cleaned:
            points.append(cleaned)

    if not points:
        points = _fallback_sentences(text, desired_points)

    return points


def _dedupe_points(points: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    cleaned: List[str] = []
    for point in points:
        normalized = re.sub(r"\s+", " ", point).strip()
        if not normalized:
            continue
        key = normalized.rstrip(".").lower()
        if key in seen:
            continue
        seen.add(key)
        cleaned.append(normalized.rstrip("."))
    return cleaned


def _select_points(points: Sequence[str], *, minimum: int = 5, maximum: int = 8) -> List[str]:
    if not points:
        return []
    trimmed = list(points[:maximum])
    if len(trimmed) < minimum and len(points) >= minimum:
        trimmed = list(points[:minimum])
    return trimmed


def _format_tldr(points: Sequence[str]) -> str:
    if not points:
        return ""
    return "\n".join(f"- {point}" for point in points)


def _ensure_sentence(text: str) -> str:
    cleaned = text.strip()
    if not cleaned.endswith(('.', '!', '?')):
        cleaned = f"{cleaned}."
    return cleaned


def _build_narrative(points: Sequence[str]) -> str:
    if not points:
        return ""
    sentences = [_ensure_sentence(point) for point in points]
    if len(sentences) <= 4:
        return " ".join(sentences)
    midpoint = math.ceil(len(sentences) / 2)
    first = " ".join(sentences[:midpoint]).strip()
    second = " ".join(sentences[midpoint:]).strip()
    if second:
        return f"{first}\n\n{second}"
    return first


def _store_summary(
    conn,
    episode_id: int,
    *,
    tl_dr: str,
    narrative: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM episode_summary WHERE episode_id = %s AND created_by IN (%s, %s)",
            (episode_id, "worker", "pipeline"),
        )
        cur.execute(
            """
            INSERT INTO episode_summary (episode_id, tl_dr, narrative, created_by)
            VALUES (%s, %s, %s, %s)
            """,
            (episode_id, tl_dr, narrative, "worker"),
        )


def summarize_episode(
    conn,
    episode_id: int,
    *,
    refresh: bool = False,
) -> SummaryResult:
    chunk_data = chunker.ensure_chunks_for_episode(conn, episode_id, refresh=refresh)
    if chunk_data is None:
        raise ValueError(f"No transcript available for episode {episode_id}")

    all_points: List[str] = []
    for chunk in chunk_data.chunks:
        desired = max(3, min(7, math.ceil(chunk.token_count / 400)))
        points = _summarize_chunk_text(chunk.text, desired)
        if points:
            chunker.update_chunk_key_points(conn, chunk.id, points)
            all_points.extend(points)
        else:
            chunker.update_chunk_key_points(conn, chunk.id, [])

    deduped = _dedupe_points(all_points)
    if not deduped:
        deduped = _dedupe_points(_fallback_sentences(chunk_data.transcript.text, 8))

    selected = _select_points(deduped)
    tl_dr = _format_tldr(selected)
    narrative = _build_narrative(selected)

    _store_summary(conn, episode_id, tl_dr=tl_dr, narrative=narrative)
    logger.info("Generated summary for episode %s (%d bullet points)", episode_id, len(selected))
    return SummaryResult(tl_dr=tl_dr, narrative=narrative, key_points=selected)


__all__ = ["SummaryResult", "summarize_episode"]
