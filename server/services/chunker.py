"""Utilities to break transcripts into manageable chunks."""

from __future__ import annotations

import hashlib
import logging
import math
import re
from dataclasses import dataclass
from typing import Iterable, List, Sequence

logger = logging.getLogger(__name__)

DEFAULT_MAX_TOKENS = 1800
DEFAULT_OVERLAP_RATIO = 0.1


@dataclass
class TranscriptRecord:
    """Representation of a transcript row."""

    id: int
    episode_id: int
    text: str
    word_count: int | None = None


@dataclass
class ChunkData:
    """Metadata about a chunk ready to be persisted."""

    chunk_index: int
    token_start: int
    token_end: int
    token_count: int
    text: str


@dataclass
class ChunkRecord(ChunkData):
    """Chunk information loaded from the database."""

    id: int
    transcript_id: int
    key_points: str | None = None
    source_hash: str | None = None


@dataclass
class ChunkingResult:
    """Return value from :func:`ensure_chunks_for_episode`."""

    transcript: TranscriptRecord
    chunks: List[ChunkRecord]


_token_pattern = re.compile(r"\S+")


def _tokenize(text: str) -> List[str]:
    return _token_pattern.findall(text)


def _tokens_to_text(tokens: Sequence[str]) -> str:
    return " ".join(tokens).strip()


def _build_chunks(tokens: Sequence[str], *, max_tokens: int, overlap_ratio: float) -> List[ChunkData]:
    if not tokens:
        return []

    chunk_size = max(max_tokens, 1)
    overlap = int(math.floor(chunk_size * overlap_ratio))
    if overlap >= chunk_size:
        overlap = max(chunk_size - 1, 0)

    chunks: List[ChunkData] = []
    start = 0
    index = 0

    while start < len(tokens):
        end = min(len(tokens), start + chunk_size)
        chunk_tokens = tokens[start:end]
        if not chunk_tokens:
            break
        text = _tokens_to_text(chunk_tokens)
        chunks.append(
            ChunkData(
                chunk_index=index,
                token_start=start,
                token_end=end,
                token_count=len(chunk_tokens),
                text=text,
            )
        )
        if end >= len(tokens):
            break
        start = max(0, end - overlap)
        if start == end:
            start += 1
        index += 1

    return chunks


def _fetch_transcript(conn, episode_id: int) -> TranscriptRecord | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, episode_id, text, word_count
            FROM transcript
            WHERE episode_id = %s AND text IS NOT NULL AND text <> ''
            ORDER BY word_count DESC NULLS LAST, id DESC
            LIMIT 1
            """,
            (episode_id,),
        )
        row = cur.fetchone()

    if not row:
        logger.debug("No transcript found for episode %s", episode_id)
        return None

    transcript = TranscriptRecord(id=row[0], episode_id=row[1], text=row[2], word_count=row[3])
    return transcript


def _compute_transcript_hash(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def _fetch_existing_chunk_state(conn, transcript_id: int) -> tuple[int, str | None]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), MIN(source_hash) FROM transcript_chunk WHERE transcript_id = %s",
            (transcript_id,),
        )
        row = cur.fetchone()
    if not row:
        return 0, None
    count = int(row[0] or 0)
    stored_hash = row[1]
    if stored_hash in (None, ""):
        stored_hash = None
    return count, stored_hash


def _persist_chunks(
    conn,
    transcript_id: int,
    text: str,
    *,
    max_tokens: int,
    overlap_ratio: float,
) -> List[ChunkData]:
    tokens = _tokenize(text)
    chunk_data = _build_chunks(tokens, max_tokens=max_tokens, overlap_ratio=overlap_ratio)
    transcript_hash = _compute_transcript_hash(text)

    with conn.cursor() as cur:
        cur.execute("DELETE FROM transcript_chunk WHERE transcript_id = %s", (transcript_id,))
        for chunk in chunk_data:
            cur.execute(
                """
                INSERT INTO transcript_chunk (
                    transcript_id, chunk_index, token_start, token_end, token_count, text, source_hash
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    transcript_id,
                    chunk.chunk_index,
                    chunk.token_start,
                    chunk.token_end,
                    chunk.token_count,
                    chunk.text,
                    transcript_hash,
                ),
            )

    logger.info("Created %d transcript chunks for transcript %s", len(chunk_data), transcript_id)
    return chunk_data


def fetch_chunks(conn, transcript_id: int) -> List[ChunkRecord]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, transcript_id, chunk_index, token_start, token_end, token_count, text, key_points, source_hash
            FROM transcript_chunk
            WHERE transcript_id = %s
            ORDER BY chunk_index
            """,
            (transcript_id,),
        )
        rows = cur.fetchall()

    chunks: List[ChunkRecord] = []
    for row in rows:
        chunks.append(
            ChunkRecord(
                id=row[0],
                transcript_id=row[1],
                chunk_index=row[2],
                token_start=row[3],
                token_end=row[4],
                token_count=row[5],
                text=row[6],
                key_points=row[7],
                source_hash=row[8] if len(row) > 8 else None,
            )
        )
    return chunks


def ensure_chunks_for_episode(
    conn,
    episode_id: int,
    *,
    refresh: bool = False,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    overlap_ratio: float = DEFAULT_OVERLAP_RATIO,
) -> ChunkingResult | None:
    transcript = _fetch_transcript(conn, episode_id)
    if transcript is None:
        return None

    chunk_count, stored_hash = _fetch_existing_chunk_state(conn, transcript.id)
    transcript_hash = _compute_transcript_hash(transcript.text)
    needs_refresh = refresh or chunk_count == 0 or stored_hash != transcript_hash
    if needs_refresh:
        _persist_chunks(
            conn,
            transcript.id,
            transcript.text,
            max_tokens=max_tokens,
            overlap_ratio=overlap_ratio,
        )

    chunks = fetch_chunks(conn, transcript.id)
    if not chunks:
        logger.warning("Transcript %s has no chunks after processing", transcript.id)
        return None
    return ChunkingResult(transcript=transcript, chunks=chunks)


def serialize_key_points(points: Iterable[str]) -> str | None:
    cleaned = [point.strip() for point in points if point and point.strip()]
    if not cleaned:
        return None
    return "\n".join(f"- {point}" for point in cleaned)


def update_chunk_key_points(conn, chunk_id: int, points: Iterable[str]) -> None:
    serialized = serialize_key_points(points)
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE transcript_chunk SET key_points = %s WHERE id = %s",
            (serialized, chunk_id),
        )
    logger.debug("Stored key points for chunk %s", chunk_id)


__all__ = [
    "ChunkingResult",
    "ChunkRecord",
    "ensure_chunks_for_episode",
    "fetch_chunks",
    "serialize_key_points",
    "update_chunk_key_points",
]
