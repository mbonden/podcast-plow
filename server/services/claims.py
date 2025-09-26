"""Claim extraction helpers for transcript chunks."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable, Dict, Iterable, List, Sequence

from worker.claim_extraction import MS_PER_WORD, Claim as ExtractedClaim, extract_claims

from . import chunker
from .normalization import canonical_domain, canonical_topic

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClaimCandidate:
    """Intermediate representation of a claim prior to persistence."""

    raw_text: str
    normalized_text: str
    topic: str
    domain: str
    risk_level: str
    start_ms: int
    end_ms: int


@dataclass(frozen=True)
class StoredClaim(ClaimCandidate):
    """Claim data persisted to the database."""

    id: int
    episode_id: int


def _adjust_bounds(claim: ExtractedClaim, *, token_start: int) -> ClaimCandidate:
    offset_ms = max(token_start, 0) * MS_PER_WORD
    start = max(0, claim.start_ms + offset_ms)
    end = claim.end_ms + offset_ms
    if end <= start:
        end = start + MS_PER_WORD
    return ClaimCandidate(
        raw_text=claim.raw_text,
        normalized_text=claim.normalized_text.strip(),
        topic=canonical_topic(claim.topic),
        domain=canonical_domain(claim.domain),
        risk_level=claim.risk_level,
        start_ms=start,
        end_ms=end,
    )


def _aggregate_candidates(
    chunks: Sequence[chunker.ChunkRecord],
    progress_callback: Callable[[int, int, chunker.ChunkRecord], None] | None = None,
) -> Dict[str, ClaimCandidate]:
    aggregated: Dict[str, ClaimCandidate] = {}
    total = len(chunks)
    for index, chunk in enumerate(chunks, start=1):
        chunk_claims = extract_claims(chunk.text)
        for claim in chunk_claims:
            if not claim.normalized_text:
                continue
            candidate = _adjust_bounds(claim, token_start=chunk.token_start)
            key = candidate.normalized_text
            existing = aggregated.get(key)
            if existing is None or candidate.start_ms < existing.start_ms:
                aggregated[key] = candidate
        if progress_callback is not None:
            progress_callback(index, total, chunk)
    return aggregated


def _load_existing_claims(conn, episode_id: int) -> tuple[Dict[str, int], List[int]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, normalized_text FROM claim WHERE episode_id = %s ORDER BY id",
            (episode_id,),
        )
        rows = cur.fetchall()

    primaries: Dict[str, int] = {}
    duplicates: List[int] = []
    for claim_id, normalized in rows:
        key = (normalized or "").strip()
        if not key:
            continue
        claim_id = int(claim_id)
        if key in primaries:
            duplicates.append(claim_id)
        else:
            primaries[key] = claim_id
    return primaries, duplicates


def _delete_claims(conn, claim_ids: Iterable[int]) -> None:
    ids = [int(cid) for cid in claim_ids if cid is not None]
    if not ids:
        return
    with conn.cursor() as cur:
        for claim_id in ids:
            cur.execute("DELETE FROM claim WHERE id = %s", (claim_id,))
    logger.debug("Removed %d duplicate claims", len(ids))


def extract_episode_claims(
    conn,
    episode_id: int,
    *,
    refresh: bool = False,
    progress_callback: Callable[[int, int, chunker.ChunkRecord | None], None] | None = None,
) -> List[StoredClaim]:
    """Populate deterministic claims for *episode_id*.

    Returns the stored claims ordered by their estimated start time.
    """

    chunk_data = chunker.ensure_chunks_for_episode(conn, episode_id, refresh=refresh)
    if chunk_data is None:
        raise ValueError(f"No transcript available for episode {episode_id}")

    total_chunks = len(chunk_data.chunks)
    if progress_callback is not None:
        progress_callback(0, total_chunks, None)

    aggregated = _aggregate_candidates(
        chunk_data.chunks,
        progress_callback=(
            lambda index, total, chunk: progress_callback(index, total, chunk)
            if progress_callback is not None
            else None
        ),
    )
    if not aggregated:
        logger.info("No claims detected for episode %s", episode_id)

    existing, duplicates = _load_existing_claims(conn, episode_id)

    stored: List[StoredClaim] = []
    ordered_items = sorted(
        aggregated.items(), key=lambda item: (item[1].start_ms, item[0])
    )

    with conn.cursor() as cur:
        for normalized, candidate in ordered_items:
            existing_id = existing.pop(normalized, None)
            if existing_id is not None:
                cur.execute(
                    """
                    UPDATE claim
                    SET raw_text = %s,
                        normalized_text = %s,
                        topic = %s,
                        domain = %s,
                        risk_level = %s,
                        start_ms = %s,
                        end_ms = %s
                    WHERE id = %s
                    """,
                    (
                        candidate.raw_text,
                        normalized,
                        candidate.topic,
                        candidate.domain,
                        candidate.risk_level,
                        candidate.start_ms,
                        candidate.end_ms,
                        existing_id,
                    ),
                )
                claim_id = existing_id
            else:
                cur.execute(
                    """
                    INSERT INTO claim (
                        episode_id,
                        raw_text,
                        normalized_text,
                        topic,
                        domain,
                        risk_level,
                        start_ms,
                        end_ms
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        episode_id,
                        candidate.raw_text,
                        normalized,
                        candidate.topic,
                        candidate.domain,
                        candidate.risk_level,
                        candidate.start_ms,
                        candidate.end_ms,
                    ),
                )
                claim_id = cur.fetchone()[0]

            stored.append(
                StoredClaim(
                    id=int(claim_id),
                    episode_id=episode_id,
                    raw_text=candidate.raw_text,
                    normalized_text=normalized,
                    topic=candidate.topic,
                    domain=candidate.domain,
                    risk_level=candidate.risk_level,
                    start_ms=candidate.start_ms,
                    end_ms=candidate.end_ms,
                )
            )

    _delete_claims(conn, duplicates)

    logger.info(
        "Extracted %d claims for episode %s", len(stored), episode_id
    )
    return stored


__all__ = ["ClaimCandidate", "StoredClaim", "extract_episode_claims"]
