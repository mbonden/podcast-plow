"""Command line helper for running the claim extraction pipeline.

This script connects to the database, reads transcripts, extracts claims using
the heuristics in :mod:`worker.claim_extraction`, and inserts (or replaces)
claim rows for each episode.  It is intentionally simple so that it can be run
manually during the milestone without introducing a full task queue.

Usage::

    DATABASE_URL=postgresql://postgres:postgres@localhost:5432/podcast_plow \
        python -m worker.claim_pipeline

Optional arguments:

``--episode``
    Limit extraction to a single episode id.  The default processes every
    transcript currently stored in the database.

The script deletes existing claims for the episode before inserting new ones so
that repeated runs stay idempotent.
"""

from __future__ import annotations

import argparse
import os
from typing import Iterable, List

import psycopg

from .claim_extraction import Claim, extract_claims


def fetch_transcripts(conn, episode_id: int | None = None) -> Iterable[tuple[int, int, str]]:
    """Yield ``(transcript_id, episode_id, text)`` tuples from the database."""

    with conn.cursor() as cur:
        if episode_id is None:
            cur.execute("SELECT id, episode_id, text FROM transcript")
        else:
            cur.execute(
                "SELECT id, episode_id, text FROM transcript WHERE episode_id = %s",
                (episode_id,),
            )
        for row in cur:
            yield row[0], row[1], row[2]


def replace_claims(conn, episode_id: int, claims: List[Claim]) -> None:
    """Replace all claims for an episode with ``claims``."""

    with conn.cursor() as cur:
        cur.execute("DELETE FROM claim WHERE episode_id = %s", (episode_id,))
        for claim in claims:
            cur.execute(
                """
                INSERT INTO claim (episode_id, start_ms, end_ms, raw_text, normalized_text, topic, domain, risk_level)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    episode_id,
                    claim.start_ms,
                    claim.end_ms,
                    claim.raw_text,
                    claim.normalized_text,
                    claim.topic,
                    claim.domain,
                    claim.risk_level,
                ),
            )


def run(episode_id: int | None = None) -> int:
    database_url = os.getenv(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/podcast_plow"
    )
    with psycopg.connect(database_url, autocommit=True) as conn:
        processed = 0
        for _, e_id, text in fetch_transcripts(conn, episode_id=episode_id):
            claims = extract_claims(text or "")
            replace_claims(conn, e_id, claims)
            processed += 1
    return processed


def main(argv: List[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Extract claims from transcripts")
    parser.add_argument(
        "--episode",
        type=int,
        help="only process the specified episode id",
    )
    args = parser.parse_args(argv)
    count = run(episode_id=args.episode)
    print(f"processed {count} transcripts")


if __name__ == "__main__":
    main()

