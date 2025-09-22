"""CLI entrypoint for background jobs such as evidence fetching."""

from __future__ import annotations

import argparse
import logging
from typing import Iterable, Tuple

from server.db.utils import db_conn
from server.services.evidence_fetcher import EvidenceFetcher

logger = logging.getLogger(__name__)


def iter_claims(conn, claim_id: int | None = None) -> Iterable[Tuple[int, str | None, str | None, str | None]]:
    with conn.cursor() as cur:
        if claim_id is not None:
            cur.execute(
                "SELECT id, normalized_text, raw_text, topic FROM claim WHERE id = %s",
                (claim_id,),
            )
        else:
            cur.execute(
                "SELECT id, normalized_text, raw_text, topic FROM claim ORDER BY id",
            )
        for row in cur.fetchall():
            yield row


def run_evidence_job(
    *,
    claim_id: int | None,
    min_results: int,
    max_results: int,
    force: bool,
    sleep_between: float,
) -> None:
    with db_conn() as conn:
        fetcher = EvidenceFetcher(
            conn,
            min_results=min_results,
            max_results=max_results,
            sleep_between=sleep_between,
        )
        rows = list(iter_claims(conn, claim_id))
        if not rows:
            if claim_id is not None:
                logger.warning("No claim found with id %s", claim_id)
            else:
                logger.warning("No claims available in database")
            return
        for cid, normalized_text, raw_text, topic in rows:
            logger.info(
                "Processing claim %s (topic=%s)",
                cid,
                topic or "?",
            )
            fetcher.process_claim(
                cid,
                normalized_text,
                raw_text,
                force=force,
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Background worker utilities")
    subparsers = parser.add_subparsers(dest="command")

    evidence_parser = subparsers.add_parser(
        "evidence",
        help="Fetch PubMed evidence for claims",
    )
    evidence_parser.add_argument("--claim-id", type=int, default=None, help="Process a single claim id")
    evidence_parser.add_argument("--min", dest="min_results", type=int, default=3, help="Minimum evidence to attach")
    evidence_parser.add_argument("--max", dest="max_results", type=int, default=10, help="Maximum evidence to attach")
    evidence_parser.add_argument("--force", action="store_true", help="Re-fetch even if evidence already exists")
    evidence_parser.add_argument(
        "--sleep",
        dest="sleep_between",
        type=float,
        default=0.34,
        help="Seconds to sleep between PubMed queries",
    )

    parser.set_defaults(command="evidence")
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "evidence":
        run_evidence_job(
            claim_id=args.claim_id,
            min_results=args.min_results,
            max_results=args.max_results,
            force=args.force,
            sleep_between=args.sleep_between,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
