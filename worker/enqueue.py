"""Ad-hoc CLI for linking evidence to claims."""

from __future__ import annotations

import logging
from typing import Iterable, List, Optional, Sequence, Tuple

import typer

from server.db.utils import db_conn
from server.services.evidence import EvidenceService, iter_claim_rows

logger = logging.getLogger(__name__)

app = typer.Typer(help="Utility commands to enqueue background work")


def _parse_id_list(value: Optional[str]) -> List[int]:
    if not value:
        return []
    parts = [item.strip() for item in value.split(",") if item.strip()]
    result: List[int] = []
    for part in parts:
        try:
            result.append(int(part))
        except ValueError as exc:  # pragma: no cover - defensive
            raise typer.BadParameter(f"Invalid id '{part}'") from exc
    return result


@app.callback()
def main(
    verbose: bool = typer.Option(
        False,
        "--verbose/--no-verbose",
        help="Enable debug logging",
    )
) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")


def _iter_target_claims(
    conn,
    *,
    claim_ids: Sequence[int] | None,
    episode_ids: Sequence[int] | None,
) -> Iterable[Tuple[int, Optional[str], Optional[str]]]:
    return iter_claim_rows(conn, claim_ids=claim_ids, episode_ids=episode_ids)


@app.command("link-evidence")
def link_evidence(
    claim_ids: Optional[str] = typer.Option(
        None,
        help="Comma separated claim ids to process",
    ),
    episode_ids: Optional[str] = typer.Option(
        None,
        help="Comma separated episode ids to process",
    ),
    min_results: int = typer.Option(2, "--min-results", help="Minimum evidence per claim"),
    max_results: int = typer.Option(10, "--max-results", help="Maximum evidence to link"),
    force: bool = typer.Option(
        False,
        "--force/--no-force",
        help="Re-process even if evidence exists",
    ),
) -> None:
    claim_id_list = _parse_id_list(claim_ids)
    episode_id_list = _parse_id_list(episode_ids)
    if claim_id_list and episode_id_list:
        raise typer.BadParameter("Provide either --claim-ids or --episode-ids, not both.")

    with db_conn() as conn:
        service = EvidenceService(
            conn,
            min_results=min_results,
            max_results=max_results,
        )
        rows = list(
            _iter_target_claims(
                conn,
                claim_ids=claim_id_list or None,
                episode_ids=episode_id_list or None,
            )
        )
        if not rows:
            typer.echo("No claims found for provided filters. Nothing to do.")
            return

        processed = 0
        for claim_id, normalized, raw in rows:
            logger.info("Linking evidence for claim %s", claim_id)
            service.process_claim(claim_id, normalized, raw, force=force)
            processed += 1

    typer.echo(f"Processed {processed} claims.")


if __name__ == "__main__":  # pragma: no cover - manual invocation
    app()

