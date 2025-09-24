from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Iterable, List, Optional

import typer

from db.utils import db_conn
from ingest import feeds as feeds_module
from ingest import summaries as summaries_module
from ingest import transcripts as transcripts_module
from services import claims as claims_service
from services import jobs as jobs_service
from services import grader as grader_service
from services import summarize as summarize_service

app = typer.Typer(help="Podcast ingestion and summarisation utilities")
jobs_app = typer.Typer(help="Background job processing commands")
enqueue_app = typer.Typer(help="Job queue helpers")
jobs_app.add_typer(enqueue_app, name="enqueue")
app.add_typer(jobs_app, name="jobs")
app.add_typer(enqueue_app, name="enqueue")

logger = logging.getLogger(__name__)


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")


def _parse_id_list(raw: str, *, label: str) -> List[int]:
    parts = [value.strip() for value in raw.split(",") if value.strip()]
    if not parts:
        raise typer.BadParameter(f"Provide at least one {label}")

    episode_ids: List[int] = []
    for part in parts:
        try:
            episode_ids.append(int(part))
        except ValueError as exc:  # pragma: no cover - defensive guard
            raise typer.BadParameter(f"Invalid {label} '{part}'") from exc
    return episode_ids


def _parse_episode_ids(raw: str) -> List[int]:
    return _parse_id_list(raw, label="episode id")


def _parse_claim_ids(raw: str) -> List[int]:
    return _parse_id_list(raw, label="claim id")


def _coerce_id_sequence(value: Any, *, field_name: str) -> List[int] | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        result: List[int] = []
        for item in value:
            if item in (None, ""):
                continue
            try:
                result.append(int(item))
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive guard
                raise ValueError(f"{field_name} must only contain integers") from exc
        return result or None
    try:
        return [int(value)]
    except (TypeError, ValueError) as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"{field_name} must be an integer or list of integers") from exc


def _process_job(conn, job: jobs_service.Job) -> None:
    if job.job_type == "summarize":
        episode_id = job.payload.get("episode_id")
        if episode_id is None:
            raise ValueError("summarize job missing episode_id in payload")
        try:
            episode_int = int(episode_id)
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive guard
            raise ValueError(f"Invalid episode_id value {episode_id!r}") from exc
        refresh_flag = bool(job.payload.get("refresh", False))
        summarize_service.summarize_episode(conn, episode_int, refresh=refresh_flag)
        return

    if job.job_type == "extract_claims":
        episode_id = job.payload.get("episode_id")
        if episode_id is None:
            raise ValueError("extract_claims job missing episode_id in payload")
        try:
            episode_int = int(episode_id)
        except (TypeError, ValueError) as exc:  # pragma: no cover - defensive guard
            raise ValueError(f"Invalid episode_id value {episode_id!r}") from exc
        refresh_flag = bool(job.payload.get("refresh", False))
        claims_service.extract_episode_claims(conn, episode_int, refresh=refresh_flag)
        return

    if job.job_type == "auto_grade":
        claim_ids = _coerce_id_sequence(job.payload.get("claim_ids"), field_name="claim_ids")
        episode_ids = _coerce_id_sequence(job.payload.get("episode_ids"), field_name="episode_ids")
        grader = grader_service.AutoGradeService(conn)
        grader.grade_claims(claim_ids=claim_ids, episode_ids=episode_ids)
        return

    raise ValueError(f"Unsupported job type: {job.job_type}")


@enqueue_app.command("summarize")
def enqueue_summarize(
    episode_ids: str = typer.Option(..., "--episode-ids", help="Comma separated list of episode ids"),
    priority: int = typer.Option(0, "--priority", "-p", help="Higher numbers run before lower priority"),
    refresh: bool = typer.Option(False, "--refresh", help="Regenerate transcript chunks before summarising"),
) -> None:
    ids = _parse_episode_ids(episode_ids)
    with db_conn() as conn:
        for episode_id in ids:
            jobs_service.enqueue_job(
                conn,
                job_type="summarize",
                payload={"episode_id": episode_id, "refresh": refresh},
                priority=priority,
            )
    typer.echo(f"Enqueued {len(ids)} summarisation job(s).")


@enqueue_app.command("extract-claims")
def enqueue_extract_claims(
    episode_ids: str = typer.Option(..., "--episode-ids", help="Comma separated list of episode ids"),
    priority: int = typer.Option(0, "--priority", "-p", help="Higher numbers run before lower priority"),
    refresh: bool = typer.Option(False, "--refresh", help="Rebuild transcript chunks before extracting"),
) -> None:
    ids = _parse_episode_ids(episode_ids)
    with db_conn() as conn:
        for episode_id in ids:
            jobs_service.enqueue_job(
                conn,
                job_type="extract_claims",
                payload={"episode_id": episode_id, "refresh": refresh},
                priority=priority,
            )
    typer.echo(f"Enqueued {len(ids)} claim extraction job(s).")


@enqueue_app.command("auto-grade")
def enqueue_auto_grade(
    claim_ids: Optional[str] = typer.Option(
        None,
        "--claim-ids",
        help="Comma separated list of claim ids to re-grade",
    ),
    episode_ids: Optional[str] = typer.Option(
        None,
        "--episode-ids",
        help="Comma separated list of episode ids whose claims should be graded",
    ),
    priority: int = typer.Option(
        0,
        "--priority",
        "-p",
        help="Higher numbers run before lower priority",
    ),
) -> None:
    claim_list = _parse_claim_ids(claim_ids) if claim_ids is not None else None
    episode_list = _parse_episode_ids(episode_ids) if episode_ids is not None else None
    if not claim_list and not episode_list:
        raise typer.BadParameter("Provide --claim-ids or --episode-ids")

    payload: dict[str, Any] = {}
    if claim_list:
        payload["claim_ids"] = claim_list
    if episode_list:
        payload["episode_ids"] = episode_list

    with db_conn() as conn:
        job = jobs_service.enqueue_job(
            conn,
            job_type="auto_grade",
            payload=payload,
            priority=priority,
        )
    typer.echo(f"Enqueued auto-grade job {job.id} targeting linked claims.")


@app.callback()
def main(verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging")) -> None:
    _configure_logging(verbose)


@app.command()
def discover(
    feeds: Path = typer.Option(..., exists=True, readable=True, dir_okay=False, help="Text file with one RSS URL per line"),
) -> None:
    """Discover podcasts and episodes from RSS feeds."""

    inserted = feeds_module.discover_from_file(feeds)
    typer.echo(f"Inserted {inserted} new episodes from feeds in {feeds}.")


@jobs_app.command("work")
def work(
    once: bool = typer.Option(False, "--once", help="Process at most one job and then exit"),
    loop: bool = typer.Option(False, "--loop", help="Continuously poll for new jobs"),
    poll_interval: float = typer.Option(5.0, "--poll-interval", help="Seconds to wait between polls when idle"),
) -> None:
    if once and loop:
        raise typer.BadParameter("Choose either --once or --loop, not both")

    should_loop = loop or not once
    poll_interval = max(poll_interval, 0.1)

    while True:
        job: jobs_service.Job | None = None
        with db_conn() as conn:
            job = jobs_service.dequeue_job(conn)
            if job is None:
                # Close connection before potentially sleeping
                pass
            else:
                logger.info("Processing job %s (%s)", job.id, job.job_type)
                try:
                    _process_job(conn, job)
                except Exception as exc:
                    logger.exception("Job %s failed", job.id)
                    jobs_service.mark_job_failed(conn, job, str(exc))
                else:
                    jobs_service.mark_job_done(conn, job.id)

        if job is None:
            if should_loop:
                logger.debug("No queued jobs; sleeping for %s seconds", poll_interval)
                time.sleep(poll_interval)
                continue
            typer.echo("No queued jobs available.")
            break

        if not should_loop:
            break


# Backwards compatibility: allow the legacy ``python manage.py work`` invocation.
app.command("work")(work)


@app.command("fetch-transcripts")
def fetch_transcripts(
    limit: Optional[int] = typer.Option(None, "--limit", "-l", min=1, help="Only process the most recent N episodes"),
) -> None:
    """Fetch and store transcripts using lightweight heuristics."""

    inserted = transcripts_module.fetch_transcripts(limit)
    typer.echo(f"Stored transcripts for {inserted} episodes.")


@app.command()
def summarize(
    limit: Optional[int] = typer.Option(None, "--limit", "-l", min=1, help="Only summarise the most recent N episodes"),
    refresh: bool = typer.Option(False, "--refresh", help="Replace any existing summaries"),
) -> None:
    """Generate heuristic TL;DR and narrative summaries for episodes."""

    updated = summaries_module.summarize(limit, refresh=refresh)
    typer.echo(f"Generated summaries for {updated} episodes.")


@app.command("summarize-episode")
def summarize_episode(
    episode_id: int = typer.Option(..., "--episode-id", "-e", help="Episode id to summarise"),
    refresh: bool = typer.Option(False, "--refresh", help="Regenerate transcript chunks before summarising"),
) -> None:
    try:
        with db_conn() as conn:
            result = summarize_service.summarize_episode(conn, episode_id, refresh=refresh)
    except ValueError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(code=1)
    typer.echo(
        f"Generated summary for episode {episode_id} ({len(result.key_points)} bullet points)."
    )


if __name__ == "__main__":
    app()
