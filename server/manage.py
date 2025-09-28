from __future__ import annotations

import logging
import pathlib
import sys
import time
from pathlib import Path
from typing import Any, Iterable, List, Optional

import click
import inspect

import typer

ROOT = pathlib.Path(__file__).resolve().parent
PROJECT_ROOT = ROOT.parent


def _extend_sys_path(paths: Iterable[pathlib.Path]) -> None:
    for path in paths:
        if not path.exists():
            continue
        path_str = str(path)
        if path_str not in sys.path:
            sys.path.append(path_str)


_extend_sys_path([PROJECT_ROOT, PROJECT_ROOT / "server", PROJECT_ROOT / "worker"])


def _patch_typer_metavar_behavior() -> None:
    """Make Typer compatible with newer Click metavar hooks."""

    # Click 8.2 started passing ``ctx`` to ``make_metavar``. Typer gained support
    # for this but some environments still ship an older signature. Guard the
    # behavior so we work regardless of the installed combination.
    argument_sig = inspect.signature(typer.core.TyperArgument.make_metavar)
    if "ctx" not in argument_sig.parameters:
        original_argument_make_metavar = typer.core.TyperArgument.make_metavar

        def _argument_make_metavar(self, ctx=None):  # type: ignore[override]
            return original_argument_make_metavar(self)

        typer.core.TyperArgument.make_metavar = _argument_make_metavar  # type: ignore[assignment]

    option_sig = inspect.signature(typer.core.TyperOption.make_metavar)
    if "ctx" in option_sig.parameters:
        original_option_make_metavar = typer.core.TyperOption.make_metavar

        def _option_make_metavar(self, ctx=None):  # type: ignore[override]
            if ctx is None:
                ctx = click.Context(click.Command(self.name or ""))
            return original_option_make_metavar(self, ctx)

        typer.core.TyperOption.make_metavar = _option_make_metavar  # type: ignore[assignment]

    parameter_sig = inspect.signature(click.core.Parameter.make_metavar)
    if "ctx" in parameter_sig.parameters:
        original_parameter_make_metavar = click.core.Parameter.make_metavar

        def _parameter_make_metavar(self, ctx=None):  # type: ignore[override]
            if ctx is None:
                ctx = click.Context(click.Command(self.name or ""))
            return original_parameter_make_metavar(self, ctx)

        click.core.Parameter.make_metavar = _parameter_make_metavar  # type: ignore[assignment]


_patch_typer_metavar_behavior()

WORKSPACE_ROOT = pathlib.Path("/workspace")
if WORKSPACE_ROOT.exists():
    _extend_sys_path([WORKSPACE_ROOT, WORKSPACE_ROOT / "server", WORKSPACE_ROOT / "worker"])

from server.db.utils import db_conn
from server.ingest import feeds as feeds_module
from server.ingest import summaries as summaries_module
from server.ingest import transcripts as transcripts_module
from server.ingest import youtube as youtube_module
from server.services import claims as claims_service
from server.services import jobs as jobs_service
from server.services import grader as grader_service
from server.services import summarize as summarize_service

logger = logging.getLogger(__name__)

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
        summarize_service.summarize_episode(
            conn,
            episode_int,
            refresh=refresh_flag,
            progress_callback=lambda completed, total, chunk: jobs_service.update_job_progress(
                conn,
                job.id,
                total_chunks=total,
                completed_chunks=completed,
                current_chunk=None if chunk is None else chunk.chunk_index,
                message=(
                    f"Summarizing chunk {completed}/{total}"
                    if total and completed
                    else "Summarizing transcript"
                ),
            ),
        )
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
        claims_service.extract_episode_claims(
            conn,
            episode_int,
            refresh=refresh_flag,
            progress_callback=lambda completed, total, chunk: jobs_service.update_job_progress(
                conn,
                job.id,
                total_chunks=total,
                completed_chunks=completed,
                current_chunk=None if chunk is None else chunk.chunk_index,
                message=(
                    f"Extracting chunk {completed}/{total}"
                    if total and completed
                    else "Extracting claims"
                ),
            ),
        )
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
    episode_ids: str = typer.Option(
        ...,
        "--episode-ids",
        "-e",
        help="Comma separated list of episode ids",
    ),
    priority: int = typer.Option(
        0,
        "--priority",
        "-p",
        help="Higher numbers run before lower priority",
    ),
    refresh: bool = typer.Option(
        False,
        "--refresh/--no-refresh",
        help="Regenerate transcript chunks before summarising",
    ),
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
    episode_ids: str = typer.Option(
        ...,
        "--episode-ids",
        "-e",
        help="Comma separated list of episode ids",
    ),
    priority: int = typer.Option(
        0,
        "--priority",
        "-p",
        help="Higher numbers run before lower priority",
    ),
    refresh: bool = typer.Option(
        False,
        "--refresh/--no-refresh",
        help="Rebuild transcript chunks before extracting",
    ),
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


@enqueue_app.command("summarize-latest")
def enqueue_summarize_latest(
    limit: int = typer.Option(5, "--limit", "-n", min=1, help="Number of episodes to enqueue"),
    refresh: bool = typer.Option(
        False,
        "--refresh/--no-refresh",
        help="Regenerate transcript chunks before summarising",
    ),
) -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM episode
                ORDER BY published_at DESC NULLS LAST, id DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

        if not rows:
            typer.echo("No recent episodes available to enqueue.")
            return

        enqueued = 0
        for row in rows:
            episode_id = row[0] if not isinstance(row, dict) else int(row.get("id"))
            jobs_service.enqueue_job(
                conn,
                job_type="summarize",
                payload={"episode_id": int(episode_id), "refresh": refresh},
            )
            enqueued += 1

    typer.echo(f"Enqueued {enqueued} summarisation job(s).")


@jobs_app.command("list")
def list_jobs() -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status, COUNT(*) FROM job_queue GROUP BY status ORDER BY status"
            )
            counts = cur.fetchall()

            cur.execute(
                """
                SELECT id, job_type, run_at
                FROM job_queue
                WHERE status = 'queued' AND run_at <= now()
                ORDER BY priority DESC, run_at, id
                LIMIT 1
                """
            )
            next_row = cur.fetchone()

            upcoming_row = None
            if next_row is None:
                cur.execute(
                    """
                    SELECT id, job_type, run_at
                    FROM job_queue
                    WHERE status = 'queued'
                    ORDER BY run_at, priority DESC, id
                    LIMIT 1
                    """
                )
                upcoming_row = cur.fetchone()

    typer.echo("Job counts by status:")
    if counts:
        for status, count in counts:
            typer.echo(f"  {status}: {count}")
    else:
        typer.echo("  (no jobs queued)")

    if next_row:
        job_id, job_type_value, run_at_value = next_row
        run_at_display = (
            run_at_value.isoformat() if hasattr(run_at_value, "isoformat") else run_at_value
        )
        typer.echo(
            f"Next runnable job: id={job_id} type={job_type_value} run_at={run_at_display}"
        )
    elif upcoming_row:
        job_id, job_type_value, run_at_value = upcoming_row
        run_at_display = (
            run_at_value.isoformat() if hasattr(run_at_value, "isoformat") else run_at_value
        )
        typer.echo(
            f"Next scheduled job: id={job_id} type={job_type_value} run_at={run_at_display}"
        )
    else:
        typer.echo("No queued jobs found.")


@app.callback()
def main(
    verbose: bool = typer.Option(
        False,
        "--verbose/--no-verbose",
        help="Enable debug logging",
    )
) -> None:
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
    loop: bool = typer.Option(
        False,
        "--loop",
        help="Continuously poll for queued jobs",
        is_flag=True,
        show_default=False,
    ),
    once: bool = typer.Option(
        False,
        "--once",
        help="Process a single job before exiting (default behavior)",
        is_flag=True,
        show_default=False,
    ),
    poll_interval: float = typer.Option(
        5.0,
        "--poll-interval",
        "-i",
        help="Seconds to wait between polls when idle",
    ),
    job_types: List[str] = typer.Option(
        [],
        "--job-type",
        "--type",
        "-t",
        help="Only process jobs matching the provided type. Use multiple --job-type options to allow more than one type.",
    ),
    max_jobs: Optional[int] = typer.Option(
        None,
        "--max-jobs",
        "-n",
        min=1,
        help="Maximum number of jobs to process before exiting",
    ),
) -> None:
    poll_interval = max(poll_interval, 0.1)

    if loop and once:
        raise typer.BadParameter(
            "--loop and --once cannot be used together", param_hint="--loop/--once"
        )

    should_loop = loop and not once

    remaining: Optional[int]
    if should_loop:
        remaining = max_jobs
    else:
        remaining = 1 if max_jobs is None else min(1, max_jobs)

    job_type_filters: List[str] = []
    for entry in job_types:
        cleaned = (entry or "").strip()
        if cleaned:
            job_type_filters.append(cleaned)

    while True:
        if remaining is not None and remaining <= 0:
            logger.info("Reached max-jobs limit; exiting")
            break

        job: jobs_service.Job | None
        with db_conn() as conn:
            job = jobs_service.dequeue_job(conn, job_types=job_type_filters or None)
            if job is None:
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

        if remaining is not None:
            remaining -= 1
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


@app.command("discover-youtube")
def discover_youtube(
    limit: Optional[int] = typer.Option(100, "--limit", "-l", min=1, help="Only scan the most recent N episodes"),
) -> None:
    """Populate missing YouTube URLs using show notes heuristics."""

    updated = youtube_module.discover_youtube_urls(limit)
    typer.echo(f"Found YouTube URLs for {updated} episodes.")


@app.command()
def summarize(
    limit: Optional[int] = typer.Option(None, "--limit", "-l", min=1, help="Only summarise the most recent N episodes"),
    refresh: bool = typer.Option(
        False,
        "--refresh/--no-refresh",
        help="Replace any existing summaries",
    ),
) -> None:
    """Generate heuristic TL;DR and narrative summaries for episodes."""

    updated = summaries_module.summarize(limit, refresh=refresh)
    typer.echo(f"Generated summaries for {updated} episodes.")


@app.command("summarize-episode")
def summarize_episode(
    episode_id: int = typer.Option(..., "--episode-id", "-e", help="Episode id to summarise"),
    refresh: bool = typer.Option(
        False,
        "--refresh/--no-refresh",
        help="Regenerate transcript chunks before summarising",
    ),
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
