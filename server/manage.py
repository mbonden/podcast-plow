from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer

from ingest import feeds as feeds_module
from ingest import summaries as summaries_module
from ingest import transcripts as transcripts_module

app = typer.Typer(help="Podcast ingestion and summarisation utilities")


def _configure_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(levelname)s %(message)s")


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


if __name__ == "__main__":
    app()
