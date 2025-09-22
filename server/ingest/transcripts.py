from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import unquote, urlparse

import requests
from bs4 import BeautifulSoup

from core.db import db_connection

logger = logging.getLogger(__name__)

USER_AGENT = "podcast-plow-ingest/0.1"
MIN_WORDS = 200


@dataclass
class EpisodeRecord:
    id: int
    title: str
    podcast_title: str
    show_notes_url: Optional[str]
    description: Optional[str]


def _normalize_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _extract_from_html(html: str) -> Optional[str]:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "header", "footer", "svg", "iframe"]):
        tag.decompose()
    paragraphs: list[str] = []
    for element in soup.find_all(["p", "li", "blockquote"]):
        text = element.get_text(" ", strip=True)
        if text:
            paragraphs.append(text)
    if not paragraphs:
        body = soup.get_text("\n", strip=True)
        if body:
            paragraphs = [line for line in body.splitlines() if line.strip()]
    if not paragraphs:
        return None
    lower_paragraphs = [p.lower() for p in paragraphs]
    for idx, para in enumerate(lower_paragraphs):
        if "transcript" in para:
            candidate = "\n\n".join(_normalize_text(p) for p in paragraphs[idx:])
            if len(candidate.split()) >= MIN_WORDS:
                return candidate
    joined = "\n\n".join(_normalize_text(p) for p in paragraphs)
    if "transcript" in joined.lower() and len(joined.split()) >= MIN_WORDS:
        return joined
    article = soup.find("article")
    if article:
        article_text = _normalize_text(article.get_text("\n", strip=True))
        if len(article_text.split()) >= MIN_WORDS:
            return article_text
    if len(joined.split()) >= MIN_WORDS:
        return joined
    return None


def _fetch_html(url: str) -> Optional[str]:
    parsed = urlparse(url)
    if parsed.scheme == "file" or (parsed.scheme == "" and url.startswith("/")):
        if parsed.scheme == "file":
            path_str = unquote(parsed.path or "")
            if parsed.netloc:
                prefix = f"/{parsed.netloc}"
            else:
                prefix = ""
            path = Path(prefix + path_str)
        else:
            path = Path(unquote(parsed.path if parsed.path else url))
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        if not path.exists():
            logger.warning("Transcript source file not found: %s", path)
            return None
        try:
            return path.read_text(encoding="utf-8")
        except OSError as exc:
            logger.warning("Failed to read %s: %s", path, exc)
            return None
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": USER_AGENT})
    except requests.RequestException as exc:
        logger.warning("Failed to download %s: %s", url, exc)
        return None
    if resp.status_code >= 400:
        logger.warning("Non-success status %s for %s", resp.status_code, url)
        return None
    resp.encoding = resp.encoding or "utf-8"
    return resp.text


def _episode_candidates(limit: Optional[int]) -> Iterable[EpisodeRecord]:
    sql = """
        SELECT e.id, e.title, p.title AS podcast_title, e.show_notes_url, e.description
        FROM episode e
        JOIN podcast p ON p.id = e.podcast_id
        LEFT JOIN transcript t ON t.episode_id = e.id
        WHERE t.id IS NULL
        ORDER BY e.published_at DESC NULLS LAST, e.id DESC
    """
    params: tuple[object, ...] = ()
    if limit is not None:
        sql += " LIMIT %s"
        params = (limit,)
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    for row in rows:
        yield EpisodeRecord(*row)


def _store_transcript(episode_id: int, text: str, *, source: str) -> None:
    words = text.split()
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO transcript (episode_id, source, lang, text, word_count, has_verbatim_ok)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (episode_id, source, "en", text, len(words), False),
            )
    logger.info("Stored transcript for episode %s (%d words)", episode_id, len(words))


def fetch_transcripts(limit: Optional[int] = None) -> int:
    inserted = 0
    for episode in _episode_candidates(limit):
        logger.info("Looking for transcript: %s — %s", episode.podcast_title, episode.title)
        if episode.show_notes_url:
            html = _fetch_html(episode.show_notes_url)
            if html:
                transcript = _extract_from_html(html)
                if transcript:
                    _store_transcript(episode.id, transcript, source="show_site")
                    inserted += 1
                    continue
        if episode.description:
            desc_text = BeautifulSoup(episode.description, "html.parser").get_text(" ", strip=True)
            if "transcript" in desc_text.lower() and len(desc_text.split()) >= MIN_WORDS:
                _store_transcript(episode.id, desc_text, source="rss_description")
                inserted += 1
                continue
        logger.info("No transcript heuristics matched for episode %s", episode.id)
    return inserted
