from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Iterable, Optional
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from core.db import db_connection

logger = logging.getLogger(__name__)


USER_AGENT = "podcast-plow-ingest/0.1"
YOUTUBE_DOMAINS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "music.youtube.com",
    "youtu.be",
    "www.youtu.be",
    "youtube-nocookie.com",
    "www.youtube-nocookie.com",
}

YOUTUBE_ID_PATTERN = re.compile(
    r"(?:youtube(?:-nocookie)?\.com/(?:watch\?v=|embed/|shorts/)|youtu\.be/)([A-Za-z0-9_-]{11})"
)


@dataclass
class EpisodeCandidate:
    id: int
    title: str
    show_notes_url: Optional[str]


def normalize_youtube_url(url: str) -> Optional[str]:
    """Return a canonical watch URL for a YouTube link, if possible."""

    try:
        parsed = urlparse(url)
    except ValueError:
        return None

    host = parsed.netloc.split(":", 1)[0].lower()
    if host not in YOUTUBE_DOMAINS:
        return None

    video_id: Optional[str] = None
    path = parsed.path or ""
    query = parse_qs(parsed.query)

    if host.endswith("youtu.be"):
        slug = path.lstrip("/")
        if slug:
            video_id = slug.split("/", 1)[0]
    elif "/watch" == path:
        candidates = query.get("v") or []
        if candidates:
            video_id = candidates[0]
    elif "/shorts/" in path:
        _, _, slug = path.partition("/shorts/")
        if slug:
            video_id = slug.split("/", 1)[0]
    elif "/embed/" in path:
        _, _, slug = path.partition("/embed/")
        if slug:
            video_id = slug.split("/", 1)[0]
    elif path.startswith("/live/"):
        video_id = path.split("/", 2)[2] if path.count("/") >= 2 else None

    if not video_id:
        # Attempt a regex extraction in case the URL contains extra components.
        match = YOUTUBE_ID_PATTERN.search(url)
        if match:
            video_id = match.group(1)

    if not video_id:
        return None

    video_id = video_id.strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
        return None

    return f"https://www.youtube.com/watch?v={video_id}"


def _fetch_html(url: str) -> Optional[str]:
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


def _extract_candidates_from_soup(soup: BeautifulSoup, base_url: str | None) -> list[str]:
    candidates: list[str] = []

    def add(url: str | None) -> None:
        if not url:
            return
        candidate_url = url.strip()
        if not candidate_url:
            return
        lowered = candidate_url.lower()
        if candidate_url.startswith("//"):
            candidate_url = f"https:{candidate_url}"
        elif lowered.startswith("youtu.be/") or lowered.startswith("www.youtu.be/"):
            candidate_url = f"https://{candidate_url}"
        elif any(
            lowered.startswith(prefix)
            for prefix in (
                "youtube.com/",
                "www.youtube.com/",
                "m.youtube.com/",
                "music.youtube.com/",
                "youtube-nocookie.com/",
                "www.youtube-nocookie.com/",
            )
        ):
            candidate_url = f"https://{candidate_url}"
        elif base_url:
            candidate_url = urljoin(base_url, candidate_url)

        normalized = normalize_youtube_url(candidate_url)
        if normalized and normalized not in candidates:
            candidates.append(normalized)

    for link_tag in soup.find_all("link", attrs={"rel": True, "href": True}):
        rel_values = link_tag.get("rel")
        if isinstance(rel_values, (list, tuple)):
            rels = {value.lower() for value in rel_values if isinstance(value, str)}
        else:
            rels = {str(rel_values).lower()}
        if rels & {"canonical", "alternate"}:
            add(link_tag.get("href"))

    for meta_name in ("og:video", "og:video:url", "og:video:secure_url", "twitter:player"):
        tag = soup.find("meta", attrs={"property": meta_name}) or soup.find(
            "meta", attrs={"name": meta_name}
        )
        if tag:
            add(tag.get("content"))

    for iframe in soup.find_all("iframe"):
        add(iframe.get("src"))

    for anchor in soup.find_all("a", href=True):
        add(anchor.get("href"))

    html_text = soup.decode(formatter="html")
    for match in YOUTUBE_ID_PATTERN.finditer(html_text):
        slug = match.group(0)
        add(slug)

    return candidates


def _extract_candidates(html: str, base_url: str | None) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    return _extract_candidates_from_soup(soup, base_url)


def _episode_candidates(limit: Optional[int]) -> Iterable[EpisodeCandidate]:
    sql = """
        SELECT e.id, e.title, e.show_notes_url
        FROM episode e
        WHERE e.youtube_url IS NULL
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
        yield EpisodeCandidate(*row)


def _find_youtube_url(episode: EpisodeCandidate) -> Optional[str]:
    if episode.show_notes_url:
        direct = normalize_youtube_url(episode.show_notes_url)
        if direct:
            return direct

        html = _fetch_html(episode.show_notes_url)
        if not html:
            return None

        candidates = _extract_candidates(html, episode.show_notes_url)
        if candidates:
            return candidates[0]

    return None


def discover_youtube_urls(limit: Optional[int] = 100) -> int:
    """Populate missing ``episode.youtube_url`` entries using heuristics."""

    updated = 0
    for episode in _episode_candidates(limit):
        candidate = _find_youtube_url(episode)
        if not candidate:
            logger.info(
                "No YouTube match for episode %s — %s", episode.id, episode.title
            )
            continue

        with db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE episode SET youtube_url = %s WHERE id = %s", (candidate, episode.id)
                )
        logger.info(
            "Matched YouTube video for episode %s — %s: %s",
            episode.id,
            episode.title,
            candidate,
        )
        updated += 1

    return updated


__all__ = [
    "discover_youtube_urls",
    "normalize_youtube_url",
]
