from __future__ import annotations

import calendar
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

import feedparser
from psycopg import Connection

from core.db import db_connection

logger = logging.getLogger(__name__)


def load_feed_urls(path: Path) -> List[str]:
    urls: List[str] = []
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        urls.append(line)
    return urls


def parse_duration(value: object) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if value < 0:
            return None
        return int(value)
    if isinstance(value, str):
        if value.isdigit():
            return int(value)
        parts = value.split(":")
        try:
            parts_int = [int(p) for p in parts]
        except ValueError:
            return None
        if len(parts_int) == 3:
            hours, minutes, seconds = parts_int
        elif len(parts_int) == 2:
            hours = 0
            minutes, seconds = parts_int
        elif len(parts_int) == 1:
            hours = 0
            minutes = 0
            seconds = parts_int[0]
        else:
            return None
        return hours * 3600 + minutes * 60 + seconds
    return None


def parse_datetime(entry: feedparser.FeedParserDict) -> Optional[datetime]:
    for key in ("published_parsed", "updated_parsed"):
        struct_time = entry.get(key)
        if struct_time:
            try:
                return datetime.fromtimestamp(calendar.timegm(struct_time), tz=timezone.utc)
            except (OverflowError, ValueError):
                continue
    return None


def extract_audio_url(entry: feedparser.FeedParserDict) -> Optional[str]:
    for enclosure in entry.get("enclosures", []) or []:
        href = enclosure.get("href")
        type_hint = (enclosure.get("type") or "").lower()
        if href and "audio" in type_hint:
            return href
    for link in entry.get("links", []) or []:
        href = link.get("href")
        type_hint = (link.get("type") or "").lower()
        rel = (link.get("rel") or "").lower()
        if href and ("audio" in type_hint or rel == "enclosure"):
            return href
    return None


def extract_description(entry: feedparser.FeedParserDict) -> Optional[str]:
    if entry.get("summary"):
        return entry["summary"]
    contents = entry.get("content")
    if contents:
        text_parts = []
        for content in contents:
            value = content.get("value")
            if value:
                text_parts.append(value)
        if text_parts:
            return "\n\n".join(text_parts)
    return None


def get_guid(entry: feedparser.FeedParserDict) -> Optional[str]:
    for key in ("id", "guid"):
        guid = entry.get(key)
        if guid:
            return str(guid)
    return None


def upsert_podcast(conn: Connection, rss_url: str, feed: feedparser.FeedParserDict) -> int:
    title = feed.get("title") or rss_url
    description = feed.get("subtitle") or feed.get("description")
    official_site = feed.get("link")
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM podcast WHERE rss_url = %s", (rss_url,))
        row = cur.fetchone()
        if row:
            podcast_id = row[0]
            cur.execute(
                """
                UPDATE podcast
                SET title = COALESCE(%s, title),
                    description = COALESCE(%s, description),
                    official_site = COALESCE(%s, official_site)
                WHERE id = %s
                """,
                (title, description, official_site, podcast_id),
            )
        else:
            cur.execute(
                """
                INSERT INTO podcast (title, rss_url, description, official_site)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (title, rss_url, description, official_site),
            )
            podcast_id = cur.fetchone()[0]
    return podcast_id


def upsert_episode(conn: Connection, podcast_id: int, entry: feedparser.FeedParserDict) -> bool:
    guid = get_guid(entry)
    show_notes_url = entry.get("link")
    description = extract_description(entry)
    published_at = parse_datetime(entry)
    duration_sec = parse_duration(entry.get("itunes_duration"))
    audio_url = extract_audio_url(entry)
    spotify_id = None
    for link in entry.get("links", []) or []:
        href = link.get("href")
        if href and "open.spotify.com" in href:
            spotify_id = href.rsplit("/", 1)[-1]
            break
    title = entry.get("title") or "Untitled Episode"

    with conn.cursor() as cur:
        if guid:
            cur.execute("SELECT id FROM episode WHERE rss_guid = %s", (guid,))
            row = cur.fetchone()
        else:
            cur.execute("SELECT id FROM episode WHERE show_notes_url = %s", (show_notes_url,))
            row = cur.fetchone()
        if row:
            episode_id = row[0]
            cur.execute(
                """
                UPDATE episode
                SET title = %s,
                    description = COALESCE(%s, description),
                    published_at = COALESCE(%s, published_at),
                    duration_sec = COALESCE(%s, duration_sec),
                    spotify_id = COALESCE(%s, spotify_id),
                    rss_guid = COALESCE(%s, rss_guid),
                    audio_url = COALESCE(%s, audio_url),
                    show_notes_url = COALESCE(%s, show_notes_url)
                WHERE id = %s
                """,
                (
                    title,
                    description,
                    published_at,
                    duration_sec,
                    spotify_id,
                    guid,
                    audio_url,
                    show_notes_url,
                    episode_id,
                ),
            )
            created = False
        else:
            cur.execute(
                """
                INSERT INTO episode (
                    podcast_id, title, description, published_at, duration_sec,
                    spotify_id, rss_guid, audio_url, show_notes_url
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    podcast_id,
                    title,
                    description,
                    published_at,
                    duration_sec,
                    spotify_id,
                    guid,
                    audio_url,
                    show_notes_url,
                ),
            )
            episode_id = cur.fetchone()[0]
            created = True
    if created:
        logger.info("Inserted episode %s (%s)", episode_id, title)
    return created


def discover_from_urls(feed_urls: Iterable[str]) -> int:
    inserted = 0
    for url in feed_urls:
        logger.info("Fetching feed %s", url)
        feed = feedparser.parse(url)
        if getattr(feed, "bozo", False):
            exc = getattr(feed, "bozo_exception", None)
            if exc:
                logger.warning("Failed to parse feed %s: %s", url, exc)
            else:
                logger.warning("Failed to parse feed %s", url)
            continue
        with db_connection() as conn:
            podcast_id = upsert_podcast(conn, url, feed.feed)
            for entry in feed.entries:
                if upsert_episode(conn, podcast_id, entry):
                    inserted += 1
    return inserted


def discover_from_file(path: Path) -> int:
    urls = load_feed_urls(path)
    if not urls:
        logger.warning("No feeds defined in %s", path)
        return 0
    return discover_from_urls(urls)
