from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

try:  # pragma: no cover - executed only when optional dependency missing
    import jinja2  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency guard
    jinja2 = None


router = APIRouter(default_response_class=HTMLResponse)
TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"
REPO_URL = "https://github.com/mbonden/podcast-plow"
DISCLAIMER_TEXT = "Content is for educational purposes only and does not constitute medical advice."


class _MissingTemplates:
    """Fallback object used when Jinja2 is not installed."""

    directory = TEMPLATE_DIR
    env = None

    def TemplateResponse(self, template_name: str, context: dict[str, Any]):  # pragma: no cover - defensive
        raise HTTPException(
            status_code=500,
            detail="HTML templates are unavailable because the optional 'jinja2' dependency is not installed.",
        )


if jinja2 is None:  # pragma: no cover - executed in minimal environments
    templates: Any = _MissingTemplates()
else:
    templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
    templates.env.globals.update(
        repo_url=REPO_URL,
        site_disclaimer=DISCLAIMER_TEXT,
    )


def _db_conn():
    from .. import app as app_module

    return app_module.db_conn()


def _serialize_episode(row: Iterable[Any]) -> dict[str, Any]:
    episode_id, title, published_at, tl_dr, narrative, claim_count = row
    return {
        "id": int(episode_id),
        "title": title,
        "published_at": published_at,
        "tl_dr": tl_dr,
        "narrative": narrative,
        "claim_count": int(claim_count or 0),
    }


def _load_recent_episodes(limit: int = 12) -> list[dict[str, Any]]:
    with _db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT e.id, e.title, e.published_at,
                   summary.tl_dr, summary.narrative,
                   COALESCE(claim_totals.count, 0) AS claim_count
            FROM episode e
            LEFT JOIN LATERAL (
                SELECT tl_dr, narrative
                FROM episode_summary
                WHERE episode_id = e.id
                ORDER BY created_at DESC
                LIMIT 1
            ) AS summary ON TRUE
            LEFT JOIN LATERAL (
                SELECT COUNT(*) AS count
                FROM claim c
                WHERE c.episode_id = e.id
            ) AS claim_totals ON TRUE
            ORDER BY e.published_at DESC NULLS LAST, e.id DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
    return [_serialize_episode(row) for row in rows]


def _load_episode_detail(episode_id: int) -> dict[str, Any]:
    with _db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT e.id, e.title, e.published_at, e.show_notes_url, e.youtube_url, e.audio_url,
                   summary.tl_dr, summary.narrative
            FROM episode e
            LEFT JOIN LATERAL (
                SELECT tl_dr, narrative
                FROM episode_summary
                WHERE episode_id = e.id
                ORDER BY created_at DESC
                LIMIT 1
            ) AS summary ON TRUE
            WHERE e.id = %s
            """,
            (episode_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Episode not found")

        episode = {
            "id": int(row[0]),
            "title": row[1],
            "published_at": row[2],
            "show_notes_url": row[3],
            "youtube_url": row[4],
            "audio_url": row[5],
            "tl_dr": row[6],
            "narrative": row[7],
        }

        cur.execute(
            """
            WITH latest_grade AS (
                SELECT DISTINCT ON (claim_id)
                    claim_id, grade, rationale
                FROM claim_grade
                ORDER BY claim_id, created_at DESC
            )
            SELECT c.id, c.normalized_text, c.topic, c.domain, c.risk_level,
                   latest_grade.grade, latest_grade.rationale
            FROM claim c
            LEFT JOIN latest_grade ON latest_grade.claim_id = c.id
            WHERE c.episode_id = %s
            ORDER BY c.start_ms NULLS LAST, c.id
            """,
            (episode_id,),
        )
        claim_rows = cur.fetchall()

        claim_ids = [int(row[0]) for row in claim_rows]
        evidence_map: dict[int, list[dict[str, Any]]] = defaultdict(list)
        if claim_ids:
            cur.execute(
                """
                SELECT ce.claim_id, es.title, es.url, es.type, es.journal, es.year,
                       ce.stance, ce.notes
                FROM claim_evidence ce
                JOIN evidence_source es ON es.id = ce.evidence_id
                WHERE ce.claim_id = ANY(%s)
                ORDER BY ce.claim_id, es.year DESC NULLS LAST, es.title
                """,
                (claim_ids,),
            )
            for claim_id, title, url, source_type, journal, year, stance, notes in cur.fetchall():
                evidence_map[int(claim_id)].append(
                    {
                        "title": title,
                        "url": url,
                        "type": source_type,
                        "journal": journal,
                        "year": year,
                        "stance": stance,
                        "notes": notes,
                    }
                )

    claims = []
    for claim_id, normalized_text, topic, domain, risk_level, grade, rationale in claim_rows:
        claims.append(
            {
                "id": int(claim_id),
                "statement": normalized_text,
                "topic": topic,
                "domain": domain,
                "risk_level": risk_level,
                "grade": grade,
                "rationale": rationale,
                "evidence": evidence_map.get(int(claim_id), []),
            }
        )

    episode["claims"] = claims
    return episode


@router.get("/", include_in_schema=False)
def homepage(request: Request) -> HTMLResponse:
    episodes = _load_recent_episodes()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "episodes": episodes,
        },
    )


@router.get("/episodes/{episode_id}/review", include_in_schema=False)
def episode_detail(request: Request, episode_id: int) -> HTMLResponse:
    episode = _load_episode_detail(episode_id)
    return templates.TemplateResponse(
        "episode.html",
        {
            "request": request,
            "episode": episode,
        },
    )
