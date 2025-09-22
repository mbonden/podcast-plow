from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import os
import psycopg

app = FastAPI(title="podcast-plow API", version="0.1.0")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/podcast_plow")

def db_conn():
    return psycopg.connect(DATABASE_URL, autocommit=True)

class EpisodeSummary(BaseModel):
    episode_id: int
    title: str
    tl_dr: str | None = None
    narrative: str | None = None

@app.get("/healthz")
def healthz():
    return {"ok": True}

@app.get("/episodes/{episode_id}")
def get_episode(episode_id: int):
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, title FROM episode WHERE id = %s", (episode_id,))
        row = cur.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "episode not found"})
        episode = {"id": row[0], "title": row[1]}
        cur.execute("SELECT tl_dr, narrative FROM episode_summary WHERE episode_id = %s ORDER BY created_at DESC LIMIT 1", (episode_id,))
        s = cur.fetchone()
        if s:
            episode["summary"] = {"tl_dr": s[0], "narrative": s[1]}
        else:
            episode["summary"] = None
        # claims (latest grade joined)
        cur.execute("""
            WITH latest_grade AS (
                SELECT DISTINCT ON (claim_id) claim_id, grade, rationale, created_at
                FROM claim_grade
                ORDER BY claim_id, created_at DESC
            )
            SELECT c.id, c.raw_text, c.normalized_text, c.topic, c.domain, lg.grade, lg.rationale
            FROM claim c
            LEFT JOIN latest_grade lg ON lg.claim_id = c.id
            WHERE c.episode_id = %s
            ORDER BY c.id
        """, (episode_id,))
        claims = []
        for r in cur.fetchall():
            claims.append({
                "id": r[0],
                "raw_text": r[1],
                "normalized_text": r[2],
                "topic": r[3],
                "domain": r[4],
                "grade": r[5],
                "grade_rationale": r[6],
            })
        episode["claims"] = claims
        return episode

@app.get("/topics/{topic}/claims")
def get_topic_claims(topic: str):
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            WITH latest_grade AS (
                SELECT DISTINCT ON (claim_id) claim_id, grade, rationale, created_at
                FROM claim_grade
                ORDER BY claim_id, created_at DESC
            )
            SELECT c.id, e.id as episode_id, e.title, c.raw_text, c.normalized_text, c.domain, lg.grade, lg.rationale
            FROM claim c
            JOIN episode e ON e.id = c.episode_id
            LEFT JOIN latest_grade lg ON lg.claim_id = c.id
            WHERE c.topic = %s
            ORDER BY e.published_at DESC NULLS LAST, e.id DESC
        """, (topic,))
        items = []
        for r in cur.fetchall():
            items.append({
                "claim_id": r[0],
                "episode_id": r[1],
                "episode_title": r[2],
                "raw_text": r[3],
                "normalized_text": r[4],
                "domain": r[5],
                "grade": r[6],
                "grade_rationale": r[7],
            })
        return {"topic": topic, "claims": items}

@app.get("/claims/{claim_id}")
def get_claim(claim_id: int):
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            WITH latest_grade AS (
                SELECT DISTINCT ON (claim_id) claim_id, grade, rationale, rubric_version, created_at
                FROM claim_grade
                ORDER BY claim_id, created_at DESC
            )
            SELECT c.id, e.title, c.topic, c.domain, c.risk_level, c.raw_text, c.normalized_text,
                   lg.grade, lg.rationale, lg.rubric_version, lg.created_at
            FROM claim c
            JOIN episode e ON e.id = c.episode_id
            LEFT JOIN latest_grade lg ON lg.claim_id = c.id
            WHERE c.id = %s
        """, (claim_id,))
        r = cur.fetchone()
        if not r:
            return JSONResponse(status_code=404, content={"error": "claim not found"})
        # evidence
        cur.execute("""
            SELECT es.id, es.title, es.year, es.type, es.journal, es.doi, es.pubmed_id, es.url, ce.stance
            FROM claim_evidence ce
            JOIN evidence_source es ON es.id = ce.evidence_id
            WHERE ce.claim_id = %s
            ORDER BY es.year DESC NULLS LAST
        """, (claim_id,))
        evidence = []
        for e_row in cur.fetchall():
            evidence.append({
                "id": e_row[0], "title": e_row[1], "year": e_row[2], "type": e_row[3],
                "journal": e_row[4], "doi": e_row[5], "pubmed_id": e_row[6],
                "url": e_row[7], "stance": e_row[8]
            })
        return {
            "claim_id": r[0],
            "episode_title": r[1],
            "topic": r[2],
            "domain": r[3],
            "risk_level": r[4],
            "raw_text": r[5],
            "normalized_text": r[6],
            "grade": r[7],
            "grade_rationale": r[8],
            "rubric_version": r[9],
            "graded_at": r[10],
            "evidence": evidence
        }

@app.get("/search")
def search(q: str = Query(..., min_length=2)):
    # Simple naive search until semantic search is added
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, title FROM episode WHERE title ILIKE %s ORDER BY published_at DESC NULLS LAST LIMIT 20", (f"%{q}%",))
        episodes = [{"id": r[0], "title": r[1]} for r in cur.fetchall()]
        cur.execute("SELECT id, raw_text, topic FROM claim WHERE raw_text ILIKE %s ORDER BY id DESC LIMIT 20", (f"%{q}%",))
        claims = [{"id": r[0], "raw_text": r[1], "topic": r[2]} for r in cur.fetchall()]
        return {"q": q, "episodes": episodes, "claims": claims}
