
from fastapi import FastAPI, Query

from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from pydantic import BaseModel
from starlette.exceptions import HTTPException as StarletteHTTPException

from core.db import db_connection  # IMPORTANT: import from /app root

try:  # pragma: no cover - exercised in Docker container
    from server.api.jobs import router as jobs_router
except ModuleNotFoundError as exc:  # pragma: no cover - exercised locally
    if exc.name not in {"server", "server.api", "server.api.jobs"}:
        raise
    from api.jobs import router as jobs_router

try:  # pragma: no cover - exercised in Docker container
    from server.ui import router as ui_router, templates as ui_templates
except ModuleNotFoundError as exc:  # pragma: no cover - exercised locally
    if exc.name not in {"server", "server.ui"}:
        raise
    from ui import router as ui_router, templates as ui_templates



app = FastAPI(title="podcast-plow API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(jobs_router)
app.include_router(ui_router, include_in_schema=False)


ADMIN_JOBS_PAGE = """
<!DOCTYPE html>
<html lang=\"en\">
  <head>
    <meta charset=\"utf-8\" />
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
    <title>Jobs console · podcast-plow</title>
    <style>
      :root {
        color-scheme: light dark;
        --bg: #0f172a;
        --surface: rgba(15, 23, 42, 0.75);
        --panel: rgba(15, 23, 42, 0.6);
        --text: #e2e8f0;
        --muted: #94a3b8;
        --accent: #38bdf8;
        --danger: #f87171;
        --success: #34d399;
        font-family: "Inter", system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }

      body {
        margin: 0;
        padding: 0;
        background: radial-gradient(circle at top, rgba(56, 189, 248, 0.15), transparent 55%),
          linear-gradient(135deg, rgba(79, 70, 229, 0.2), rgba(15, 23, 42, 0.95));
        min-height: 100vh;
        color: var(--text);
      }

      a {
        color: inherit;
      }

      h1,
      h2,
      h3 {
        font-weight: 600;
        letter-spacing: -0.01em;
      }

      main {
        max-width: 1200px;
        margin: 0 auto;
        padding: 32px 24px 48px;
        display: grid;
        gap: 24px;
      }

      .panel {
        background: var(--panel);
        backdrop-filter: blur(16px);
        border: 1px solid rgba(148, 163, 184, 0.2);
        border-radius: 16px;
        padding: 24px;
        box-shadow: 0 18px 30px rgba(15, 23, 42, 0.35);
      }

      header {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        align-items: baseline;
        justify-content: space-between;
      }

      header h1 {
        margin: 0;
        font-size: 1.75rem;
      }

      .filters {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        align-items: center;
      }

      select,
      input,
      button,
      textarea {
        border-radius: 999px;
        border: 1px solid rgba(148, 163, 184, 0.2);
        background: rgba(15, 23, 42, 0.65);
        color: var(--text);
        padding: 8px 14px;
        font: inherit;
        transition: border-color 160ms ease, box-shadow 160ms ease;
      }

      textarea {
        border-radius: 16px;
        min-height: 80px;
        resize: vertical;
      }

      button {
        cursor: pointer;
        border-radius: 12px;
        background: linear-gradient(135deg, rgba(59, 130, 246, 0.9), rgba(14, 165, 233, 0.9));
        border: none;
        font-weight: 600;
        padding: 10px 18px;
        color: #0f172a;
        box-shadow: 0 10px 20px rgba(14, 165, 233, 0.35);
      }

      button:disabled {
        opacity: 0.6;
        cursor: progress;
        box-shadow: none;
      }

      label {
        font-size: 0.875rem;
        color: var(--muted);
      }

      table {
        width: 100%;
        border-collapse: collapse;
        margin-top: 18px;
      }

      th,
      td {
        padding: 12px 14px;
        text-align: left;
      }

      thead th {
        font-size: 0.75rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: var(--muted);
        border-bottom: 1px solid rgba(148, 163, 184, 0.2);
      }

      tbody tr {
        transition: background 140ms ease;
        cursor: pointer;
      }

      tbody tr:hover,
      tbody tr.active {
        background: rgba(59, 130, 246, 0.1);
      }

      tbody td {
        border-bottom: 1px solid rgba(148, 163, 184, 0.08);
      }

      .status-pill {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        border-radius: 999px;
        padding: 4px 10px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }

      .status-queued {
        background: rgba(250, 204, 21, 0.15);
        color: #facc15;
      }

      .status-running {
        background: rgba(56, 189, 248, 0.2);
        color: #38bdf8;
      }

      .status-done {
        background: rgba(52, 211, 153, 0.2);
        color: var(--success);
      }

      .status-failed {
        background: rgba(248, 113, 113, 0.2);
        color: var(--danger);
      }

      .flash {
        border-radius: 12px;
        padding: 12px 16px;
        margin-top: 12px;
        font-size: 0.95rem;
        display: none;
      }

      .flash.show {
        display: block;
      }

      .flash.success {
        background: rgba(52, 211, 153, 0.12);
        border: 1px solid rgba(16, 185, 129, 0.4);
        color: var(--success);
      }

      .flash.error {
        background: rgba(248, 113, 113, 0.12);
        border: 1px solid rgba(248, 113, 113, 0.35);
        color: var(--danger);
      }

      .grid {
        display: grid;
        gap: 24px;
      }

      @media (min-width: 900px) {
        .grid.two {
          grid-template-columns: 1.4fr 1fr;
        }
      }

      .job-detail {
        white-space: pre-wrap;
        background: rgba(15, 23, 42, 0.55);
        border-radius: 12px;
        padding: 16px;
        font-family: "SFMono-Regular", Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        font-size: 0.85rem;
        overflow-x: auto;
        border: 1px solid rgba(148, 163, 184, 0.15);
      }

      .widgets {
        display: grid;
        gap: 20px;
      }

      .widgets form {
        display: grid;
        gap: 12px;
        background: rgba(148, 163, 184, 0.05);
        border-radius: 16px;
        padding: 18px 20px;
      }

      .form-row {
        display: grid;
        gap: 6px;
      }

      .subtle {
        color: var(--muted);
        font-size: 0.85rem;
      }

      .actions {
        display: flex;
        gap: 12px;
        align-items: center;
      }
    </style>
  </head>
  <body>
    <main>
      <section class=\"panel\">
        <header>
          <h1>Background jobs</h1>
          <div class=\"filters\">
            <label>
              Status
              <select id=\"status-filter\">
                <option value=\"\">All</option>
                <option value=\"queued\">Queued</option>
                <option value=\"running\">Running</option>
                <option value=\"done\">Done</option>
                <option value=\"failed\">Failed</option>
              </select>
            </label>
            <label>
              Type
              <input id=\"type-filter\" placeholder=\"summarize\" />
            </label>
            <div class=\"actions\">
              <button id=\"refresh-btn\" type=\"button\">Refresh</button>
              <span class=\"subtle\" id=\"last-refresh\">&nbsp;</span>
            </div>
          </div>
        </header>

        <p class=\"subtle\" id=\"job-summary\">Loading jobs…</p>

        <div class=\"grid two\">
          <div>
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Priority</th>
                  <th>Updated</th>
                </tr>
              </thead>
              <tbody id=\"jobs-table-body\"></tbody>
            </table>
          </div>
          <div>
            <h2>Job detail</h2>
            <div id=\"job-detail\" class=\"job-detail\">Select a job to inspect payload and result.</div>
          </div>
        </div>
      </section>

      <section class=\"panel\">
        <h2>Enqueue jobs</h2>
        <p class=\"subtle\">Run worker tasks against selected episodes. Separate episode ids with commas.</p>
        <div class=\"widgets\">
          <form data-job-type=\"summarize\">
            <h3>Summaries</h3>
            <div class=\"form-row\">
              <label for=\"summarize-ids\">Episode IDs</label>
              <input id=\"summarize-ids\" name=\"episode_ids\" placeholder=\"101,102\" autocomplete=\"off\" />
            </div>
            <div class=\"form-row\">
              <label>
                <input type=\"checkbox\" name=\"refresh\" value=\"true\" /> Refresh transcript chunks first
              </label>
            </div>
            <button type=\"submit\">Queue summaries</button>
            <div class=\"flash\"></div>
          </form>

          <form data-job-type=\"extract_claims\">
            <h3>Extract claims</h3>
            <div class=\"form-row\">
              <label for=\"extract-ids\">Episode IDs</label>
              <input id=\"extract-ids\" name=\"episode_ids\" placeholder=\"201,202\" autocomplete=\"off\" />
            </div>
            <div class=\"form-row\">
              <label>
                <input type=\"checkbox\" name=\"refresh\" value=\"true\" /> Refresh transcript chunks first
              </label>
            </div>
            <button type=\"submit\">Queue claim extraction</button>
            <div class=\"flash\"></div>
          </form>

          <form data-job-type=\"link_evidence\">
            <h3>Link evidence</h3>
            <div class=\"form-row\">
              <label for=\"link-ids\">Episode IDs</label>
              <input id=\"link-ids\" name=\"episode_ids\" placeholder=\"301,302\" autocomplete=\"off\" />
            </div>
            <button type=\"submit\">Queue evidence linking</button>
            <div class=\"flash\"></div>
          </form>

          <form data-job-type=\"auto_grade\">
            <h3>Auto-grade</h3>
            <div class=\"form-row\">
              <label for=\"grade-ids\">Episode IDs</label>
              <input id=\"grade-ids\" name=\"episode_ids\" placeholder=\"401,402\" autocomplete=\"off\" />
            </div>
            <button type=\"submit\">Queue auto-grading</button>
            <div class=\"flash\"></div>
          </form>
        </div>
      </section>
    </main>

    <script>
      const jobsTable = document.querySelector('#jobs-table-body');
      const jobDetail = document.querySelector('#job-detail');
      const statusFilter = document.querySelector('#status-filter');
      const typeFilter = document.querySelector('#type-filter');
      const jobSummary = document.querySelector('#job-summary');
      const lastRefresh = document.querySelector('#last-refresh');
      const refreshBtn = document.querySelector('#refresh-btn');
      let activeJobId = null;
      let refreshHandle = null;
      let loading = false;

      function formatTimestamp(value) {
        if (!value) return '—';
        try {
          const date = new Date(value);
          if (Number.isNaN(date.getTime())) {
            return value;
          }
          return date.toLocaleString();
        } catch (err) {
          return value;
        }
      }

      function setJobDetail(job) {
        if (!job) {
          jobDetail.textContent = 'Select a job to inspect payload and result.';
          return;
        }
        const lines = [
          `Job #${job.id} (${job.job_type})`,
          `Status: ${job.status}`,
          job.priority != null ? `Priority: ${job.priority}` : null,
          job.created_at ? `Created: ${formatTimestamp(job.created_at)}` : null,
          job.updated_at ? `Updated: ${formatTimestamp(job.updated_at)}` : null,
          job.error ? `Error: ${job.error}` : null,
          '',
          'Payload:',
          JSON.stringify(job.payload, null, 2),
        ].filter(Boolean);
        if (job.result !== undefined && job.result !== null && job.result !== '') {
          lines.push('', 'Result:', typeof job.result === 'string' ? job.result : JSON.stringify(job.result, null, 2));
        }
        jobDetail.textContent = lines.join('\n');
      }

      async function fetchJSON(url, options) {
        const response = await fetch(url, options);
        if (!response.ok) {
          const text = await response.text();
          throw new Error(text || response.statusText);
        }
        return response.json();
      }

      function createStatusPill(status) {
        const span = document.createElement('span');
        const normalized = (status || '').toLowerCase();
        span.className = `status-pill status-${normalized}`;
        span.textContent = status;
        return span;
      }

      function renderJobs(jobs) {
        jobsTable.replaceChildren();
        if (!jobs.length) {
          jobSummary.textContent = 'No jobs match the current filters.';
          return;
        }
        jobSummary.textContent = `${jobs.length} job${jobs.length === 1 ? '' : 's'} shown.`;
        for (const job of jobs) {
          const tr = document.createElement('tr');
          tr.dataset.jobId = job.id;
          if (job.id === activeJobId) {
            tr.classList.add('active');
          }
          const idCell = document.createElement('td');
          idCell.textContent = job.id;
          tr.appendChild(idCell);

          const typeCell = document.createElement('td');
          typeCell.textContent = job.job_type;
          tr.appendChild(typeCell);

          const statusCell = document.createElement('td');
          statusCell.appendChild(createStatusPill(job.status));
          tr.appendChild(statusCell);

          const priorityCell = document.createElement('td');
          priorityCell.textContent = job.priority ?? '0';
          tr.appendChild(priorityCell);

          const updatedCell = document.createElement('td');
          updatedCell.textContent = formatTimestamp(job.updated_at || job.created_at);
          tr.appendChild(updatedCell);

          tr.addEventListener('click', async () => {
            if (activeJobId === job.id) {
              return;
            }
            activeJobId = job.id;
            document.querySelectorAll('#jobs-table-body tr').forEach((row) => row.classList.remove('active'));
            tr.classList.add('active');
            try {
              const detail = await fetchJSON(`/jobs/${job.id}`);
              setJobDetail(detail);
            } catch (err) {
              console.error(err);
              jobDetail.textContent = `Failed to load job ${job.id}: ${err.message}`;
            }
          });

          jobsTable.appendChild(tr);
        }
      }

      async function refreshJobs() {
        if (loading) {
          return;
        }
        loading = true;
        jobSummary.textContent = 'Loading jobs…';
        try {
          const params = new URLSearchParams({ limit: '50' });
          if (statusFilter.value) {
            params.set('status', statusFilter.value);
          }
          const typeValue = typeFilter.value.trim();
          if (typeValue) {
            params.set('type', typeValue);
          }
          const data = await fetchJSON(`/jobs?${params.toString()}`);
          renderJobs(data.jobs || []);
          lastRefresh.textContent = `Last refresh ${new Date().toLocaleTimeString()}`;
          if (activeJobId) {
            const matching = (data.jobs || []).find((job) => job.id === activeJobId);
            if (matching) {
              try {
                const detail = await fetchJSON(`/jobs/${activeJobId}`);
                setJobDetail(detail);
              } catch (err) {
                console.error(err);
              }
            }
          }
        } catch (err) {
          console.error(err);
          jobSummary.textContent = `Failed to load jobs: ${err.message}`;
        } finally {
          loading = false;
        }
      }

      function parseEpisodeIds(raw) {
        if (!raw) return [];
        return raw
          .split(/[^0-9]+/)
          .map((value) => value.trim())
          .filter(Boolean)
          .map((value) => Number.parseInt(value, 10))
          .filter((value) => Number.isInteger(value) && value > 0);
      }

      function showFlash(container, message, kind) {
        const flash = container.querySelector('.flash');
        flash.textContent = message;
        flash.className = `flash show ${kind}`;
        setTimeout(() => {
          flash.className = 'flash';
          flash.textContent = '';
        }, 6000);
      }

      async function submitJobForm(form) {
        const jobType = form.dataset.jobType;
        const idsInput = form.querySelector('input[name="episode_ids"]');
        const episodeIds = parseEpisodeIds(idsInput?.value || '');
        if (!episodeIds.length) {
          showFlash(form, 'Provide one or more numeric episode ids.', 'error');
          return;
        }
        const refresh = form.querySelector('input[name="refresh"]')?.checked ?? false;
        let payload;
        if (jobType === 'auto_grade') {
          payload = [{ type: jobType, payload: { episode_ids: episodeIds } }];
        } else if (jobType === 'link_evidence') {
          payload = episodeIds.map((id) => ({ type: jobType, payload: { episode_id: id } }));
        } else {
          payload = episodeIds.map((id) => ({ type: jobType, payload: { episode_id: id, refresh } }));
        }

        const button = form.querySelector('button[type="submit"]');
        button.disabled = true;
        try {
          const body = { jobs: payload };
          const response = await fetchJSON('/jobs', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
          });
          idsInput.value = '';
          if (refresh) {
            const checkbox = form.querySelector('input[name="refresh"]');
            if (checkbox) checkbox.checked = false;
          }
          const accepted = response.accepted?.length || 0;
          const reused = response.reused?.length || 0;
          showFlash(
            form,
            `Queued ${accepted} job${accepted === 1 ? '' : 's'}${
              reused ? ` (${reused} reused)` : ''
            }.`,
            'success'
          );
          refreshJobs();
        } catch (err) {
          console.error(err);
          showFlash(form, err.message || 'Failed to enqueue job(s).', 'error');
        } finally {
          button.disabled = false;
        }
      }

      document.querySelectorAll('form[data-job-type]').forEach((form) => {
        form.addEventListener('submit', (event) => {
          event.preventDefault();
          submitJobForm(form);
        });
      });

      statusFilter.addEventListener('change', refreshJobs);
      typeFilter.addEventListener('change', () => {
        activeJobId = null;
        setJobDetail(null);
        refreshJobs();
      });
      refreshBtn.addEventListener('click', refreshJobs);

      refreshJobs();
      refreshHandle = setInterval(refreshJobs, 5000);

      window.addEventListener('beforeunload', () => {
        if (refreshHandle) {
          clearInterval(refreshHandle);
        }
      });
    </script>
  </body>
</html>
"""


@app.get("/admin/jobs", response_class=HTMLResponse, include_in_schema=False)
def admin_jobs_console() -> HTMLResponse:
    """Serve the lightweight developer jobs console UI."""

    return HTMLResponse(content=ADMIN_JOBS_PAGE)


def db_conn():
    return db_connection()


class EpisodeSummary(BaseModel):
    episode_id: int
    title: str
    tl_dr: str | None = None
    narrative: str | None = None


def _parse_bullet_points(raw: str | None) -> list[str]:
    if not raw:
        return []
    items: list[str] = []
    for line in raw.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        for prefix in ("- ", "* ", "• "):
            if stripped.startswith(prefix):
                stripped = stripped[len(prefix) :].strip()
                break
        items.append(stripped)
    return items

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
            SELECT c.id, c.raw_text, c.normalized_text, c.topic, c.domain,
                   c.risk_level, c.start_ms, c.end_ms, lg.grade, lg.rationale
            FROM claim c
            LEFT JOIN latest_grade lg ON lg.claim_id = c.id
            WHERE c.episode_id = %s
            ORDER BY c.start_ms NULLS LAST, c.id
        """, (episode_id,))
        claims = []
        for r in cur.fetchall():
            claims.append({
                "id": r[0],
                "raw_text": r[1],
                "normalized_text": r[2],
                "topic": r[3],
                "domain": r[4],
                "risk_level": r[5],
                "start_ms": r[6],
                "end_ms": r[7],
                "grade": r[8],
                "grade_rationale": r[9],
            })
        episode["claims"] = claims
        return episode


@app.get("/episodes/{episode_id}/outline")
def get_episode_outline(episode_id: int):
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, title FROM episode WHERE id = %s", (episode_id,))
        row = cur.fetchone()
        if not row:
            return JSONResponse(status_code=404, content={"error": "episode not found"})

        cur.execute(
            """
            SELECT start_ms, end_ms, heading, bullet_points
            FROM episode_outline
            WHERE episode_id = %s
            ORDER BY start_ms NULLS LAST, id
            """,
            (episode_id,),
        )
        outline_rows = cur.fetchall()
        if not outline_rows:
            return JSONResponse(status_code=404, content={"error": "outline not available"})

        outline_items = []
        for start_ms, end_ms, heading, bullet_points in outline_rows:
            item = {
                "start_ms": start_ms,
                "end_ms": end_ms,
                "heading": heading,
            }
            bullets = _parse_bullet_points(bullet_points)
            if bullets:
                item["bullet_points"] = bullets
            outline_items.append(item)

        return {"episode_id": row[0], "title": row[1], "outline": outline_items}

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
            SELECT c.id, e.id as episode_id, e.title, c.raw_text, c.normalized_text,
                   c.domain, c.risk_level, c.start_ms, c.end_ms, lg.grade, lg.rationale
            FROM claim c
            JOIN episode e ON e.id = c.episode_id
            LEFT JOIN latest_grade lg ON lg.claim_id = c.id
            WHERE c.topic = %s
            ORDER BY e.published_at DESC NULLS LAST, e.id DESC, c.start_ms NULLS LAST
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
                "risk_level": r[6],
                "start_ms": r[7],
                "end_ms": r[8],
                "grade": r[9],
                "grade_rationale": r[10],
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
    """Return episodes and claims that match the supplied search query."""

    like_pattern = f"%{q}%"
    with db_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, title, published_at
            FROM episode
            WHERE title ILIKE %s
            ORDER BY published_at DESC NULLS LAST, id DESC
            LIMIT 20
            """,
            (like_pattern,),
        )
        episodes = [
            {
                "id": row[0],
                "title": row[1],
                "published_at": row[2],
            }
            for row in cur.fetchall()
        ]

        cur.execute(
            """
            WITH latest_grade AS (
                SELECT DISTINCT ON (claim_id)
                    claim_id,
                    grade,
                    rationale,
                    rubric_version,
                    created_at
                FROM claim_grade
                ORDER BY claim_id, created_at DESC
            )
            SELECT
                c.id,
                c.raw_text,
                c.normalized_text,
                c.topic,
                c.domain,
                c.risk_level,
                c.episode_id,
                e.title,
                e.published_at,
                lg.grade,
                lg.rationale,
                lg.rubric_version,
                lg.created_at
            FROM claim c
            JOIN episode e ON e.id = c.episode_id
            LEFT JOIN latest_grade lg ON lg.claim_id = c.id
            WHERE
                c.raw_text ILIKE %s
                OR c.normalized_text ILIKE %s
                OR c.topic ILIKE %s
            ORDER BY e.published_at DESC NULLS LAST, c.id DESC
            LIMIT 50
            """,
            (like_pattern, like_pattern, like_pattern),
        )

        claims = []
        for row in cur.fetchall():
            claims.append(
                {
                    "id": row[0],
                    "raw_text": row[1],
                    "normalized_text": row[2],
                    "topic": row[3],
                    "domain": row[4],
                    "risk_level": row[5],
                    "episode_id": row[6],
                    "episode_title": row[7],
                    "episode_published_at": row[8],
                    "grade": row[9],
                    "grade_rationale": row[10],
                    "rubric_version": row[11],
                    "graded_at": row[12],
                }
            )

        return {"q": q, "episodes": episodes, "claims": claims}

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")


