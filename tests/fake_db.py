from __future__ import annotations

"""Helpers to simulate a minimal Postgres-like database for tests.

The production application uses psycopg to talk to Postgres.  Tests in this
repository run without an actual database server, so we provide a very small
in-memory substitute that understands the handful of SQL statements used in
the API handlers and seed data.
"""

import datetime as dt
from dataclasses import dataclass
import re
from typing import Any, Dict, List, Sequence, Tuple


NOW_SENTINEL = object()


def _as_datetime(value: Any) -> dt.datetime:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
    return dt.datetime.now(tz=dt.timezone.utc)


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.strip().lower().split())


def _ilike_match(value: str, pattern: str) -> bool:
    if value is None or pattern is None:
        return False

    regex_parts: List[str] = []
    for ch in pattern:
        if ch == "%":
            regex_parts.append(".*")
        elif ch == "_":
            regex_parts.append(".")
        else:
            regex_parts.append(re.escape(ch))

    regex = "".join(regex_parts)
    return re.fullmatch(regex, value, re.IGNORECASE) is not None


def _split_value_tuples(values_part: str) -> List[str]:
    tuples: List[str] = []
    depth = 0
    start = None
    in_string = False
    escaped = False

    for idx, ch in enumerate(values_part):
        if ch == "'" and not escaped:
            in_string = not in_string
        if ch == "\\" and not escaped:
            escaped = True
        else:
            escaped = False

        if in_string:
            continue

        if ch == "(":
            if depth == 0:
                start = idx
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0 and start is not None:
                tuples.append(values_part[start : idx + 1])
        elif ch == ";" and depth == 0:
            break

    return tuples


def _parse_value(token: str) -> Any:
    token = token.strip()
    if not token:
        return None
    lowered = token.lower()
    if lowered == "null":
        return None
    if lowered == "now()":
        return NOW_SENTINEL
    if token.startswith("'") and token.endswith("'"):
        return token[1:-1].replace("''", "'")
    try:
        return int(token)
    except ValueError:
        return token


def _parse_tuple(tuple_str: str) -> List[Any]:
    assert tuple_str.startswith("(") and tuple_str.endswith(")")
    values: List[str] = []
    current: List[str] = []
    in_string = False
    escaped = False

    for ch in tuple_str[1:-1]:
        if ch == "'" and not escaped:
            in_string = not in_string
            current.append(ch)
        elif ch == "," and not in_string:
            values.append("".join(current).strip())
            current = []
        else:
            current.append(ch)

        if ch == "\\" and not escaped:
            escaped = True
        else:
            escaped = False

    if current:
        values.append("".join(current).strip())

    return [_parse_value(token) for token in values]


def parse_insert(sql: str) -> Tuple[str, List[Dict[str, Any]]]:
    statement = sql.strip().rstrip(";")
    upper = statement.upper()
    if not upper.startswith("INSERT INTO"):
        raise ValueError(f"Unsupported SQL: {sql}")

    before_values, values_part = statement.split("VALUES", 1)
    table_section = before_values[len("INSERT INTO") :].strip()
    table_name, column_part = table_section.split("(", 1)
    table = table_name.strip()
    columns = [col.strip() for col in column_part.rstrip(") ").split(",")]

    rows: List[Dict[str, Any]] = []
    for tuple_str in _split_value_tuples(values_part):
        parsed_values = _parse_tuple(tuple_str)
        row = {col: val for col, val in zip(columns, parsed_values)}
        rows.append(row)

    return table, rows


@dataclass
class FakeCursor:
    db: "FakeDatabase"
    _rows: List[Tuple[Any, ...]] | None = None
    _index: int = 0

    def execute(self, sql: str, params: Sequence[Any] | None = None) -> None:
        self._rows = self.db.execute(sql, params or ())
        self._index = 0

    def fetchone(self) -> Tuple[Any, ...] | None:
        if not self._rows:
            return None
        if self._index >= len(self._rows):
            return None
        row = self._rows[self._index]
        self._index += 1
        return row

    def fetchall(self) -> List[Tuple[Any, ...]]:
        if not self._rows:
            return []
        if self._index == 0:
            self._index = len(self._rows)
            return list(self._rows)
        remaining = self._rows[self._index :]
        self._index = len(self._rows)
        return list(remaining)

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class FakeConnection:
    def __init__(self, db: "FakeDatabase") -> None:
        self._db = db

    def cursor(self) -> FakeCursor:
        return FakeCursor(self._db)

    def close(self) -> None:  # pragma: no cover - compatibility shim
        return None

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class FakeDatabase:
    def __init__(self) -> None:
        self.tables: Dict[str, List[Dict[str, Any]]] = {
            "podcast": [],
            "episode": [],
            "episode_summary": [],
            "claim": [],
            "evidence_source": [],
            "claim_evidence": [],
            "claim_grade": [],
            "transcript": [],
            "transcript_chunk": [],
            "job_queue": [],
            "job": [],
        }
        self._auto_ids: Dict[str, int] = {
            "podcast": 1,
            "episode": 1,
            "episode_summary": 1,
            "claim": 1,
            "evidence_source": 1,
            "claim_grade": 1,
            "transcript": 1,
            "transcript_chunk": 1,
            "job_queue": 1,
            "job": 1,
        }
        self._insert_order = 0
        self._clock = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def execute(self, sql: str, params: Sequence[Any]) -> List[Tuple[Any, ...]]:
        stripped = sql.strip()
        normalized = _normalize_sql(stripped)

        if normalized.startswith("insert into"):
            returning_columns: List[str] | None = None
            match = re.search(r"\breturning\b", stripped, re.IGNORECASE)
            statement = stripped
            if match:
                returning_part = stripped[match.end() :].strip().rstrip(";")
                statement = stripped[: match.start()].strip()
                returning_columns = [col.strip() for col in returning_part.split(",") if col.strip()]
            inserted = self._handle_insert(statement, params)
            if returning_columns:
                rows: List[Tuple[Any, ...]] = []
                for row in inserted:
                    rows.append(tuple(row.get(column) for column in returning_columns))
                return rows
            return []

        if "from episode where id = %s" in normalized:
            episode_id = params[0]
            episode = self._find_one("episode", episode_id)
            return [(episode["id"], episode["title"]) ] if episode else []

        if "from episode_summary" in normalized and "limit 1" in normalized:
            episode_id = params[0]
            summaries = [r for r in self.tables["episode_summary"] if r["episode_id"] == episode_id]
            summaries.sort(key=lambda r: r.get("created_at", 0), reverse=True)
            if summaries:
                top = summaries[0]
                return [(top.get("tl_dr"), top.get("narrative"))]
            return []

        if normalized.startswith("delete from episode_summary where episode_id = %s and created_by in (%s, %s)"):
            episode_id, creator_a, creator_b = params
            allowed = {creator_a, creator_b}
            self.tables["episode_summary"] = [
                row
                for row in self.tables["episode_summary"]
                if not (
                    row.get("episode_id") == episode_id
                    and row.get("created_by") in allowed
                )
            ]
            return []

        if normalized.startswith("with latest_grade as") and "where c.episode_id = %s" in normalized:
            return self._select_episode_claims(params[0])

        if normalized.startswith("with latest_grade as") and "where c.topic = %s" in normalized:
            return self._select_topic_claims(params[0])

        if normalized.startswith("with latest_grade as") and "where c.id = %s" in normalized:
            return self._select_claim_detail(params[0])

        if normalized == "select id, episode_id from claim order by id":
            rows = sorted(self.tables["claim"], key=lambda r: r.get("id", 0))
            return [
                (row.get("id"), row.get("episode_id"))
                for row in rows
            ]

        if normalized.startswith(
            "select id, normalized_text from claim where episode_id = %s order by id"
        ):
            episode_id = params[0]
            rows = [
                (row.get("id"), row.get("normalized_text"))
                for row in sorted(
                    self.tables["claim"],
                    key=lambda r: r.get("id", 0),
                )
                if row.get("episode_id") == episode_id
            ]
            return rows

        if normalized.startswith("select es.id, es.title") and "from claim_evidence" in normalized:
            return self._select_claim_evidence(params[0])

        if normalized.startswith("delete from claim where id = %s"):
            claim_id = params[0]
            self.tables["claim"] = [
                row for row in self.tables["claim"] if row.get("id") != claim_id
            ]
            return []

        if normalized.startswith("update claim set raw_text = %s"):
            (
                raw_text,
                normalized_text,
                topic,
                domain,
                risk_level,
                start_ms,
                end_ms,
                claim_id,
            ) = params
            row = self._find_one("claim", claim_id)
            if not row:
                return []
            row.update(
                {
                    "raw_text": raw_text,
                    "normalized_text": normalized_text,
                    "topic": topic,
                    "domain": domain,
                    "risk_level": risk_level,
                    "start_ms": start_ms,
                    "end_ms": end_ms,
                }
            )
            row["updated_at"] = self._tick()
            return []

        if normalized.startswith("select id, title from episode where title ilike %s"):
            pattern = params[0]
            matches = [
                episode
                for episode in self.tables["episode"]
                if _ilike_match(episode.get("title", ""), pattern)
            ]

            matches.sort(
                key=lambda episode: (
                    0 if episode.get("published_at") is not None else 1,
                    -(episode.get("published_at") or 0),
                    -episode.get("id", 0),
                )
            )

            limited = matches[:20]
            return [(episode.get("id"), episode.get("title")) for episode in limited]

        if normalized.startswith("select id, raw_text, topic from claim where raw_text ilike %s"):
            pattern = params[0]
            matches = [
                claim
                for claim in self.tables["claim"]
                if _ilike_match(claim.get("raw_text", ""), pattern)
            ]

            matches.sort(key=lambda claim: -claim.get("id", 0))
            limited = matches[:20]
            return [
                (
                    claim.get("id"),
                    claim.get("raw_text"),
                    claim.get("topic"),
                )
                for claim in limited
            ]

        if normalized.startswith(
            "select id, episode_id, text, word_count from transcript where episode_id = %s"
        ):
            episode_id = params[0]
            transcripts = [
                row
                for row in self.tables["transcript"]
                if row.get("episode_id") == episode_id and row.get("text") not in (None, "")
            ]
            transcripts.sort(
                key=lambda row: (
                    row.get("word_count") is None,
                    -(row.get("word_count") or 0),
                    -row.get("id", 0),
                )
            )
            if transcripts:
                top = transcripts[0]
                return [
                    (
                        top.get("id"),
                        top.get("episode_id"),
                        top.get("text"),
                        top.get("word_count"),
                    )
                ]
            return []

        if normalized.startswith(
            "select count(*) from transcript_chunk where transcript_id = %s"
        ):
            transcript_id = params[0]
            count = sum(
                1
                for row in self.tables["transcript_chunk"]
                if row.get("transcript_id") == transcript_id
            )
            return [(count,)]

        if normalized.startswith("delete from transcript_chunk where transcript_id = %s"):
            transcript_id = params[0]
            self.tables["transcript_chunk"] = [
                row
                for row in self.tables["transcript_chunk"]
                if row.get("transcript_id") != transcript_id
            ]
            return []

        if normalized.startswith("update transcript_chunk set key_points = %s where id = %s"):
            key_points, chunk_id = params
            row = self._find_one("transcript_chunk", chunk_id)
            if not row:
                return []
            row["key_points"] = key_points
            return []

        if normalized.startswith(
            "select id, transcript_id, chunk_index, token_start, token_end, token_count, text, key_points from transcript_chunk where transcript_id = %s"
        ):
            transcript_id = params[0]
            rows = [
                row
                for row in self.tables["transcript_chunk"]
                if row.get("transcript_id") == transcript_id
            ]
            rows.sort(key=lambda row: row.get("chunk_index", 0))
            return [
                (
                    row.get("id"),
                    row.get("transcript_id"),
                    row.get("chunk_index"),
                    row.get("token_start"),
                    row.get("token_end"),
                    row.get("token_count"),
                    row.get("text"),
                    row.get("key_points"),
                )
                for row in rows
            ]

        if normalized.startswith("select id from evidence_source where pubmed_id = %s"):
            pubmed_id = params[0]
            for row in self.tables["evidence_source"]:
                if row.get("pubmed_id") == pubmed_id:
                    return [(row.get("id"),)]
            return []

        if normalized.startswith("select id from evidence_source where doi = %s"):
            doi = params[0]
            for row in self.tables["evidence_source"]:
                if row.get("doi") == doi:
                    return [(row.get("id"),)]
            return []

        if normalized.startswith(
            "select stance, notes from claim_evidence where claim_id = %s and evidence_id = %s"
        ):
            claim_id, evidence_id = params
            for row in self.tables["claim_evidence"]:
                if row.get("claim_id") == claim_id and row.get("evidence_id") == evidence_id:
                    return [(row.get("stance"), row.get("notes"))]
            return []

        if normalized.startswith(
            "select count(*) from claim_evidence where claim_id = %s and stance is not null"
        ):
            claim_id = params[0]
            count = sum(
                1
                for row in self.tables["claim_evidence"]
                if row.get("claim_id") == claim_id and row.get("stance") is not None
            )
            return [(count,)]

        if normalized.startswith(
            "update claim_evidence set stance = %s, notes = %s where claim_id = %s and evidence_id = %s"
        ):
            stance, notes, claim_id, evidence_id = params
            for row in self.tables["claim_evidence"]:
                if row.get("claim_id") == claim_id and row.get("evidence_id") == evidence_id:
                    row["stance"] = stance
                    row["notes"] = notes
                    break
            return []

        if normalized.startswith("update evidence_source set"):
            evidence_id = params[-1]
            row = self._find_one("evidence_source", evidence_id)
            if not row:
                return []
            row["title"] = params[0]
            row["year"] = params[1]
            if "pubmed_id = coalesce" in normalized:
                pubmed_id = params[2]
                if pubmed_id not in (None, ""):
                    row["pubmed_id"] = pubmed_id
            else:
                doi = params[2]
                if doi not in (None, ""):
                    row["doi"] = doi
            row["url"] = params[3]
            row["type"] = params[4]
            row["journal"] = params[5]
            return []

        if normalized.startswith(
            "select id, job_type, payload, status, priority, run_at, attempts, max_attempts, last_error from job_queue"
        ):
            status = params[0] if params else None
            type_filters = list(params[1:])
            candidates: List[Tuple[Dict[str, Any], dt.datetime]] = []
            now = dt.datetime.now(tz=dt.timezone.utc)
            for row in self.tables["job_queue"]:
                if status is not None and row.get("status") != status:
                    continue
                run_at = _as_datetime(row.get("run_at"))
                if run_at > now:
                    continue
                if type_filters and row.get("job_type") not in type_filters:
                    continue
                candidates.append((row, run_at))
            if not candidates:
                return []
            candidates.sort(
                key=lambda item: (
                    -int(item[0].get("priority", 0) or 0),
                    item[1],
                    int(item[0].get("id", 0) or 0),
                )
            )
            row, _ = candidates[0]
            return [
                (
                    row.get("id"),
                    row.get("job_type"),
                    row.get("payload"),
                    row.get("status"),
                    row.get("priority"),
                    row.get("run_at"),
                    row.get("attempts"),
                    row.get("max_attempts"),
                    row.get("last_error"),
                )
            ]

        if normalized.startswith(
            "select id, job_type, status, payload, result, error, created_at, updated_at from job where id = %s"
        ):
            job_id = params[0]
            row = self._find_one("job", job_id)
            if not row:
                return []
            return [
                (
                    row.get("id"),
                    row.get("job_type"),
                    row.get("status"),
                    row.get("payload"),
                    row.get("result"),
                    row.get("error"),
                    row.get("created_at"),
                    row.get("updated_at"),
                )
            ]

        if normalized.startswith(
            "select id, job_type, status, payload, result, error, created_at, updated_at from job"
        ):
            rows = list(self.tables["job"])
            param_index = 0
            if "where status = %s" in normalized:
                status = params[param_index]
                param_index += 1
                rows = [row for row in rows if row.get("status") == status]
            rows.sort(key=lambda row: row.get("id", 0), reverse="order by id desc" in normalized)
            if "limit %s" in normalized:
                limit = int(params[param_index])
                rows = rows[:limit]
            return [
                (
                    row.get("id"),
                    row.get("job_type"),
                    row.get("status"),
                    row.get("payload"),
                    row.get("result"),
                    row.get("error"),
                    row.get("created_at"),
                    row.get("updated_at"),
                )
                for row in rows
            ]

        if normalized.startswith(
            "update job set status = %s, error = %s, updated_at = now() where id = %s returning id"
        ):
            status, error, job_id = params
            row = self._find_one("job", job_id)
            if not row:
                return []
            row["status"] = status
            row["error"] = error
            row["updated_at"] = self._tick()
            return [(row.get("id"),)]

        if normalized.startswith(
            "update job set status = %s, updated_at = now() where id = %s"
        ):
            status, job_id = params
            row = self._find_one("job", job_id)
            if not row:
                return []
            row["status"] = status
            row["updated_at"] = self._tick()
            return []

        if normalized.startswith(
            "update job_queue set status = %s, attempts = attempts + 1, started_at = now(), updated_at = now() where id = %s"
        ):
            status, job_id = params
            row = self._find_one("job_queue", job_id)
            if not row:
                return []
            row["status"] = status
            row["attempts"] = row.get("attempts", 0) + 1
            row["started_at"] = self._tick()
            row["updated_at"] = self._tick()
            return []

        if normalized.startswith(
            "update job_queue set status = %s, finished_at = now(), last_error = null, updated_at = now() where id = %s"
        ):
            status, job_id = params
            row = self._find_one("job_queue", job_id)
            if not row:
                return []
            row["status"] = status
            row["finished_at"] = self._tick()
            row["last_error"] = None
            row["updated_at"] = self._tick()
            return []

        if normalized.startswith(
            "update job_queue set status = %s, finished_at = now(), last_error = %s, updated_at = now() where id = %s"
        ):
            status, error, job_id = params
            row = self._find_one("job_queue", job_id)
            if not row:
                return []
            row["status"] = status
            row["finished_at"] = self._tick()
            row["last_error"] = error
            row["updated_at"] = self._tick()
            return []

        if normalized.startswith(
            "update job_queue set status = %s, run_at = %s, last_error = %s, started_at = null, finished_at = null, updated_at = now() where id = %s"
        ):
            status, run_at, error, job_id = params
            row = self._find_one("job_queue", job_id)
            if not row:
                return []
            row["status"] = status
            row["run_at"] = run_at
            row["last_error"] = error
            row["started_at"] = None
            row["finished_at"] = None
            row["updated_at"] = self._tick()
            return []

        raise ValueError(f"Unsupported SQL for fake db: {sql}")

    # helpers -----------------------------------------------------------------

    def _handle_insert(self, sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
        table, rows = parse_insert(sql)
        param_iter = iter(params)
        inserted: List[Dict[str, Any]] = []
        for row in rows:
            materialized: Dict[str, Any] = {}
            for key, value in row.items():
                if value == "%s":
                    try:
                        materialized[key] = next(param_iter)
                    except StopIteration:
                        materialized[key] = value
                else:
                    materialized[key] = value
            inserted.append(self._insert_row(table, materialized))
        return inserted

    def _insert_row(self, table: str, row: Dict[str, Any]) -> Dict[str, Any]:
        processed: Dict[str, Any] = {}
        for key, value in row.items():
            if value is NOW_SENTINEL:
                processed[key] = self._tick()
            else:
                processed[key] = value

        if table in self._auto_ids:
            if "id" not in processed or processed["id"] is None:
                processed["id"] = self._auto_ids[table]
                self._auto_ids[table] += 1
            else:
                self._auto_ids[table] = max(self._auto_ids[table], int(processed["id"]) + 1)

        processed.setdefault("__order", self._insert_order)
        self._insert_order += 1

        if table in {"episode", "episode_summary", "claim", "claim_grade"}:
            processed.setdefault("created_at", self._tick())

        if table == "claim_grade":
            processed.setdefault("rubric_version", "auto-v1")

        if table == "job_queue":
            processed.setdefault("status", "queued")
            processed["priority"] = int(processed.get("priority", 0) or 0)
            processed["attempts"] = int(processed.get("attempts", 0) or 0)
            processed["max_attempts"] = int(processed.get("max_attempts", 3) or 3)
            processed["run_at"] = _as_datetime(processed.get("run_at"))
            processed.setdefault("last_error", None)
            processed.setdefault("created_at", self._tick())
            processed.setdefault("updated_at", processed.get("created_at"))
            processed.setdefault("started_at", None)
            processed.setdefault("finished_at", None)

        if table == "job":
            processed.setdefault("status", "queued")
            processed.setdefault("payload", {})
            processed.setdefault("result", None)
            processed.setdefault("error", None)
            processed.setdefault("created_at", self._tick())
            processed.setdefault("updated_at", processed.get("created_at"))

        if table == "transcript_chunk":
            processed.setdefault("key_points", None)

        self.tables[table].append(processed)
        return processed

    def _tick(self) -> int:
        self._clock += 1
        return self._clock

    def _find_one(self, table: str, pk: int) -> Dict[str, Any] | None:
        for row in self.tables[table]:
            if row.get("id") == pk:
                return row
        return None

    def _latest_grade(self, claim_id: int) -> Dict[str, Any] | None:
        grades = [g for g in self.tables["claim_grade"] if g["claim_id"] == claim_id]
        grades.sort(key=lambda g: g.get("created_at", 0))
        return grades[-1] if grades else None

    def _select_episode_claims(self, episode_id: int) -> List[Tuple[Any, ...]]:
        claims = [c for c in self.tables["claim"] if c["episode_id"] == episode_id]
        claims.sort(key=lambda c: c["id"])
        rows: List[Tuple[Any, ...]] = []
        for claim in claims:
            latest = self._latest_grade(claim["id"])
            rows.append(
                (
                    claim["id"],
                    claim.get("raw_text"),
                    claim.get("normalized_text"),
                    claim.get("topic"),
                    claim.get("domain"),
                    claim.get("risk_level"),
                    claim.get("start_ms"),
                    claim.get("end_ms"),
                    latest.get("grade") if latest else None,
                    latest.get("rationale") if latest else None,
                )
            )
        return rows

    def _select_topic_claims(self, topic: str) -> List[Tuple[Any, ...]]:
        claims = [c for c in self.tables["claim"] if c.get("topic") == topic]
        entries: List[Tuple[int, Dict[str, Any], Dict[str, Any], Dict[str, Any]]] = []
        for claim in claims:
            episode = self._find_one("episode", claim["episode_id"])
            if not episode:
                continue
            latest = self._latest_grade(claim["id"])
            entries.append((claim["id"], claim, episode, latest))

        def sort_key(item: Tuple[int, Dict[str, Any], Dict[str, Any], Dict[str, Any]]):
            _, claim, episode, _ = item
            published_at = episode.get("published_at")
            # Nulls last when sorting desc -> treat None as smaller priority.
            return (
                0 if published_at is not None else 1,
                -(published_at or 0),
                -episode.get("id", 0),
            )

        entries.sort(key=sort_key)

        rows: List[Tuple[Any, ...]] = []
        for claim_id, claim, episode, latest in entries:
            rows.append(
                (
                    claim_id,
                    episode["id"],
                    episode.get("title"),
                    claim.get("raw_text"),
                    claim.get("normalized_text"),
                    claim.get("domain"),
                    claim.get("risk_level"),
                    claim.get("start_ms"),
                    claim.get("end_ms"),
                    latest.get("grade") if latest else None,
                    latest.get("rationale") if latest else None,
                )
            )
        return rows

    def _select_claim_detail(self, claim_id: int) -> List[Tuple[Any, ...]]:
        claim = self._find_one("claim", claim_id)
        if not claim:
            return []
        episode = self._find_one("episode", claim["episode_id"])
        latest = self._latest_grade(claim_id)
        return [
            (
                claim_id,
                episode.get("title") if episode else None,
                claim.get("topic"),
                claim.get("domain"),
                claim.get("risk_level"),
                claim.get("raw_text"),
                claim.get("normalized_text"),
                latest.get("grade") if latest else None,
                latest.get("rationale") if latest else None,
                latest.get("rubric_version") if latest else None,
                latest.get("created_at") if latest else None,
            )
        ]

    def _select_claim_evidence(self, claim_id: int) -> List[Tuple[Any, ...]]:
        rows: List[Tuple[Any, ...]] = []
        for link in self.tables["claim_evidence"]:
            if link["claim_id"] != claim_id:
                continue
            evidence = self._find_one("evidence_source", link["evidence_id"])
            if not evidence:
                continue
            rows.append(
                (
                    evidence.get("id"),
                    evidence.get("title"),
                    evidence.get("year"),
                    evidence.get("type"),
                    evidence.get("journal"),
                    evidence.get("doi"),
                    evidence.get("pubmed_id"),
                    evidence.get("url"),
                    link.get("stance"),
                )
            )

        rows.sort(key=lambda r: (r[2] is None, -(r[2] or 0)))
        return rows


__all__ = ["FakeDatabase", "FakeConnection", "parse_insert", "NOW_SENTINEL"]

