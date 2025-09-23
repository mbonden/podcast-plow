from __future__ import annotations

"""Helpers to simulate a minimal Postgres-like database for tests.

The production application uses psycopg to talk to Postgres.  Tests in this
repository run without an actual database server, so we provide a very small
in-memory substitute that understands the handful of SQL statements used in
the API handlers and seed data.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Sequence, Tuple


NOW_SENTINEL = object()


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.strip().lower().split())


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

    def __enter__(self) -> "FakeCursor":  # pragma: no cover - convenience
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:  # pragma: no cover - convenience
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
        }
        self._auto_ids: Dict[str, int] = {
            "podcast": 1,
            "episode": 1,
            "episode_summary": 1,
            "claim": 1,
            "evidence_source": 1,
            "claim_grade": 1,
            "transcript": 1,
        }
        self._insert_order = 0
        self._clock = 0

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def execute(self, sql: str, params: Sequence[Any]) -> List[Tuple[Any, ...]]:
        stripped = sql.strip()
        normalized = _normalize_sql(stripped)

        if normalized.startswith("insert into"):
            inserted_rows = self._handle_insert(stripped, params)
            if "returning id" in normalized:
                return [(row.get("id"),) for row in inserted_rows]
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

        if normalized.startswith("with latest_grade as") and "where c.episode_id = %s" in normalized:
            return self._select_episode_claims(params[0])

        if normalized.startswith("select id, normalized_text, raw_text from claim"):
            return self._select_claim_rows(normalized, params)

        if normalized.startswith("with latest_grade as") and "where c.topic = %s" in normalized:
            return self._select_topic_claims(params[0])

        if normalized.startswith("with latest_grade as") and "where c.id = %s" in normalized:
            return self._select_claim_detail(params[0])

        if normalized.startswith("select es.id, es.title") and "from claim_evidence" in normalized:
            return self._select_claim_evidence(params[0])

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

        raise ValueError(f"Unsupported SQL for fake db: {sql}")

    # helpers -----------------------------------------------------------------

    def _handle_insert(self, sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
        table, rows = parse_insert(sql)
        inserted: List[Dict[str, Any]] = []
        param_index = 0
        for row in rows:
            resolved: Dict[str, Any] = {}
            for key, value in row.items():
                if isinstance(value, str) and value == "%s":
                    if param_index >= len(params):
                        raise ValueError("Not enough parameters supplied for insert")
                    resolved[key] = params[param_index]
                    param_index += 1
                else:
                    resolved[key] = value
            inserted.append(self._insert_row(table, resolved))
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
            processed.setdefault("rubric_version", "v1")

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

    def _select_claim_rows(
        self, normalized: str, params: Sequence[Any]
    ) -> List[Tuple[Any, ...]]:
        claims = list(self.tables["claim"])
        param_index = 0
        if " where " in normalized:
            _, tail = normalized.split(" where ", 1)
            where_clause = tail.split(" order by ", 1)[0]
            if "id = any(%s)" in where_clause:
                ids = set(params[param_index])
                param_index += 1
                claims = [c for c in claims if c.get("id") in ids]
            if "episode_id = any(%s)" in where_clause:
                episode_ids = set(params[param_index])
                param_index += 1
                claims = [c for c in claims if c.get("episode_id") in episode_ids]
        claims.sort(key=lambda c: c.get("id", 0))
        return [
            (c.get("id"), c.get("normalized_text"), c.get("raw_text"))
            for c in claims
        ]


__all__ = ["FakeDatabase", "FakeConnection", "parse_insert", "NOW_SENTINEL"]

