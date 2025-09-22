"""Database helpers used across the project."""

from __future__ import annotations

import os
import psycopg

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/podcast_plow",
)


def db_conn() -> psycopg.Connection:
    """Return a new autocommit connection to Postgres."""

    return psycopg.connect(DATABASE_URL, autocommit=True)


__all__ = ["DATABASE_URL", "db_conn"]
