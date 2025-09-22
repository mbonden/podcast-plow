"""Database helper utilities for the podcast-plow services."""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

import psycopg


DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/podcast_plow"


def get_database_url() -> str:
    """Return the configured database URL.

    The CLI tools and API server share the same helper so behaviour is
    consistent regardless of whether the code runs inside Docker or locally.
    """

    return os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)


@contextmanager
def db_connection(*, autocommit: bool = True) -> Iterator[psycopg.Connection]:
    """Context manager that yields a configured psycopg connection."""

    conn = psycopg.connect(get_database_url(), autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


__all__ = ["db_connection", "get_database_url"]
