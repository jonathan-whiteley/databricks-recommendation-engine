"""Lakebase (PostgreSQL) connection pool for the recommender app."""

import os
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2 import pool

_pool: pool.SimpleConnectionPool | None = None


def get_pool() -> pool.SimpleConnectionPool:
    """Get or create the connection pool. Reads config from environment variables."""
    global _pool
    if _pool is None:
        _pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=5,
            host=os.environ["LAKEBASE_HOST"],
            port=int(os.environ.get("LAKEBASE_PORT", "5432")),
            dbname=os.environ["LAKEBASE_DATABASE"],
            user=os.environ["LAKEBASE_USER"],
            password=os.environ["LAKEBASE_PASSWORD"],
            sslmode=os.environ.get("LAKEBASE_SSLMODE", "require"),
        )
    return _pool


@contextmanager
def get_connection() -> Generator:
    """Context manager that gets a connection from the pool and returns it when done."""
    p = get_pool()
    conn = p.getconn()
    try:
        yield conn
    finally:
        p.putconn(conn)
