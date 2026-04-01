"""Lakebase (PostgreSQL) connection for the recommender app.

Supports two modes:
1. Databricks Apps: Uses LAKEBASE_PG_URL env var (set automatically by app resources)
2. OAuth token: Uses Databricks SDK to generate short-lived tokens

Connection is recreated per-request since OAuth tokens expire after 1 hour
and the pool can't inject fresh tokens into existing connections.
"""

import os
import uuid
from contextlib import contextmanager
from typing import Generator

import psycopg2


def _get_connection_params() -> dict:
    """Build connection parameters from environment."""
    # Option 1: Full PG URL from Databricks Apps resource binding
    pg_url = os.environ.get("LAKEBASE_PG_URL")
    if pg_url:
        return {"dsn": pg_url}

    # Option 2: Individual env vars with static password
    if os.environ.get("LAKEBASE_HOST") and os.environ.get("LAKEBASE_PASSWORD"):
        return {
            "host": os.environ["LAKEBASE_HOST"],
            "port": int(os.environ.get("LAKEBASE_PORT", "5432")),
            "dbname": os.environ.get("LAKEBASE_DATABASE", "databricks_postgres"),
            "user": os.environ["LAKEBASE_USER"],
            "password": os.environ["LAKEBASE_PASSWORD"],
            "sslmode": "require",
        }

    # Option 3: OAuth via Databricks SDK (for local dev / notebook testing)
    instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME")
    if instance_name:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        instance = w.database.get_database_instance(name=instance_name)
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[instance_name],
        )
        return {
            "host": instance.read_write_dns,
            "port": 5432,
            "dbname": os.environ.get("LAKEBASE_DATABASE", "databricks_postgres"),
            "user": w.current_user.me().user_name,
            "password": cred.token,
            "sslmode": "require",
        }

    raise RuntimeError(
        "No Lakebase connection configured. Set LAKEBASE_PG_URL, "
        "LAKEBASE_HOST+LAKEBASE_PASSWORD, or LAKEBASE_INSTANCE_NAME."
    )


@contextmanager
def get_connection() -> Generator:
    """Get a fresh database connection. Caller is responsible for closing."""
    params = _get_connection_params()
    conn = psycopg2.connect(**params)
    try:
        yield conn
    finally:
        conn.close()
