"""Lakebase (PostgreSQL) connection for the recommender app.

Connection modes (tried in order):
1. PG* env vars from Databricks Apps resource binding + OAuth token
2. Static credentials via LAKEBASE_HOST + LAKEBASE_PASSWORD
3. OAuth via Databricks SDK with LAKEBASE_INSTANCE_NAME
"""

import logging
import os
import uuid
from contextlib import contextmanager
from typing import Generator

import psycopg2

logger = logging.getLogger(__name__)


def _get_connection_params() -> dict:
    """Build connection parameters from environment."""
    # Option 1: Databricks Apps resource binding sets PG* env vars.
    # We use these for host/port/db/user but generate an OAuth token for the password.
    if os.environ.get("PGHOST") and os.environ.get("PGUSER"):
        from databricks.sdk import WorkspaceClient

        instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME", "")
        w = WorkspaceClient()
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[instance_name] if instance_name else None,
        )
        logger.info(f"Using PG* env vars with OAuth token (host={os.environ['PGHOST'][:30]})")
        return {
            "host": os.environ["PGHOST"],
            "port": int(os.environ.get("PGPORT", "5432")),
            "dbname": os.environ.get("PGDATABASE", "databricks_postgres"),
            "user": os.environ["PGUSER"],
            "password": cred.token,
            "sslmode": os.environ.get("PGSSLMODE", "require"),
        }

    # Option 2: Static credentials
    if os.environ.get("LAKEBASE_HOST") and os.environ.get("LAKEBASE_PASSWORD"):
        logger.info("Using static LAKEBASE_HOST credentials")
        return {
            "host": os.environ["LAKEBASE_HOST"],
            "port": int(os.environ.get("LAKEBASE_PORT", "5432")),
            "dbname": os.environ.get("LAKEBASE_DATABASE", "databricks_postgres"),
            "user": os.environ["LAKEBASE_USER"],
            "password": os.environ["LAKEBASE_PASSWORD"],
            "sslmode": "require",
        }

    # Option 3: OAuth via SDK (local dev)
    instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME")
    if instance_name:
        from databricks.sdk import WorkspaceClient

        logger.info(f"Using SDK OAuth for instance: {instance_name}")
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
        "No Lakebase connection configured. Set PG* env vars (via app resource), "
        "LAKEBASE_HOST+LAKEBASE_PASSWORD, or LAKEBASE_INSTANCE_NAME."
    )


@contextmanager
def get_connection() -> Generator:
    """Get a fresh database connection."""
    params = _get_connection_params()
    conn = psycopg2.connect(**params)
    try:
        yield conn
    finally:
        conn.close()
