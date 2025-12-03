"""Database connection and query execution for Databricks."""

from typing import Any, Dict, List, Optional
from contextlib import contextmanager

import pandas as pd
from databricks import sql

from .config import DatabricksConfig


@contextmanager
def get_connection(config: DatabricksConfig):
    """Create a Databricks SQL connection context manager."""
    connection = sql.connect(
        server_hostname=config.server_hostname,
        http_path=config.http_path,
        access_token=config.access_token,
    )
    try:
        yield connection
    finally:
        connection.close()


def execute_query(
    config: DatabricksConfig,
    query: str,
    parameters: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """Execute a query and return results as a DataFrame."""
    with get_connection(config) as conn:
        cursor = conn.cursor()
        cursor.execute(query, parameters or {})

        # Fetch results and column names
        results = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()

        return pd.DataFrame(results, columns=columns)


def execute_write(
    config: DatabricksConfig,
    query: str,
    parameters: Optional[Dict[str, Any]] = None,
) -> None:
    """Execute a write query (INSERT, UPDATE, etc.)."""
    with get_connection(config) as conn:
        cursor = conn.cursor()
        cursor.execute(query, parameters or {})
        cursor.close()


def execute_batch_write(
    config: DatabricksConfig,
    query: str,
    batch_parameters: List[Dict[str, Any]],
) -> None:
    """Execute a batch write query with multiple parameter sets."""
    with get_connection(config) as conn:
        cursor = conn.cursor()
        cursor.executemany(query, batch_parameters)
        cursor.close()
