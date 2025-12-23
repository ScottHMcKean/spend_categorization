"""Query functions for invoice data retrieval.

Supports both test mode (MockBackend) and prod mode (LakebaseBackend).
"""

from typing import List, Optional
import pandas as pd

from .config import Config
from .database import get_backend, DatabaseBackend


def search_invoices(
    config: Config,
    search_term: str,
    search_fields: Optional[List[str]] = None,
    limit: int = 100,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """
    Search for invoices by text match across specified fields.

    Args:
        config: Application configuration
        search_term: Text to search for
        search_fields: List of column names to search in
        limit: Maximum number of results to return
        backend: Optional backend instance (uses global if not provided)

    Returns:
        DataFrame with matching invoices
    """
    if backend is None:
        backend = get_backend()

    if search_fields is None:
        search_fields = config.search.default_fields

    # Build WHERE clause with ILIKE conditions for each field (PostgreSQL)
    where_conditions = " OR ".join(
        [f"{field} ILIKE :search_pattern" for field in search_fields]
    )

    schema_prefix = _get_schema_prefix(config)
    query = f"""
        SELECT *
        FROM {schema_prefix}{config.tables.invoices}
        WHERE {where_conditions}
        ORDER BY invoice_date DESC
        LIMIT {limit}
    """

    parameters = {"search_pattern": f"%{search_term}%"}
    return backend.execute_query(query, parameters)


def get_flagged_invoices(
    config: Config,
    limit: int = 100,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """
    Retrieve invoices flagged for review (low confidence or missing category).

    Args:
        config: Application configuration
        limit: Maximum number of results to return
        backend: Optional backend instance (uses global if not provided)

    Returns:
        DataFrame with flagged invoices
    """
    if backend is None:
        backend = get_backend()

    schema_prefix = _get_schema_prefix(config)

    # Use custom view if configured, otherwise use default logic
    if config.tables.flagged_view:
        query = f"""
            SELECT *
            FROM {schema_prefix}{config.tables.flagged_view}
            ORDER BY flag_priority DESC, invoice_date DESC
            LIMIT {limit}
        """
    else:
        # Default: flag invoices with low confidence or missing categories
        threshold = config.flagging.low_confidence_threshold
        query = f"""
            SELECT *
            FROM {schema_prefix}{config.tables.invoices}
            WHERE confidence_score < {threshold} OR category IS NULL
            ORDER BY confidence_score ASC, invoice_date DESC
            LIMIT {limit}
        """

    return backend.execute_query(query)


def get_invoices_by_ids(
    config: Config,
    invoice_ids: List[str],
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """
    Retrieve specific invoices by their IDs.

    Args:
        config: Application configuration
        invoice_ids: List of invoice IDs to retrieve
        backend: Optional backend instance (uses global if not provided)

    Returns:
        DataFrame with requested invoices
    """
    if backend is None:
        backend = get_backend()

    if not invoice_ids:
        return pd.DataFrame()

    # Create placeholders for IN clause
    placeholders = ", ".join([f"'{id}'" for id in invoice_ids])

    schema_prefix = _get_schema_prefix(config)
    query = f"""
        SELECT *
        FROM {schema_prefix}{config.tables.invoices}
        WHERE invoice_id IN ({placeholders})
    """

    return backend.execute_query(query)


def get_available_categories(
    config: Config,
    backend: Optional[DatabaseBackend] = None,
) -> List[str]:
    """
    Get list of distinct categories from the invoices table.

    Args:
        config: Application configuration
        backend: Optional backend instance (uses global if not provided)

    Returns:
        List of category strings
    """
    if backend is None:
        backend = get_backend()

    schema_prefix = _get_schema_prefix(config)
    query = f"""
        SELECT DISTINCT category
        FROM {schema_prefix}{config.tables.invoices}
        WHERE category IS NOT NULL
        ORDER BY category
    """

    df = backend.execute_query(query)
    return df["category"].tolist()


def get_invoice_by_transaction_id(
    config: Config,
    transaction_id: str,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """
    Retrieve a single invoice by transaction ID.

    Args:
        config: Application configuration
        transaction_id: Transaction ID to look up
        backend: Optional backend instance (uses global if not provided)

    Returns:
        DataFrame with the invoice (may be empty if not found)
    """
    if backend is None:
        backend = get_backend()

    schema_prefix = _get_schema_prefix(config)
    query = f"""
        SELECT *
        FROM {schema_prefix}{config.tables.invoices}
        WHERE transaction_id = :transaction_id
    """

    return backend.execute_query(query, {"transaction_id": transaction_id})


def _get_schema_prefix(config: Config) -> str:
    """
    Get the schema prefix for table references.

    In prod mode with Lakebase, we use schema.table format.
    In test mode, we don't need a prefix.
    """
    # In test mode, MockBackend handles everything internally
    if config.app.is_test_mode:
        return ""
    # In prod mode, return empty - table names are fully qualified in config
    return ""
