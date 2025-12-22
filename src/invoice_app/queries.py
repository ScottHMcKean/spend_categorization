"""Query functions for invoice data retrieval.

Supports both test mode (MockBackend) and prod mode (LakebaseBackend).
"""

from typing import Dict, List, Optional
import pandas as pd

from .config import AppConfig, LakebaseConfig
from .database import get_backend, DatabaseBackend


def search_invoices(
    app_config: AppConfig,
    search_term: str,
    search_fields: Optional[List[str]] = None,
    limit: int = 100,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """
    Search for invoices by text match across specified fields.

    Args:
        app_config: Application configuration
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
        search_fields = app_config.search_fields or [
            "invoice_number",
            "vendor_name",
            "description",
        ]

    # Build WHERE clause with ILIKE conditions for each field (PostgreSQL)
    where_conditions = " OR ".join(
        [f"{field} ILIKE :search_pattern" for field in search_fields]
    )

    schema_prefix = _get_schema_prefix(app_config)
    query = f"""
        SELECT *
        FROM {schema_prefix}{app_config.invoices_table}
        WHERE {where_conditions}
        ORDER BY invoice_date DESC
        LIMIT {limit}
    """

    parameters = {"search_pattern": f"%{search_term}%"}
    return backend.execute_query(query, parameters)


def get_flagged_invoices(
    app_config: AppConfig,
    limit: int = 100,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """
    Retrieve invoices flagged for review (low confidence or missing category).

    Args:
        app_config: Application configuration
        limit: Maximum number of results to return
        backend: Optional backend instance (uses global if not provided)

    Returns:
        DataFrame with flagged invoices
    """
    if backend is None:
        backend = get_backend()

    schema_prefix = _get_schema_prefix(app_config)

    # Use custom view if configured, otherwise use default logic
    if app_config.flagged_invoices_view:
        query = f"""
            SELECT *
            FROM {schema_prefix}{app_config.flagged_invoices_view}
            ORDER BY flag_priority DESC, invoice_date DESC
            LIMIT {limit}
        """
    else:
        # Default: flag invoices with low confidence or missing categories
        threshold = app_config.low_confidence_threshold
        query = f"""
            SELECT *
            FROM {schema_prefix}{app_config.invoices_table}
            WHERE confidence_score < {threshold} OR category IS NULL
            ORDER BY confidence_score ASC, invoice_date DESC
            LIMIT {limit}
        """

    return backend.execute_query(query)


def get_invoices_by_ids(
    app_config: AppConfig,
    invoice_ids: List[str],
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """
    Retrieve specific invoices by their IDs.

    Args:
        app_config: Application configuration
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

    schema_prefix = _get_schema_prefix(app_config)
    query = f"""
        SELECT *
        FROM {schema_prefix}{app_config.invoices_table}
        WHERE invoice_id IN ({placeholders})
    """

    return backend.execute_query(query)


def get_available_categories(
    app_config: AppConfig,
    backend: Optional[DatabaseBackend] = None,
) -> List[str]:
    """
    Get list of distinct categories from the invoices table.

    Args:
        app_config: Application configuration
        backend: Optional backend instance (uses global if not provided)

    Returns:
        List of category strings
    """
    if backend is None:
        backend = get_backend()

    schema_prefix = _get_schema_prefix(app_config)
    query = f"""
        SELECT DISTINCT category
        FROM {schema_prefix}{app_config.invoices_table}
        WHERE category IS NOT NULL
        ORDER BY category
    """

    df = backend.execute_query(query)
    return df["category"].tolist()


def get_invoice_by_transaction_id(
    app_config: AppConfig,
    transaction_id: str,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """
    Retrieve a single invoice by transaction ID.

    Args:
        app_config: Application configuration
        transaction_id: Transaction ID to look up
        backend: Optional backend instance (uses global if not provided)

    Returns:
        DataFrame with the invoice (may be empty if not found)
    """
    if backend is None:
        backend = get_backend()

    schema_prefix = _get_schema_prefix(app_config)
    query = f"""
        SELECT *
        FROM {schema_prefix}{app_config.invoices_table}
        WHERE transaction_id = :transaction_id
    """

    return backend.execute_query(query, {"transaction_id": transaction_id})


def _get_schema_prefix(app_config: AppConfig) -> str:
    """
    Get the schema prefix for table references.

    In prod mode with Lakebase, we use schema.table format.
    In test mode, we don't need a prefix.

    Args:
        app_config: Application configuration

    Returns:
        Schema prefix string (e.g., "public." or "")
    """
    # In test mode, MockBackend handles everything internally
    if app_config.is_test_mode:
        return ""

    # In prod mode, we need the schema prefix
    # This is typically configured in LakebaseConfig
    # For now, return empty and let the full table name be used
    return ""


# Legacy compatibility functions that accept DatabricksConfig
# These are kept for backward compatibility with existing code


def search_invoices_legacy(
    db_config,
    app_config: AppConfig,
    search_term: str,
    search_fields: Optional[List[str]] = None,
    limit: int = 100,
) -> pd.DataFrame:
    """Legacy search_invoices that accepts db_config (ignored)."""
    return search_invoices(app_config, search_term, search_fields, limit)


def get_flagged_invoices_legacy(
    db_config,
    app_config: AppConfig,
    limit: int = 100,
) -> pd.DataFrame:
    """Legacy get_flagged_invoices that accepts db_config (ignored)."""
    return get_flagged_invoices(app_config, limit)


def get_invoices_by_ids_legacy(
    db_config,
    app_config: AppConfig,
    invoice_ids: List[str],
) -> pd.DataFrame:
    """Legacy get_invoices_by_ids that accepts db_config (ignored)."""
    return get_invoices_by_ids(app_config, invoice_ids)


def get_available_categories_legacy(
    db_config,
    app_config: AppConfig,
) -> List[str]:
    """Legacy get_available_categories that accepts db_config (ignored)."""
    return get_available_categories(app_config)
