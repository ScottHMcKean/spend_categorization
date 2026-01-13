"""Query functions for invoice and categorization data.

Reads from Lakebase synced tables:
- invoices_sync: synced from silver.invoices
- categorization_sync: synced from silver.cat_bootstrap
"""

from typing import List, Optional

import pandas as pd

from src.config import Config
from .database import DatabaseBackend, get_backend


def search_invoices(
    config: Config,
    search_term: str,
    search_fields: Optional[List[str]] = None,
    limit: int = 100,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """Search invoices by text match across fields."""
    if backend is None:
        backend = get_backend()

    if search_fields is None:
        search_fields = config.search_fields

    where_conditions = " OR ".join(
        [f"{field} ILIKE :search_pattern" for field in search_fields]
    )

    query = f"""
        SELECT *
        FROM {config.invoices_sync}
        WHERE {where_conditions}
        ORDER BY invoice_date DESC
        LIMIT {limit}
    """

    return backend.execute_query(query, {"search_pattern": f"%{search_term}%"})


def get_flagged_invoices(
    config: Config,
    limit: int = 100,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """Get invoices flagged for review (low confidence or missing category)."""
    if backend is None:
        backend = get_backend()

    threshold = config.low_confidence_threshold

    # Join invoices with categorization to get confidence
    query = f"""
        SELECT i.*, c.category as llm_category, c.confidence
        FROM {config.invoices_sync} i
        LEFT JOIN {config.categorization_sync} c ON i.invoice_id = c.invoice_id
        WHERE c.confidence < {threshold} OR c.category IS NULL
        ORDER BY c.confidence ASC, i.invoice_date DESC
        LIMIT {limit}
    """

    return backend.execute_query(query)


def get_invoices_by_ids(
    config: Config,
    invoice_ids: List[str],
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """Get invoices by their IDs."""
    if backend is None:
        backend = get_backend()

    if not invoice_ids:
        return pd.DataFrame()

    placeholders = ", ".join([f"'{id}'" for id in invoice_ids])

    query = f"""
        SELECT *
        FROM {config.invoices_sync}
        WHERE invoice_id IN ({placeholders})
    """

    return backend.execute_query(query)


def get_available_categories(
    config: Config,
    backend: Optional[DatabaseBackend] = None,
) -> List[str]:
    """Get distinct categories from categorization results."""
    if backend is None:
        backend = get_backend()

    query = f"""
        SELECT DISTINCT category
        FROM {config.categorization_sync}
        WHERE category IS NOT NULL
        ORDER BY category
    """

    df = backend.execute_query(query)
    return df["category"].tolist()


def get_invoice_with_categorization(
    config: Config,
    invoice_id: str,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """Get invoice with its categorization data."""
    if backend is None:
        backend = get_backend()

    query = f"""
        SELECT i.*, c.category as llm_category, c.confidence, c.run_id
        FROM {config.invoices_sync} i
        LEFT JOIN {config.categorization_sync} c ON i.invoice_id = c.invoice_id
        WHERE i.invoice_id = :invoice_id
    """

    return backend.execute_query(query, {"invoice_id": invoice_id})
