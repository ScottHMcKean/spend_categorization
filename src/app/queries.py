"""Query functions for invoice and categorization data.

Uses Delta tables directly:
- invoices: Main invoice table
- cat_*: Categorization tables
- cat_reviews: Human reviews
"""

import logging
from typing import List, Optional

import pandas as pd

from src.config import Config
from .database import DatabaseBackend

logger = logging.getLogger(__name__)


def search_invoices(
    config: Config,
    search_term: str = "",
    limit: int = 50,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """Search invoices by term."""
    if backend is None:
        from .database import get_backend
        backend = get_backend()

    where_conditions = "1=1"
    parameters = {}
    
    if search_term:
        where_conditions = """
            (LOWER(order_id) LIKE LOWER(:search_pattern)
             OR LOWER(description) LIKE LOWER(:search_pattern)
             OR LOWER(supplier) LIKE LOWER(:search_pattern))
        """
        parameters["search_pattern"] = f"%{search_term}%"

    query = f"""
        SELECT *
        FROM {config.full_invoices_table_path}
        WHERE {where_conditions}
        ORDER BY date DESC
        LIMIT {limit}
    """
    
    # For MockBackend, handle search in-memory
    if hasattr(backend, '_invoices'):
        df = backend._invoices.copy()
        
        if search_term:
            search_lower = search_term.lower()
            mask = (
                df['order_id'].str.lower().str.contains(search_lower, na=False) |
                df['description'].str.lower().str.contains(search_lower, na=False) |
                df['supplier'].str.lower().str.contains(search_lower, na=False)
            )
            df = df[mask]
        
        df = df.sort_values('date', ascending=False).head(limit)
        return df
    
    return backend.execute_query(query, parameters)


def get_flagged_invoices(
    config: Config,
    threshold: float = 0.7,
    limit: int = 50,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """Get invoices with low confidence categorizations."""
    if backend is None:
        from .database import get_backend
        backend = get_backend()

    # Use active categorization table
    cat_table = config.active_categorization_table_path
    
    query = f"""
        SELECT i.*, c.pred_level_1, c.pred_level_2, c.pred_level_3, c.confidence
        FROM {config.full_invoices_table_path} i
        LEFT JOIN {cat_table} c ON i.order_id = c.order_id
        WHERE c.confidence < {threshold} OR c.confidence IS NULL
        ORDER BY c.confidence ASC
        LIMIT {limit}
    """
    
    # For MockBackend, we need to simulate the join
    if hasattr(backend, '_invoices'):
        invoices = backend._invoices.copy()
        categorizations = backend._categorizations.copy()
        
        # Merge invoices with categorizations
        result = invoices.merge(
            categorizations[['order_id', 'pred_level_1', 'pred_level_2', 'pred_level_3', 'confidence']],
            on='order_id',
            how='left'
        )
        
        # Filter by confidence threshold
        result = result[(result['confidence'] < threshold) | (result['confidence'].isna())]
        result = result.sort_values('confidence', ascending=True).head(limit)
        
        return result
    
    return backend.execute_query(query)


def get_invoices_by_ids(
    config: Config,
    invoice_ids: List[str],
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """Get invoices by order IDs."""
    if backend is None:
        from .database import get_backend
        backend = get_backend()

    if not invoice_ids:
        return pd.DataFrame()

    placeholders = ", ".join([f"'{id}'" for id in invoice_ids])
    
    query = f"""
        SELECT *
        FROM {config.full_invoices_table_path}
        WHERE order_id IN ({placeholders})
    """
    
    return backend.execute_query(query)


def get_available_categories(
    config: Config,
    backend: Optional[DatabaseBackend] = None,
) -> List[str]:
    """Get list of available categories."""
    if backend is None:
        from .database import get_backend
        backend = get_backend()

    query = f"""
        SELECT DISTINCT category_level_2
        FROM {config.full_categories_table_path}
        WHERE category_level_2 IS NOT NULL
        ORDER BY category_level_2
    """
    
    df = backend.execute_query(query)
    return df["category_level_2"].tolist() if not df.empty else []


def get_invoice_with_categorization(
    config: Config,
    invoice_id: str,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """Get invoice with categorization details."""
    if backend is None:
        from .database import get_backend
        backend = get_backend()

    # Use active categorization table
    cat_table = config.active_categorization_table_path
    
    query = f"""
        SELECT i.*, c.pred_level_1, c.pred_level_2, c.pred_level_3, c.confidence, c.source
        FROM {config.full_invoices_table_path} i
        LEFT JOIN {cat_table} c ON i.order_id = c.order_id
        WHERE i.order_id = :invoice_id
    """
    
    return backend.execute_query(query, {"invoice_id": invoice_id})
