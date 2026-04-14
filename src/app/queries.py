"""Query functions for invoice and categorization data.

All read operations attempt to use preloaded DataFrames from
st.session_state first (populated by home.py on startup), falling
back to database queries only when session data is unavailable.
Write operations always go through the database backend.
"""

import logging
from typing import List, Optional

import pandas as pd

from src.config import Config
from .database import DatabaseBackend

logger = logging.getLogger(__name__)


def _session_df(key: str) -> Optional[pd.DataFrame]:
    """Return a preloaded DataFrame from Streamlit session_state, or None."""
    try:
        import streamlit as st
        df = st.session_state.get(f"df_{key}")
        if df is not None and not df.empty:
            return df
    except Exception:
        pass
    return None


def search_invoices(
    config: Config,
    search_term: str = "",
    limit: int = 50,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """Search invoices by term.  Uses in-memory data when available."""
    cached = _session_df("invoices")
    if cached is not None:
        df = cached.copy()
        if search_term:
            s = search_term.lower()
            mask = pd.Series(False, index=df.index)
            for col in ("order_id", "description", "supplier"):
                if col in df.columns:
                    mask = mask | df[col].astype(str).str.lower().str.contains(s, na=False)
            df = df[mask]
        if "date" in df.columns:
            df = df.sort_values("date", ascending=False)
        return df.head(limit)

    if backend is None:
        from .database import get_backend
        backend = get_backend()

    if hasattr(backend, "_invoices"):
        df = backend._invoices.copy()
        if search_term:
            s = search_term.lower()
            mask = (
                df["order_id"].str.lower().str.contains(s, na=False)
                | df["description"].str.lower().str.contains(s, na=False)
                | df["supplier"].str.lower().str.contains(s, na=False)
            )
            df = df[mask]
        return df.sort_values("date", ascending=False).head(limit)

    where = "1=1"
    params = {}
    if search_term:
        where = (
            "(LOWER(order_id) LIKE LOWER(:search_pattern)"
            " OR LOWER(description) LIKE LOWER(:search_pattern)"
            " OR LOWER(supplier) LIKE LOWER(:search_pattern))"
        )
        params["search_pattern"] = f"%{search_term}%"

    query = f"""
        SELECT * FROM {config.full_invoices_table_path}
        WHERE {where} ORDER BY date DESC LIMIT {limit}
    """
    return backend.execute_query(query, params)


def get_flagged_invoices(
    config: Config,
    threshold: float = 3.5,
    limit: int = 50,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """Get invoices with low confidence categorizations."""
    invoices_df = _session_df("invoices")
    cat_key = f"cat_{config.categorization_source}"
    cat_df = _session_df(cat_key)

    if invoices_df is not None and cat_df is not None:
        pred_cols = [c for c in ["order_id", "pred_level_1", "pred_level_2", "pred_level_3", "confidence"]
                     if c in cat_df.columns]
        result = invoices_df.merge(cat_df[pred_cols], on="order_id", how="left")
        if "confidence" in result.columns:
            result["confidence"] = pd.to_numeric(result["confidence"], errors="coerce")
            result = result[(result["confidence"] < threshold) | (result["confidence"].isna())]
            result = result.sort_values("confidence", ascending=True)
        return result.head(limit)

    if backend is None:
        from .database import get_backend
        backend = get_backend()

    if hasattr(backend, "_invoices"):
        invoices = backend._invoices.copy()
        categorizations = backend._categorizations.copy()
        pred_cols = [c for c in ["order_id", "pred_level_1", "pred_level_2", "pred_level_3", "confidence"]
                     if c in categorizations.columns]
        result = invoices.merge(categorizations[pred_cols], on="order_id", how="left")
        result = result[(result["confidence"] < threshold) | (result["confidence"].isna())]
        return result.sort_values("confidence", ascending=True).head(limit)

    cat_table = config.active_categorization_table_path
    query = f"""
        SELECT i.*, c.pred_level_1, c.pred_level_2, c.confidence
        FROM {config.full_invoices_table_path} i
        LEFT JOIN {cat_table} c ON i.order_id = c.order_id
        WHERE c.confidence < {threshold} OR c.confidence IS NULL
        ORDER BY c.confidence ASC LIMIT {limit}
    """
    return backend.execute_query(query)


def get_invoices_by_ids(
    config: Config,
    invoice_ids: List[str],
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """Get invoices by order IDs."""
    if not invoice_ids:
        return pd.DataFrame()

    cached = _session_df("invoices")
    if cached is not None:
        return cached[cached["order_id"].isin(invoice_ids)].copy()

    if backend is None:
        from .database import get_backend
        backend = get_backend()

    placeholders = ", ".join([f"'{id}'" for id in invoice_ids])
    query = f"""
        SELECT * FROM {config.full_invoices_table_path}
        WHERE order_id IN ({placeholders})
    """
    return backend.execute_query(query)


def get_available_categories(
    config: Config,
    backend: Optional[DatabaseBackend] = None,
) -> List[str]:
    """Get list of available categories."""
    cached = _session_df("categories")
    if cached is not None and "category_level_2" in cached.columns:
        return sorted(cached["category_level_2"].dropna().unique().tolist())

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
    invoices_df = _session_df("invoices")
    cat_key = f"cat_{config.categorization_source}"
    cat_df = _session_df(cat_key)

    if invoices_df is not None and cat_df is not None:
        inv_row = invoices_df[invoices_df["order_id"] == invoice_id]
        cat_cols = [c for c in ["order_id", "pred_level_1", "pred_level_2", "confidence", "source"]
                    if c in cat_df.columns]
        return inv_row.merge(cat_df[cat_cols], on="order_id", how="left")

    if backend is None:
        from .database import get_backend
        backend = get_backend()

    cat_table = config.active_categorization_table_path
    query = f"""
        SELECT i.*, c.pred_level_1, c.pred_level_2, c.confidence, c.source
        FROM {config.full_invoices_table_path} i
        LEFT JOIN {cat_table} c ON i.order_id = c.order_id
        WHERE i.order_id = :invoice_id
    """
    return backend.execute_query(query, {"invoice_id": invoice_id})
