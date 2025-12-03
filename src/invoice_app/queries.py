"""Query functions for invoice data retrieval."""

from typing import Dict, List, Optional
import pandas as pd

from .config import DatabricksConfig, AppConfig
from .database import execute_query


def search_invoices(
    db_config: DatabricksConfig,
    app_config: AppConfig,
    search_term: str,
    search_fields: Optional[List[str]] = None,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Search for invoices by text match across specified fields.
    
    Args:
        db_config: Databricks connection configuration
        app_config: Application configuration
        search_term: Text to search for
        search_fields: List of column names to search in
        limit: Maximum number of results to return
        
    Returns:
        DataFrame with matching invoices
    """
    if search_fields is None:
        search_fields = ["invoice_number", "vendor_name", "description"]
    
    # Build WHERE clause with LIKE conditions for each field
    where_conditions = " OR ".join(
        [f"{field} LIKE :search_pattern" for field in search_fields]
    )
    
    query = f"""
        SELECT *
        FROM {db_config.catalog}.{db_config.schema}.{app_config.invoices_table}
        WHERE {where_conditions}
        ORDER BY invoice_date DESC
        LIMIT {limit}
    """
    
    parameters = {"search_pattern": f"%{search_term}%"}
    return execute_query(db_config, query, parameters)


def get_flagged_invoices(
    db_config: DatabricksConfig,
    app_config: AppConfig,
    limit: int = 100,
) -> pd.DataFrame:
    """
    Retrieve invoices flagged for review.
    
    Args:
        db_config: Databricks connection configuration
        app_config: Application configuration
        limit: Maximum number of results to return
        
    Returns:
        DataFrame with flagged invoices
    """
    # Use custom view if configured, otherwise use default logic
    if app_config.flagged_invoices_view:
        query = f"""
            SELECT *
            FROM {db_config.catalog}.{db_config.schema}.{app_config.flagged_invoices_view}
            ORDER BY flag_priority DESC, invoice_date DESC
            LIMIT {limit}
        """
    else:
        # Default: flag invoices with low confidence or missing categories
        query = f"""
            SELECT *
            FROM {db_config.catalog}.{db_config.schema}.{app_config.invoices_table}
            WHERE confidence_score < 0.7 OR category IS NULL
            ORDER BY confidence_score ASC, invoice_date DESC
            LIMIT {limit}
        """
    
    return execute_query(db_config, query)


def get_invoices_by_ids(
    db_config: DatabricksConfig,
    app_config: AppConfig,
    invoice_ids: List[str],
) -> pd.DataFrame:
    """
    Retrieve specific invoices by their IDs.
    
    Args:
        db_config: Databricks connection configuration
        app_config: Application configuration
        invoice_ids: List of invoice IDs to retrieve
        
    Returns:
        DataFrame with requested invoices
    """
    if not invoice_ids:
        return pd.DataFrame()
    
    # Create placeholders for IN clause
    placeholders = ", ".join([f"'{id}'" for id in invoice_ids])
    
    query = f"""
        SELECT *
        FROM {db_config.catalog}.{db_config.schema}.{app_config.invoices_table}
        WHERE invoice_id IN ({placeholders})
    """
    
    return execute_query(db_config, query)


def get_available_categories(
    db_config: DatabricksConfig,
    app_config: AppConfig,
) -> List[str]:
    """
    Get list of distinct categories from the invoices table.
    
    Args:
        db_config: Databricks connection configuration
        app_config: Application configuration
        
    Returns:
        List of category strings
    """
    query = f"""
        SELECT DISTINCT category
        FROM {db_config.catalog}.{db_config.schema}.{app_config.invoices_table}
        WHERE category IS NOT NULL
        ORDER BY category
    """
    
    df = execute_query(db_config, query)
    return df["category"].tolist()


