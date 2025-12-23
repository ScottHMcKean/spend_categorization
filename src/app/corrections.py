"""Type 2 Slowly Changing Dimension writer for invoice corrections.

Supports both test mode (MockBackend) and prod mode (LakebaseBackend with PostgreSQL).
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional
import pandas as pd

from .config import Config
from .database import get_backend, DatabaseBackend


def write_correction(
    config: Config,
    invoice_id: str,
    transaction_ids: List[str],
    corrected_category: str,
    comment: Optional[str] = None,
    corrected_by: str = "user",
    backend: Optional[DatabaseBackend] = None,
) -> None:
    """
    Write a single invoice correction to the Type 2 SCD table.
    
    This creates new records for each transaction with:
    - New classification
    - Timestamp of change
    - Comment explaining the change
    - User who made the change
    
    Args:
        config: Application configuration
        invoice_id: ID of the invoice being corrected
        transaction_ids: List of transaction IDs to update
        corrected_category: New category classification
        comment: Optional comment explaining the correction
        corrected_by: Username of person making the correction
        backend: Optional backend instance (uses global if not provided)
    """
    if backend is None:
        backend = get_backend()
    
    current_timestamp = datetime.now(timezone.utc).isoformat()
    schema_prefix = _get_schema_prefix(config)
    
    for transaction_id in transaction_ids:
        # Close out the current record (set end_date)
        close_query = f"""
            UPDATE {schema_prefix}{config.corrections_table}
            SET end_date = :end_date,
                is_current = FALSE
            WHERE transaction_id = :transaction_id
                AND is_current = TRUE
        """
        
        backend.execute_write(
            close_query,
            {"end_date": current_timestamp, "transaction_id": transaction_id}
        )
        
        # Insert new record with corrected category
        insert_query = f"""
            INSERT INTO {schema_prefix}{config.corrections_table}
            (transaction_id, invoice_id, category, start_date, end_date, 
             is_current, comment, corrected_by, correction_timestamp)
            VALUES (:transaction_id, :invoice_id, :category, :start_date, NULL,
                    TRUE, :comment, :corrected_by, :correction_timestamp)
        """
        
        backend.execute_write(
            insert_query,
            {
                "transaction_id": transaction_id,
                "invoice_id": invoice_id,
                "category": corrected_category,
                "start_date": current_timestamp,
                "comment": comment,
                "corrected_by": corrected_by,
                "correction_timestamp": current_timestamp,
            }
        )


def write_corrections_batch(
    config: Config,
    corrections: List[Dict],
    backend: Optional[DatabaseBackend] = None,
) -> None:
    """
    Write multiple invoice corrections in a batch.
    
    Args:
        config: Application configuration
        corrections: List of correction dictionaries with keys:
            - invoice_id
            - transaction_ids
            - corrected_category
            - comment (optional)
            - corrected_by
        backend: Optional backend instance (uses global if not provided)
    """
    if backend is None:
        backend = get_backend()
    
    for correction in corrections:
        write_correction(
            config=config,
            invoice_id=correction["invoice_id"],
            transaction_ids=correction["transaction_ids"],
            corrected_category=correction["corrected_category"],
            comment=correction.get("comment"),
            corrected_by=correction.get("corrected_by", "user"),
            backend=backend,
        )


def get_correction_history(
    config: Config,
    transaction_id: str,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """
    Get the full correction history for a transaction.
    
    Args:
        config: Application configuration
        transaction_id: Transaction ID to get history for
        backend: Optional backend instance (uses global if not provided)
        
    Returns:
        DataFrame with all historical records for the transaction
    """
    if backend is None:
        backend = get_backend()
    
    schema_prefix = _get_schema_prefix(config)
    query = f"""
        SELECT *
        FROM {schema_prefix}{config.corrections_table}
        WHERE transaction_id = :transaction_id
        ORDER BY start_date DESC
    """
    
    return backend.execute_query(query, {"transaction_id": transaction_id})


def initialize_corrections_table(
    config: Config,
    backend: Optional[DatabaseBackend] = None,
) -> None:
    """
    Create the Type 2 SCD corrections table if it doesn't exist.
    
    Uses PostgreSQL syntax for Lakebase.
    
    Args:
        config: Application configuration
        backend: Optional backend instance (uses global if not provided)
    """
    if backend is None:
        backend = get_backend()
    
    schema_prefix = _get_schema_prefix(config)
    table = config.corrections_table
    
    # PostgreSQL-compatible table creation
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_prefix}{table} (
            correction_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            transaction_id VARCHAR(100) NOT NULL,
            invoice_id VARCHAR(100) NOT NULL,
            category VARCHAR(200) NOT NULL,
            start_date TIMESTAMPTZ NOT NULL,
            end_date TIMESTAMPTZ,
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            comment TEXT,
            corrected_by VARCHAR(100),
            correction_timestamp TIMESTAMPTZ NOT NULL,
            CONSTRAINT unique_current_transaction UNIQUE (transaction_id, start_date)
        )
    """
    
    backend.execute_write(create_query)
    
    # Create indexes for common queries
    index_queries = [
        f"CREATE INDEX IF NOT EXISTS idx_{table}_transaction ON {schema_prefix}{table} (transaction_id)",
        f"CREATE INDEX IF NOT EXISTS idx_{table}_invoice ON {schema_prefix}{table} (invoice_id)",
        f"CREATE INDEX IF NOT EXISTS idx_{table}_current ON {schema_prefix}{table} (is_current) WHERE is_current = TRUE",
    ]
    
    for index_query in index_queries:
        try:
            backend.execute_write(index_query)
        except Exception:
            pass  # Index may already exist


def initialize_invoices_table(
    config: Config,
    backend: Optional[DatabaseBackend] = None,
) -> None:
    """
    Create the invoices table if it doesn't exist.
    
    Uses PostgreSQL syntax for Lakebase.
    
    Args:
        config: Application configuration
        backend: Optional backend instance (uses global if not provided)
    """
    if backend is None:
        backend = get_backend()
    
    schema_prefix = _get_schema_prefix(config)
    table = config.invoices_table
    
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {schema_prefix}{table} (
            invoice_id VARCHAR(100) PRIMARY KEY,
            invoice_number VARCHAR(100) NOT NULL,
            transaction_id VARCHAR(100) NOT NULL UNIQUE,
            vendor_name VARCHAR(300) NOT NULL,
            invoice_date DATE NOT NULL,
            amount DECIMAL(15, 2) NOT NULL,
            category VARCHAR(200),
            confidence_score DECIMAL(5, 4),
            description TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """
    
    backend.execute_write(create_query)
    
    # Create indexes
    index_queries = [
        f"CREATE INDEX IF NOT EXISTS idx_{table}_vendor ON {schema_prefix}{table} (vendor_name)",
        f"CREATE INDEX IF NOT EXISTS idx_{table}_date ON {schema_prefix}{table} (invoice_date)",
        f"CREATE INDEX IF NOT EXISTS idx_{table}_category ON {schema_prefix}{table} (category)",
        f"CREATE INDEX IF NOT EXISTS idx_{table}_confidence ON {schema_prefix}{table} (confidence_score)",
    ]
    
    for index_query in index_queries:
        try:
            backend.execute_write(index_query)
        except Exception:
            pass


def _get_schema_prefix(config: Config) -> str:
    """Get the schema prefix for table references."""
    if config.is_test_mode:
        return ""
    return ""

