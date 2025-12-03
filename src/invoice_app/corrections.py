"""Type 2 Slowly Changing Dimension writer for invoice corrections."""

from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd

from .config import DatabricksConfig, AppConfig
from .database import execute_write, execute_query


def write_correction(
    db_config: DatabricksConfig,
    app_config: AppConfig,
    invoice_id: str,
    transaction_ids: List[str],
    corrected_category: str,
    comment: Optional[str] = None,
    corrected_by: str = "user",
) -> None:
    """
    Write a single invoice correction to the Type 2 SCD table.
    
    This creates new records for each transaction with:
    - New classification
    - Timestamp of change
    - Comment explaining the change
    - User who made the change
    
    Args:
        db_config: Databricks connection configuration
        app_config: Application configuration
        invoice_id: ID of the invoice being corrected
        transaction_ids: List of transaction IDs to update
        corrected_category: New category classification
        comment: Optional comment explaining the correction
        corrected_by: Username of person making the correction
    """
    current_timestamp = datetime.utcnow().isoformat()
    
    for transaction_id in transaction_ids:
        # Close out the current record (set end_date)
        close_query = f"""
            UPDATE {db_config.catalog}.{db_config.schema}.{app_config.corrections_table}
            SET end_date = :end_date,
                is_current = FALSE
            WHERE transaction_id = :transaction_id
                AND is_current = TRUE
        """
        
        execute_write(
            db_config,
            close_query,
            {"end_date": current_timestamp, "transaction_id": transaction_id}
        )
        
        # Insert new record with corrected category
        insert_query = f"""
            INSERT INTO {db_config.catalog}.{db_config.schema}.{app_config.corrections_table}
            (transaction_id, invoice_id, category, start_date, end_date, 
             is_current, comment, corrected_by, correction_timestamp)
            VALUES (:transaction_id, :invoice_id, :category, :start_date, NULL,
                    TRUE, :comment, :corrected_by, :correction_timestamp)
        """
        
        execute_write(
            db_config,
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
    db_config: DatabricksConfig,
    app_config: AppConfig,
    corrections: List[Dict],
) -> None:
    """
    Write multiple invoice corrections in a batch.
    
    Args:
        db_config: Databricks connection configuration
        app_config: Application configuration
        corrections: List of correction dictionaries with keys:
            - invoice_id
            - transaction_ids
            - corrected_category
            - comment (optional)
            - corrected_by
    """
    for correction in corrections:
        write_correction(
            db_config=db_config,
            app_config=app_config,
            invoice_id=correction["invoice_id"],
            transaction_ids=correction["transaction_ids"],
            corrected_category=correction["corrected_category"],
            comment=correction.get("comment"),
            corrected_by=correction.get("corrected_by", "user"),
        )


def get_correction_history(
    db_config: DatabricksConfig,
    app_config: AppConfig,
    transaction_id: str,
) -> pd.DataFrame:
    """
    Get the full correction history for a transaction.
    
    Args:
        db_config: Databricks connection configuration
        app_config: Application configuration
        transaction_id: Transaction ID to get history for
        
    Returns:
        DataFrame with all historical records for the transaction
    """
    query = f"""
        SELECT *
        FROM {db_config.catalog}.{db_config.schema}.{app_config.corrections_table}
        WHERE transaction_id = :transaction_id
        ORDER BY start_date DESC
    """
    
    return execute_query(db_config, query, {"transaction_id": transaction_id})


def initialize_corrections_table(
    db_config: DatabricksConfig,
    app_config: AppConfig,
) -> None:
    """
    Create the Type 2 SCD corrections table if it doesn't exist.
    
    Args:
        db_config: Databricks connection configuration
        app_config: Application configuration
    """
    create_query = f"""
        CREATE TABLE IF NOT EXISTS {db_config.catalog}.{db_config.schema}.{app_config.corrections_table} (
            transaction_id STRING NOT NULL,
            invoice_id STRING NOT NULL,
            category STRING NOT NULL,
            start_date TIMESTAMP NOT NULL,
            end_date TIMESTAMP,
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            comment STRING,
            corrected_by STRING,
            correction_timestamp TIMESTAMP NOT NULL,
            CONSTRAINT pk_corrections PRIMARY KEY (transaction_id, start_date)
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
    """
    
    execute_write(db_config, create_query)


