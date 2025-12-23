"""Reviews table writer for human-in-the-loop corrections.

The app writes reviews to Lakebase. Databricks ingests these back to Delta
and merges into gold.labels using SCD2.
"""

from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd

from .config import AppConfig
from .database import DatabaseBackend, get_backend


def write_review(
    config: AppConfig,
    invoice_id: str,
    category: str,
    schema_id: str = "default",
    prompt_id: str = "default",
    labeler_id: Optional[str] = None,
    source: str = "human",
    backend: Optional[DatabaseBackend] = None,
) -> None:
    """
    Write a single review to the reviews table.

    Args:
        config: App configuration
        invoice_id: Invoice being reviewed
        category: Assigned category
        schema_id: Category schema version
        prompt_id: Prompt version used
        labeler_id: User making the review
        source: 'human' or 'bootstrap'
        backend: Optional backend instance
    """
    if backend is None:
        backend = get_backend()

    if labeler_id is None:
        labeler_id = config.default_user

    now = datetime.now(timezone.utc).isoformat()
    table = config.reviews_table

    query = f"""
        INSERT INTO {table}
        (invoice_id, schema_id, prompt_id, category, label_version_date,
         labeler_id, source, is_current, created_at)
        VALUES (:invoice_id, :schema_id, :prompt_id, :category, :label_version_date,
                :labeler_id, :source, TRUE, :created_at)
    """

    backend.execute_write(
        query,
        {
            "invoice_id": invoice_id,
            "schema_id": schema_id,
            "prompt_id": prompt_id,
            "category": category,
            "label_version_date": now,
            "labeler_id": labeler_id,
            "source": source,
            "created_at": now,
        },
    )


def write_reviews_batch(
    config: AppConfig,
    reviews: List[Dict],
    backend: Optional[DatabaseBackend] = None,
) -> None:
    """
    Write multiple reviews in a batch.

    Args:
        config: App configuration
        reviews: List of review dicts with keys:
            - invoice_id
            - category
            - schema_id (optional)
            - prompt_id (optional)
            - labeler_id (optional)
            - source (optional, default 'human')
        backend: Optional backend instance
    """
    if backend is None:
        backend = get_backend()

    for review in reviews:
        write_review(
            config=config,
            invoice_id=review["invoice_id"],
            category=review["category"],
            schema_id=review.get("schema_id", "default"),
            prompt_id=review.get("prompt_id", "default"),
            labeler_id=review.get("labeler_id"),
            source=review.get("source", "human"),
            backend=backend,
        )


def get_review_history(
    config: AppConfig,
    invoice_id: str,
    backend: Optional[DatabaseBackend] = None,
) -> pd.DataFrame:
    """Get all reviews for an invoice."""
    if backend is None:
        backend = get_backend()

    table = config.reviews_table
    query = f"""
        SELECT *
        FROM {table}
        WHERE invoice_id = :invoice_id
        ORDER BY created_at DESC
    """

    return backend.execute_query(query, {"invoice_id": invoice_id})


def initialize_reviews_table(
    config: AppConfig,
    backend: Optional[DatabaseBackend] = None,
) -> None:
    """Create the reviews table if it doesn't exist."""
    if backend is None:
        backend = get_backend()

    table = config.reviews_table

    create_query = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            label_id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
            invoice_id VARCHAR(100) NOT NULL,
            schema_id VARCHAR(100) NOT NULL,
            prompt_id VARCHAR(100) NOT NULL,
            category VARCHAR(200) NOT NULL,
            label_version_date TIMESTAMPTZ NOT NULL,
            labeler_id VARCHAR(100),
            source VARCHAR(20) NOT NULL DEFAULT 'human',
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """

    backend.execute_write(create_query)

    # Create indexes
    indexes = [
        f"CREATE INDEX IF NOT EXISTS idx_{table}_invoice ON {table} (invoice_id)",
        f"CREATE INDEX IF NOT EXISTS idx_{table}_current ON {table} (is_current) WHERE is_current = TRUE",
    ]

    for idx in indexes:
        try:
            backend.execute_write(idx)
        except Exception:
            pass

