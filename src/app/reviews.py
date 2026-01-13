"""Review submission functions.

Writes to cat_reviews table (append-only).
"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from src.config import Config
from .database import DatabaseBackend

logger = logging.getLogger(__name__)


def write_review(
    config: Config,
    order_id: str,
    source: str,
    original_level_1: str,
    original_level_2: str,
    original_level_3: str,
    reviewed_level_1: str,
    reviewed_level_2: str,
    reviewed_level_3: str,
    reviewer: str,
    review_status: str = "corrected",
    comments: str = "",
    backend: Optional[DatabaseBackend] = None,
) -> None:
    """Write a single review to cat_reviews table."""
    if backend is None:
        from .database import get_backend
        backend = get_backend()

    query = f"""
        INSERT INTO {config.full_cat_reviews_table_path}
        (order_id, source, reviewer, review_date, 
         original_level_1, original_level_2, original_level_3,
         reviewed_level_1, reviewed_level_2, reviewed_level_3,
         review_status, comments, created_at)
        VALUES 
        (:order_id, :source, :reviewer, :review_date,
         :original_level_1, :original_level_2, :original_level_3,
         :reviewed_level_1, :reviewed_level_2, :reviewed_level_3,
         :review_status, :comments, :created_at)
    """
    
    parameters = {
        "order_id": order_id,
        "source": source,
        "reviewer": reviewer,
        "review_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "original_level_1": original_level_1,
        "original_level_2": original_level_2,
        "original_level_3": original_level_3,
        "reviewed_level_1": reviewed_level_1,
        "reviewed_level_2": reviewed_level_2,
        "reviewed_level_3": reviewed_level_3,
        "review_status": review_status,
        "comments": comments,
        "created_at": datetime.now(timezone.utc),
    }
    
    backend.execute_write(query, parameters)
    logger.info(f"Wrote review for order_id={order_id}")


def write_reviews_batch(
    config: Config,
    reviews: List[Dict],
    backend: Optional[DatabaseBackend] = None,
) -> None:
    """Write multiple reviews."""
    for review in reviews:
        write_review(config, **review, backend=backend)


def get_review_history(
    config: Config,
    order_id: str,
    backend: Optional[DatabaseBackend] = None,
) -> List[Dict]:
    """Get review history for an order."""
    if backend is None:
        from .database import get_backend
        backend = get_backend()

    query = f"""
        SELECT *
        FROM {config.full_cat_reviews_table_path}
        WHERE order_id = :order_id
        ORDER BY created_at DESC
    """
    
    df = backend.execute_query(query, {"order_id": order_id})
    return df.to_dict("records") if not df.empty else []


def initialize_reviews_table(
    config: Config,
    backend: Optional[DatabaseBackend] = None,
) -> None:
    """Initialize cat_reviews table if needed."""
    if backend is None:
        from .database import get_backend
        backend = get_backend()

    query = f"""
        CREATE TABLE IF NOT EXISTS {config.full_cat_reviews_table_path} (
            review_id STRING,
            order_id STRING,
            source STRING,
            reviewer STRING,
            review_date DATE,
            original_level_1 STRING,
            original_level_2 STRING,
            original_level_3 STRING,
            reviewed_level_1 STRING,
            reviewed_level_2 STRING,
            reviewed_level_3 STRING,
            review_status STRING,
            comments STRING,
            created_at TIMESTAMP
        )
    """
    
    backend.execute_write(query)
    logger.info("Initialized cat_reviews table")
