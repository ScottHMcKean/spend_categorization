"""Versioning utilities for prompts and category changes."""

import hashlib
import logging
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def compute_categories_hash(categories: Dict) -> str:
    """
    Compute a hash of the category hierarchy for change detection.
    
    Args:
        categories: Category hierarchy dictionary
        
    Returns:
        SHA256 hash of the stringified categories
    """
    # Sort keys for consistent hashing
    import json
    categories_str = json.dumps(categories, sort_keys=True)
    return hashlib.sha256(categories_str.encode()).hexdigest()[:16]


def check_categories_changed(spark, config, current_hash: str) -> bool:
    """
    Check if categories have changed since last recorded version.
    
    Args:
        spark: Spark session
        config: Config object
        current_hash: Current categories hash
        
    Returns:
        True if categories have changed or no previous version exists
    """
    try:
        # Try to get the latest category string
        latest_df = spark.sql(f"""
            SELECT categories_hash 
            FROM {config.full_categories_str_table_path}
            ORDER BY created_at DESC 
            LIMIT 1
        """)
        
        if latest_df.count() == 0:
            logger.info("No previous category version found")
            return True
            
        latest_hash = latest_df.first()["categories_hash"]
        changed = latest_hash != current_hash
        
        if changed:
            logger.info(f"Categories changed: {latest_hash} -> {current_hash}")
        else:
            logger.info("Categories unchanged")
            
        return changed
        
    except Exception as e:
        logger.warning(f"Could not check category changes: {e}")
        return True  # Assume changed if we can't check


def check_prompt_changed(spark, config, current_prompt: str) -> bool:
    """
    Check if prompt has changed since last recorded version.
    
    Args:
        spark: Spark session
        config: Config object
        current_prompt: Current prompt text
        
    Returns:
        True if prompt has changed or no previous version exists
    """
    try:
        # Try to get the latest prompt
        latest_df = spark.sql(f"""
            SELECT prompt 
            FROM {config.full_prompts_table_path}
            ORDER BY created_at DESC 
            LIMIT 1
        """)
        
        if latest_df.count() == 0:
            logger.info("No previous prompt version found")
            return True
            
        latest_prompt = latest_df.first()["prompt"]
        changed = latest_prompt.strip() != current_prompt.strip()
        
        if changed:
            logger.info("Prompt has changed")
        else:
            logger.info("Prompt unchanged")
            
        return changed
        
    except Exception as e:
        logger.warning(f"Could not check prompt changes: {e}")
        return True  # Assume changed if we can't check


def save_prompt_version(spark, config, prompt: str, categories_str: str) -> None:
    """
    Save a new prompt version (only if changed).
    
    Args:
        spark: Spark session
        config: Config object
        prompt: Prompt text
        categories_str: Categories as string (for prompt context)
    """
    from pyspark.sql.functions import current_timestamp, lit
    
    # Check if prompt changed
    if not check_prompt_changed(spark, config, prompt):
        logger.info("Prompt unchanged, skipping save")
        return
    
    # Create prompts table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.full_prompts_table_path} (
            prompt STRING,
            categories STRING,
            created_at TIMESTAMP
        ) USING DELTA
    """)
    
    # Save new version
    prompt_df = spark.createDataFrame(
        [(prompt, categories_str)],
        ["prompt", "categories"]
    ).withColumn("created_at", current_timestamp())
    
    prompt_df.write.format("delta").mode("append").saveAsTable(
        config.full_prompts_table_path
    )
    
    logger.info(f"Saved new prompt version to {config.full_prompts_table_path}")


def save_category_summary(
    spark,
    config,
    categories: Dict,
    categories_str: str,
    summary_stats: Optional[Dict] = None
) -> None:
    """
    Save category summary (only if categories changed).
    
    Args:
        spark: Spark session
        config: Config object
        categories: Category hierarchy dictionary
        categories_str: Categories as formatted string
        summary_stats: Optional dictionary with summary statistics
    """
    from pyspark.sql.functions import current_timestamp, lit
    
    # Compute hash
    categories_hash = compute_categories_hash(categories)
    
    # Check if changed
    if not check_categories_changed(spark, config, categories_hash):
        logger.info("Categories unchanged, skipping save")
        return
    
    # Create table if not exists
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.full_categories_str_table_path} (
            categories_hash STRING,
            categories_string STRING,
            level_1_count INT,
            level_2_count INT,
            level_3_count INT,
            total_categories INT,
            created_at TIMESTAMP
        ) USING DELTA
    """)
    
    # Compute stats if not provided
    if summary_stats is None:
        level_1_count = len(categories)
        level_2_count = sum(len(v) for v in categories.values())
        level_3_count = sum(len(l3) for v in categories.values() for l3 in v.values())
        total_categories = level_1_count + level_2_count + level_3_count
    else:
        level_1_count = summary_stats.get("level_1_count", 0)
        level_2_count = summary_stats.get("level_2_count", 0)
        level_3_count = summary_stats.get("level_3_count", 0)
        total_categories = summary_stats.get("total_categories", 0)
    
    # Save new version
    summary_df = spark.createDataFrame(
        [(
            categories_hash,
            categories_str,
            level_1_count,
            level_2_count,
            level_3_count,
            total_categories
        )],
        [
            "categories_hash",
            "categories_string",
            "level_1_count",
            "level_2_count",
            "level_3_count",
            "total_categories"
        ]
    ).withColumn("created_at", current_timestamp())
    
    summary_df.write.format("delta").mode("append").saveAsTable(
        config.full_categories_str_table_path
    )
    
    logger.info(
        f"Saved new category summary to {config.full_categories_str_table_path} "
        f"(hash: {categories_hash})"
    )
