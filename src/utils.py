"""Common utilities for Spend Categorization.

Includes:
- Spark session management (works on Databricks and locally via Databricks Connect)
- Sample data generation for testing
"""

import random
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    """
    Get or create a Spark session.

    Works in both environments:
    - On Databricks: Returns the existing Spark session
    - Locally: Creates a session via Databricks Connect (requires configuration)

    For local development, configure Databricks Connect by running:
        databricks configure

    Or set environment variables:
        DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_CLUSTER_ID

    Returns:
        SparkSession: Active Spark session
    """
    # Try to get existing session first (works on Databricks)
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing

    # No active session - create via Databricks Connect
    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.serverless().getOrCreate()


def is_running_on_databricks() -> bool:
    """Check if code is running on Databricks (vs locally with Connect)."""
    try:
        existing = SparkSession.getActiveSession()
        if existing is None:
            return False
        # Check for Databricks-specific config
        return (
            existing.conf.get("spark.databricks.clusterUsageTags.clusterName", None)
            is not None
        )
    except Exception:
        return False


# Sample data configuration
SAMPLE_VENDORS = [
    "Acme Office Supplies",
    "TechCorp Software",
    "Global Hardware Inc",
    "CloudServices Ltd",
    "Office Depot",
    "Amazon Business",
    "Staples",
    "Dell Technologies",
    "Microsoft Corporation",
    "Adobe Systems",
    "Steel Supply Co",
    "Industrial Components Ltd",
    "FastFreight Logistics",
    "Precision Bearings Inc",
    "ElectroParts USA",
    "SafetyFirst Equipment",
]

SAMPLE_CATEGORIES = [
    "Office Supplies",
    "Software",
    "Hardware",
    "Cloud Services",
    "Consulting",
    "Marketing",
    "Travel",
    "Utilities",
    "Raw Materials",
    "Components",
    "MRO",
    "Logistics & Freight",
    "Bearings & Seals",
    "Electrical Assemblies",
    "Safety & PPE",
]

SAMPLE_DESCRIPTIONS = [
    "Printer paper and toner supplies",
    "Annual software license renewal",
    "Laptop computers for engineering team",
    "Cloud storage subscription - annual",
    "Professional services consulting",
    "Digital marketing campaign Q4",
    "Business travel expenses - conference",
    "Office internet service monthly",
    "Steel plates for manufacturing line",
    "Electronic components batch order",
    "Maintenance supplies and tools",
    "Freight shipping services - international",
    "Roller bearings for turbine assembly",
    "Control cabinet components",
    "Safety equipment and PPE supplies",
]


def generate_sample_invoices(
    count: int = 100,
    seed: Optional[int] = None,
    low_confidence_ratio: float = 0.3,
    null_category_ratio: float = 0.1,
) -> pd.DataFrame:
    """
    Generate sample invoice data for testing.

    Args:
        count: Number of invoices to generate
        seed: Random seed for reproducibility (None for random)
        low_confidence_ratio: Fraction of invoices with low confidence (0.3-0.69)
        null_category_ratio: Fraction of invoices with null category

    Returns:
        DataFrame with sample invoice data
    """
    if seed is not None:
        random.seed(seed)

    invoices = []
    base_date = datetime(2024, 1, 1)

    for i in range(count):
        invoice_date = base_date + timedelta(days=random.randint(0, 365))
        vendor = random.choice(SAMPLE_VENDORS)
        category = random.choice(SAMPLE_CATEGORIES)
        description = random.choice(SAMPLE_DESCRIPTIONS)
        amount = round(random.uniform(50, 5000), 2)

        # Simulate low confidence for some invoices
        if random.random() < low_confidence_ratio:
            confidence = round(random.uniform(0.3, 0.69), 4)
        else:
            confidence = round(random.uniform(0.7, 0.99), 4)

        # Some invoices have null category
        if random.random() < null_category_ratio:
            category = None
            confidence = 0.0

        invoices.append(
            {
                "invoice_id": f"INV{i+1:06d}",
                "invoice_number": f"2024-{i+1:04d}",
                "transaction_id": f"TXN{i+1:06d}",
                "vendor_name": vendor,
                "invoice_date": invoice_date.strftime("%Y-%m-%d"),
                "amount": amount,
                "category": category,
                "confidence_score": confidence,
                "description": description,
            }
        )

    return pd.DataFrame(invoices)


def get_sample_categories() -> List[str]:
    """Get list of sample categories for testing."""
    return SAMPLE_CATEGORIES.copy()
