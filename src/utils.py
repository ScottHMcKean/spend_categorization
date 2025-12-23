"""Common utilities for Spend Categorization.

Includes:
- Spark session management (works on Databricks and locally via Databricks Connect)
- CategorizeConfig for notebooks 1-6
- Sample data generation for testing
"""

import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd
import yaml
from pydantic import BaseModel, ConfigDict
from pyspark.sql import SparkSession


def is_running_on_databricks() -> bool:
    """Check if code is running on Databricks (vs locally with Connect).

    Uses multiple detection methods:
    1. Check for DATABRICKS_RUNTIME_VERSION environment variable
    2. Check for /databricks path existence
    3. Try to import dbutils
    """
    # Method 1: Environment variable (most reliable)
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return True

    # Method 2: Check for Databricks filesystem
    if os.path.exists("/databricks"):
        return True

    # Method 3: Try dbutils import
    try:
        from pyspark.dbutils import DBUtils  # noqa: F401

        return True
    except ImportError:
        pass

    return False


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


# =============================================================================
# CategorizeConfig for Notebooks 1-6
# =============================================================================


class CategorizeConfig(BaseModel):
    """Single configuration model for notebooks 1-6 (categorization pipeline).

    Flattens sql and data_generation settings into one model.
    """

    model_config = ConfigDict(extra="ignore")

    # Unity Catalog location
    catalog: str = "main"
    schema_name: str = "default"

    # SQL warehouse settings (optional, for direct SQL access)
    server_hostname: str = ""
    http_path: str = ""
    access_token: str = ""

    @classmethod
    def from_yaml(cls, config_path: Optional[str] = None) -> "CategorizeConfig":
        """Load categorize configuration from YAML file."""
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config.yaml"
        else:
            config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        flat_data = {}

        # Prefer data_generation for catalog/schema (primary source)
        if "data_generation" in data:
            dg = data["data_generation"]
            flat_data["catalog"] = dg.get("catalog", "main")
            flat_data["schema_name"] = dg.get("schema", "default")

        # SQL settings (optional override or additional config)
        if "sql" in data:
            sql = data["sql"]
            # Only override catalog/schema if not already set from data_generation
            if "catalog" not in flat_data:
                flat_data["catalog"] = sql.get("catalog", "main")
            if "schema_name" not in flat_data:
                flat_data["schema_name"] = sql.get("schema", "default")
            flat_data["server_hostname"] = sql.get("server_hostname", "")
            flat_data["http_path"] = sql.get("http_path", "")
            flat_data["access_token"] = sql.get("access_token", "")

        return cls.model_validate(flat_data)


def load_categorize_config(config_path: Optional[str] = None) -> CategorizeConfig:
    """Load configuration for categorization notebooks (1-6)."""
    return CategorizeConfig.from_yaml(config_path)


# =============================================================================
# Sample Data Generation
# =============================================================================

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
