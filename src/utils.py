"""Common utilities for Spend Categorization.

- Spark session management (Databricks and Databricks Connect)
- CategorizeConfig for notebooks 3-6
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
    """Check if running on Databricks vs locally with Connect."""
    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        return True
    if os.path.exists("/databricks"):
        return True
    try:
        from pyspark.dbutils import DBUtils  # noqa: F401

        return True
    except ImportError:
        pass
    return False


def get_spark() -> SparkSession:
    """Get or create a Spark session."""
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing

    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.serverless().getOrCreate()


class CategorizeConfig(BaseModel):
    """Configuration for categorization notebooks (3-6).

    Tables created:
    - prompts: Prompt versions for classification
    - cat_bootstrap: LLM bootstrap classifications
    - cat_catboost: CatBoost predictions
    - cat_vectorsearch: Vector search predictions
    """

    model_config = ConfigDict(extra="ignore")

    catalog: str = "main"
    schema_name: str = "default"

    # Table names
    prompts_table: str = "prompts"
    cat_bootstrap_table: str = "cat_bootstrap"
    cat_catboost_table: str = "cat_catboost"
    cat_vectorsearch_table: str = "cat_vectorsearch"

    @property
    def full_prompts(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.prompts_table}"

    @property
    def full_cat_bootstrap(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.cat_bootstrap_table}"

    @property
    def full_cat_catboost(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.cat_catboost_table}"

    @property
    def full_cat_vectorsearch(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.cat_vectorsearch_table}"

    @classmethod
    def from_yaml(cls, config_path: Optional[str] = None) -> "CategorizeConfig":
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config.yaml"
        else:
            config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        cat = data.get("categorize", {})
        tables = cat.get("tables", {})

        return cls.model_validate(
            {
                "catalog": cat.get("catalog", "main"),
                "schema_name": cat.get("schema", "default"),
                "prompts_table": tables.get("prompts", "prompts"),
                "cat_bootstrap_table": tables.get("cat_bootstrap", "cat_bootstrap"),
                "cat_catboost_table": tables.get("cat_catboost", "cat_catboost"),
                "cat_vectorsearch_table": tables.get(
                    "cat_vectorsearch", "cat_vectorsearch"
                ),
            }
        )


def load_categorize_config(config_path: Optional[str] = None) -> CategorizeConfig:
    """Load configuration for categorization notebooks."""
    return CategorizeConfig.from_yaml(config_path)


# Sample data for testing
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
    """Generate sample invoice data for testing."""
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

        if random.random() < low_confidence_ratio:
            confidence = round(random.uniform(0.3, 0.69), 4)
        else:
            confidence = round(random.uniform(0.7, 0.99), 4)

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
