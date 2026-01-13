"""Common utilities for Spend Categorization.

Simple utilities for Spark session management.
"""

import os
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
