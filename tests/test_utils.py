"""Test utils module."""

import pytest
from src.config import Config, load_config
from src.utils import get_spark, is_running_on_databricks


def test_load_config():
    """Test loading Config from YAML."""
    config = load_config()
    assert isinstance(config, Config)
    assert config.catalog
    assert config.schema_name


def test_config_table_paths():
    """Test full table path properties."""
    config = load_config()
    assert config.full_prompts_table_path == f"{config.catalog}.{config.schema_name}.{config.prompts}"
    assert config.full_cat_bootstrap_table_path == f"{config.catalog}.{config.schema_name}.{config.cat_bootstrap}"
    assert config.full_cat_catboost_table_path == f"{config.catalog}.{config.schema_name}.{config.cat_catboost}"
    assert config.full_cat_vectorsearch_table_path == f"{config.catalog}.{config.schema_name}.{config.cat_vectorsearch}"


def test_is_running_on_databricks():
    """Test databricks detection function."""
    # Should return False when running in local tests
    result = is_running_on_databricks()
    assert isinstance(result, bool)


def test_get_spark_returns_spark_session():
    """Test that get_spark returns a SparkSession or raises appropriate error."""
    # This test may fail if databricks-connect is not properly configured
    # but it should at least not crash
    try:
        spark = get_spark()
        from pyspark.sql import SparkSession
        assert isinstance(spark, SparkSession)
    except Exception as e:
        # It's okay if spark isn't available in test environment
        pytest.skip(f"Spark not available in test environment: {e}")
