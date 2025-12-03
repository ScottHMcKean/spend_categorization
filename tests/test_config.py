"""Test configuration module."""

import os
import pytest
from invoice_app.config import DatabricksConfig, AppConfig


def test_databricks_config_from_env(monkeypatch):
    """Test loading DatabricksConfig from environment variables."""
    monkeypatch.setenv("DATABRICKS_SERVER_HOSTNAME", "test.databricks.com")
    monkeypatch.setenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/test")
    monkeypatch.setenv("DATABRICKS_ACCESS_TOKEN", "test_token")
    monkeypatch.setenv("DATABRICKS_CATALOG", "test_catalog")
    monkeypatch.setenv("DATABRICKS_SCHEMA", "test_schema")
    
    config = DatabricksConfig.from_env()
    
    assert config.server_hostname == "test.databricks.com"
    assert config.http_path == "/sql/1.0/warehouses/test"
    assert config.access_token == "test_token"
    assert config.catalog == "test_catalog"
    assert config.schema == "test_schema"


def test_databricks_config_validation():
    """Test validation of required configuration values."""
    config = DatabricksConfig(
        server_hostname="",
        http_path="",
        access_token="",
    )
    
    with pytest.raises(ValueError, match="DATABRICKS_SERVER_HOSTNAME is required"):
        config.validate()


def test_databricks_config_defaults():
    """Test default values for DatabricksConfig."""
    config = DatabricksConfig(
        server_hostname="test.databricks.com",
        http_path="/sql/1.0/warehouses/test",
        access_token="test_token",
    )
    
    assert config.catalog == "main"
    assert config.schema == "default"


def test_app_config_from_env(monkeypatch):
    """Test loading AppConfig from environment variables."""
    monkeypatch.setenv("INVOICES_TABLE", "test_invoices")
    monkeypatch.setenv("CORRECTIONS_TABLE", "test_corrections")
    monkeypatch.setenv("FLAGGED_INVOICES_VIEW", "test_flagged")
    monkeypatch.setenv("PAGE_SIZE", "100")
    
    config = AppConfig.from_env()
    
    assert config.invoices_table == "test_invoices"
    assert config.corrections_table == "test_corrections"
    assert config.flagged_invoices_view == "test_flagged"
    assert config.page_size == 100


def test_app_config_defaults():
    """Test default values for AppConfig."""
    # Clear environment variables
    for key in ["INVOICES_TABLE", "CORRECTIONS_TABLE", "FLAGGED_INVOICES_VIEW", "PAGE_SIZE"]:
        os.environ.pop(key, None)
    
    config = AppConfig.from_env()
    
    assert config.invoices_table == "invoices"
    assert config.corrections_table == "invoice_corrections"
    assert config.flagged_invoices_view is None
    assert config.page_size == 50

