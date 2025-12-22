"""Test configuration module."""

import pytest
from invoice_app.config import DatabricksConfig, AppConfig, LakebaseConfig, load_config


def test_databricks_config_from_dict():
    """Test loading DatabricksConfig from dictionary."""
    config_dict = {
        "databricks": {
            "server_hostname": "test.databricks.com",
            "http_path": "/sql/1.0/warehouses/test",
            "access_token": "test_token",
            "catalog": "test_catalog",
            "schema": "test_schema",
        }
    }
    
    config = DatabricksConfig.from_dict(config_dict)
    
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
    
    with pytest.raises(ValueError, match="server_hostname is required"):
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


def test_lakebase_config_from_dict():
    """Test loading LakebaseConfig from dictionary."""
    config_dict = {
        "lakebase": {
            "instance_name": "my-lakebase-instance",
            "database": "my_database",
            "user": "admin",
            "schema": "app_schema",
        }
    }
    
    config = LakebaseConfig.from_dict(config_dict)
    
    assert config.instance_name == "my-lakebase-instance"
    assert config.database == "my_database"
    assert config.user == "admin"
    assert config.schema == "app_schema"


def test_lakebase_config_defaults():
    """Test default values for LakebaseConfig."""
    config_dict = {
        "lakebase": {
            "instance_name": "test-instance",
        }
    }
    
    config = LakebaseConfig.from_dict(config_dict)
    
    assert config.instance_name == "test-instance"
    assert config.database == "databricks_postgres"
    assert config.user == "databricks"
    assert config.schema == "public"


def test_lakebase_config_validation():
    """Test validation of required LakebaseConfig values."""
    config = LakebaseConfig(instance_name="")
    
    with pytest.raises(ValueError, match="instance_name is required"):
        config.validate()


def test_app_config_from_dict():
    """Test loading AppConfig from dictionary."""
    config_dict = {
        "tables": {
            "invoices": "test_invoices",
            "corrections": "test_corrections",
            "flagged_view": "test_flagged",
        },
        "app": {
            "mode": "prod",
            "page_size": 100,
            "default_user": "admin",
        },
    }
    
    config = AppConfig.from_dict(config_dict)
    
    assert config.invoices_table == "test_invoices"
    assert config.corrections_table == "test_corrections"
    assert config.flagged_invoices_view == "test_flagged"
    assert config.page_size == 100
    assert config.mode == "prod"
    assert config.is_prod_mode is True
    assert config.is_test_mode is False


def test_app_config_defaults():
    """Test default values for AppConfig."""
    config = AppConfig.from_dict({})
    
    assert config.invoices_table == "invoices"
    assert config.corrections_table == "invoice_corrections"
    assert config.flagged_invoices_view is None
    assert config.page_size == 50
    assert config.mode == "test"
    assert config.is_test_mode is True
    assert config.demo_mode is True  # Backward compatibility


def test_app_config_demo_mode_backwards_compatibility():
    """Test that demo_mode=true maps to mode=test for backward compatibility."""
    config_dict = {
        "demo_mode": True,
        "app": {
            "mode": "prod",  # Should be overridden by demo_mode
        }
    }
    
    config = AppConfig.from_dict(config_dict)
    
    assert config.mode == "test"
    assert config.is_test_mode is True
    assert config.demo_mode is True


def test_load_config():
    """Test loading configuration from YAML file."""
    # This will load the actual config.yaml
    config = load_config()
    
    assert isinstance(config, dict)
    assert "app" in config or "company" in config
