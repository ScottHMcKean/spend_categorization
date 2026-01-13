"""Test configuration module."""

import pytest
from src.config import Config, load_config


def test_config_defaults():
    """Test Config default values."""
    # Can't create without required fields, so test loading from YAML
    config = load_config()
    assert config.app_mode in ["test", "prod"]
    assert config.page_size == 50


def test_load_config():
    """Test loading config from YAML file."""
    config = load_config()
    assert isinstance(config, Config)
    
    # Test generate fields
    assert config.company_name
    assert config.catalog
    assert config.schema_name
    assert config.invoices
    
    # Test categorize fields
    assert config.prompts
    assert config.cat_bootstrap
    
    # Test app fields
    assert config.app_mode in ["test", "prod"]
    assert config.invoices_sync
    assert config.reviews


def test_config_mode_properties():
    """Test is_test_mode and is_prod_mode properties."""
    config = load_config()
    if config.app_mode == "test":
        assert config.is_test_mode is True
        assert config.is_prod_mode is False
    else:
        assert config.is_test_mode is False
        assert config.is_prod_mode is True


def test_config_table_paths():
    """Test full table path properties work."""
    config = load_config()
    
    # Generate paths
    assert config.full_invoices_table_path == f"{config.catalog}.{config.schema_name}.{config.invoices}"
    assert config.full_categories_table_path == f"{config.catalog}.{config.schema_name}.{config.categories_table}"
    
    # Categorize paths
    assert config.full_prompts_table_path == f"{config.catalog}.{config.schema_name}.{config.prompts}"
    assert config.full_cat_bootstrap_table_path == f"{config.catalog}.{config.schema_name}.{config.cat_bootstrap}"
