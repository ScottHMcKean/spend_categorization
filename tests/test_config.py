"""Test configuration module."""

import pytest
from src.app.config import AppConfig, Config, load_config


def test_config_defaults():
    """Test AppConfig default values."""
    config = AppConfig()
    assert config.mode == "test"
    assert config.page_size == 50
    assert config.invoices_sync == "invoices_sync"
    assert config.categorization_sync == "categorization_sync"
    assert config.reviews_table == "reviews"


def test_config_from_dict():
    """Test creating AppConfig from dict values."""
    config = AppConfig(
        mode="prod",
        page_size=100,
        lakebase_instance="test-instance",
        invoices_sync="custom_invoices",
        categorization_sync="custom_cat",
        reviews_table="custom_reviews",
    )
    assert config.mode == "prod"
    assert config.page_size == 100
    assert config.lakebase_instance == "test-instance"
    assert config.invoices_sync == "custom_invoices"


def test_app_config_mode_properties():
    """Test is_test_mode and is_prod_mode properties."""
    test_config = AppConfig(mode="test")
    assert test_config.is_test_mode is True
    assert test_config.is_prod_mode is False

    prod_config = AppConfig(mode="prod")
    assert prod_config.is_test_mode is False
    assert prod_config.is_prod_mode is True


def test_load_config():
    """Test loading config from YAML file."""
    config = load_config()
    assert isinstance(config, AppConfig)
    assert config.mode in ["test", "prod"]


def test_config_backward_compat_alias():
    """Test that Config is an alias for AppConfig."""
    assert Config is AppConfig
