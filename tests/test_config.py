"""Test configuration module."""

from src.app.config import AppConfig, Config, load_config


def test_config_defaults():
    """Test default values for AppConfig."""
    config = AppConfig()

    assert config.mode == "test"
    assert config.is_test_mode is True
    assert config.is_prod_mode is False
    assert config.page_size == 50
    assert config.lakebase_instance == ""
    assert config.lakebase_dbname == "databricks_postgres"
    assert config.invoices_table == "invoices"
    assert config.corrections_table == "invoice_corrections"
    assert config.flagged_view is None
    assert config.low_confidence_threshold == 0.7
    assert config.max_results == 500


def test_config_from_dict():
    """Test loading AppConfig from dictionary."""
    data = {
        "mode": "prod",
        "page_size": 100,
        "lakebase_instance": "my-instance",
        "lakebase_dbname": "my_database",
        "invoices_table": "test_invoices",
        "corrections_table": "test_corrections",
        "flagged_view": "test_flagged",
        "low_confidence_threshold": 0.8,
    }

    config = AppConfig.model_validate(data)

    assert config.mode == "prod"
    assert config.is_prod_mode is True
    assert config.page_size == 100
    assert config.lakebase_instance == "my-instance"
    assert config.lakebase_dbname == "my_database"
    assert config.invoices_table == "test_invoices"
    assert config.corrections_table == "test_corrections"
    assert config.flagged_view == "test_flagged"
    assert config.low_confidence_threshold == 0.8


def test_app_config_mode_properties():
    """Test is_test_mode and is_prod_mode properties."""
    test_config = AppConfig(mode="test")
    assert test_config.is_test_mode is True
    assert test_config.is_prod_mode is False

    prod_config = AppConfig(mode="prod")
    assert prod_config.is_test_mode is False
    assert prod_config.is_prod_mode is True


def test_load_config():
    """Test loading configuration from YAML file."""
    config = load_config()

    assert isinstance(config, AppConfig)
    assert config.mode is not None
    assert config.invoices_table is not None


def test_config_backward_compat_alias():
    """Test that Config is an alias for AppConfig."""
    assert Config is AppConfig
