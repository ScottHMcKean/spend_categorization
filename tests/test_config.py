"""Test configuration module."""

from invoice_app.config import Config, AppConfig, TablesConfig, load_config


def test_config_defaults():
    """Test default values for Config."""
    config = Config()

    assert config.app.mode == "test"
    assert config.app.is_test_mode is True
    assert config.app.is_prod_mode is False
    assert config.app.page_size == 50
    assert config.app.lakebase_instance == ""
    assert config.app.lakebase_dbname == "databricks_postgres"
    assert config.tables.invoices == "invoices"
    assert config.tables.corrections == "invoice_corrections"
    assert config.tables.flagged_view is None
    assert config.flagging.low_confidence_threshold == 0.7
    assert config.search.max_results == 500


def test_config_from_dict():
    """Test loading Config from dictionary."""
    data = {
        "app": {
            "mode": "prod",
            "page_size": 100,
            "lakebase_instance": "my-instance",
            "lakebase_dbname": "my_database",
        },
        "tables": {
            "invoices": "test_invoices",
            "corrections": "test_corrections",
            "flagged_view": "test_flagged",
        },
        "flagging": {
            "low_confidence_threshold": 0.8,
        },
    }

    config = Config.model_validate(data)

    assert config.app.mode == "prod"
    assert config.app.is_prod_mode is True
    assert config.app.page_size == 100
    assert config.app.lakebase_instance == "my-instance"
    assert config.app.lakebase_dbname == "my_database"
    assert config.tables.invoices == "test_invoices"
    assert config.tables.corrections == "test_corrections"
    assert config.tables.flagged_view == "test_flagged"
    assert config.flagging.low_confidence_threshold == 0.8


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

    assert isinstance(config, Config)
    assert config.app is not None
    assert config.tables is not None


def test_config_extra_fields_allowed():
    """Test that extra fields in YAML (like company, data_generation) are allowed."""
    data = {
        "app": {"mode": "test"},
        "company": {"name": "Test Company"},  # Extra field
        "data_generation": {"rows": 1000},  # Extra field
    }

    config = Config.model_validate(data)
    assert config.app.mode == "test"
    # Extra fields should not cause errors
