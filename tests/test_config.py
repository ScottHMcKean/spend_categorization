"""Test configuration module."""

from src.config import Config, ColumnMap, AppTableDef, load_config


def test_config_loads():
    """Test loading config from YAML file."""
    config = load_config()
    assert isinstance(config, Config)
    assert config.company_name
    assert config.catalog
    assert config.schema_name
    assert config.invoices


def test_config_table_paths():
    """Test full table path properties work."""
    config = load_config()
    expected = f"{config.catalog}.{config.schema_name}.{config.invoices}"
    assert config.full_invoices_table_path == expected


def test_config_column_mapping():
    """Test that column mapping loads from YAML."""
    config = load_config()
    assert isinstance(config.col, ColumnMap)
    assert config.col.id
    assert config.col.amount
    assert config.col.category_l1


def test_config_app_tables():
    """Test that app tables load from YAML."""
    config = load_config()
    assert "invoices" in config.app_tables
    assert isinstance(config.app_tables["invoices"], AppTableDef)
    assert config.app_tables["invoices"].table == "invoices"


def test_config_app_table_path():
    """Test app_table_path helper."""
    config = load_config()
    path = config.app_table_path("invoices")
    assert path == f"{config.catalog}.{config.schema_name}.invoices"


def test_config_app_table_native():
    """Test native table detection."""
    config = load_config()
    assert config.app_table_is_native("cat_reviews") is True
    assert config.app_table_is_native("invoices") is False


def test_config_lakebase_required():
    """Test that lakebase_instance is set."""
    config = load_config()
    assert config.lakebase_instance
