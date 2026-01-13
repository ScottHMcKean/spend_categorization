"""Test configuration module - generate sections."""

import pytest
import pandas as pd
from pathlib import Path
from src.config import Config, load_config


def test_load_config():
    """Test loading Config from YAML."""
    config = load_config()
    assert isinstance(config, Config)
    assert config.company_name == "Borealis Wind Systems"
    assert config.industry == "Wind turbine manufacturing"
    assert config.categories_file == "categories.csv"


def test_config_has_categories_from_csv():
    """Test that categories are loaded from CSV."""
    config = load_config()
    assert isinstance(config.categories, dict)
    assert "Direct" in config.categories
    assert "Indirect" in config.categories
    assert "Non-Procureable" in config.categories
    
    # Check a specific category hierarchy
    assert "Raw Materials" in config.categories["Direct"]
    assert "Structural steel plate" in config.categories["Direct"]["Raw Materials"]


def test_config_table_paths():
    """Test full table path properties."""
    config = load_config()
    assert config.full_invoices_raw_table_path == f"{config.catalog}.{config.schema_name}.{config.invoices_raw}"
    assert config.full_invoices_table_path == f"{config.catalog}.{config.schema_name}.{config.invoices}"
    assert config.full_categories_table_path == f"{config.catalog}.{config.schema_name}.{config.categories_table}"


def test_get_category_descriptions():
    """Test getting Level 3 descriptions from CSV."""
    config = load_config()
    descriptions = config.get_category_descriptions()
    
    assert isinstance(descriptions, dict)
    assert len(descriptions) > 0
    
    # Check specific descriptions
    assert "Structural steel plate" in descriptions
    assert "Heavy gauge steel" in descriptions["Structural steel plate"]
    assert "CAD license" in descriptions
    assert "Computer-aided design" in descriptions["CAD license"]


def test_csv_file_exists():
    """Test that the categories CSV file exists."""
    config = load_config()
    csv_path = Path(__file__).parent.parent / config.categories_file
    assert csv_path.exists()
    
    # Verify CSV structure
    df = pd.read_csv(csv_path)
    assert "category_level_1" in df.columns
    assert "category_level_2" in df.columns
    assert "category_level_3" in df.columns
    assert "category_level_3_description" in df.columns
    assert "cost_centre" in df.columns
    assert len(df) > 0


def test_config_has_required_fields():
    """Test that config has all required fields."""
    config = load_config()
    
    # Company info
    assert config.company_name
    assert config.industry
    assert len(config.cost_centres) > 0
    assert len(config.plants) > 0
    
    # Delta location
    assert config.catalog
    assert config.schema_name
    
    # Table names
    assert config.invoices_raw
    assert config.invoices
    assert config.categories_table
    
    # LLM settings
    assert config.small_llm_endpoint
    assert config.large_llm_endpoint
    assert config.prompt
    
    # Data generation
    assert config.rows > 0
    assert config.start_date
    assert config.end_date
    assert len(config.distribution) > 0
    assert len(config.python_columns) > 0
    assert len(config.llm_columns) > 0


def test_category_cost_centre_mapping():
    """Test that category to cost centre mapping exists."""
    config = load_config()
    assert len(config.category_cost_centre_mapping) > 0
    
    # Verify some mappings
    assert "Raw Materials" in config.category_cost_centre_mapping
    assert config.category_cost_centre_mapping["Raw Materials"].startswith("CC-")
