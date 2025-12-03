# Project Structure Summary

## Root Directory Files

- `config.yaml` - Single configuration file for all settings (database, app, UI, etc.)
- `app.py` - Main Streamlit application entry point
- `examples.py` - Usage examples for the invoice app modules
- `run.sh` - Helper script for common commands
- `pyproject.toml` - Python dependencies and project metadata
- `README.md` - Comprehensive documentation

## Source Code (`src/`)

### Main Package (`src/invoice_app/`)
- `__init__.py` - Package initialization and exports
- `config.py` - Configuration management with YAML support
- `database.py` - Databricks connection and query execution
- `queries.py` - Invoice query functions
- `corrections.py` - Type 2 SCD write operations
- `ui_components.py` - Streamlit UI components
- `demo_data.py` - Demo data generator for testing without Databricks

### Utility Scripts (`src/`)
- `setup_database.py` - Database table and view creation script
- `generate_sample_data.py` - Sample data generation for Databricks

## Tests (`tests/`)
- `conftest.py` - Pytest fixtures
- `test_config.py` - Configuration tests

## Data Files
- `us_tm_mfg_transactions.csv` - Sample spend categorization dataset
- Notebook files (0_eda.ipynb, 1_batch_inference_naive.ipynb, etc.)

## Key Features

✅ **Single YAML Configuration** - All settings in `config.yaml`
✅ **Demo Mode** - Test the UI without Databricks connection
✅ **Modular Design** - Clean separation of concerns
✅ **Type 2 SCD** - Full audit trail for corrections
✅ **Comprehensive Tests** - Pytest-based testing
✅ **Helper Scripts** - Easy commands via `run.sh`


