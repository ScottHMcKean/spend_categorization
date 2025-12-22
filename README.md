# Spend Categorization Solution Accelerator

A comprehensive solution for spend analysis and invoice classification correction using machine learning and Databricks. This solution accelerator includes:

1. **ML Model Development**: Notebooks demonstrating various approaches to spend categorization (EDA, naive inference, vector search, CatBoost)
2. **Invoice Correction Tool**: A production-ready Streamlit application for human-in-the-loop classification corrections with full audit trails

## Overview

This solution accelerator demonstrates how to use machine learning and generative AI to categorize spend transactions and serve corrections through an interactive application on Databricks. It includes a complete workflow from model development to production deployment with human oversight.

## Dataset

The included dataset (`us_tm_mfg_transactions.csv`) contains US federal Time and Materials (T&M) contract actions up to $1,000,000 across selected industries from FY 2015-2024, sourced from USAspending.gov. It is intended for spend analysis, vendor profiling, and categorization experiments in capital- and asset-intensive sectors.

### Dataset Details

Each row represents a contract action filtered to:
- **Time Period**: FY 2015-2024
- **Award Amount**: ≤ $1,000,000 USD total obligations
- **Type of Contract Pricing**: Time and Materials

**NAICS sectors**:
- 21 - Mining, Quarrying, and Oil and Gas Extraction
- 22 - Utilities
- 23 - Construction
- 31, 32, 33 - Manufacturing (all three 2-digit manufacturing groupings)

Core fields typically include award/transaction identifiers, action date, recipient (vendor) name, award description, obligated amount, contract pricing type, NAICS code, and agency/department metadata.

### Reproducing the Dataset

To reproduce: go to `USAspending.gov` → "Advanced Search" → "Awards". Set filters:
- Time Period: FY 2015-2024 (select FY 2015 through FY 2024)
- Award Amounts: "$1,000,000.00 and below"
- Award Type: "Contracts"
- Type of Contract Pricing: "Time and Materials"
- NAICS: select codes with first two digits 21, 22, 23, 31, 32, 33

Run the search, then use "Download" → "Transactions".

---

## Invoice Classification Correction Tool

A Streamlit application for correcting invoice classifications with Databricks integration. This tool provides a fast, user-friendly interface for reviewing and correcting invoice categorizations, with full Type 2 SCD (Slowly Changing Dimension) tracking of all changes.

### Features

- **Invoice Search**: Search invoices by invoice number, vendor name, or description
- **Flagged Invoice Review**: Automatically surface invoices with low confidence scores or missing categories
- **Batch Correction**: Select multiple invoices and correct them in a streamlined review interface
- **Type 2 SCD Tracking**: All corrections are logged with full historical tracking
- **Databricks Integration**: Direct connection to Databricks lakehouse tables

### Architecture

This application follows a modular design pattern with clear separation of concerns:

```
spend_categorization/
├── app.py                          # Main Streamlit application
├── config.yaml                     # Application configuration
├── src/
│   ├── setup_database.py           # Database setup script
│   └── invoice_app/                # Reusable modules
│       ├── __init__.py
│       ├── config.py               # Configuration (AppConfig, LakebaseConfig)
│       ├── database.py             # Backend abstraction (MockBackend, LakebaseBackend)
│       ├── queries.py              # Invoice query functions
│       ├── corrections.py          # Type 2 SCD write logic
│       ├── demo_data.py            # Demo data generators
│       └── ui_components.py        # Streamlit UI components
├── tests/                          # Test suite
│   ├── conftest.py                 # Pytest fixtures
│   ├── test_config.py              # Configuration tests
│   └── test_database.py            # Database backend tests
├── notebooks/
│   └── 6_infrastructure_test.ipynb # Infrastructure verification notebook
├── pyproject.toml                  # Python dependencies
└── config.yaml                     # Application configuration
```

#### Database Backend Abstraction

The application uses a backend abstraction layer that supports multiple database backends:

- **MockBackend**: In-memory storage for test mode, no external dependencies
- **LakebaseBackend**: PostgreSQL on Databricks via psycopg2 with OAuth authentication

---

## Infrastructure Setup

The application supports two operating modes:

| Mode | Backend | Use Case |
|------|---------|----------|
| `test` | MockBackend (in-memory) | Local development, demos, UI testing |
| `prod` | Lakebase (PostgreSQL on Databricks) | Production deployment with persistent storage |

### Test Mode (Default)

Test mode requires no database setup. It uses an in-memory mock backend with sample data:

```bash
# Run directly in test mode (default)
uv run streamlit run app.py
```

### Prod Mode (Lakebase)

Prod mode uses **Lakebase** (PostgreSQL on Databricks) for persistent storage.

#### Step 1: Create a Lakebase Instance

1. Navigate to the Lakebase App in your Databricks workspace
2. Click "New project" to create a new Lakebase Postgres instance
3. Note the instance name for configuration

#### Step 2: Configure Lakebase Connection

Update `config.yaml` with your Lakebase settings:

```yaml
app:
  mode: prod  # Switch from 'test' to 'prod'

lakebase:
  instance_name: "your-lakebase-instance"  # Your Lakebase instance name
  database: "spend_categorization"          # Database name
  user: "databricks"                        # OAuth user (default)
  schema: "public"                          # Schema for tables
```

#### Step 3: Initialize Database Tables

Run the setup script to create tables and optionally load sample data:

```bash
# Check connection
uv run python src/setup_database.py --check-connection

# Initialize tables
uv run python src/setup_database.py --init-tables

# Initialize tables and load sample data
uv run python src/setup_database.py --init-tables --load-sample-data --sample-count 500
```

#### Step 4: Run the Application

```bash
uv run streamlit run app.py
```

### Verifying Infrastructure Setup

Use the included test notebook to verify all components are working:

```bash
# Open the infrastructure test notebook
uv run jupyter lab 6_infrastructure_test.ipynb
```

Or run the automated tests:

```bash
# Run all tests
uv run pytest tests/ -v

# Run only infrastructure-related tests
uv run pytest tests/test_database.py -v
```

---

## Quick Start

### Prerequisites

- Python 3.10 or higher
- `uv` package manager installed ([install uv](https://github.com/astral-sh/uv))
- Access to a Databricks workspace with SQL warehouse
- An existing invoices table in Databricks (or use the sample data generator)

### Step 1: Install Dependencies

```bash
# Install all dependencies including Streamlit
uv pip install -e ".[dev]"

# Or use the helper script
./run.sh install
```

### Step 2: Configure Environment

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` with your Databricks credentials:

```bash
# Get these from your Databricks workspace
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_ACCESS_TOKEN=your-personal-access-token

# Your data location
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=default

# Table names (adjust if needed)
INVOICES_TABLE=invoices
CORRECTIONS_TABLE=invoice_corrections
FLAGGED_INVOICES_VIEW=flagged_invoices_view
```

#### Getting Databricks Credentials

1. **Server Hostname**: 
   - In Databricks, go to SQL Warehouses
   - Click your warehouse → Connection Details
   - Copy "Server hostname"

2. **HTTP Path**:
   - Same location as hostname
   - Copy "HTTP path"

3. **Access Token**:
   - Click your profile → User Settings → Access tokens
   - Generate new token
   - Copy and save securely

### Step 3: Set Up Database Tables

Run the setup script to create the corrections table and flagged invoices view:

```bash
uv run python setup_database.py

# Or use the helper script
./run.sh setup
```

This will:
- Create the `invoice_corrections` Type 2 SCD table
- Create the `flagged_invoices_view` view
- Optionally initialize corrections from your existing invoices

#### Optional: Generate Sample Data

If you don't have an existing invoices table, generate sample data:

```bash
uv run python generate_sample_data.py

# Or use the helper script
./run.sh sample
```

### Step 4: Run the Application

Start the Streamlit app:

```bash
uv run streamlit run app.py

# Or use the helper script
./run.sh app
```

The app will open in your browser at `http://localhost:8501`

---

## Using the Application

### 1. Search for Invoices

1. Navigate to the "🔍 Search" tab
2. Enter search terms (invoice number, vendor, or description)
3. Optionally filter by category and set result limit
4. Click "Search"
5. Select invoices using checkboxes
6. Click "Add Selected to Review Queue"

### 2. Review Flagged Invoices

1. Navigate to the "🚩 Flagged Invoices" tab
2. Click "Load Flagged" to retrieve low-confidence or missing-category invoices
3. Select invoices using checkboxes
4. Click "Add Selected to Review Queue"

### 3. Correct Classifications

1. Navigate to the "✏️ Review & Correct" tab
2. Review each invoice in the queue
3. Select the correct category from the dropdown
4. Add optional comments explaining the correction
5. Click "Submit Corrections" to save all changes

All corrections are written to the Type 2 SCD table with full historical tracking.

---

## Database Schema

### Source Invoices Table

Expected schema for the invoices table:

```sql
CREATE TABLE invoices (
    invoice_id STRING NOT NULL,
    invoice_number STRING,
    transaction_id STRING,
    vendor_name STRING,
    invoice_date DATE,
    amount DECIMAL(18,2),
    category STRING,
    confidence_score DOUBLE,
    description STRING
)
```

### Type 2 SCD Corrections Table

The application will create this table automatically:

```sql
CREATE TABLE invoice_corrections (
    transaction_id STRING NOT NULL,
    invoice_id STRING NOT NULL,
    category STRING NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    comment STRING,
    corrected_by STRING,
    correction_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (transaction_id, start_date)
)
```

### Optional Flagged Invoices View

Create a custom view to define which invoices should be flagged:

```sql
CREATE VIEW flagged_invoices_view AS
SELECT *, 
    CASE 
        WHEN confidence_score < 0.5 THEN 1
        WHEN confidence_score < 0.7 THEN 2
        ELSE 3
    END as flag_priority
FROM invoices
WHERE confidence_score < 0.7 OR category IS NULL
```

---

## Type 2 SCD Implementation

This application implements Type 2 Slowly Changing Dimensions for historical tracking:

- Each correction creates a new record with `start_date` = current timestamp
- Previous record is updated with `end_date` = current timestamp and `is_current` = FALSE
- Current record always has `is_current` = TRUE and `end_date` = NULL
- Full audit trail includes who made the change, when, and why (comments)

This approach enables:
- Complete historical tracking of all classification changes
- Point-in-time queries to see classifications at any historical date
- Full audit trail for compliance and analysis
- Easy rollback to previous classifications if needed

---

## Development

### Project Principles

- **Simple, modular design**: Reusable modules with single responsibilities in `src/`
- **Functions over classes**: Favor functional composition over inheritance
- **Real integrations**: Tests use databricks-connect when possible, avoiding mocks
- **Minimal, atomic tests**: Each test focuses on one specific behavior
- **uv for dependency management**: Use `uv run` for all Python execution

### Running Tests

Run the test suite using pytest:

```bash
uv run pytest

# Or use the helper script
./run.sh test
```

### Helper Commands

The `run.sh` script provides convenient commands:

```bash
./run.sh install    # Install dependencies
./run.sh setup      # Set up database tables and views
./run.sh sample     # Generate sample invoice data
./run.sh app        # Start the Streamlit application
./run.sh test       # Run the test suite
./run.sh example    # Run usage examples
./run.sh clean      # Clean up cache files
./run.sh help       # Show all available commands
```

### Code Examples

See `examples.py` for usage examples of the core modules:

```bash
uv run python examples.py
```

---

## Dependencies

### Production
- `databricks-sql-connector` - Databricks SQL connection
- `pandas` - Data manipulation
- `pyarrow` - Arrow format support for Databricks

### Development
- `streamlit` - Web application framework
- `pytest` - Testing framework
- `pytest-mock` - Mocking utilities for tests
- `ipykernel` - Jupyter notebook support

---

## Notebooks

This solution accelerator includes several notebooks demonstrating different approaches to spend categorization:

| Notebook | Description |
|----------|-------------|
| `0_generate_data.ipynb` | Generate synthetic spend transaction data |
| `1_eda_prep.ipynb` | Exploratory data analysis and data preparation |
| `2_pred_naive.ipynb` | Naive batch inference approach |
| `3_optimizer_eval.ipynb` | Optimizer evaluation and hyperparameter tuning |
| `4_vector_search.ipynb` | Vector search-based categorization |
| `5_catboost.ipynb` | CatBoost model for spend categorization |
| `6_infrastructure_test.ipynb` | **Infrastructure verification and testing** |

### Infrastructure Test Notebook

The `6_infrastructure_test.ipynb` notebook provides a comprehensive check of all components:

1. **Configuration Loading**: Verifies config.yaml is properly loaded
2. **Backend Initialization**: Tests MockBackend (test mode) and LakebaseBackend (prod mode)
3. **Query Functions**: Validates search, flagged invoices, and category retrieval
4. **Correction Functions**: Tests the Type 2 SCD write operations
5. **Connection Verification**: For prod mode, verifies Lakebase connectivity

Run this notebook before deploying to production to ensure all components are ready.

---

## Troubleshooting

### Connection Error

If you see "Database Not Connected":
- Check your `.env` file has all required values
- Verify your Databricks access token is valid
- Ensure your SQL warehouse is running

### No Invoices Found

If searches return no results:
- Verify the `INVOICES_TABLE` name is correct
- Check that your Databricks credentials have access to the table
- Try a broader search term

### Module Not Found

If you see import errors:
```bash
# Reinstall dependencies
uv pip install -e ".[dev]"
```

---

## Customization

### Adding Custom Search Fields

Edit `src/invoice_app/queries.py` to add more searchable fields:

```python
search_fields = ["invoice_number", "vendor_name", "description", "custom_field"]
```

### Customizing Flagged Invoice Logic

Modify the `flagged_invoices_view` SQL view or update `get_flagged_invoices()` in `src/invoice_app/queries.py` to implement your business-specific flagging rules.

### Extending the UI

Add new components in `src/invoice_app/ui_components.py` and integrate them into the main app in `app.py`.

---

## Contributing

1. Keep modules focused and testable
2. Use `uv run` for all Python execution
3. Follow the existing patterns for queries and UI components
4. Add tests for new functionality
5. Update documentation as needed

---

## License

See LICENSE file for details.

---

## Solution Accelerator Roadmap

Future enhancements could include:
- Integration with ML models for automated categorization
- Bulk import/export functionality
- Advanced analytics dashboard
- Role-based access control
- Email notifications for flagged invoices
- API endpoints for programmatic access
- Integration with existing ERP systems

---

## Support and Feedback

For questions, issues, or contributions, please open an issue in this repository.
