# Spend Categorization Solution Accelerator

A Databricks solution for spend analysis and invoice classification using machine learning. Demonstrates how AI can help enterprises gain visibility into their procurement spend and correct misclassified invoices.

## The Story: Borealis Wind Systems

This solution is built around a realistic scenario: **Borealis Wind Systems**, a $4.2B wind turbine manufacturer with 6 global plants facing a "Silent Rust" problem—unclassified spend eating their margins.

**The Problem:**
- 40,000 active suppliers with 60% of spend unclassified or misclassified
- Same parts coded differently across plants ("PPE" vs "Safety Gear" vs "Shop Floor Supplies")
- No volume discounts due to fragmented spend visibility
- Engineers buying $50 "custom" parts that were $2 from existing contracts

**The Solution:**
- AI-powered spend classification across 2M invoice lines
- Clean taxonomy mapping (UNSPSC-style hierarchy)
- Human-in-the-loop correction interface for accuracy
- $42M identified savings in 12 months

## Sample Data

This accelerator generates synthetic spend data for Borealis Wind Systems via `0_data.ipynb`:

- **10,000 invoice transactions** across 2 years
- **3-level category hierarchy**: Direct → Indirect → Non-Procureable
- **32 Level 2 categories** (Bearings & Seals, IT & Software, MRO, etc.)
- **~150 Level 3 categories** (specific items like "Blade shear web", "CAD license")
- **6 plants** across US, Germany, Vietnam, and Brazil
- **Realistic descriptions** generated via LLM batch inference

The data generation uses `config.yaml` to define the company structure, category hierarchy, and distribution parameters.

---

## Solution Components

### Notebooks

| Notebook | Description |
|----------|-------------|
| `0_data.ipynb` | Generate synthetic spend transactions with LLM-enhanced descriptions |
| `1_infrastructure.ipynb` | Setup and test database infrastructure |
| `2_eda_prep.ipynb` | Exploratory data analysis and train/test splits |
| `3_pred_naive.ipynb` | Baseline: naive LLM classification |
| `4_optimizer_eval.ipynb` | MLflow prompt optimization and evaluation |
| `5_vector_search.ipynb` | RAG approach with vector similarity |
| `6_catboost.ipynb` | Traditional ML with CatBoost |

### Invoice Correction App

A Streamlit application for human-in-the-loop classification corrections:

- **Search** invoices by number, vendor, or description
- **Review** flagged low-confidence classifications  
- **Correct** with full Type 2 SCD audit trail
- Works in **test mode** (in-memory) or **prod mode** (Lakebase PostgreSQL)

---

## Quick Start

### Prerequisites

- Python 3.12+
- [uv](https://github.com/astral-sh/uv) package manager
- Databricks workspace (for prod mode and notebook execution)

### Install Dependencies

```bash
uv pip install -e ".[dev]"
```

### Run the App (Test Mode)

```bash
uv run streamlit run app.py
```

Opens at `http://localhost:8501` with sample data—no database required.

### Run Notebooks

The notebooks work both on Databricks and locally via Databricks Connect:

```bash
# Configure Databricks Connect for local development
databricks configure

# Run notebooks locally
uv run jupyter lab
```

---

## Production Setup (Lakebase)

### 1. Create Lakebase Instance

In Databricks: Compute → Lakebase Postgres → Create database instance

### 2. Configure Connection

Update `config.yaml`:

```yaml
app:
  mode: prod
  lakebase_instance: "your-instance-name"
  lakebase_dbname: "databricks_postgres"
```

### 3. Initialize & Test

Run `1_infrastructure.ipynb` to:
- Verify Spark and database connectivity
- Create required tables
- Load sample data (optional)
- Test all query and correction functions

### 4. Run the App

```bash
uv run streamlit run app.py
```

---

## Architecture

```
spend_categorization/
├── app.py                      # Streamlit application
├── config.yaml                 # Configuration (Pydantic models)
├── src/
│   ├── utils.py                # Spark session & sample data utilities
│   └── invoice_app/            # Application modules
│       ├── config.py           # Configuration loading
│       ├── database.py         # Backend abstraction (Mock/Lakebase)
│       ├── queries.py          # Invoice query functions
│       ├── corrections.py      # Type 2 SCD write logic
│       └── ui_components.py    # Streamlit components
├── tests/                      # Pytest test suite
└── *.ipynb                     # Analysis notebooks
```

### Database Backends

| Mode | Backend | Use Case |
|------|---------|----------|
| `test` | MockBackend | Local dev, demos, no database needed |
| `prod` | LakebaseBackend | Production with PostgreSQL on Databricks |

---

## Category Hierarchy

The sample data uses a 3-level spend taxonomy:

**Direct** (10 L2 categories)
- Raw Materials, Components, Sub-Assemblies
- Blades & Hub Parts, Electrical Assemblies, Hydraulic Systems
- Bearings & Seals, Fasteners, Castings & Forgings, Packaging Materials

**Indirect** (18 L2 categories)
- MRO, IT & Software, Facilities & Utilities, Logistics & Freight
- Professional Services, Office Supplies, Training & HR, Travel & Entertainment
- Safety & PPE, Marketing, Telecom, Security, Cleaning, Rent & Leases, etc.

**Non-Procureable** (4 L2 categories)
- Payroll, Government Services & Taxes, Finance Charges, Miscellaneous

---

## Development

### Run Tests

```bash
uv run pytest tests/ -v
```

### Project Principles

- **Simple, modular design**: Functions over classes, composition over inheritance
- **Real integrations**: Use databricks-connect for Spark, avoid mocks where possible
- **uv for everything**: `uv run` for all Python execution

---

## License

See LICENSE file for details.
