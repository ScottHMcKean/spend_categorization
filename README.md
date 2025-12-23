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

We keep app writes narrow: the app only writes corrections (app.reviews), Databricks handles promotion to gold and reverse ETL, avoiding complex business logic in the app tier.

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

Delta = system of record, Lakebase = low‑latency OLTP surface for the app, wired via synced tables and a small review table.

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

We version everything that affects classification: prompts, category schemas, and label versions, and always carry schema_id and prompt_id through invoices, bootstrap, and gold tables.
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

## Notes

In my application, I take invoice data, a category spreadsheet, and a prompt. Update append only tables for the categories (saved as a string) and the prompt (saved as a string). Then bootstrap generate the classifications. I then want to take the bootstrap generated categories and review them using an app. The reviewer will look at the invoices, either by a) searching for them, or b) taking invoices flagged with low confidence during the LLM bootstrapping. They will then correct the invoices and save the reviewed invoices to a 'gold' dataset that corresponds with the categories and workflow they are using (these should correspond to a date more than a specific version). We can then used the reviewed invoices to a) evaluate the prompt and llm classification, b) optimize the bootstrap prompt, c) train a catboost model, and d) use vector search to do RAG. I want to design a delta + lakebase schema for that that works with both the backend (bootstrap, optimize, catboost etc) and the app (corrections, searching, flagging).

## Schema -- Delta

Use Delta tables for the “truth” in a Medallion layout (bronze/silver/gold) and Lakebase Postgres tables as the low‑latency store your app talks to, linked via synced tables and IDs rather than duplicating business logic.

### Bronze (raw)
bronze.invoices_raw: one row per raw invoice line or document; store file metadata and raw parsed text.​

### Silver (normalized + bootstrap)

silver.invoices: cleaned invoice facts (invoice_id, customer_id, amounts, dates, text fields, embeddings reference, etc.).

silver.categories: append‑only definitions of category spreadsheets (schema_id, version, JSON/string of categories, created_at).

silver.prompts: append‑only prompt versions (prompt_id, version, prompt_text, created_at, linked schema_id).

silver.bootstrap_categorization: results of the LLM bootstrap run (invoice_id, prompt_id, schema_id, llm_category, full_llm_output, confidence, run_id, created_at).

### Gold (reviewed labels + evaluation)

gold.invoice_labels: human‑reviewed “gold” labels for each workflow/category schema (label_id, invoice_id, schema_id, prompt_id, label_version_date, category, labeler_id, source = 'bootstrap'|'human', is_current, created_at).

gold.catboost_training_set: denormalized training rows (invoice_id, features…, gold_category, schema_id, prompt_id, label_version_date).

## Schema -- Lakebase

Use Lakebase Postgres as the transactional layer for search, review, and corrections

Synced (read‑mostly, from Delta) – created via reverse ETL synced tables:​

app.invoices_synced: synced from silver.invoices (primary key: invoice_id) – app uses this to display/search invoices.

app.bootstrap_synced: synced from silver.bootstrap_classifications (primary key: invoice_id + run_id) – includes llm_category and confidence so you can list “low confidence” invoices.

app.reviews: the app writes directly here, Databricks ingests/CDC this back into Delta and merges into gold.invoice_labels.

## Workflow

### LLM bootstrap phase

Pipeline reads silver.invoices, silver.category_schemas, and silver.prompts, generates classifications with ai_ functions, and writes to silver.bootstrap_classifications with confidence and a run_id.

### Surfacing items for review in the app

Use app.bootstrap_synced to query invoices where confidence < threshold or where reviewers search by invoice_id/keywords.​

App joins app.invoices_synced to show invoice context, and when a reviewer corrects a label, it writes a row into app.reviews.

### Promoting reviews to gold

A Databricks pipeline periodically reads app.reviews (via Lakebase → Delta sync or CDC) and:

Inserts/updates gold.invoice_labels using SCD2‑style for a full label history per invoice. This can be very useful if categories are changed over time.

