# Spend Categorization Solution Accelerator

AI-powered spend categorization using machine learning and generative AI.

## Use Case: Borealis Wind Systems

A $4.2B wind turbine manufacturer with 6 global plants.

**Problem:**
- 40,000 active suppliers, 60% of spend unclassified
- Same parts coded differently across plants
- No volume discounts due to fragmented visibility

**Solution:**
- AI classification across 2M invoice lines
- Clean 3-level taxonomy
- Human-in-the-loop corrections
- $42M identified savings

## Delta Tables

| Layer | Table | Created By | Description |
|-------|-------|------------|-------------|
| Bronze | `invoices_raw` | 0_generate | Python-generated invoice data |
| Silver | `invoices` | 0_generate | LLM-enhanced with descriptions, supplier info |
| Silver | `categories` | 0_generate | Category hierarchy from config |
| Silver | `prompts` | 3_bootstrap | Prompt versions for classification |
| Silver | `cat_bootstrap` | 3_bootstrap | LLM bootstrap classifications |
| Silver | `cat_catboost` | 5_catboost | CatBoost predictions |
| Silver | `cat_vectorsearch` | 6_vector_search | Vector search predictions |
| Gold | `labels` | Pipeline | Human-reviewed labels (SCD2) |

## Lakebase Tables

| Table | Synced From | Purpose |
|-------|-------------|---------|
| `invoices_sync` | invoices | App reads for display |
| `categorization_sync` | cat_bootstrap | Low-confidence filtering |
| `reviews` | App writes | Ingested back to Delta |

## Notebooks

| Notebook | Description |
|----------|-------------|
| `0_generate.ipynb` | Create invoices_raw, invoices, categories |
| `1_infrastructure.ipynb` | Lakebase setup, Delta-to-Lakebase sync |
| `2_eda.ipynb` | Exploratory analysis, train/test splits |
| `3_bootstrap.ipynb` | LLM bootstrap classification |
| `4_optimize_eval.ipynb` | MLflow prompt optimization |
| `5_catboost.ipynb` | CatBoost model training |
| `6_vector_search.ipynb` | RAG with vector similarity |

## Quick Start

### Install
```bash
uv pip install -e ".[dev]"
```

### Run App (Test Mode)
```bash
uv run streamlit run app.py
```

### Run Notebooks
```bash
databricks configure
uv run jupyter lab
```

## Production Setup

1. **Create Lakebase Instance**: Compute > Lakebase Postgres > Create

2. **Configure** `config.yaml`:
```yaml
app:
  mode: prod
  lakebase_instance: "your-instance"
```

3. **Initialize**: Run `1_infrastructure.ipynb`

4. **Run App**: `uv run streamlit run app.py`

## Project Structure

```
spend_categorization/
├── app.py                 # Streamlit app
├── config.yaml            # generate, categorize, app modules
├── src/
│   ├── generate.py        # GenerateConfig (0_generate)
│   ├── utils.py           # CategorizeConfig, Spark session
│   └── app/               # Streamlit modules
│       ├── config.py      # AppConfig
│       ├── database.py    # Mock/Lakebase backends
│       ├── queries.py     # Invoice queries
│       └── reviews.py     # Review writes
├── tests/
└── *.ipynb                # Notebooks
```

## Config Modules

**generate**: Company, categories, invoices_raw → invoices → categories tables

**categorize**: prompts, cat_bootstrap, cat_catboost, cat_vectorsearch tables

**app**: Lakebase tables (invoices_sync, categorization_sync, reviews)

## Development

```bash
uv run pytest tests/ -v
```

## License

See LICENSE file.
