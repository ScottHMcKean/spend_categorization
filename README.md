# Spend Categorization

Databricks procurement accelerator that classifies invoices using an LLM bootstrap, prompt optimization, CatBoost, and vector search. Includes a Streamlit review app and DABs jobs for end-to-end pipeline orchestration. Everything runs on serverless compute.

## Pipeline

```
0_generate.ipynb     Generate synthetic invoices with LLM-enriched descriptions
        |
1_eda.ipynb          EDA, combined text features, train/test split
        |
        +-- 2_bootstrap.ipynb       LLM classification (confidence 1-5, rationale)
        |           |
        |   3_optimize.ipynb        Prompt optimization via MLflow + corrections
        |
        +-- 4_catboost.ipynb        Hierarchical CatBoost (L1 -> L2)
        |
        +-- 5_vectorsearch.ipynb    Few-shot RAG via vector search index
        |
        +-- 6_sync_to_lakebase.ipynb Sync Delta tables to Lakebase PostgreSQL
```

## Databricks Jobs (DABs)

Defined in `databricks.yml` + `resources/jobs.yml`. All tasks run on serverless compute.

| Job | Purpose | Tasks |
|-----|---------|-------|
| `spend_categorization_pipeline` | Full end-to-end run | generate -> eda -> bootstrap -> optimize, catboost, vectorsearch -> sync_to_lakebase |
| `quantify_accuracy` | Re-classify and measure accuracy | bootstrap -> optimize, catboost, vectorsearch (parallel where independent) |

```bash
databricks bundle deploy
databricks bundle run spend_categorization_pipeline
databricks bundle run quantify_accuracy
```

## Streamlit App

Multi-page app deployed via Databricks Apps (`app.yaml`). Entry point is `home.py`.

| Page | File | Description |
|------|------|-------------|
| Home | `home.py` | Landing page, architecture overview, workflow |
| Spend Review | `pages/1_Spend_Review.py` | Search invoices, flag low-confidence items, submit corrections |
| Analytics | `pages/2_Analytics.py` | Spend treemaps, model accuracy, per-category precision/recall |
| Platform | `pages/3_Platform.py` | Architecture docs, notebook guide, table schemas |
| Comparison | `pages/4_Comparison.py` | Side-by-side vs. incumbent tooling |

```bash
databricks apps deploy spend-categorization --source-code-path .
```

## Tables (`shm.spend`)

| Table | Source | Description |
|-------|--------|-------------|
| `invoices_raw` | 0_generate | Python-generated invoice data |
| `invoices` | 0_generate | Invoices with LLM-enriched descriptions |
| `categories` | 0_generate | 3-level category hierarchy from CSV |
| `combined` | 1_eda | Invoices with combined text features |
| `train` / `test` | 1_eda | Train/test split |
| `categories_str` | 2_bootstrap | Markdown category strings for prompts |
| `prompts` | 2_bootstrap, 3_optimize | Versioned prompts (append-only) |
| `cat_bootstrap` | 2_bootstrap | LLM predictions (pred_level_1/2/3, confidence 1-5, rationale) |
| `corrections` | 2_bootstrap | Misclassified examples with LLM feedback |
| `cat_optimize` | 3_optimize | Predictions from optimized prompt |
| `cat_catboost` | 4_catboost | CatBoost L1/L2 predictions with pred_level_3, confidence, source |
| `cat_vectorsearch` | 5_vectorsearch | Vector search RAG predictions (parsed from LLM JSON) |
| `cat_reviews` | App | Append-only human corrections |

All `cat_*` tables share a standardized prediction schema: `order_id`, `pred_level_1`, `pred_level_2`, `pred_level_3`, `confidence` (DOUBLE, 1-5 scale), `source` (STRING).

## App Modes

Set in `config.yaml` under `app.mode`:

| Mode | Backend | Description |
|------|---------|-------------|
| `test` | `MockBackend` | In-memory CSV data from `assets/` |
| `prod` | `ServerlessBackend` | WorkspaceClient statement execution API |
| `lakebase` | `LakebaseBackend` | Databricks Lakebase PostgreSQL |

## File Structure

```
spend_categorization/
  databricks.yml                 DABs bundle config (dev/prod targets)
  resources/jobs.yml             Job definitions (pipeline + accuracy)
  app.yaml                       Databricks Apps deployment config
  config.yaml                    Unified app/pipeline configuration
  home.py                        Streamlit entry point
  pages/
    1_Spend_Review.py            Human-in-the-loop review
    2_Analytics.py               Spend analytics + model metrics
    3_Platform.py                Architecture documentation
    4_Comparison.py              Competitor comparison
  src/
    config.py                    Config class (from config.yaml)
    utils.py                     get_spark() serverless session
    categorize.py                Categorization utilities
    versioning.py                Prompt versioning
    app/
      database.py                Backend abstraction (Mock/Serverless/Lakebase)
      queries.py                 SQL query functions
      reviews.py                 Review submission logic
      ui_components.py           Streamlit UI components
  0_generate.ipynb               Data generation
  1_eda.ipynb                    EDA + train/test split
  2_bootstrap.ipynb              LLM bootstrap classification
  3_optimize.ipynb               Prompt optimization
  4_catboost.ipynb               CatBoost training + prediction
  5_vectorsearch.ipynb           Vector search RAG classification
  assets/
    categories.csv               Category hierarchy (3 levels)
    invoices.csv                 Mock invoice data (test mode)
    cat_bootstrap.csv            Mock bootstrap predictions (test mode)
    cat_catboost.csv             Mock CatBoost predictions (test mode)
    cat_vectorsearch.csv         Mock vector search predictions (test mode)
    cat_reviews.csv              Mock reviews (test mode)
  tests/
    conftest.py                  Pytest fixtures
    test_config.py               Config loading tests
    test_database.py             Backend tests
    test_generate.py             Generation tests
    test_utils.py                Utility tests
  pyproject.toml                 Dependencies (uv)
```

## Development

```bash
uv sync                          # install dependencies
uv run pytest                    # run tests
uv run streamlit run home.py     # run app locally (test mode)
```

Set `app.mode: lakebase` in `config.yaml` to use Lakebase PostgreSQL for low-latency app queries (recommended for deployed apps). Set `app.mode: prod` to query Delta tables via the SDK statement execution API.

## New Dataset Onboarding

To classify a new company's invoices:

1. Prepare a CSV/Parquet with columns: `order_id`, `supplier`, `description`, `total`, `date`, `cost_centre`, `unit_price`, `category_level_1`, `category_level_2`, `category_level_3` (ground truth columns optional)
2. Update `config.yaml`: set `catalog`, `schema`, and category `distribution` to match the new dataset
3. Update `assets/categories.csv` with the new category hierarchy
4. Run the pipeline: `databricks bundle run spend_categorization_pipeline`
5. Use the Spend Review page to curate ground truth via human corrections

## Recommended Improvements

- **Ingest notebook**: Add a `0_ingest.ipynb` that accepts real CSV/Parquet data, validates schema, and writes to the invoices table -- bypassing synthetic data generation entirely
- **Retraining feedback loop**: Add a `7_retrain.ipynb` that merges `cat_reviews` human corrections back into CatBoost training data, using reviewed labels as ground truth
- **Schema validation**: Add a utility function that validates an input DataFrame has required columns before pipeline execution
- **L1-agnostic generation**: Make `0_generate.ipynb` read L1 category names from `categories.csv` instead of hardcoding `Direct`/`Indirect`/`Non-Procureable`
- **Full-dataset vectorsearch**: Extend `5_vectorsearch.ipynb` to classify the entire dataset, not just a 500-row test sample
