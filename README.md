# Spend Categorization

Databricks procurement accelerator that classifies invoices into a 3-level category hierarchy using LLM bootstrap, CatBoost, and vector search. Includes a React + FastAPI app for reviewing classifications and monitoring pipeline status. Runs on serverless compute.

## Data Flow

The pipeline has two jobs: a full pipeline (`spend_categorization_pipeline`) and an accuracy-only job (`quantify_accuracy`). Both are fully parameterized -- you can point them at any catalog/schema/endpoint.

### Full Pipeline

```
                          +-- config.yaml defaults --+
                          |  catalog: shm             |
                          |  schema: spend            |
                          |  llm_endpoint: claude-4-5 |
                          +---------------------------+

[0_generate]
  reads:  config.yaml (company, categories CSV, plants, cost centres)
  writes: {catalog}.{schema}.invoices_raw   (Python-generated rows)
          {catalog}.{schema}.invoices       (LLM-enriched descriptions via AI_QUERY)
          {catalog}.{schema}.categories     (3-level hierarchy from assets/categories.csv)
      |
      v
[1_eda]
  reads:  {catalog}.{schema}.invoices
  writes: combined text features, basic EDA
      |
      +------+------------------+-----------------+
      |      |                  |                 |
      v      v                  v                 v
[2_bootstrap]  [4_catboost]    [5_vectorsearch]
  reads:       reads:          reads:
    invoices     invoices        invoices
    categories   (train/test)    categories (hierarchy)
    prompts                      vs_index (vector search)
  writes:      writes:          writes:
    cat_bootstrap  cat_catboost    invoices_vs (combined text)
    prompts        MLflow model    vs_index (delta sync index)
    categories_str                 cat_vectorsearch
      |                |                 |
      v                |                 |
[3_optimize]           |                 |
  reads:               |                 |
    cat_bootstrap      |                 |
    corrections        |                 |
  writes:              |                 |
    MLflow prompt      |                 |
      |                |                 |
      +----------------+-----------------+
                       |
                       v
              [6_sync_to_lakebase]
                reads:  all Delta tables above
                writes: Lakebase PostgreSQL synced tables
                        cat_reviews (native, writable)
                       |
                       v
              React + FastAPI App
                reads:  Lakebase synced tables
                writes: cat_reviews (human corrections)
```

### Table Flow Detail

| Step | Input Tables | Output Tables | Records |
|------|-------------|---------------|---------|
| 0_generate | config.yaml, categories.csv | `invoices` (N rows), `categories` (102 rows) | Configurable via `rows` in config.yaml |
| 1_eda | `invoices`, `categories` | Combined text features | Same as invoices |
| 2_bootstrap | `invoices`, `categories`, `prompts` | `cat_bootstrap` | All invoices (batched, 1000/batch, MERGE prevents duplicates) |
| 3_optimize | `cat_bootstrap`, `invoices` | MLflow prompt, `corrections` | 50 misclassified samples for LLM feedback |
| 4_catboost | `invoices` (80% train / 20% test) | `cat_catboost` | Test set only (20% of invoices) |
| 5_vectorsearch | `invoices`, `vs_index` | `cat_vectorsearch`, `invoices_vs` | Test set only (20% of invoices) |
| 6_sync_to_lakebase | All `cat_*` + `invoices` + `categories` | Lakebase synced tables | Mirrors Delta source |

### Lakebase Sync Mapping

| Delta Source | Lakebase Target | Mode | Writable |
|-------------|----------------|------|----------|
| `invoices` | `invoices_synced` | SNAPSHOT | No |
| `categories` | `categories_synced` | SNAPSHOT | No |
| `cat_bootstrap` | `cat_bootstrap_synced` | SNAPSHOT | No |
| `cat_catboost` | `cat_catboost_synced` | SNAPSHOT | No |
| `cat_vectorsearch` | `cat_vectorsearch_synced` | SNAPSHOT | No |
| -- | `cat_reviews` (native) | -- | Yes |

## Running the Pipeline

### Deploy and run with defaults

```bash
databricks bundle deploy
databricks bundle run spend_categorization_pipeline
```

### Run with a different catalog/schema (new dataset)

```bash
databricks bundle run spend_categorization_pipeline \
  --params catalog=my_catalog,schema=my_schema,llm_endpoint=databricks-claude-sonnet-4-5
```

### Run accuracy-only (skip generate, skip sync)

```bash
databricks bundle run quantify_accuracy \
  --params catalog=shm,schema=spend
```

### Bundle Variables

These are set in `databricks.yml` and flow into job parameters:

| Variable | Default | Used By |
|----------|---------|---------|
| `catalog` | `shm` | All notebooks |
| `schema` | `spend` | All notebooks |
| `llm_endpoint` | `databricks-claude-sonnet-4-5` | 0_generate, 2_bootstrap, 3_optimize |

Job-level parameters also include `batch_size` (default 1000) and `max_batches` (default 100) for controlling LLM batch inference costs.

## Notebooks

All notebooks live in `notebooks/` and are self-contained.

| # | Notebook | Purpose | Output |
|---|----------|---------|--------|
| 0 | `0_generate.ipynb` | Generate synthetic invoices with LLM-enriched descriptions | `invoices`, `categories` |
| 1 | `1_eda.ipynb` | EDA, combined text features, train/test split | `combined`, `train`, `test` |
| 2 | `2_bootstrap.ipynb` | LLM classification (all rows, batched, confidence 1-5) | `cat_bootstrap`, `prompts` |
| 3 | `3_optimize.ipynb` | Prompt optimization from human reviews | Updated prompt in MLflow |
| 4 | `4_catboost.ipynb` | Hierarchical CatBoost (L1 -> L2) | `cat_catboost`, MLflow model |
| 5 | `5_vectorsearch.ipynb` | Few-shot RAG via vector search index | `cat_vectorsearch` |
| 6 | `6_sync_to_lakebase.ipynb` | Sync Delta tables to Lakebase PostgreSQL | Synced tables + native `cat_reviews` |

## App (React + FastAPI)

The app lives in `packages/app/` and is built with the Databricks `apx` scaffold. The FastAPI backend wraps the existing Python modules (`src/app/database.py`, `src/app/queries.py`, `src/app/reviews.py`) and preloads all tables into memory at startup for fast responses.

| Page | Route | Description |
|------|-------|-------------|
| Overview | `/` | Pipeline Sankey diagram, live table status, workflow steps |
| Classifications | `/classifications` | Browse by source (Bootstrap, CatBoost, Vector Search, Reviews), inline corrections |
| Spend Review | `/review` | Search invoices, flag low-confidence items, one-by-one review queue |
| Analytics | `/analytics` | Spend treemaps, monthly trends, top suppliers, model coverage |
| Platform | `/comparison` | Databricks strengths (6 cards), future projects, TCO comparison |

## Tables (`shm.spend`)

| Table | Source | Description |
|-------|--------|-------------|
| `invoices` | 0_generate | Invoices with LLM-enriched descriptions |
| `categories` | 0_generate | 3-level category hierarchy from CSV |
| `cat_bootstrap` | 2_bootstrap | LLM predictions (all invoices, batched) |
| `cat_catboost` | 4_catboost | CatBoost L1/L2 predictions |
| `cat_vectorsearch` | 5_vectorsearch | Vector search RAG predictions |
| `cat_reviews` | App | Append-only human corrections |

All `cat_*` tables share: `order_id`, `pred_level_1`, `pred_level_2`, `pred_level_3`, `confidence`, `source`.

## Development

```bash
uv sync
uv run pytest

# Start the app locally (backend + frontend dev servers)
cd packages/app && apx dev start
```

## Deployment

```bash
databricks bundle deploy
databricks bundle run spend_categorization_pipeline
cd packages/app && apx build && databricks apps deploy spend-app
```
