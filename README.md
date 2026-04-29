# Spend Categorization

Databricks procurement accelerator that classifies invoices against any
number of pluggable taxonomies (3-level Direct/Indirect, GL accounts,
UNSPSC commodities, …) using LLM `ai_classify`, supervised CatBoost,
hybrid pgvector search, and an agentic reviewer. Includes a React +
FastAPI app that switches between taxonomies via a single dropdown.
Runs on serverless compute.

## How it works

```
0. Define taxonomies      ─┐
                           │  schema_registry, <name>_taxonomy
1. Ingest invoices         │  invoices
2. LLM bootstrap labels    │  cat_predictions (source='ai_classify')
3. Train + index           │  cat_predictions (source='catboost' | 'pgvector')
4. Human review            │  cat_predictions (source='agent_review') + cat_reviews
5. Classified analytics    │  vw_spend_by_<schema>
                           ┘
```

Every classifier writes into one unified table — `cat_predictions`,
keyed on `(order_id, schema_name, source)` — so adding a new method or a
new taxonomy never forks the schema.

## Notebooks (`notebooks/`)

| # | Notebook | Purpose |
|---|----------|---------|
| 0 | `0_define_taxonomies.ipynb` | Load every `ClassificationSchema` → `<name>_taxonomy`; publish `schema_registry`; create empty `cat_predictions`. |
| 1 | `1_ingest.ipynb` | Generate or load invoices. Replace this notebook with your own ingestion (Lakeflow Connect, Auto Loader, etc.). |
| 2 | `2_bootstrap.ipynb` | `ai_classify` for every taxonomy ≤ 500 leaves; merges into `cat_predictions` with `source='ai_classify'`. |
| 3 | `3_train_and_index.ipynb` | CatBoost (3-level only) + UNSPSC pgvector hybrid search (~150k codes); merges into `cat_predictions` with `source='catboost' / 'pgvector'`. |
| 4 | `4_review.ipynb` | Agent reviews low-confidence rows + syncs every Delta table to Lakebase + creates writable `cat_reviews`. |
| 5 | `5_analytics.ipynb` | Creates `vw_spend_by_<schema>` views joining invoices to predictions. |

## Adding a new taxonomy

```python
# src/schemas/my_new_taxonomy.py
from .base import ClassificationSchema, SchemaSpec, _resolve_asset
import pandas as pd

class MyNewSchema(ClassificationSchema):
    def __init__(self):
        super().__init__(SchemaSpec(
            name="my_new",
            display_name="My New Taxonomy",
            source_path="assets/my_new.csv",
            code_column="id", label_column="name",
            level_columns=["category", "subcategory", "name"],
        ))
    def _load_taxonomy_df(self) -> pd.DataFrame:
        return pd.read_csv(_resolve_asset(self.spec.source_path))
```

Then register it in `src/schemas/__init__.py` (one line) and re-run
notebook 0. The app's schema dropdown picks it up automatically.

## Tables

Single source of truth for every prediction:

| Table | Written by | Description |
|-------|-----------|-------------|
| `invoices` | nb 1 | Source invoice rows |
| `<name>_taxonomy` | nb 0 | One per taxonomy: `code, label, level_path, …` |
| `schema_registry` | nb 0 | Metadata (display name, leaf count) for the UI |
| `cat_predictions` | nbs 2, 3, 4 | Unified `(order_id, schema_name, source)` predictions |
| `cat_reviews` | App | Append-only human corrections (Lakebase native) |
| `vw_spend_by_<schema>` | nb 5 | Best-confidence prediction per invoice per taxonomy |

Lakebase synced mirrors: `invoices_synced`, `cat_predictions_synced`,
`schema_registry_synced`, `three_level_taxonomy_synced`,
`gl_map_taxonomy_synced`. UNSPSC stays in pgvector only (~150k rows).

## Deploy

One command builds the app, deploys the bundle (jobs + Lakebase + app),
pushes the app source, and runs the pipeline:

```bash
./scripts/deploy.sh                  # full pipeline
./scripts/deploy.sh --skip-run       # deploy only
./scripts/deploy.sh --app-only       # build + deploy app, skip pipeline
./scripts/deploy.sh -t prod          # prod target
```

Manual:

```bash
databricks bundle deploy
databricks bundle run spend_categorization_pipeline
cd packages/app && apx build && databricks apps deploy spend-app
```

Override defaults at run time:

```bash
databricks bundle run spend_categorization_pipeline \
  --params catalog=my_catalog,schema=my_schema,llm_endpoint=databricks-claude-sonnet-4-5
```

## App (React + FastAPI)

`packages/app/` — built with the Databricks `apx` scaffold. The schema
selector in the navbar drives every page; selection persists in
`localStorage`. All routes read from `cat_predictions` filtered by the
chosen schema.

| Page | Route | Description |
|------|-------|-------------|
| Overview | `/` | Pipeline Sankey, live table status |
| Classifications | `/classifications` | Predictions for the active schema, filterable by source; inline corrections submit to `cat_reviews` |
| Spend Review | `/review` | Search invoices and queue low-confidence ones for review |
| Analytics | `/analytics` | Spend treemaps, monthly trends, top suppliers, source coverage |
| Platform | `/comparison` | Databricks strengths, TCO comparison |

API (every route is schema-aware):

| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/schemas` | Registered taxonomies (falls back to in-process Python registry) |
| GET | `/api/schemas/{schema}/taxonomy` | Full normalized taxonomy |
| GET | `/api/predictions/{schema}` | Filterable by `source` |
| GET | `/api/predictions/{schema}/{order_id}` | All sources for one invoice |
| GET | `/api/invoices/flagged?schema_name=…&threshold=…` | Low-confidence invoices for a schema |
| GET | `/api/analytics/summary?schema_name=…` | Spend + per-source counts |
| GET | `/api/analytics/accuracy/{schema}?source=…` | Macro precision/recall/F1 (3-level only — needs ground truth) |
| POST | `/api/reviews` | Append human corrections |

## Development

```bash
uv sync --extra dev
uv run pytest                                # 31 tests
cd packages/app && apx dev start             # backend + frontend dev servers
```

## Agentic review

`src/agents/review_agent.py` — the agent receives an invoice plus the
existing prediction and is given six hierarchy tools (`list_level1`,
`list_level2`, `list_level3`, `get_category_description`,
`search_categories`, `validate_path`). It must call
`submit_classification` exactly once with a suggested L1/L2/L3, an
integer confidence 1-5, an `agrees_with_bootstrap` flag, and a short
rationale — the full trace of tool calls is persisted alongside the
prediction (`source='agent_review'`).
