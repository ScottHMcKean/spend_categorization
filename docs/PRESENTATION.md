<!--
Spend Categorization on Databricks — presentation deck.
Render with Marp:   marp PRESENTATION.md -o presentation.pdf
Or open in VS Code's Marp preview extension.
-->
---
marp: true
theme: default
size: 16:9
paginate: true
header: 'Spend Categorization on Databricks'
footer: 'github.com/<repo>'
style: |
  section { font-family: 'Inter', system-ui, sans-serif; padding: 40px 60px; }
  h1 { color: #FF3621; }
  h2 { color: #0B2026; border-bottom: 2px solid #E5EBF0; padding-bottom: 8px; }
  code { background: #F7F9FA; padding: 2px 6px; border-radius: 4px; }
  pre { background: #0B2026; color: #fff; padding: 16px; border-radius: 8px; }
  table { border-collapse: collapse; }
  th, td { border: 1px solid #E5EBF0; padding: 6px 12px; text-align: left; }
  th { background: #F7F9FA; }
---

<!-- _class: lead -->

# Spend Categorization on Databricks

A pluggable, taxonomy-aware procurement classifier

3-Level · GL accounts · UNSPSC (~150k codes) · all in one app

---

## The problem

Procurement teams need to map every invoice to a category — but:

- Categories aren't one-size-fits-all: **internal hierarchy**, **GL accounts**, **UNSPSC**, sometimes all three
- LLM-only is expensive and fragile at 100k+ codes
- Hand-coding one classifier per taxonomy is brittle

**Goal:** add a new taxonomy in one Python file, no rewrite of the pipeline or app.

---

## How it works

```
0. Define taxonomies        → schema_registry, <name>_taxonomy
1. Ingest invoices          → invoices
2. LLM bootstrap labels     → cat_predictions (ai_classify / chat)
3. Train + index            → cat_predictions (catboost / tsvector)
4. Human review             → cat_predictions (agent_review) + cat_reviews
5. Classified analytics     → vw_spend_by_<schema>
```

Six notebooks. One unified predictions table. One schema dropdown drives the whole app.

---

## Architecture

```
        ┌──────────────────────────┐
        │  Bundle deploy (DABs)    │
        └──────────────┬───────────┘
                       │
   ┌───────────────────┼─────────────────────────┐
   │                   │                         │
   ▼                   ▼                         ▼
┌─────────┐    ┌────────────────┐         ┌──────────────┐
│ Jobs    │    │ Lakebase       │         │ Databricks   │
│ (6 nbs) │───▶│ Postgres       │◀────────│ App          │
│ on      │    │ + pgvector     │         │ (FastAPI +   │
│serverless│    │ + tsvector     │         │  React)      │
└─────────┘    └────────────────┘         └──────────────┘
   │                   ▲
   ▼                   │ snapshot sync
┌─────────────────────┴──┐
│ Unity Catalog Delta    │
│  invoices              │
│  cat_predictions       │
│  schema_registry       │
│  <name>_taxonomy       │
│  cat_reviews (native)  │
└────────────────────────┘
```

---

## One predictions table — every method writes here

```sql
CREATE TABLE cat_predictions (
  order_id      STRING,
  schema_name   STRING,   -- 'three_level' | 'gl_map' | 'unspsc' | …
  code          STRING,   -- canonical leaf code
  label         STRING,
  level_path    ARRAY<STRING>,
  confidence    DOUBLE,
  rationale     STRING,
  source        STRING,   -- 'ai_classify' | 'catboost' | 'tsvector' | 'agent_review'
  candidates    ARRAY<STRUCT<code, label, score>>,
  classified_at TIMESTAMP
)
```

Keyed on `(order_id, schema_name, source)`. A new method → just emits rows with a new `source`.

---

## Pluggable schemas

```python
class GLMapSchema(ClassificationSchema):
    def __init__(self):
        super().__init__(SchemaSpec(
            name="gl_map",
            display_name="GL Account Map",
            source_path="assets/new_gl_map.xlsx",
            code_column="GL Account",
            label_column="objval_desc",
            level_columns=["nodedesc02", "Level 2", "Level 3", ...],
        ))
    def _load_taxonomy_df(self) -> pd.DataFrame:
        return pd.read_excel(_resolve_asset(self.spec.source_path))
```

Add it to `src/schemas/__init__.py` (one line).
Re-run notebook `0_define_taxonomies`.
The app dropdown picks it up automatically.

---

## Classification strategy is per-schema

| Schema | Codes | Strategy | Notes |
|---|---:|---|---|
| `three_level` | 102 | Hierarchical `ai_classify` (L1 → L2) | ≤20-label cap respected |
| `gl_map` | 292 | Parallel chat completions w/ JSON schema | All candidates in system prompt; cached |
| `unspsc` | ~150k | Postgres tsvector (`ts_rank_cd`) | No embeddings — saves $$ vs pgvector |

`STRATEGY` is a separate map (`src/schemas/__init__.py`) — not a property of the schema. Easy to swap.

---

## Why tsvector for UNSPSC

Initially designed with pgvector embeddings. Critical question we asked:

> "Couldn't we just do tsvector on Postgres for this?"

| | pgvector | tsvector | tsvector + LLM rerank |
|---|---|---|---|
| Ingest cost | ~$15-30 | $0 | $0 |
| Per-query | tiny | tiny | ~$0.001 |
| Synonym handling | ✅ | only via stemming | ✅ |
| Setup | embed 150k codes (1hr+) | GIN index (<1min) | GIN + LLM call |

**Verdict:** When invoice text and UNSPSC titles share vocabulary, tsvector is good enough. Embeddings come back if recall is poor.

---

## Agentic reviewer (notebook 4)

Bootstrap predictions with `confidence < threshold` get a second look:

- **Tools** the agent can call: `list_level1`, `list_level2`, `list_level3`, `get_category_description`, `search_categories`, `validate_path`
- **Forced final tool**: `submit_classification(L1, L2, L3, confidence: 1-5, agrees_with_bootstrap, rationale)`
- **Output**: row in `cat_predictions` with `source='agent_review'` + full tool-call trace

Implementation: ~150 lines, OpenAI-compat tool calling against Foundation Models.

---

## App: FastAPI + React + Lakebase

```
Browser → Databricks App SP → FastAPI (uvicorn) → Lakebase Postgres
                                  │
                                  └─ TanStack Query → schemaName from
                                     localStorage drives every fetch
```

- React TanStack Router file-based routes
- `SchemaProvider` context: dropdown switches `schema_name` → all queries re-fetch
- Backend serves only `cat_predictions` filtered by `schema_name` — no per-method routes
- Native `cat_reviews` Postgres table for human corrections (writable; everything else snapshot-synced from Delta)

---

## Endpoints

| Method | Path | Notes |
|---|---|---|
| GET | `/api/schemas` | falls back to in-process registry if table not loaded |
| GET | `/api/schemas/{n}/taxonomy` | normalized taxonomy rows |
| GET | `/api/predictions/{n}` | filterable by `source` |
| GET | `/api/predictions/{n}/{order_id}` | all methods for one invoice |
| GET | `/api/invoices/flagged?schema_name=…` | low-confidence rows |
| GET | `/api/analytics/summary?schema_name=…` | spend + per-source counts |
| POST | `/api/reviews` | append corrections |

A new method does **not** require new routes.

---

## One-shot deploy

```bash
./scripts/deploy.sh                 # full pipeline
./scripts/deploy.sh --skip-run      # deploy only
./scripts/deploy.sh --app-only      # build + push app
./scripts/deploy.sh -t prod         # prod target
```

The script:
1. Builds the React frontend (`apx build`)
2. Builds the root `spend-categorization` wheel
3. Stitches the wheels into the app's `requirements.txt`
4. `databricks bundle deploy` (jobs + Lakebase + app resources)
5. `databricks apps deploy` (push source)
6. Runs the pipeline

---

## Cost knobs

| Knob | Default | Purpose |
|---|---:|---|
| `rows` (config.yaml) | 1000 | Synthetic invoice count (was 5000) |
| `run_pgvector` | false | Off by default — opt in when you want UNSPSC |
| `run_catboost` | true | Cheap; train on bootstrap labels |
| `unspsc_sample` | 50 | When pgvector is enabled, sample size for queries |
| `review_sample_size` | 20 | How many low-confidence rows the agent audits |

Switch all on for "the full demo," off for "quick smoke."

---

## Lessons learned (failure modes hit)

1. **`ai_classify` 20-label cap** — switched to hierarchical for three_level and parallel chat for gl_map.
2. **Per-row `ai_query` with 100+ candidates** — too slow (2hr+ on 5k rows). Avoid.
3. **Delta column names with spaces** rejected — sanitize to snake_case before write.
4. **Wheel install with stale `pyproject.toml`** — bump version each iteration.
5. **Lakebase SP needs explicit Postgres `GRANT USAGE` + `SELECT`** — workspace-level "CAN_CONNECT" is necessary but not sufficient.
6. **Embedding 150k codes** — questioned if it was worth it; tsvector is "good enough" for taxonomies that share vocabulary with invoices.

---

## Tests + observability

- 31 unit tests (schemas, agent toolset, app routes)
- Pipeline runs surface in Databricks Jobs UI with per-task run pages
- App logs via `databricks apps logs spend-app -p default`
- `vw_spend_by_<schema>` views give Lakeview/Genie a clean entry point

```bash
uv sync --extra dev
uv run pytest                     # 31 tests
cd packages/app && apx dev start  # local FastAPI + Vite HMR
```

---

## Live demo

**App**: `https://spend-app-984752964297111.11.azure.databricksapps.com`

What to click through:
1. Schema dropdown (top nav) — switch between three_level / gl_map / unspsc
2. Classifications page — predictions table, filter by `source`
3. Spend Review — flagged invoices for the active schema
4. Analytics — spend trends + source coverage
5. Submit a correction — writes to native Lakebase `cat_reviews`

---

## Add the next taxonomy in 5 minutes

1. Drop the source file in `assets/`
2. Subclass `ClassificationSchema` in `src/schemas/<name>.py`
3. Register it in `src/schemas/__init__.py`
4. Choose a strategy: ai_classify, ai chat, tsvector, or roll your own
5. `databricks bundle run spend_categorization_pipeline`

Six notebooks. One predictions table. One dropdown. Done.

---

<!-- _class: lead -->

# Questions?

`./scripts/deploy.sh` to try it on your own catalog.
