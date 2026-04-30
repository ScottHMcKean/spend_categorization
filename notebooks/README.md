# Pipeline notebooks

Seven-step accelerator that turns raw invoices into classified spend
across any number of taxonomies. Each notebook does one thing.

```
0_define_taxonomies   ──►  taxonomies + schema_registry + empty cat_predictions
                            │
1_ingest              ──►  invoices Delta table
                            │
                  ┌─────────┴─────────┐
2_index_taxonomies        3_classify          ──►  cat_predictions rows
  builds tsvector            picks the strategy        with one row per
  indexes per schema         declared on the spec      (order_id, schema, source)
                            │
4_train_catboost      ──►  catboost rows in cat_predictions (optional)
                            │
5_review              ──►  agent_review rows + Lakebase sync + cat_reviews
                            │
6_analytics           ──►  vw_spend_by_<schema> views
```

## Strategy decision rule

Each schema declares its strategy via `SchemaSpec.classify_strategy`:

| Strategy | When to pick | How |
|----------|--------------|-----|
| `ai_classify` | leaf count ≤ 300 | Hierarchical SQL `ai_classify` walks the schema's `level_columns`. Each level sees ≤20 candidates (the primitive's hard cap). |
| `retrieval_llm` | leaf count > 300 | Per invoice, tsvector top-K retrieval feeds a closed-set LLM prompt; the model picks. |
| `tsvector` | optional baseline | Top-1 from the tsvector index, no LLM. Cheap reference for benchmarking. |

The current schemas:

| Schema | Leaves | Strategy |
|--------|--------|----------|
| `three_level` | 102 | `ai_classify` |
| `gl_map` | 292 | `ai_classify` |
| `unspsc` | ~150,000 | `retrieval_llm` |

## Adding a new schema

1. **Subclass `ClassificationSchema`** in `src/schemas/<name>.py`.
   Implement `_load_taxonomy_df` (read your asset, return a pandas
   DataFrame with the columns named in the spec). Set the spec's
   `classify_strategy` based on the rule above.
2. **Register it** in `src/schemas/__init__.py` `_DEFAULT_REGISTRY`.
3. **Re-run** notebooks 0 → 6 (or just the affected ones — the DAG is
   idempotent and resumable).

That's the entire surface area. No edits to notebooks 2 / 3 / 4 / 5 / 6
are required for a new schema; they iterate `list_schemas()` and read
each schema's strategy off the spec.

## Confidence

Every prediction lands a `confidence` value in [0, 1] in
`cat_predictions.confidence`, normalized via `src.confidence`:

| Source | Raw signal | Normalization |
|--------|------------|---------------|
| `ai_classify` | none (SQL primitive returns label only) | `NULL` (the agent reviewer is the calibration layer for this source) |
| `retrieval_llm` | model self-reports 1–5 | `(x − 1) / 4` |
| `tsvector` | `ts_rank` (tiny, unbounded) | mean of margin, soft-clipped strength, token coverage |
| `catboost` | `predict_proba.max()` | already 0–1 |
| `agent_review` | model self-reports 1–5 | `(x − 1) / 4` |

The unified scale means the `vw_spend_by_<schema>` views can rank
across sources with one ORDER BY.

## Pipeline orchestration

The DAG in `resources/jobs.yml`:

```
define_taxonomies ─┬─► ingest ──────────────┐
                   └─► index_taxonomies ────┴─► classify ─► train_catboost ─► review ─► analytics
```

Run end-to-end:

```bash
databricks bundle run spend_categorization_pipeline --profile DEFAULT
```

## Optional flags

- `also_run_tsvector` (default `false`) — also write a `source='tsvector'`
  baseline row for each schema, useful for benchmarking the LLM
  strategies against a cheap reference.
- `run_catboost` (default `true`) — set false to skip the CatBoost
  training step; the rest of the pipeline still works.
- `limit_rows` (default `0` = all) — classify only the first N invoices.
  Useful for fast iteration / cost control.
