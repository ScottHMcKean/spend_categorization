# Pipeline notebooks

Six steps that turn raw invoices into classified spend across any number of
taxonomies. Each notebook does one thing. See `../docs/DESIGN.md` for the
full design and `../DATA_FLOW.md` for the tables.

```
0_define_taxonomies   ──►  taxonomies + schema_registry + empty
                            predictions / reviews / ground_truth tables
                            │
1_ingest              ──►  invoices
                            │
2_embed               ──►  pgvector `examples` table (taxonomy leaves +
                            ground-truth invoices), embedded via Unity Gateway
                            │
3_classify            ──►  predictions (source='llm'): embed invoice →
                            retrieve similar examples → LLM picks a code.
                            MLflow-traced; writes status/reason/rationale.
                            │
4_train_catboost      ──►  predictions (source='catboost'): train on
                            ground_truth, predict every invoice (optional)
                            │
5_analytics           ──►  vw_spend_by_<schema> views
```

There is **one classifier design** for every schema: an LLM grounded by
pgvector retrieval, plus optional CatBoost. No per-schema strategy to pick.

Human review is an **in-app action**, not a notebook — the app appends to
`reviews` and upserts `ground_truth`, which then feeds notebooks 2 and 4 on
the next run.

## Adding a new taxonomy

1. Subclass `ClassificationSchema` in `src/schemas/<name>.py` — implement
   `_load_taxonomy_df` (read your asset, return a pandas DataFrame with the
   columns named in the spec). Optionally set `top_k`.
2. Register it in `src/schemas/__init__.py`.
3. Re-run notebooks 0 → 2 → 3 (→ 4 → 5). No edits to the notebooks needed —
   they iterate `list_schemas()`.

## Run end-to-end

```bash
databricks bundle run spend_categorization_pipeline --profile DEFAULT
```

## Useful parameters

- `limit_rows` (classify, default `0` = all) — classify only the first N
  invoices for a fast, cheap run.
- `run_catboost` (default `true`) — set false to skip CatBoost training.
- `min_rows` (train, default `50`) — minimum ground-truth rows before
  CatBoost will train.
- `embed_endpoint` / `llm_endpoint` — the Unity Gateway endpoints for
  embeddings and chat.
```
