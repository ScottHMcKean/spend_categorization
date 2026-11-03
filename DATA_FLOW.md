# Spend Categorization — Data Flow

> This is the reference for the tables and how data moves between them.
> The authoritative design (rationale, classifiers, feedback loop, tracing)
> lives in [`docs/DESIGN.md`](docs/DESIGN.md). When the two disagree, fix one
> in the same change.

## The model in one picture

```
 invoices ───embed──► pgvector (taxonomies + ground-truth examples)
    │                        ▲  retrieve similar examples
    │                        │
    ├──► LLM + pgvector ──────┐
    │                         ├──► predictions (source='llm' | 'catboost')
    └──► CatBoost ────────────┘        │
         (trained on ground_truth)     │ surfaced in app
                                        ▼
                                     reviews  (append-only audit log)
                                        │  approve / correct
                                        ▼
                                   ground_truth  (accepted label)
                                        │
                                        └──► trains CatBoost + seeds pgvector examples
                                             (loop back to the top)
```

Unity Catalog Delta is the system of record. pgvector (Lakebase Postgres) is
the serving + retrieval index — a mirror, never the source of truth. All LLM
and embedding calls go through **Unity Gateway**.

## Tables

| Table | Grain | Written by | Role |
|-------|-------|-----------|------|
| `invoices` | one invoice/order line | ingest (nb 1) | The spend. |
| `<name>_taxonomy` | one leaf | nb 0 | A category tree (one per schema). |
| `schema_registry` | one schema | nb 0 | Taxonomy metadata that drives the app dropdown. |
| `predictions` | `(order_id, schema_name, source)` | classify (nb 3), train_catboost (nb 4) | Machine predictions, `source ∈ {llm, catboost}`. |
| `reviews` | one review event | app | Append-only audit log of human actions. |
| `ground_truth` | `(order_id, schema_name)` | bulk import + app | Current accepted label; CatBoost training set + LLM example source. |

### `predictions`
`order_id, schema_name, source, code, label, level_path, confidence,
status, reason, rationale, candidates[], model_version, trace_id,
classified_at`.

- `status` is `classified` or `unclassified`; `reason` explains why
  (`ok`, `low_confidence`, `no_candidates`, `parse_error`, `llm_error`,
  `out_of_taxonomy`) so an unclassified row is self-explaining.
- `rationale` is the model's short reasoning; `trace_id` deep-links to the
  MLflow trace for the full "thinking."

### `reviews`
`review_id, order_id, schema_name, reviewed_source, action, code, label,
level_path, reviewer, comment, created_at`. Append-only — never updated or
deleted. `action ∈ {approved, corrected, rejected}`.

### `ground_truth`
`order_id, schema_name, code, label, level_path, origin, review_id,
labeled_by, labeled_at`. `origin ∈ {imported, human_review}`. Upserted on key
`(order_id, schema_name)` — one accepted label per invoice per taxonomy.

## The feedback loop

1. **Classify.** LLM (grounded by pgvector-retrieved similar examples) and
   CatBoost (trained on `ground_truth`) each write a row to `predictions`.
2. **Surface.** The app shows predictions, their confidence, rationale,
   retrieved candidates, and `reason`; low-confidence rows are queued for review.
3. **Review.** A human approves / corrects / rejects → one row appended to
   `reviews`, and the accepted label upserted into `ground_truth`.
4. **Improve.** `ground_truth` grows → CatBoost retrains and new ground-truth
   examples are embedded into pgvector, so both classifiers get better. Loop.

## Pipeline notebooks

```
0_define_taxonomies  → taxonomies + schema_registry + empty predictions/reviews/ground_truth
1_ingest             → invoices
2_index              → embed taxonomies + ground truth into pgvector (Unity Gateway)
3_classify           → LLM + pgvector  → predictions(source='llm')   [MLflow-traced]
4_train_catboost     → train on ground_truth → predictions(source='catboost')
5_analytics          → spend + coverage + accuracy views
```

Review is an in-app action, not a batch notebook.
