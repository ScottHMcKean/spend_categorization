# Spend Categorization — Design & Data Model

> Status: proposal / working spec. Started 20261102.
> Audience: the accelerator team and the customer (Borouge). This is the
> single source of truth for *what we are building and why*. When code and
> this document disagree, fix one of them in the same change.

## 1. Why we are redoing this

The accelerator worked in the Borouge engagement, but it grew too many ways
to do the same thing, and the documentation drifted into three conflicting
pictures of the data model. To run this in production — where every
classification has to be **tracked, audited, and explained** — we are
cutting it back to one clear path.

Goals, in priority order:

1. **Simpler.** Two classifiers only: an **LLM grounded by pgvector
   retrieval**, and **CatBoost**. Nothing else.
2. **One data model.** Spend, categorization, corrections, and ground truth
   each have exactly one home, and the feedback loop between them is explicit.
3. **Governed LLM access** through **Unity Gateway** — every model call is
   logged and auditable.
4. **Visible feedback loop + ground truth.** The team can see where ground
   truth lives, bulk-load it, and watch corrections flow back into it.
5. **Explainable.** For any invoice you can answer "why was it classified
   this way?" and "why was it *not* classified?" from traces and a reason
   code — no guessing.

Non-goals: adding taxonomies is still one Python file (we keep the pluggable
multi-taxonomy design), but we are not adding new classification *methods*.

## 2. What we remove

| Removed | Why |
|---|---|
| `ai_classify` hierarchical strategy | 20-label cap forces brittle tree-walking; real taxonomies (UNSPSC ~150k, GL ~300) are too big. |
| `tsvector` top-1 baseline + `retrieval_llm` tsvector path | Replaced by one pgvector retrieval path (same backend as the app). |
| Agentic reviewer with 6 hierarchy tools | Over-built for the value; human review in the app is the audit path instead. |
| `cat_bootstrap` / `cat_catboost` / `cat_vectorsearch` separate tables (old `DATA_FLOW.md`) | Superseded by the unified model below. |
| Conflicting docs | `DATA_FLOW.md` and the strategy sections of `PRESENTATION.md` get rewritten to match this. |

Net effect: `src/categorize.py` collapses from ~650 lines of five strategies
to two classifiers; the strategy enum on `SchemaSpec` goes away.

## 3. Target architecture

```
                         Unity Gateway (governed LLM + embeddings)
                                   ▲            ▲
                                   │            │
┌──────────┐   ingest   ┌──────────┴──┐  embed  │
│  source  │ ─────────► │  invoices   │ ───────►│        ┌───────────────┐
│  spend   │            │  (UC Delta) │         └───────►│   pgvector    │
└──────────┘            └──────┬──────┘  retrieve similar │  (Lakebase)   │
                               │          examples ◄──────┤  - taxonomies │
                   ┌───────────┴───────────┐              │  - ground     │
                   ▼                       ▼              │    truth      │
            ┌─────────────┐        ┌──────────────┐       └───────────────┘
            │ LLM + pgvec │        │   CatBoost    │
            │ classifier  │        │  (on ground   │
            │             │        │   truth)      │
            └──────┬──────┘        └──────┬────────┘
                   └───────────┬──────────┘
                               ▼
                     ┌───────────────────┐
                     │   predictions     │  source = 'llm' | 'catboost'
                     │   (UC Delta)      │  + confidence, reason, trace_id
                     └─────────┬─────────┘
                               │ surfaced in app
                               ▼
                     ┌───────────────────┐   append-only audit log
                     │     reviews       │ ─────────────┐
                     │   (human events)  │              │
                     └───────────────────┘              ▼
                                               ┌───────────────────┐
                                               │   ground_truth    │  current accepted
                                               │   (UC Delta)      │  label per invoice
                                               └─────────┬─────────┘
                                                         │ trains CatBoost
                                                         │ + seeds pgvector examples
                                                         └──────────► (loop back up)
```

Everything persists in **Unity Catalog Delta** as the system of record.
**pgvector (Lakebase Postgres)** is the serving + retrieval layer for the app
and the LLM classifier — it is a mirror/index, never the source of truth.

## 4. The data model (one canonical model)

Five logical entities. This is the heart of the "clear data model" ask —
spend, categorization, corrections, and ground truth are each in exactly one
place.

### 4.1 `invoices` — the spend
One row per invoice/order line. The spend system of record.

| Column | Type | Notes |
|---|---|---|
| `order_id` | STRING | Primary key. |
| `date` | DATE | |
| `supplier` | STRING | |
| `description` | STRING | Free text — the main classification signal. |
| `cost_centre` | STRING | |
| `amount` / `total` | DECIMAL | Spend value for aggregation. |
| `currency` | STRING | |
| ...plant/region/etc. | | Carried through as-is. |

### 4.2 `<name>_taxonomy` + `schema_registry` — the categories
Unchanged in shape. One taxonomy table per schema (`code`, `label`,
`level_path`), plus a registry row (display name, leaf count) that drives the
app dropdown. Pluggable: a new taxonomy is still one Python subclass + one
registry line.

### 4.3 `predictions` — the categorization (machine)
One row per **(order_id, schema_name, source)**, `source ∈ {llm, catboost}`.

| Column | Type | Notes |
|---|---|---|
| `order_id` | STRING | FK → invoices. |
| `schema_name` | STRING | FK → schema_registry. |
| `source` | STRING | `llm` or `catboost`. |
| `code`, `label`, `level_path` | | The predicted leaf. NULL when unclassified. |
| `confidence` | DOUBLE | Normalized 0–1 across both sources. |
| `status` | STRING | `classified` \| `unclassified`. |
| `reason` | STRING | **Why it is (un)classified** — see §6. |
| `rationale` | STRING | The LLM's short reasoning / "thinking". |
| `candidates` | ARRAY<STRUCT<code,label,score>> | What pgvector retrieved. |
| `model_version` | STRING | Endpoint name / CatBoost run id. |
| `trace_id` | STRING | MLflow trace for this prediction (§6). |
| `classified_at` | TIMESTAMP | |

### 4.4 `reviews` — the corrections (append-only audit log)
One row per human review *event*. Never updated or deleted — this is the
audit trail.

| Column | Type | Notes |
|---|---|---|
| `review_id` | STRING | PK. |
| `order_id`, `schema_name` | | What was reviewed. |
| `reviewed_source` | STRING | Which prediction the human looked at. |
| `action` | STRING | `approved` \| `corrected` \| `rejected`. |
| `code`, `label`, `level_path` | | The human's chosen label (for approve/correct). |
| `reviewer` | STRING | Who. |
| `comment` | STRING | |
| `created_at` | TIMESTAMP | When. |

### 4.5 `ground_truth` — the accepted label (training set)
One row per **(order_id, schema_name)** — the *current* accepted truth.
This is the single, visible place the user asked for. It is:

- **Seeded in bulk** by the team importing already-classified spend ("pass in
  the classified ground truth").
- **Grown** by the feedback loop: every `approved`/`corrected` review upserts
  the accepted label here.
- **Consumed** as (a) the CatBoost training set and (b) the few-shot examples
  the LLM classifier retrieves from pgvector.

| Column | Type | Notes |
|---|---|---|
| `order_id`, `schema_name` | | Composite key. |
| `code`, `label`, `level_path` | | The accepted label. |
| `origin` | STRING | `imported` \| `human_review`. |
| `review_id` | STRING | FK → reviews when origin = human_review. |
| `labeled_by` | STRING | |
| `labeled_at` | TIMESTAMP | |

### 4.6 pgvector (Lakebase) — retrieval index, not a source
Holds embeddings for two corpora, both written through Unity Gateway's
embedding endpoint:

- **Taxonomy embeddings** — every `code`/`label`/`level_path`. Used for
  cold-start candidate narrowing.
- **Ground-truth example embeddings** — each labeled invoice's text. Used to
  retrieve "similar examples" for few-shot prompting.

## 5. The two classifiers

### 5.1 LLM + pgvector retrieval (primary)
Per invoice:

1. Embed the invoice text (supplier + description + cost centre) via Unity
   Gateway.
2. Retrieve top-K **similar examples** from pgvector:
   - **Warm** (ground truth exists): nearest labeled invoices → strong
     few-shot signal, plus their labels become the candidate set.
   - **Cold start** (no/thin ground truth): nearest taxonomy leaves →
     candidate codes.
3. Prompt the LLM (via Unity Gateway) with the invoice + the retrieved
   candidates/examples; it returns `{code, confidence, rationale}` restricted
   to the candidate set.
4. Normalize confidence, capture `rationale`, write to `predictions`
   (`source='llm'`) with the `trace_id`.

This scales to arbitrarily large taxonomies — the LLM only ever sees K
candidates, never the full tree, so there is **no 20-label cap** and no
hierarchical tree-walking.

> **Open decision (please confirm):** "similar examples" — I am designing for
> *both* corpora with a cold→warm progression (taxonomy leaves at cold start,
> labeled invoices once ground truth exists). If you only want labeled-invoice
> examples, say so and I drop taxonomy-embedding candidate narrowing.

### 5.2 CatBoost (supervised)
Trained on `ground_truth`. Features from invoice text + structured columns.
Predicts over the taxonomy's leaf set; `predict_proba.max()` is the
confidence. Writes `predictions` (`source='catboost'`). Retrained whenever
ground truth grows meaningfully. Cheap, fast, improves as corrections
accumulate — the ML half of the feedback loop.

## 6. Logging, tracing, and "why wasn't it classified?"

Two complementary mechanisms:

1. **MLflow Tracing** wraps every LLM classification: the embedding call, the
   pgvector retrieval (query + the K candidates returned), the prompt, the raw
   model response, the parse step, and the final decision. The `trace_id`
   lands on the `predictions` row, so the app can deep-link to the exact
   trace. This is how we surface the model's "thinking."
2. **An explicit `reason` + `status`** on every prediction, so unclassified
   rows are self-explaining without opening a trace:

   | `reason` | Meaning |
   |---|---|
   | `ok` | Classified with confidence above threshold. |
   | `low_confidence` | Model answered but below threshold → queued for review. |
   | `no_candidates` | pgvector returned nothing to classify against. |
   | `parse_error` | LLM output could not be parsed to a valid code. |
   | `llm_error` | Gateway/endpoint error (captured, retried, then logged). |
   | `out_of_taxonomy` | Model chose a code not in the taxonomy. |

Plus **Unity Gateway** request/response logging (inference tables) gives a
governed, independent audit of every model call — cost, latency, payloads —
separate from our application traces.

## 7. App changes (track / audit / inspect)

The React + FastAPI app keeps its schema dropdown. Changes:

- **Ground Truth page (new):** browse/search `ground_truth`, see counts per
  schema, and **bulk-import** classified spend (CSV/table) — the team's path
  to seed training data.
- **Classification detail:** for each invoice show the prediction, the
  `rationale` ("thinking"), the retrieved `candidates`, the `confidence`, the
  `reason`, and a link to the MLflow trace.
- **Review flow:** approve / correct / reject → writes a `reviews` event →
  upserts `ground_truth`. The screen shows how many corrections have fed back.
- **Analytics:** spend by category, plus coverage ("% classified", broken down
  by `reason") and accuracy vs ground truth.

## 8. Pipeline (notebooks) after the cut

```
0_define_taxonomies   → taxonomies + schema_registry + empty predictions/reviews/ground_truth
1_ingest              → invoices
2_index               → embed taxonomies + ground truth into pgvector (Unity Gateway)
3_classify            → LLM + pgvector  → predictions(source=llm)   [traced]
4_train_catboost      → train on ground_truth → predictions(source=catboost)
5_analytics           → spend + coverage + accuracy views
```

The standalone `5_review` notebook goes away — review is an app action, not a
batch job. `ground_truth` is upserted transactionally by the app and by the
bulk-import path, and read by notebooks 2 and 4.

## 9. Work plan

- **Phase 0 — this doc.** Agree the model and the open decision in §5.1.
- **Phase 1 — data model.** Create `predictions` / `reviews` / `ground_truth`
  in notebook 0; delete the old strategy enum and `DATA_FLOW.md` drift.
- **Phase 2 — classifiers.** Rewrite `src/categorize.py` to the two paths;
  wire Unity Gateway for LLM + embeddings; add MLflow tracing + `reason`.
- **Phase 3 — feedback loop.** Ground-truth upsert from reviews + bulk import;
  retrain hook.
- **Phase 4 — app.** Ground Truth page, classification detail with trace link,
  review→ground-truth wiring.
- **Phase 5 — comms.** Redraw `docs/PRESENTATION.md` around this story; refresh
  `README.md`.

## 10. Open decisions

1. §5.1 — "similar examples": taxonomy leaves + labeled invoices (proposed), or
   labeled invoices only?
2. Confidence threshold for `low_confidence` (per schema, or global)?
3. Does Borouge have an existing classified spend export we seed `ground_truth`
   from on day one, and in what shape?
4. Keep all three demo taxonomies, or ship with Borouge's real one only?
```
