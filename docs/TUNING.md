# Tuning the classifier with the feedback loop

> How to take a cold-start run (like the first skunkworks run: three_level
> 83% classified, gl_map 47%, unspsc 0%) to production accuracy — and how an
> agent can do it for you. This is deliberately prescriptive: follow the
> steps in order.

## The one idea

**You do not guess at tuning. You read the `reason` column.** Every
prediction row records *why* it landed where it did, and each reason maps to
exactly one lever. Ground truth is the fuel for almost every lever.

## Step 0 — the scorecard (how you measure)

Everything is measured from `predictions` joined to `ground_truth`. Three
numbers per schema, per source:

| Metric | Definition | Target |
|---|---|---|
| **Coverage** | % of invoices with `status='classified'` | ≥ 95% |
| **Accuracy** | of invoices that have ground truth, % where predicted `code` = ground-truth `code` | ≥ 90% |
| **Reason mix** | the distribution of `reason` over the unclassified rows | drives the lever |

Notebook `6_evaluate` prints this scorecard. You re-read it every cycle —
tuning is "move these numbers," nothing more.

## Step 1 — diagnose from the reason mix

Look at the dominant `reason` for a schema and pull the matching lever. **Pull
one lever at a time, re-score, keep it only if the scorecard improved.**

| Dominant symptom | What it means | Lever (in priority order) |
|---|---|---|
| `no_candidates` high (e.g. unspsc 100%) | pgvector returned nothing — the taxonomy isn't embedded and there's no ground truth to retrieve | 1. Seed ground truth for that schema (even 50–100 rows). 2. Embed the taxonomy (lower `max_taxonomy_rows` or raise it to cover the schema). 3. Raise `top_k`. |
| `out_of_taxonomy` high (e.g. gl_map 53%) | the LLM answered but picked a code **not in the retrieved candidates** (or returned empty) — a recall or prompt problem | 1. Add ground-truth examples (strongest lever — real invoice→code pairs). 2. Raise `top_k` so the right code is in the candidate set. 3. Enrich example text (add code definitions/synonyms). 4. Tighten the prompt. |
| `parse_error` | the model didn't return clean JSON | tighten the JSON instruction; lower `max_tokens`. |
| classified but **wrong** (accuracy low, confidence high) | the model confidently chose a near-neighbour | add targeted ground truth for the confused classes. |
| classified but **low confidence** | the model is unsure | review these first — they are the highest-value rows to turn into ground truth. |

The levers are all small, named knobs: `top_k` (per schema in `SchemaSpec`),
`max_taxonomy_rows` and the embedding corpus (notebook 2), the prompt
(`_build_prompt` in `src/categorize.py`), and — above all — **ground truth**.

## Step 2 — the feedback loop *is* the tuning loop

```
classify (nb 3/4)
   │   predictions + reason + confidence
   ▼
review the right rows, in this order:            ← Step 3
   1. every out_of_taxonomy / no_candidates
   2. classified with confidence < threshold
   3. llm-vs-catboost disagreements
   │   confirm / correct  →  reviews  +  ground_truth
   ▼
re-embed (nb 2) + retrain CatBoost (nb 4)         ← ground truth now fuels both
   ▼
re-classify (nb 3) + re-score (nb 6)              ← compare to last cycle
   ▼
pull the lever the scorecard points to → repeat
```

**How much to label?** Start with the 200 lowest-confidence + all
unclassified invoices per schema (or ~5–10 examples per leaf you care about),
whichever is smaller. Re-measure, then label the next worst slice. Accuracy
climbs fastest on the rows the model is currently worst at — which is exactly
what the review priority order surfaces.

**Why it works:** ground truth improves the system twice — the labelled
invoices become pgvector few-shot examples (helping the LLM pick the right
code) *and* the CatBoost training set (a second, cheap classifier). The gl_map
`out_of_taxonomy` problem and the unspsc `no_candidates` problem both dissolve
once there is ground truth to retrieve.

## Step 3 — can an agent do this automatically?

Yes. The reason→lever table above is a decision rule, so it automates cleanly.
Two options, simplest first:

### A. Rule-based tuner (recommended)
A plain loop — no LLM in the optimizer, fully auditable:

```
read scorecard → pick the dominant reason → apply its mapped lever
  → reclassify a sample → keep the change if accuracy/coverage improved, else revert
  → stop when targets are met or no lever helps
```

It only ever turns the named knobs (`top_k`, `max_taxonomy_rows`, embedding
corpus, prompt variant). Every step is logged with before/after scores, so the
team can see *why* it made each change. This fits the "keep it simple" bar and
is the right first build.

### B. MLflow GenAI optimizer (advanced, opt-in later)
Treat `ground_truth` as an MLflow evaluation dataset, score with an exact-match
/ Correctness scorer, and run `optimize_prompts()` (GEPA) to auto-rewrite the
classifier prompt; monitor with MLflow evaluation in production. More moving
parts, but Databricks-native and powerful once the manual loop is proven.
See the `databricks-mlflow-evaluation` skill.

**Recommendation:** ship the scorecard + the rule-based tuner first. Reach for
the MLflow optimizer only when you want automated prompt rewriting beyond the
knobs above.

## The human's job, in one line

Label the rows the app puts in front of you (worst first). The loop — and, if
you want, the tuner — does the rest.
