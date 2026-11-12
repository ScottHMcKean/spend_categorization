"""Rule-based tuner (see docs/TUNING.md).

Right now it tunes the one cheapest, highest-impact knob: `top_k` (how many
pgvector candidates the LLM sees). It measures each candidate value against
the labelled sample and reports the best — no LLM in the optimizer, every
number logged, so the team can see why a value was chosen.

It is deliberately a grid search, not magic: embed the sample once, then for
each `top_k` re-run retrieval + the LLM and score accuracy/coverage against
ground truth. Pick the value that scores best; if none beats the current
setting, keep the current one.

Extending it to other levers (prompt variants, embedding corpus) follows the
same shape: add a dimension to the grid, re-score, keep the best.
"""

from __future__ import annotations

from . import vectorstore
from .categorize import _build_prompt, _embed_in_batches, invoice_text
from .llm import chat_json


def tune_top_k(
    spark,
    schema,
    *,
    catalog: str,
    schema_db: str,
    pg_connect,
    client,
    chat_endpoint: str,
    embed_endpoint: str,
    grid=(10, 20, 30, 50),
    sample_size: int = 100,
) -> list[dict]:
    """Grid-search `top_k` for one schema against its ground truth.

    Returns a list of ``{top_k, accuracy, coverage, n}`` dicts, best first,
    and prints a recommendation. Needs ground truth for the schema — seed it
    first (bulk import or review).
    """
    sample = spark.sql(f"""
        SELECT i.order_id, i.supplier, i.description, i.cost_centre, g.code AS truth
        FROM {catalog}.{schema_db}.ground_truth g
        JOIN {catalog}.{schema_db}.invoices i ON i.order_id = g.order_id
        WHERE g.schema_name = '{schema.name}' AND g.code IS NOT NULL
        LIMIT {sample_size}
    """).toPandas()

    if sample.empty:
        print(f"tuner: no ground truth for {schema.name}; seed it first.")
        return []

    texts = [invoice_text(r.supplier, r.description, r.cost_centre) for r in sample.itertuples()]
    vectors = _embed_in_batches(client, embed_endpoint, texts)
    truths = list(sample["truth"].astype(str))
    order_ids = list(sample["order_id"].astype(str))

    conn = pg_connect()
    results: list[dict] = []
    try:
        for k in grid:
            correct = classified = 0
            for text, vec, truth, oid in zip(texts, vectors, truths, order_ids):
                code = _predict_one(conn, schema_db, schema.name, vec, text, client, chat_endpoint, k,
                                    exclude_order_id=oid)
                if code is not None:
                    classified += 1
                    if str(code) == truth:
                        correct += 1
            n = len(texts)
            results.append({
                "top_k": k,
                "accuracy": round(correct / n, 3),
                "coverage": round(classified / n, 3),
                "n": n,
            })
            print(f"  top_k={k:>3}: accuracy={correct / n:.3f} coverage={classified / n:.3f} (n={n})")
    finally:
        conn.close()

    results.sort(key=lambda r: (r["accuracy"], r["coverage"]), reverse=True)
    best = results[0]
    print(f"\nRecommended top_k for {schema.name}: {best['top_k']} "
          f"(accuracy {best['accuracy']}, coverage {best['coverage']}). "
          f"Set it on the schema's SchemaSpec and re-run notebook 3.")
    return results


def _predict_one(conn, schema_db, schema_name, vector, text, client, chat_endpoint, top_k,
                 exclude_order_id=None):
    """Classify one invoice for a given top_k. Returns the chosen code or None."""
    candidates = vectorstore.search(conn, schema_db, schema_name, vector, top_k,
                                    exclude_order_id=exclude_order_id)
    if not candidates:
        return None
    by_code = {c["code"]: c for c in candidates}
    answer = chat_json(client, chat_endpoint, _build_prompt(candidates), text)
    if not answer:
        return None
    code = str(answer.get("code", "")).strip()
    return code if code in by_code else None
