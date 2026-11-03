"""The two classifiers. Everything writes to the `predictions` table.

1. `classify_with_llm` — embed each invoice, retrieve similar examples from
   pgvector, and ask the LLM (via Unity Gateway) to pick a code from those
   candidates. This is the primary classifier and scales to any taxonomy
   size, because the model only ever sees K candidates, never the whole tree.

2. `train_catboost` — train a CatBoost model on `ground_truth` and predict
   every invoice. The supervised half of the feedback loop; it gets better
   as ground truth grows.

Both write rows with a clear `source` ('llm' or 'catboost'), a 0-1
`confidence`, and a `status`/`reason` so an unclassified invoice explains
itself. The LLM path is wrapped in MLflow tracing so you can see exactly
what was retrieved and what the model replied.

Kept deliberately small and linear — read it top to bottom.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Callable, Optional

from . import vectorstore
from .confidence import from_llm_1to5, from_proba
from .llm import chat_json, embed

logger = logging.getLogger(__name__)

# Columns of the `predictions` table, in order. One source of truth for the
# MERGE so we never drift from notebook 0's CREATE TABLE.
_PREDICTION_COLUMNS = [
    "order_id", "schema_name", "source", "code", "label", "level_path",
    "confidence", "status", "reason", "rationale", "candidates",
    "model_version", "trace_id", "classified_at",
]


def invoice_text(supplier, description, cost_centre) -> str:
    """Build the text we classify from — one place, used everywhere."""
    parts = [str(x) for x in (supplier, description, cost_centre) if x]
    return " | ".join(parts)


def _to_list(value) -> list:
    """Coerce a level_path cell to a plain list.

    pandas `itertuples()` yields `level_path` as a numpy array, so the usual
    `value or []` idiom raises "truth value of an array is ambiguous". This
    handles None/NaN/array/list uniformly.
    """
    if value is None:
        return []
    try:
        return list(value)
    except TypeError:
        return []


def _embed_in_batches(client, endpoint: str, texts: list[str], batch_size: int = 128) -> list[list[float]]:
    """Embed many texts in fixed-size batches to respect endpoint limits.

    Databricks embedding endpoints cap inputs per request (e.g.
    `databricks-gte-large-en` allows 150), so keep the batch well under that.
    """
    vectors: list[list[float]] = []
    for start in range(0, len(texts), batch_size):
        vectors.extend(embed(client, endpoint, texts[start:start + batch_size]))
    return vectors


# ---------------------------------------------------------------------------
# Building the pgvector example store (notebook 2)
# ---------------------------------------------------------------------------

def build_examples(
    spark,
    schema,
    *,
    catalog: str,
    schema_db: str,
    invoices_table: str = "invoices",
    max_taxonomy_rows: int = 10_000,
) -> list[dict]:
    """Collect the rows we embed into pgvector for one schema.

    Two kinds:
      * taxonomy     — one per leaf (cold-start candidates). Skipped for very
                       large taxonomies (e.g. UNSPSC's 150k) to bound cost;
                       those rely on ground-truth examples instead.
      * ground_truth — one per labelled invoice (few-shot examples).
    """
    rows: list[dict] = []

    tax = spark.table(f"{catalog}.{schema_db}.{schema.name}_taxonomy") \
               .select("code", "label", "level_path")
    tax_count = tax.count()
    if tax_count <= max_taxonomy_rows:
        for r in tax.toPandas().itertuples():
            path = [p for p in _to_list(r.level_path) if p]
            rows.append({"kind": "taxonomy", "code": str(r.code), "label": r.label,
                         "level_path": list(path),
                         "text": " | ".join([r.label] + path)})
    else:
        print(f"  taxonomy has {tax_count:,} leaves (> {max_taxonomy_rows:,}); "
              f"skipping taxonomy embeddings, using ground truth only.")

    gt = spark.sql(f"""
        SELECT g.code, g.label, g.level_path,
               i.supplier, i.description, i.cost_centre
        FROM {catalog}.{schema_db}.ground_truth g
        JOIN {catalog}.{schema_db}.{invoices_table} i ON i.order_id = g.order_id
        WHERE g.schema_name = '{schema.name}' AND g.code IS NOT NULL
    """).toPandas()
    for r in gt.itertuples():
        rows.append({"kind": "ground_truth", "code": str(r.code), "label": r.label,
                     "level_path": _to_list(r.level_path),
                     "text": invoice_text(r.supplier, r.description, r.cost_centre)})
    return rows


def embed_examples_into_pgvector(
    spark,
    schema,
    *,
    catalog: str,
    schema_db: str,
    conn,
    client,
    embed_endpoint: str,
    invoices_table: str = "invoices",
    batch_size: int = 128,
) -> int:
    """Build, embed, and store this schema's examples in pgvector.

    Returns the number of example rows written.
    """
    rows = build_examples(spark, schema, catalog=catalog, schema_db=schema_db,
                          invoices_table=invoices_table)
    if not rows:
        print(f"  no examples for {schema.name} yet (empty taxonomy + no ground truth).")
        return 0

    embeddings = _embed_in_batches(client, embed_endpoint, [r["text"] for r in rows], batch_size)

    vectorstore.ensure_table(conn, schema_db, dim=len(embeddings[0]))
    n = vectorstore.replace_examples(conn, schema_db, schema.name, rows, embeddings)
    kinds = {}
    for r in rows:
        kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
    print(f"  {schema.name}: embedded {n} examples {kinds}")
    return n


# ---------------------------------------------------------------------------
# Classifier 1: LLM grounded by pgvector retrieval
# ---------------------------------------------------------------------------

def _build_prompt(candidates: list[dict]) -> str:
    """List the retrieved candidates for the model to choose from.

    Ground-truth examples are shown as 'invoice text -> code' so they act as
    few-shot examples; taxonomy rows just list the code and its path.
    """
    lines = []
    for c in candidates:
        path = " > ".join(p for p in c["level_path"] if p) or c["label"]
        if c["kind"] == "ground_truth":
            lines.append(f'- {c["code"]} ({path})  ← similar invoice: "{c["text"]}"')
        else:
            lines.append(f'- {c["code"]} ({path})')
    return (
        "You are classifying a procurement invoice. Pick the ONE best code "
        "from the candidates below. If none fit, reply with code \"\".\n"
        'Reply with a single-line JSON object only:\n'
        '  {"code": "<exact code or empty>", "confidence": <1-5>, "reason": "<short why>"}\n'
        "confidence: 1=guess, 5=certain.\n\n"
        "CANDIDATES:\n" + "\n".join(lines)
    )


def classify_with_llm(
    spark,
    schema,
    *,
    catalog: str,
    schema_db: str,
    pg_connect: Callable[[], object],
    client,
    chat_endpoint: str,
    embed_endpoint: str,
    invoices_table: str = "invoices",
    top_k: int = 20,
    limit_rows: int = 0,
) -> int:
    """Classify every invoice for one schema. Writes `source='llm'` rows.

    Returns the number of predictions written (classified + unclassified).
    """
    import mlflow

    limit = f"LIMIT {limit_rows}" if limit_rows > 0 else ""
    invoices = spark.sql(f"""
        SELECT order_id, supplier, description, cost_centre
        FROM {catalog}.{schema_db}.{invoices_table}
        ORDER BY order_id {limit}
    """).toPandas()

    texts = [invoice_text(r.supplier, r.description, r.cost_centre) for r in invoices.itertuples()]
    vectors = _embed_in_batches(client, embed_endpoint, texts) if texts else []

    conn = pg_connect()
    rows: list[dict] = []
    now = datetime.utcnow()
    try:
        for inv, text, vector in zip(invoices.itertuples(), texts, vectors):
            rows.append(_classify_one_llm(
                mlflow, conn, schema, inv.order_id, text, vector,
                client, chat_endpoint, schema_db, top_k, now,
            ))
    finally:
        conn.close()

    write_predictions(spark, catalog, schema_db, rows)
    classified = sum(1 for r in rows if r["status"] == "classified")
    print(f"llm: {classified}/{len(rows)} classified for schema={schema.name}")
    return len(rows)


def _classify_one_llm(mlflow, conn, schema, order_id, text, vector,
                      client, chat_endpoint, schema_db, top_k, now) -> dict:
    """Classify a single invoice, wrapped in one MLflow trace."""
    def base(**over):
        row = {
            "order_id": str(order_id), "schema_name": schema.name, "source": "llm",
            "code": None, "label": None, "level_path": [], "confidence": None,
            "status": "unclassified", "reason": "no_candidates", "rationale": None,
            "candidates": [], "model_version": chat_endpoint, "trace_id": None,
            "classified_at": now,
        }
        row.update(over)
        return row

    with mlflow.start_span(name="classify_invoice") as span:
        span.set_inputs({"order_id": str(order_id), "text": text})
        trace_id = _safe_trace_id(span)

        candidates = vectorstore.search(conn, schema_db, schema.name, vector, top_k)
        span.set_attributes({"candidate_count": len(candidates)})
        if not candidates:
            return base(trace_id=trace_id, reason="no_candidates")

        by_code = {c["code"]: c for c in candidates}
        answer = chat_json(client, chat_endpoint, _build_prompt(candidates), text)
        span.set_outputs({"answer": answer})

        if not answer:
            return base(trace_id=trace_id, reason="parse_error",
                        candidates=_candidate_structs(candidates))

        code = str(answer.get("code", "")).strip()
        if not code:
            return base(trace_id=trace_id, reason="out_of_taxonomy",
                        rationale=str(answer.get("reason") or ""),
                        candidates=_candidate_structs(candidates))
        if code not in by_code:
            return base(trace_id=trace_id, reason="out_of_taxonomy",
                        rationale=str(answer.get("reason") or ""),
                        candidates=_candidate_structs(candidates))

        chosen = by_code[code]
        return base(
            code=chosen["code"], label=chosen["label"], level_path=chosen["level_path"],
            confidence=from_llm_1to5(answer.get("confidence")),
            status="classified", reason="ok",
            rationale=str(answer.get("reason") or ""),
            candidates=_candidate_structs(candidates), trace_id=trace_id,
        )


def _candidate_structs(candidates: list[dict]) -> list[dict]:
    """Shrink retrieved candidates to the {code,label,score} we persist."""
    return [{"code": c["code"], "label": c["label"], "score": round(1.0 - c["distance"], 4)}
            for c in candidates]


def _safe_trace_id(span) -> Optional[str]:
    """Best-effort trace id; tracing must never break classification."""
    try:
        return span.trace_id
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Classifier 2: CatBoost trained on ground truth
# ---------------------------------------------------------------------------

def train_catboost(
    spark,
    schema,
    *,
    catalog: str,
    schema_db: str,
    invoices_table: str = "invoices",
    min_rows: int = 50,
) -> int:
    """Train CatBoost on `ground_truth` and predict every invoice.

    Writes `source='catboost'` rows. Returns the number written, or 0 when
    there isn't enough ground truth yet (the pipeline still works without it).
    """
    import pandas as pd
    from catboost import CatBoostClassifier

    labelled = spark.sql(f"""
        SELECT i.order_id,
               i.supplier, i.description, i.cost_centre,
               g.code
        FROM {catalog}.{schema_db}.ground_truth g
        JOIN {catalog}.{schema_db}.{invoices_table} i ON i.order_id = g.order_id
        WHERE g.schema_name = '{schema.name}' AND g.code IS NOT NULL
    """).toPandas()

    if len(labelled) < min_rows:
        print(f"catboost: only {len(labelled)} ground-truth rows for {schema.name} "
              f"(need {min_rows}); skipping.")
        return 0

    labelled["text"] = [invoice_text(r.supplier, r.description, r.cost_centre)
                        for r in labelled.itertuples()]
    model = CatBoostClassifier(iterations=200, verbose=0, text_features=["text"])
    model.fit(labelled[["text"]], labelled["code"])
    run_id = f"catboost-{uuid.uuid4().hex[:8]}"

    invoices = spark.sql(f"""
        SELECT order_id, supplier, description, cost_centre
        FROM {catalog}.{schema_db}.{invoices_table}
    """).toPandas()
    invoices["text"] = [invoice_text(r.supplier, r.description, r.cost_centre)
                        for r in invoices.itertuples()]

    codes = model.predict(invoices[["text"]]).flatten()
    confidences = model.predict_proba(invoices[["text"]]).max(axis=1)

    # Look up label + level_path for each predicted code from the taxonomy.
    tax = spark.table(f"{catalog}.{schema_db}.{schema.name}_taxonomy") \
               .select("code", "label", "level_path").toPandas()
    lookup = {str(r.code): (r.label, _to_list(r.level_path)) for r in tax.itertuples()}

    now = datetime.utcnow()
    rows = []
    for order_id, code, conf in zip(invoices["order_id"], codes, confidences):
        label, level_path = lookup.get(str(code), (str(code), []))
        rows.append({
            "order_id": str(order_id), "schema_name": schema.name, "source": "catboost",
            "code": str(code), "label": label, "level_path": level_path,
            "confidence": from_proba(conf), "status": "classified", "reason": "ok",
            "rationale": "CatBoost predict_proba", "candidates": [],
            "model_version": run_id, "trace_id": None, "classified_at": now,
        })

    write_predictions(spark, catalog, schema_db, rows)
    print(f"catboost: wrote {len(rows)} predictions for schema={schema.name}")
    return len(rows)


# ---------------------------------------------------------------------------
# Shared writer
# ---------------------------------------------------------------------------

def write_predictions(spark, catalog: str, schema_db: str, rows: list[dict]) -> None:
    """MERGE rows into `predictions`, keyed on (order_id, schema_name, source)."""
    if not rows:
        return
    from pyspark.sql.types import (
        ArrayType, DoubleType, StringType, StructField, StructType, TimestampType,
    )
    candidate_struct = StructType([
        StructField("code", StringType()),
        StructField("label", StringType()),
        StructField("score", DoubleType()),
    ])
    pred_schema = StructType([
        StructField("order_id", StringType()),
        StructField("schema_name", StringType()),
        StructField("source", StringType()),
        StructField("code", StringType()),
        StructField("label", StringType()),
        StructField("level_path", ArrayType(StringType())),
        StructField("confidence", DoubleType()),
        StructField("status", StringType()),
        StructField("reason", StringType()),
        StructField("rationale", StringType()),
        StructField("candidates", ArrayType(candidate_struct)),
        StructField("model_version", StringType()),
        StructField("trace_id", StringType()),
        StructField("classified_at", TimestampType()),
    ])
    ordered = [{c: r.get(c) for c in _PREDICTION_COLUMNS} for r in rows]
    sdf = spark.createDataFrame(ordered, schema=pred_schema)
    sdf.createOrReplaceTempView("_new_predictions")
    spark.sql(f"""
        MERGE INTO {catalog}.{schema_db}.predictions t
        USING _new_predictions s
          ON t.order_id = s.order_id
         AND t.schema_name = s.schema_name
         AND t.source = s.source
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
