"""Classification strategy functions used by notebook 3 (classify).

Three strategies, picked per schema by ``schema.spec.classify_strategy``:

* ``classify_ai_classify`` – hierarchical SQL ``ai_classify`` walking the
  schema's ``level_columns`` step by step, capped at ≤20 candidates per
  call (the primitive's hard limit). Best when the schema has ≤300 leaves
  AND a clean tree (each level branches ≤20).

* ``classify_retrieval_llm`` – per invoice, query the schema's tsvector
  index for top-K candidates, build a prompt listing those K options,
  and let an LLM pick. Best when the leaf count is too large for any
  hierarchical decomposition (UNSPSC, ~150k leaves).

* ``classify_tsvector`` – top-1 from the tsvector index, no LLM. Cheap
  baseline; useful for benchmarking the LLM strategies.

All three write into the unified ``cat_predictions`` table with a clear
``source`` label and a confidence normalized via ``src.confidence``.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Callable, Optional

from .confidence import (
    normalize_llm_self_report,
    normalize_tsvector,
)

logger = logging.getLogger(__name__)


_AI_CLASSIFY_MAX_LABELS = 20
_DEFAULT_MAX_DEPTH = 3


# ---------------------------------------------------------------------------
# Hierarchical ai_classify
# ---------------------------------------------------------------------------

def _children_by_prefix(taxonomy_pdf) -> dict:
    """Map ``(parent_tuple,) -> sorted list[children]`` from a pandas
    taxonomy with a ``level_path`` column."""
    by_prefix: dict[tuple, set[str]] = {}
    for path in taxonomy_pdf["level_path"]:
        if path is None:
            continue
        path_list = list(path)
        for i, child in enumerate(path_list):
            prefix = tuple(path_list[:i])
            by_prefix.setdefault(prefix, set()).add(str(child))
    return {k: sorted(v) for k, v in by_prefix.items()}


def _sql_array(values: list[str]) -> str:
    """Render a Python list as a Spark SQL ARRAY(...) literal with
    single-quoted, properly-escaped string elements."""
    parts = []
    for v in values:
        s = str(v).replace("'", "''")
        parts.append(f"'{s}'")
    return "ARRAY(" + ", ".join(parts) + ")"


def classify_ai_classify(
    spark,
    schema,
    *,
    catalog: str,
    schema_db: str,
    invoices_table: str = "invoices",
    limit_rows: int = 0,
    max_depth: int = _DEFAULT_MAX_DEPTH,
) -> int:
    """Hierarchical ``ai_classify`` driven by ``schema.spec.level_columns``.

    Writes one row per invoice into ``cat_predictions`` with
    ``source='ai_classify'``. Returns the number of rows merged.

    The hierarchy is walked top-down. At each level, for each distinct
    predicted parent-path so far, we issue one ``ai_classify`` call
    against that parent's children. UNION ALL the branches at each
    level. After ``max_depth`` levels of LLM prediction, the deepest
    leaf under the predicted path is picked deterministically.
    """
    from src.confidence import normalize_llm_self_report  # local re-import for notebook contexts
    spark.sql(f"USE {catalog}.{schema_db}")

    tax_pdf = spark.table(f"{catalog}.{schema_db}.{schema.name}_taxonomy").toPandas()
    by_prefix = _children_by_prefix(tax_pdf)
    levels = list(schema.spec.level_columns) or [schema.spec.code_column]
    depth = min(len(levels), max_depth)

    limit_clause = f"LIMIT {limit_rows}" if limit_rows > 0 else ""
    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW _invoice_text AS
    SELECT order_id,
           CONCAT(
             'Supplier: ', COALESCE(supplier, ''), '\\n',
             'Description: ', COALESCE(description, ''), '\\n',
             'Cost Centre: ', COALESCE(cost_centre, '')
           ) AS text
    FROM {catalog}.{schema_db}.{invoices_table}
    {limit_clause}
    """)

    prev_view = "_invoice_text"
    pred_cols: list[str] = []

    for level_idx in range(depth):
        prefixes = sorted(p for p in by_prefix if len(p) == level_idx)
        if not prefixes:
            break

        branches: list[str] = []
        skipped_branches: list[str] = []
        for prefix in prefixes:
            children = by_prefix[prefix]
            if not children:
                continue
            if len(children) > _AI_CLASSIFY_MAX_LABELS:
                logger.warning(
                    "schema=%s level=%d prefix=%s has %d children, capping at %d",
                    schema.name, level_idx, prefix, len(children), _AI_CLASSIFY_MAX_LABELS,
                )
                children = children[:_AI_CLASSIFY_MAX_LABELS]

            where_parts = [
                f"{pred_cols[i]} = '{str(v).replace(chr(39), chr(39)+chr(39))}'"
                for i, v in enumerate(prefix)
            ]
            where_clause = " AND ".join(where_parts) if where_parts else "1=1"
            cols = ["order_id", "text"] + pred_cols

            if len(children) == 1:
                # Deterministic — skip the LLM.
                only = str(children[0]).replace("'", "''")
                skipped_branches.append(f"""
                  SELECT {", ".join(cols)}, '{only}' AS pred_L{level_idx}
                  FROM {prev_view} WHERE {where_clause}
                """)
            else:
                arr = _sql_array(children)
                branches.append(f"""
                  SELECT {", ".join(cols)},
                         ai_classify(text, {arr}) AS pred_L{level_idx}
                  FROM {prev_view} WHERE {where_clause}
                """)

        all_branches = branches + skipped_branches
        if not all_branches:
            break
        union_sql = "\n          UNION ALL\n          ".join(all_branches)
        next_view = f"_step_{level_idx}"
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {next_view} AS {union_sql}")
        prev_view = next_view
        pred_cols.append(f"pred_L{level_idx}")

    # Build a deterministic leaf-picker join against the taxonomy. We rank
    # taxonomy rows by code so the choice is stable.
    pred_path_sql = "ARRAY(" + ", ".join(pred_cols) + ")" if pred_cols else "ARRAY()"
    join_conds = " AND ".join(
        f"tax.level_path[{i}] = p.{c}" for i, c in enumerate(pred_cols)
    ) or "1=1"

    spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW _ai_classify_resolved AS
    WITH p AS (
        SELECT * FROM {prev_view}
    ),
    tax AS (
        SELECT code, label, level_path
        FROM {catalog}.{schema_db}.{schema.name}_taxonomy
    ),
    joined AS (
        SELECT p.order_id,
               COALESCE(tax.code, {pred_cols[-1] if pred_cols else "''"}) AS code,
               COALESCE(tax.label, {pred_cols[-1] if pred_cols else "''"}) AS label,
               COALESCE(tax.level_path, {pred_path_sql}) AS level_path,
               ROW_NUMBER() OVER (PARTITION BY p.order_id ORDER BY tax.code NULLS LAST) AS rn
        FROM p LEFT JOIN tax ON {join_conds}
    )
    SELECT order_id, code, label, level_path FROM joined WHERE rn = 1
    """)

    spark.sql(f"""
    MERGE INTO {catalog}.{schema_db}.cat_predictions t
    USING (
      SELECT order_id,
             '{schema.name}' AS schema_name,
             code,
             label,
             level_path,
             CAST(NULL AS DOUBLE) AS confidence,
             'hierarchical ai_classify (depth={depth})' AS rationale,
             'ai_classify' AS source,
             ARRAY() AS candidates,
             current_timestamp() AS classified_at
      FROM _ai_classify_resolved
    ) s
    ON t.order_id = s.order_id AND t.schema_name = s.schema_name AND t.source = s.source
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

    n = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {catalog}.{schema_db}.cat_predictions
    WHERE schema_name='{schema.name}' AND source='ai_classify'
    """).collect()[0]["n"]
    return int(n)


# ---------------------------------------------------------------------------
# Retrieval-augmented LLM (tsvector top-K → ai_query)
# ---------------------------------------------------------------------------

_STOPWORDS = {
    "supplier", "description", "cost", "centre", "center", "for", "use",
    "and", "the", "of", "with", "on", "services", "service", "products",
    "product", "group", "company", "industries", "solutions", "systems",
    "inc", "llc", "corp", "co", "ltd", "plc",
}


def or_tsquery(text: str) -> tuple[str, int]:
    toks: list[str] = []
    seen: set[str] = set()
    for t in re.split(r"[^A-Za-z]+", text or ""):
        tl = t.lower()
        if len(tl) >= 4 and tl not in _STOPWORDS and tl not in seen:
            toks.append(tl)
            seen.add(tl)
    return " | ".join(toks), len(toks)


def classify_retrieval_llm(
    spark,
    schema,
    *,
    catalog: str,
    schema_db: str,
    pg_connect: Callable[[], Any],
    invoices_table: str = "invoices",
    llm_endpoint: str,
    top_k: Optional[int] = None,
    max_workers: int = 4,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> int:
    """Top-K retrieval → LLM classifier.

    Per invoice:
      1. Tokenize text and build OR-token tsquery (stopword-filtered).
      2. Query Lakebase tsvector index for top-K candidates.
      3. Send candidates + invoice to ``chat_with_retry``; the LLM
         returns ``{"code": "...", "confidence": 1-5}``.
      4. Normalize confidence via ``confidence.normalize_llm_self_report``.

    ``pg_connect`` is a no-arg callable that returns a fresh psycopg2
    connection (notebook 2 builds it from a Lakebase credential token).

    Writes ``source='retrieval_llm'``. Returns the number of merged rows.
    """
    from databricks.sdk import WorkspaceClient
    from src.llm import chat_with_retry

    spark.sql(f"USE {catalog}.{schema_db}")
    k = top_k or schema.spec.retrieval_top_k

    invoices = spark.sql(f"""
      SELECT order_id, supplier, description, cost_centre,
             CONCAT_WS(' | ', supplier, description, cost_centre) AS qtext
      FROM {catalog}.{schema_db}.{invoices_table}
      ORDER BY order_id
    """).toPandas()

    table = f'"{schema_db}"."{schema.name}_text"'
    topk_sql = f"""
    SELECT code, label, level_path, ts_rank(tsv, q, 1) AS score
    FROM {table}, to_tsquery('english', %s) q
    WHERE tsv @@ q
    ORDER BY ts_rank(tsv, q, 1) DESC
    LIMIT {k};
    """

    client = WorkspaceClient().serving_endpoints.get_open_ai_client()
    lock = threading.Lock()
    done = [0]
    misses = [0]

    # Thread-local connection pool: ThreadPoolExecutor reuses worker
    # threads, so each worker holds onto one psycopg2 connection across
    # rows instead of opening a new one per invoice. Cuts retrieval
    # overhead from ~1s/row of connection setup to ~0.
    _local = threading.local()

    def _get_pg_conn():
        conn = getattr(_local, "conn", None)
        if conn is not None:
            try:
                with conn.cursor() as c:
                    c.execute("SELECT 1")
                return conn
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                _local.conn = None
        _local.conn = pg_connect()
        return _local.conn

    def _classify_one(row) -> Optional[dict]:
        tsq, n_tokens = or_tsquery(row["qtext"])
        if not tsq:
            with lock:
                misses[0] += 1
            return None

        try:
            conn = _get_pg_conn()
            with conn.cursor() as cur:
                cur.execute(topk_sql, (tsq,))
                hits = cur.fetchall()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            with lock:
                misses[0] += 1
            return None

        if not hits:
            with lock:
                misses[0] += 1
            return None

        candidates = [
            {"code": str(h[0]), "label": h[1], "level_path": list(h[2] or []), "score": float(h[3])}
            for h in hits
        ]

        # Build the LLM prompt.
        cand_lines = []
        by_code = {}
        for c in candidates:
            path = " > ".join(p for p in (c["level_path"] or []) if p) or c["label"]
            cand_lines.append(f"- {c['code']}: {path}")
            by_code[c["code"]] = c
        cand_text = "\n".join(cand_lines)

        system_prompt = (
            f"Pick exactly ONE code from the list below for the invoice. "
            f"Reply ONLY with a JSON object on a single line:\n"
            f'  {{"code":"<exact code>","confidence":<integer 1-5>}}\n'
            f"Confidence: 1=cannot tell, 5=exact match. No prose.\n\n"
            f"CANDIDATES:\n{cand_text}"
        )

        try:
            resp = chat_with_retry(
                client,
                model=llm_endpoint,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": str(row["qtext"])},
                ],
                temperature=0.0,
                max_tokens=64,
            )
            content = (resp.choices[0].message.content or "").strip()
            data = _extract_json(content)
            code = str((data or {}).get("code", "")).strip() if data else None
            conf_raw = (data or {}).get("confidence") if data else None
        except Exception as e:
            logger.warning("retrieval_llm: %s for %s", e, row.get("order_id"))
            with lock:
                misses[0] += 1
            return None

        if code not in by_code:
            with lock:
                misses[0] += 1
            return None

        chosen = by_code[code]
        confidence = normalize_llm_self_report(conf_raw)

        with lock:
            done[0] += 1
            if progress_cb is not None:
                progress_cb(done[0], len(invoices))

        return {
            "order_id": str(row["order_id"]),
            "schema_name": schema.name,
            "code": chosen["code"],
            "label": chosen["label"],
            "level_path": chosen["level_path"],
            "confidence": float(confidence) if confidence is not None else None,
            "rationale": f"retrieval_llm top-{len(candidates)} via tsvector",
            "source": "retrieval_llm",
            "candidates": candidates,
            "classified_at": datetime.utcnow(),
        }

    rows: list[dict] = []
    work = invoices.to_dict(orient="records")
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        for result in ex.map(_classify_one, work):
            if result:
                rows.append(result)

    print(f"retrieval_llm: {len(rows)}/{len(invoices)} succeeded, misses={misses[0]}")
    if not rows:
        return 0

    from pyspark.sql.types import (
        StructType, StructField, StringType, ArrayType, DoubleType, TimestampType,
    )
    pred_schema = StructType([
        StructField("order_id", StringType()),
        StructField("schema_name", StringType()),
        StructField("code", StringType()),
        StructField("label", StringType()),
        StructField("level_path", ArrayType(StringType())),
        StructField("confidence", DoubleType()),
        StructField("rationale", StringType()),
        StructField("source", StringType()),
        StructField("candidates", ArrayType(StructType([
            StructField("code", StringType()),
            StructField("label", StringType()),
            StructField("score", DoubleType()),
        ]))),
        StructField("classified_at", TimestampType()),
    ])
    # Drop level_path from candidates before writing (matches existing schema).
    norm_rows = []
    for r in rows:
        norm_rows.append({
            **r,
            "candidates": [
                {"code": c["code"], "label": c["label"], "score": float(c["score"])}
                for c in r["candidates"]
            ],
        })
    sdf = spark.createDataFrame(norm_rows, schema=pred_schema)
    sdf.createOrReplaceTempView("_retrieval_llm_predictions")
    spark.sql(f"""
    MERGE INTO {catalog}.{schema_db}.cat_predictions t
    USING _retrieval_llm_predictions s
    ON t.order_id = s.order_id AND t.schema_name = s.schema_name AND t.source = s.source
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    return len(rows)


# ---------------------------------------------------------------------------
# tsvector-only baseline (top-1)
# ---------------------------------------------------------------------------

def classify_tsvector(
    spark,
    schema,
    *,
    catalog: str,
    schema_db: str,
    pg_connect: Callable[[], Any],
    invoices_table: str = "invoices",
    top_k: int = 20,
) -> int:
    """Top-1 from tsvector — cheap baseline, no LLM. Writes
    ``source='tsvector'``."""
    spark.sql(f"USE {catalog}.{schema_db}")
    invoices = spark.sql(f"""
      SELECT order_id, CONCAT_WS(' | ', supplier, description, cost_centre) AS qtext
      FROM {catalog}.{schema_db}.{invoices_table}
      ORDER BY order_id
    """).toPandas()

    table = f'"{schema_db}"."{schema.name}_text"'
    topk_sql = f"""
    SELECT code, label, level_path, ts_rank(tsv, q, 1) AS score
    FROM {table}, to_tsquery('english', %s) q
    WHERE tsv @@ q
    ORDER BY ts_rank(tsv, q, 1) DESC
    LIMIT {top_k};
    """

    rows: list[dict] = []
    misses = 0
    now = datetime.utcnow()
    with pg_connect() as conn, conn.cursor() as cur:
        for _, r in invoices.iterrows():
            tsq, n_tokens = or_tsquery(r["qtext"])
            if not tsq:
                misses += 1
                continue
            try:
                cur.execute(topk_sql, (tsq,))
                hits = cur.fetchall()
            except Exception:
                conn.rollback()
                misses += 1
                continue
            if not hits:
                misses += 1
                continue
            cands = [
                {"code": str(c[0]), "label": c[1], "score": float(c[3])}
                for c in hits
            ]
            top_code, top_label, top_lp, top_score = hits[0]
            confidence = normalize_tsvector([c["score"] for c in cands], n_tokens)
            rows.append({
                "order_id": str(r["order_id"]),
                "schema_name": schema.name,
                "code": str(top_code),
                "label": top_label,
                "level_path": list(top_lp or []),
                "confidence": confidence,
                "rationale": (
                    f"tsvector OR-token (margin+strength+coverage); "
                    f"top1={float(top_score):.4f}, hits={len(cands)}, tokens={n_tokens}"
                ),
                "source": "tsvector",
                "candidates": cands,
                "classified_at": now,
            })

    print(f"tsvector: {len(rows)}/{len(invoices)} succeeded, misses={misses}")
    if not rows:
        return 0

    from pyspark.sql.types import (
        StructType, StructField, StringType, ArrayType, DoubleType, TimestampType,
    )
    pred_schema = StructType([
        StructField("order_id", StringType()),
        StructField("schema_name", StringType()),
        StructField("code", StringType()),
        StructField("label", StringType()),
        StructField("level_path", ArrayType(StringType())),
        StructField("confidence", DoubleType()),
        StructField("rationale", StringType()),
        StructField("source", StringType()),
        StructField("candidates", ArrayType(StructType([
            StructField("code", StringType()),
            StructField("label", StringType()),
            StructField("score", DoubleType()),
        ]))),
        StructField("classified_at", TimestampType()),
    ])
    sdf = spark.createDataFrame(rows, schema=pred_schema)
    sdf.createOrReplaceTempView("_tsvector_predictions")
    spark.sql(f"""
    MERGE INTO {catalog}.{schema_db}.cat_predictions t
    USING _tsvector_predictions s
    ON t.order_id = s.order_id AND t.schema_name = s.schema_name AND t.source = s.source
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)
    return len(rows)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_JSON_RE = re.compile(r"\{[^{}]*\}")


def _extract_json(text: str) -> Optional[dict]:
    """Best-effort extraction of a JSON object from possibly-noisy LLM
    output (markdown fences, leading/trailing chatter)."""
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        pass
    m = _JSON_RE.search(text)
    if m:
        try:
            return json.loads(m.group(0))
        except Exception:
            return None
    return None


# ---------------------------------------------------------------------------
# Strategy dispatcher
# ---------------------------------------------------------------------------

def run_strategy_for(
    spark,
    schema,
    *,
    catalog: str,
    schema_db: str,
    invoices_table: str = "invoices",
    pg_connect: Optional[Callable[[], Any]] = None,
    llm_endpoint: Optional[str] = None,
    limit_rows: int = 0,
) -> tuple[str, int]:
    """Run whichever strategy the schema declares. Returns
    ``(source_label, n_rows_written)``."""
    strategy = schema.spec.classify_strategy
    if strategy == "ai_classify":
        n = classify_ai_classify(
            spark, schema,
            catalog=catalog, schema_db=schema_db,
            invoices_table=invoices_table, limit_rows=limit_rows,
        )
        return "ai_classify", n
    if strategy == "retrieval_llm":
        if pg_connect is None or llm_endpoint is None:
            raise ValueError(f"retrieval_llm needs pg_connect and llm_endpoint (schema={schema.name})")
        n = classify_retrieval_llm(
            spark, schema,
            catalog=catalog, schema_db=schema_db,
            invoices_table=invoices_table,
            pg_connect=pg_connect, llm_endpoint=llm_endpoint,
        )
        return "retrieval_llm", n
    if strategy == "tsvector":
        if pg_connect is None:
            raise ValueError(f"tsvector strategy needs pg_connect (schema={schema.name})")
        n = classify_tsvector(
            spark, schema,
            catalog=catalog, schema_db=schema_db,
            invoices_table=invoices_table,
            pg_connect=pg_connect,
        )
        return "tsvector", n
    raise ValueError(f"unknown classify_strategy '{strategy}' for schema {schema.name}")
