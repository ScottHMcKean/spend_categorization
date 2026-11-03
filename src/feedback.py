"""The feedback loop: reviews -> ground_truth.

Two ways ground truth grows:
  * `record_review`      — one human review from the app (approve/correct/reject).
                           Appends to `reviews` and, for approve/correct,
                           upserts the accepted label into `ground_truth`.
  * `import_ground_truth` — bulk-seed `ground_truth` from already-classified
                           spend the team hands us.

`ground_truth` is keyed on (order_id, schema_name) — one accepted label per
invoice per taxonomy. It is the CatBoost training set and the pgvector
few-shot example source, so growing it is what makes the classifier better.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from .categorize import _to_list

_GT_COLUMNS = ["order_id", "schema_name", "code", "label", "level_path",
               "origin", "review_id", "labeled_by", "labeled_at"]
_REVIEW_COLUMNS = ["review_id", "order_id", "schema_name", "reviewed_source",
                   "action", "code", "label", "level_path", "reviewer",
                   "comment", "created_at"]


def record_review(
    spark,
    *,
    catalog: str,
    schema_db: str,
    order_id: str,
    schema_name: str,
    reviewed_source: str,
    action: str,                       # 'approved' | 'corrected' | 'rejected'
    code: Optional[str] = None,
    label: Optional[str] = None,
    level_path: Optional[list] = None,
    reviewer: str,
    comment: str = "",
) -> str:
    """Record one human review. Returns the new review_id.

    `approved`/`corrected` also upsert the accepted (code, label, level_path)
    into `ground_truth`. `rejected` only logs the event.
    """
    review_id = uuid.uuid4().hex
    now = datetime.utcnow()
    _append_reviews(spark, catalog, schema_db, [{
        "review_id": review_id, "order_id": str(order_id), "schema_name": schema_name,
        "reviewed_source": reviewed_source, "action": action,
        "code": code, "label": label, "level_path": list(level_path or []),
        "reviewer": reviewer, "comment": comment, "created_at": now,
    }])

    if action in ("approved", "corrected") and code is not None:
        _upsert_ground_truth(spark, catalog, schema_db, [{
            "order_id": str(order_id), "schema_name": schema_name,
            "code": str(code), "label": label, "level_path": list(level_path or []),
            "origin": "human_review", "review_id": review_id,
            "labeled_by": reviewer, "labeled_at": now,
        }])
    return review_id


def import_ground_truth(
    spark,
    *,
    catalog: str,
    schema_db: str,
    schema_name: str,
    labeled,                           # pandas DataFrame with at least order_id, code
    labeled_by: str = "import",
) -> int:
    """Bulk-seed `ground_truth` from a labelled table/DataFrame.

    `labeled` needs `order_id` and `code`; `label`/`level_path` are filled from
    the schema's taxonomy when absent. Returns the number of rows upserted.
    """
    df = labeled.copy()
    tax = spark.table(f"{catalog}.{schema_db}.{schema_name}_taxonomy") \
               .select("code", "label", "level_path").toPandas()
    lookup = {str(r.code): (r.label, _to_list(r.level_path)) for r in tax.itertuples()}

    now = datetime.utcnow()
    rows = []
    for r in df.itertuples():
        code = str(r.code)
        label = getattr(r, "label", None)
        level_path = getattr(r, "level_path", None)
        if label is None or level_path is None:
            tax_label, tax_path = lookup.get(code, (code, []))
            label = label or tax_label
            level_path = level_path if level_path is not None else tax_path
        rows.append({
            "order_id": str(r.order_id), "schema_name": schema_name,
            "code": code, "label": label, "level_path": _to_list(level_path),
            "origin": "imported", "review_id": None,
            "labeled_by": labeled_by, "labeled_at": now,
        })
    _upsert_ground_truth(spark, catalog, schema_db, rows)
    print(f"ground_truth: upserted {len(rows)} rows for schema={schema_name}")
    return len(rows)


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------

def _append_reviews(spark, catalog: str, schema_db: str, rows: list[dict]) -> None:
    if not rows:
        return
    sdf = spark.createDataFrame([{c: r.get(c) for c in _REVIEW_COLUMNS} for r in rows],
                                schema=_reviews_schema())
    sdf.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema_db}.reviews")


def _upsert_ground_truth(spark, catalog: str, schema_db: str, rows: list[dict]) -> None:
    if not rows:
        return
    sdf = spark.createDataFrame([{c: r.get(c) for c in _GT_COLUMNS} for r in rows],
                                schema=_ground_truth_schema())
    sdf.createOrReplaceTempView("_new_ground_truth")
    spark.sql(f"""
        MERGE INTO {catalog}.{schema_db}.ground_truth t
        USING _new_ground_truth s
          ON t.order_id = s.order_id AND t.schema_name = s.schema_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def _ground_truth_schema():
    from pyspark.sql.types import (ArrayType, StringType, StructField,
                                   StructType, TimestampType)
    return StructType([
        StructField("order_id", StringType()),
        StructField("schema_name", StringType()),
        StructField("code", StringType()),
        StructField("label", StringType()),
        StructField("level_path", ArrayType(StringType())),
        StructField("origin", StringType()),
        StructField("review_id", StringType()),
        StructField("labeled_by", StringType()),
        StructField("labeled_at", TimestampType()),
    ])


def _reviews_schema():
    from pyspark.sql.types import (ArrayType, StringType, StructField,
                                   StructType, TimestampType)
    return StructType([
        StructField("review_id", StringType()),
        StructField("order_id", StringType()),
        StructField("schema_name", StringType()),
        StructField("reviewed_source", StringType()),
        StructField("action", StringType()),
        StructField("code", StringType()),
        StructField("label", StringType()),
        StructField("level_path", ArrayType(StringType())),
        StructField("reviewer", StringType()),
        StructField("comment", StringType()),
        StructField("created_at", TimestampType()),
    ])
