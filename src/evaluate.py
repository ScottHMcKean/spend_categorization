"""The scorecard — how you measure tuning progress (see docs/TUNING.md).

Two read-only views over `predictions` (joined to `ground_truth` for
accuracy). You re-read these every tuning cycle; tuning is just "move these
numbers."
"""

from __future__ import annotations


def scorecard(spark, catalog: str, schema_db: str):
    """Per (schema_name, source): coverage, and accuracy on the labelled subset.

    * coverage  = classified / total invoices
    * accuracy  = correct / labelled  (labelled = invoices that have ground truth)
    Returns a Spark DataFrame.
    """
    return spark.sql(f"""
        WITH p AS (
          SELECT pr.*, gt.code AS truth_code
          FROM {catalog}.{schema_db}.predictions pr
          LEFT JOIN {catalog}.{schema_db}.ground_truth gt
            ON gt.order_id = pr.order_id AND gt.schema_name = pr.schema_name
        )
        SELECT schema_name, source,
               COUNT(*)                                        AS total,
               SUM(CASE WHEN status='classified' THEN 1 ELSE 0 END) AS classified,
               ROUND(AVG(CASE WHEN status='classified' THEN 1.0 ELSE 0.0 END), 3) AS coverage,
               COUNT(truth_code)                               AS labelled,
               SUM(CASE WHEN truth_code IS NOT NULL AND code = truth_code THEN 1 ELSE 0 END) AS correct,
               ROUND(
                 SUM(CASE WHEN truth_code IS NOT NULL AND code = truth_code THEN 1 ELSE 0 END)
                 / NULLIF(COUNT(truth_code), 0), 3)            AS accuracy,
               ROUND(AVG(confidence), 3)                       AS avg_conf
        FROM p
        GROUP BY schema_name, source
        ORDER BY schema_name, source
    """)


def reason_breakdown(spark, catalog: str, schema_db: str):
    """Per (schema_name, source, reason): how many rows — the lever signal.

    Read the dominant reason among unclassified rows and pull the matching
    lever from docs/TUNING.md.
    """
    return spark.sql(f"""
        SELECT schema_name, source, status, reason, COUNT(*) AS n
        FROM {catalog}.{schema_db}.predictions
        GROUP BY schema_name, source, status, reason
        ORDER BY schema_name, source, n DESC
    """)
