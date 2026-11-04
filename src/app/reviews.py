"""Write human review events to the native Lakebase `reviews` table.

One row per review event (append-only), matching the `reviews` schema from
notebook 0 / 7_sync: review_id, order_id, schema_name, reviewed_source,
action, code, label, level_path, reviewer, comment, created_at.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from src.config import Config

from .database import DatabaseBackend

logger = logging.getLogger(__name__)


def _reviews_path(config: Config) -> str:
    """catalog.schema.reviews — the backend rewrites this to the native
    Lakebase table at execution time."""
    return config.app_table_path("reviews")


def write_reviews_batch(
    config: Config,
    reviews: List[Dict],
    backend: Optional[DatabaseBackend] = None,
) -> None:
    """Append review events. Each dict: order_id, schema_name, reviewed_source,
    action, code, label, level_path, reviewer, comment."""
    if backend is None:
        from .database import get_backend
        backend = get_backend()

    table = _reviews_path(config)
    query = f"""
        INSERT INTO {table}
        (review_id, order_id, schema_name, reviewed_source, action,
         code, label, level_path, reviewer, comment, created_at)
        VALUES
        (%(review_id)s, %(order_id)s, %(schema_name)s, %(reviewed_source)s, %(action)s,
         %(code)s, %(label)s, %(level_path)s, %(reviewer)s, %(comment)s, %(created_at)s)
    """
    now = datetime.now(timezone.utc)
    queries = []
    for r in reviews:
        queries.append((query, {
            "review_id": uuid.uuid4().hex,
            "order_id": str(r["order_id"]),
            "schema_name": r.get("schema_name", ""),
            "reviewed_source": r.get("reviewed_source", ""),
            "action": r.get("action", "corrected"),
            "code": r.get("code"),
            "label": r.get("label"),
            "level_path": list(r.get("level_path") or []),
            "reviewer": r.get("reviewer", "user"),
            "comment": r.get("comment", ""),
            "created_at": now,
        }))

    from .database import LakebaseBackend
    if isinstance(backend, LakebaseBackend) and len(queries) > 1:
        backend.execute_writes_batch(queries)
    else:
        for q, params in queries:
            backend.execute_write(q, params)
    logger.info(f"Wrote {len(queries)} review events")


def get_review_history(
    config: Config,
    order_id: str,
    backend: Optional[DatabaseBackend] = None,
) -> List[Dict]:
    """Return review events for one invoice, newest first."""
    if backend is None:
        from .database import get_backend
        backend = get_backend()
    query = f"""
        SELECT * FROM {_reviews_path(config)}
        WHERE order_id = :order_id
        ORDER BY created_at DESC
    """
    df = backend.execute_query(query, {"order_id": order_id})
    return df.to_dict("records") if not df.empty else []
