"""API routes for the Spend Categorization app.

v2: every classification lives in `cat_predictions`, keyed by
`(order_id, schema_name, source)`. The selected schema in the UI maps
straight onto a `schema_name` filter.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import HTTPException, Request

from .core import create_router
from .models import (
    AccuracyResponse,
    AgentReviewOut,
    AgentToolCall,
    AnalyticsSummary,
    CategoryAccuracy,
    ReviewIn,
    StatusResponse,
    TableStatus,
    VersionOut,
)

_candidate_root = (
    Path(__file__).resolve().parents[5] if len(Path(__file__).resolve().parents) > 5 else None
)
if _candidate_root and (_candidate_root / "src" / "config.py").exists():
    if str(_candidate_root) not in sys.path:
        sys.path.insert(0, str(_candidate_root))

router = create_router()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _df_to_dicts(df: pd.DataFrame, limit: int | None = None) -> list[dict]:
    if df.empty:
        return []
    out = df.head(limit) if limit else df
    return out.where(out.notna(), None).to_dict(orient="records")


def _safe_col(df: pd.DataFrame, col: str) -> bool:
    return col in df.columns


def _predictions(request: Request, schema_name: str | None = None) -> pd.DataFrame:
    df: pd.DataFrame = request.app.state.data.get("cat_predictions", pd.DataFrame())
    if df.empty:
        return df
    if schema_name and "schema_name" in df.columns:
        df = df[df["schema_name"] == schema_name]
    return df


def _schema_registry(request: Request) -> pd.DataFrame:
    return request.app.state.data.get("schema_registry", pd.DataFrame())


# ---------------------------------------------------------------------------
# Identity / status
# ---------------------------------------------------------------------------

@router.get("/version", response_model=VersionOut, operation_id="version")
async def version() -> VersionOut:
    return VersionOut.from_metadata()


@router.get("/me", response_model=dict, operation_id="getMe")
async def get_me(request: Request):
    user_email = request.headers.get("x-forwarded-email")
    user_name = request.headers.get("x-forwarded-preferred-username")
    if user_email:
        return {"username": user_email}
    if user_name:
        return {"username": user_name}
    try:
        from databricks.sdk import WorkspaceClient

        ws = WorkspaceClient()
        me = ws.current_user.me()
        email = me.emails[0].value if me.emails else me.user_name
        return {"username": email}
    except Exception:
        return {"username": "user"}


@router.get("/status", response_model=StatusResponse, operation_id="getStatus")
async def get_status(request: Request) -> StatusResponse:
    cfg = request.app.state.project_config
    return StatusResponse(
        tables=[TableStatus(**t) for t in request.app.state.table_status],
        backend="Lakebase PostgreSQL",
        catalog=cfg.catalog,
        schema_name=cfg.schema_name,
    )


@router.post("/data/reload", response_model=StatusResponse, operation_id="reloadData")
async def reload_data(request: Request) -> StatusResponse:
    cfg = request.app.state.project_config
    backend = request.app.state.backend
    new_status: list[dict[str, Any]] = []
    for label, tdef in cfg.app_tables.items():
        try:
            df = backend.execute_query(f"SELECT * FROM {cfg.app_table_path(label)}")
            request.app.state.data[label] = df
            n = len(df.columns)
            cols = ", ".join(df.columns[:6]) + ("..." if n > 6 else "")
            new_status.append({
                "table": label, "description": tdef.description,
                "rows": len(df), "columns": n, "column_names": cols,
                "status": "ok" if len(df) > 0 else "empty",
            })
        except Exception:
            request.app.state.data[label] = pd.DataFrame()
            new_status.append({
                "table": label, "description": tdef.description,
                "rows": 0, "columns": 0, "column_names": "--", "status": "missing",
            })
    request.app.state.table_status = new_status
    return StatusResponse(
        tables=[TableStatus(**t) for t in new_status],
        backend="Lakebase PostgreSQL",
        catalog=cfg.catalog,
        schema_name=cfg.schema_name,
    )


# ---------------------------------------------------------------------------
# Schemas (taxonomies)
# ---------------------------------------------------------------------------

@router.get("/schemas", response_model=list[dict], operation_id="listSchemas")
async def list_schemas_route(request: Request) -> list[dict]:
    """Registered classification schemas. Falls back to the in-process Python
    registry when the `schema_registry` Delta table hasn't been populated yet
    (so the UI dropdown works before any pipeline run)."""
    df = _schema_registry(request)
    if not df.empty:
        return _df_to_dicts(df)
    try:
        from src.schemas import STRATEGY, list_schemas as _list_py_schemas

        return [
            {
                "name": s.name,
                "display_name": s.display_name,
                "description": s.spec.description,
                "classify_method": STRATEGY.get(s.name, "ai_classify"),
                "taxonomy_table": f"{s.name}_taxonomy",
                "level_columns": list(s.spec.level_columns),
                "code_column": s.spec.code_column,
                "label_column": s.spec.label_column,
                "leaf_count": None,
                "loaded": False,
            }
            for s in _list_py_schemas()
        ]
    except Exception:
        return []


def _taxonomy_df(request: Request, schema_name: str) -> pd.DataFrame:
    """Resolve a taxonomy DataFrame.

    Order of preference:
      1. Pre-loaded ``app.state.data['<schema>_taxonomy']`` (when the
         table is registered in ``config.yaml`` ``app.tables``).
      2. Direct Lakebase query of the synced table (works even if
         the table isn't in ``app_tables`` — the LakebaseBackend
         rewrites ``catalog.schema.<name>`` to
         ``"<schema>"."<name>_synced"``).
      3. Fall back to loading from the Python schema registry
         (parses bundled Excel/CSV assets).
    Result is cached on ``app.state._taxonomy_fallback_cache``.
    """
    df: pd.DataFrame = request.app.state.data.get(f"{schema_name}_taxonomy", pd.DataFrame())
    if not df.empty:
        return df

    cache: dict[str, pd.DataFrame] = request.app.state.__dict__.setdefault(
        "_taxonomy_fallback_cache", {}
    )
    if schema_name in cache:
        return cache[schema_name]

    cfg = request.app.state.project_config
    backend = request.app.state.backend
    table_path = f"{cfg.catalog}.{cfg.schema_name}.{schema_name}_taxonomy"
    try:
        loaded = backend.execute_query(f"SELECT * FROM {table_path}")
        if not loaded.empty:
            keep = [c for c in ("code", "label", "level_path", "description") if c in loaded.columns]
            loaded = loaded[keep].reset_index(drop=True) if keep else loaded.reset_index(drop=True)
            cache[schema_name] = loaded
            return loaded
    except Exception:
        pass

    try:
        from src.schemas import get_schema

        loaded = get_schema(schema_name).load_taxonomy()
        keep = [c for c in ("code", "label", "level_path", "description") if c in loaded.columns]
        loaded = loaded[keep].reset_index(drop=True) if keep else loaded.reset_index(drop=True)
        cache[schema_name] = loaded
        return loaded
    except Exception:
        return pd.DataFrame()


@router.get(
    "/schemas/{schema_name}/taxonomy",
    response_model=list[dict],
    operation_id="getSchemaTaxonomy",
)
async def get_schema_taxonomy(request: Request, schema_name: str, limit: int = 1000) -> list[dict]:
    df = _taxonomy_df(request, schema_name)
    return _df_to_dicts(df, limit)


# ---------------------------------------------------------------------------
# Predictions (replaces /classifications/{source})
# ---------------------------------------------------------------------------

@router.get(
    "/predictions/{schema_name}",
    response_model=list[dict],
    operation_id="getPredictions",
)
async def get_predictions(
    request: Request, schema_name: str, source: str | None = None, limit: int = 500,
) -> list[dict]:
    df = _predictions(request, schema_name)
    if df.empty:
        return []
    if source:
        df = df[df["source"] == source]
    if "classified_at" in df.columns:
        df = df.sort_values("classified_at", ascending=False)
    return _df_to_dicts(df, limit)


@router.get(
    "/predictions/{schema_name}/{order_id}",
    response_model=list[dict],
    operation_id="getPredictionsForInvoice",
)
async def get_predictions_for_invoice(
    request: Request, schema_name: str, order_id: str,
) -> list[dict]:
    df = _predictions(request, schema_name)
    if df.empty:
        return []
    df = df[df["order_id"] == order_id]
    return _df_to_dicts(df)


@router.get(
    "/invoices/{order_id}/predictions",
    response_model=dict,
    operation_id="getInvoicePredictionsByOrder",
)
async def get_invoice_predictions(request: Request, order_id: str) -> dict:
    """Return all predictions for one invoice, grouped by schema_name."""
    df: pd.DataFrame = request.app.state.data.get("cat_predictions", pd.DataFrame())
    if df.empty:
        return {}
    sub = df[df["order_id"] == order_id]
    if sub.empty:
        return {}
    out: dict[str, list[dict]] = {}
    for schema_name, grp in sub.groupby("schema_name"):
        out[str(schema_name)] = _df_to_dicts(grp)
    return out


@router.get(
    "/predictions/{schema_name}/summary",
    response_model=dict,
    operation_id="getPredictionsSummary",
)
async def get_predictions_summary(request: Request, schema_name: str) -> dict:
    """Confidence + source distribution for a schema. Used by the
    Classifications page header."""
    df = _predictions(request, schema_name)
    if df.empty:
        return {"total": 0, "sources": []}

    sources: list[dict[str, Any]] = []
    if "source" in df.columns:
        for src, grp in df.groupby("source"):
            conf = pd.to_numeric(grp.get("confidence"), errors="coerce") if "confidence" in grp.columns else None
            with_conf = int(conf.notna().sum()) if conf is not None else 0
            avg_conf = float(conf.dropna().mean()) if conf is not None and with_conf else None
            sources.append(
                {
                    "source": str(src),
                    "count": int(len(grp)),
                    "with_confidence": with_conf,
                    "avg_confidence": round(avg_conf, 3) if avg_conf is not None else None,
                }
            )
    sources.sort(key=lambda x: x["count"], reverse=True)
    return {"total": int(len(df)), "sources": sources}


# ---------------------------------------------------------------------------
# Invoices (schema-agnostic)
# ---------------------------------------------------------------------------

@router.get("/invoices", response_model=list[dict], operation_id="listInvoices")
async def list_invoices(request: Request, search: str = "", limit: int = 200) -> list[dict]:
    cfg = request.app.state.project_config
    c = cfg.col
    df: pd.DataFrame = request.app.state.data.get("invoices", pd.DataFrame())
    if df.empty:
        return []
    if search:
        s = search.lower()
        mask = pd.Series(False, index=df.index)
        for col_name in (c.id, c.description, c.supplier):
            if _safe_col(df, col_name):
                mask = mask | df[col_name].astype(str).str.lower().str.contains(s, na=False)
        df = df[mask]
    if _safe_col(df, c.date):
        df = df.sort_values(c.date, ascending=False)
    return _df_to_dicts(df, limit)


@router.get(
    "/invoices/flagged",
    response_model=list[dict],
    operation_id="listFlaggedInvoices",
)
async def list_flagged(
    request: Request,
    schema_name: str = "three_level",
    threshold: float = 3.5,
    limit: int = 50,
) -> list[dict]:
    """Low-confidence predictions for the given schema, joined to invoice info."""
    cfg = request.app.state.project_config
    c = cfg.col
    invoices = request.app.state.data.get("invoices", pd.DataFrame())
    preds = _predictions(request, schema_name)
    if invoices.empty or preds.empty:
        return []

    if "confidence" in preds.columns:
        preds = preds.copy()
        preds["confidence"] = pd.to_numeric(preds["confidence"], errors="coerce")
        flagged = preds[(preds["confidence"] < threshold) | preds["confidence"].isna()]
    else:
        flagged = preds

    keep = ["order_id", "code", "label", "level_path", "confidence", "source", "rationale"]
    flagged = flagged[[k for k in keep if k in flagged.columns]]
    merged = invoices.merge(flagged, left_on=c.id, right_on="order_id", how="inner")
    if "confidence" in merged.columns:
        merged = merged.sort_values("confidence", ascending=True, na_position="first")
    return _df_to_dicts(merged, limit)


# ---------------------------------------------------------------------------
# Reviews
# ---------------------------------------------------------------------------

def _normalize_review_payload(r: ReviewIn) -> dict[str, Any]:
    """Derive reviewed_level_1/2/3 from a leaf-level pick when present.

    The cat_reviews table has fixed L1/L2/L3 columns. For any taxonomy
    depth, we map: L1 = path[0], L2 = path[1] (if present), L3 = the
    deepest non-root level. This preserves the human's pick across
    schemas without changing the table shape.
    """
    payload = r.model_dump(exclude={"reviewed_code", "reviewed_label", "reviewed_level_path"})
    path = r.reviewed_level_path or []
    if path:
        payload["reviewed_level_1"] = path[0] if len(path) > 0 else ""
        payload["reviewed_level_2"] = path[1] if len(path) > 1 else ""
        payload["reviewed_level_3"] = path[-1] if len(path) > 2 else (path[1] if len(path) > 1 else (r.reviewed_label or ""))
    elif r.reviewed_label and not payload.get("reviewed_level_2") and not payload.get("reviewed_level_3"):
        payload["reviewed_level_3"] = r.reviewed_label
    return payload


@router.post("/reviews", response_model=dict, operation_id="submitReviews")
async def submit_reviews(request: Request, reviews: list[ReviewIn]) -> dict:
    from src.app.reviews import write_reviews_batch

    cfg = request.app.state.project_config
    backend = request.app.state.backend
    write_reviews_batch(cfg, [_normalize_review_payload(r) for r in reviews], backend=backend)
    try:
        df = backend.execute_query(f"SELECT * FROM {cfg.app_table_path('cat_reviews')}")
        request.app.state.data["cat_reviews"] = df
    except Exception:
        pass
    return {"submitted": len(reviews)}


# ---------------------------------------------------------------------------
# Agent review (single invoice, on-demand)
# ---------------------------------------------------------------------------


def _build_taxonomy_toolset(request: Request, schema_name: str):
    """Lazily load the schema's taxonomy as a TaxonomyToolset.

    Cached on app.state to avoid rebuilding on every call.
    """
    from src.agents.tools import TaxonomyToolset

    cache: dict[str, Any] = request.app.state.__dict__.setdefault("_toolset_cache", {})
    if schema_name in cache:
        return cache[schema_name]

    df = _taxonomy_df(request, schema_name)
    if df.empty:
        raise HTTPException(
            status_code=404, detail=f"taxonomy unavailable for schema '{schema_name}'"
        )
    toolset = TaxonomyToolset(taxonomy=df)
    cache[schema_name] = toolset
    return toolset


def _coerce_path(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if v is not None and str(v).strip()]
    try:
        return [str(v) for v in list(value) if v is not None and str(v).strip()]
    except TypeError:
        return []


@router.post(
    "/agent/review/{schema_name}/{order_id}",
    response_model=AgentReviewOut,
    operation_id="runAgentReview",
)
async def run_agent_review(
    request: Request,
    schema_name: str,
    order_id: str,
    source: str | None = None,
) -> AgentReviewOut:
    try:
        return await _run_agent_review_impl(request, schema_name, order_id, source)
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001 — surface the real cause to the UI
        import traceback as _tb

        tb = _tb.format_exc()
        raise HTTPException(
            status_code=500,
            detail=f"agent review failed: {type(e).__name__}: {e}\n{tb[-1500:]}",
        )


async def _run_agent_review_impl(
    request: Request,
    schema_name: str,
    order_id: str,
    source: str | None,
) -> AgentReviewOut:
    """Run the review agent on a single invoice for the active schema.

    Uses the same code path as notebook 4 — same toolset, same model
    endpoint — so what the user sees in-app matches the offline
    pipeline exactly.
    """
    from src.agents.review_agent import TaxonomyReviewAgent

    cfg = request.app.state.project_config
    c = cfg.col

    invoices: pd.DataFrame = request.app.state.data.get("invoices", pd.DataFrame())
    inv_match = invoices[invoices[c.id] == order_id] if not invoices.empty and c.id in invoices.columns else pd.DataFrame()
    if inv_match.empty:
        raise HTTPException(status_code=404, detail=f"order_id '{order_id}' not in invoices")
    inv = inv_match.iloc[0]

    preds = _predictions(request, schema_name)
    pred_match = preds[preds["order_id"] == order_id] if not preds.empty else pd.DataFrame()
    if source and not pred_match.empty and "source" in pred_match.columns:
        pred_match = pred_match[pred_match["source"] == source]
    if pred_match.empty:
        # No bootstrap prediction yet -- the agent reviews from scratch.
        boot_code = boot_label = None
        boot_path: list[str] = []
        boot_conf = None
        boot_rationale = None
        boot_source = source or ""
    else:
        # Pick the highest-confidence prediction so the agent reviews the
        # canonical one rather than an arbitrary row.
        pm = pred_match.copy()
        if "confidence" in pm.columns:
            pm["_conf"] = pd.to_numeric(pm["confidence"], errors="coerce").fillna(0)
            pm = pm.sort_values("_conf", ascending=False)
        row = pm.iloc[0]
        boot_code = row.get("code")
        boot_label = row.get("label")
        boot_path = _coerce_path(row.get("level_path"))
        boot_conf = row.get("confidence")
        try:
            boot_conf = float(boot_conf) if boot_conf is not None else None
        except (TypeError, ValueError):
            boot_conf = None
        boot_rationale = row.get("rationale")
        boot_source = row.get("source") or ""

    invoice_text = " | ".join(
        str(inv.get(col, "")).strip()
        for col in (c.supplier, c.description, "cost_centre", c.amount)
        if col in inv.index and inv.get(col) is not None and str(inv.get(col)).strip()
    )

    toolset = _build_taxonomy_toolset(request, schema_name)

    # Resolve LLM client + model. Fall back gracefully if Foundation
    # Models aren't reachable (e.g. local dev without Databricks creds).
    try:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient().serving_endpoints.get_open_ai_client()
        model = cfg.large_llm_endpoint
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"LLM client unavailable: {type(e).__name__}: {e}",
        )

    # Try to resolve a friendlier display name for the system prompt.
    try:
        from src.schemas import get_schema

        display_name = get_schema(schema_name).display_name
    except Exception:
        display_name = schema_name

    agent = TaxonomyReviewAgent(
        openai_client=client,
        model=model,
        toolset=toolset,
        schema_display_name=display_name,
        max_steps=12,
    )

    result = agent.review(
        order_id=order_id,
        invoice_text=invoice_text,
        bootstrap_code=str(boot_code) if boot_code else None,
        bootstrap_label=str(boot_label) if boot_label else None,
        bootstrap_level_path=boot_path,
        bootstrap_confidence=boot_conf,
        bootstrap_rationale=str(boot_rationale) if boot_rationale else None,
    )

    return AgentReviewOut(
        order_id=order_id,
        schema_name=schema_name,
        bootstrap={
            "code": boot_code,
            "label": boot_label,
            "level_path": boot_path,
            "confidence": boot_conf,
            "rationale": boot_rationale,
            "source": boot_source,
        },
        suggested_code=result.suggested_code,
        suggested_label=result.suggested_label,
        suggested_level_path=result.suggested_level_path,
        suggested_level_1=result.suggested_level_1,
        suggested_level_2=result.suggested_level_2,
        suggested_level_3=result.suggested_level_3,
        confidence=result.agent_confidence,
        agrees_with_bootstrap=result.agrees_with_bootstrap,
        rationale=result.rationale or "",
        tool_calls=[AgentToolCall(**tc) for tc in result.tool_calls],
        error=result.error,
        latency_seconds=result.latency_seconds,
    )


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

@router.get("/analytics/summary", response_model=AnalyticsSummary, operation_id="getAnalyticsSummary")
async def analytics_summary(request: Request, schema_name: str | None = None) -> AnalyticsSummary:
    cfg = request.app.state.project_config
    c = cfg.col
    data = request.app.state.data
    invoices = data.get("invoices", pd.DataFrame())
    reviews = data.get("cat_reviews", pd.DataFrame())
    preds = _predictions(request, schema_name)

    total_spend = float(invoices[c.amount].sum()) if not invoices.empty and _safe_col(invoices, c.amount) else 0
    supplier_count = int(invoices[c.supplier].nunique()) if not invoices.empty and _safe_col(invoices, c.supplier) else 0

    predictions_by_source: list[dict[str, Any]] = []
    if not preds.empty and "source" in preds.columns:
        g = preds.groupby("source").size().reset_index(name="count")
        predictions_by_source = g.to_dict(orient="records")

    # Spend by predicted level_path[0] / level_path[1] for the ACTIVE schema.
    # Falls back to invoice ground-truth columns only when no predictions exist
    # (i.e. before any pipeline has run).
    spend_by_l1: list[dict[str, Any]] = []
    spend_by_l2: list[dict[str, Any]] = []
    if not preds.empty and not invoices.empty and _safe_col(invoices, c.amount):
        # One prediction per (order_id, source). Pick the highest-confidence
        # source (ties broken by recency) so each invoice contributes once.
        p = preds.copy()
        if "confidence" in p.columns:
            p["_conf"] = pd.to_numeric(p["confidence"], errors="coerce").fillna(0)
        else:
            p["_conf"] = 0
        if "classified_at" in p.columns:
            p = p.sort_values(["order_id", "_conf", "classified_at"], ascending=[True, False, False])
        else:
            p = p.sort_values(["order_id", "_conf"], ascending=[True, False])
        p = p.drop_duplicates(subset=["order_id"], keep="first")

        def _l(path: Any, idx: int) -> str | None:
            if isinstance(path, list) and len(path) > idx and path[idx]:
                return str(path[idx])
            return None

        p["_l1"] = p["level_path"].apply(lambda x: _l(x, 0))
        p["_l2"] = p["level_path"].apply(lambda x: _l(x, 1))
        merged = invoices[[c.id, c.amount]].rename(columns={c.id: "order_id", c.amount: "_amount"}).merge(
            p[["order_id", "_l1", "_l2"]], on="order_id", how="inner"
        )

        if "_l1" in merged.columns:
            g = merged.dropna(subset=["_l1"]).groupby("_l1")["_amount"].sum().reset_index()
            g.columns = ["category_level_1", "total"]
            spend_by_l1 = g.sort_values("total", ascending=False).to_dict(orient="records")
        if "_l2" in merged.columns:
            g = merged.dropna(subset=["_l1", "_l2"]).groupby(["_l1", "_l2"])["_amount"].sum().reset_index()
            g.columns = ["category_level_1", "category_level_2", "total"]
            spend_by_l2 = g.sort_values("total", ascending=False).head(20).to_dict(orient="records")

    # Final fallback: ground-truth invoice categories (only relevant for three_level demos).
    if not spend_by_l1 and not invoices.empty and _safe_col(invoices, c.category_l1) and _safe_col(invoices, c.amount):
        g = invoices.groupby(c.category_l1)[c.amount].sum().reset_index()
        g.columns = ["category_level_1", "total"]
        spend_by_l1 = g.sort_values("total", ascending=False).to_dict(orient="records")
    if not spend_by_l2 and not invoices.empty and _safe_col(invoices, c.category_l2) and _safe_col(invoices, c.amount):
        g = invoices.groupby([c.category_l1, c.category_l2])[c.amount].sum().reset_index()
        g.columns = ["category_level_1", "category_level_2", "total"]
        spend_by_l2 = g.sort_values("total", ascending=False).head(20).to_dict(orient="records")

    monthly_trend: list[dict[str, Any]] = []
    if not invoices.empty and _safe_col(invoices, c.date) and _safe_col(invoices, c.amount):
        tmp = invoices.copy()
        tmp[c.date] = pd.to_datetime(tmp[c.date], errors="coerce")
        tmp["month"] = tmp[c.date].dt.to_period("M").astype(str)
        g = tmp.groupby("month")[c.amount].sum().reset_index()
        g.columns = ["month", "total"]
        monthly_trend = g.to_dict(orient="records")

    top_suppliers: list[dict[str, Any]] = []
    if not invoices.empty and _safe_col(invoices, c.supplier) and _safe_col(invoices, c.amount):
        g = invoices.groupby(c.supplier)[c.amount].sum().nlargest(15).reset_index()
        g.columns = ["supplier", "total"]
        top_suppliers = g.to_dict(orient="records")

    region_category: list[dict[str, Any]] = []
    if not invoices.empty and _safe_col(invoices, c.region) and _safe_col(invoices, c.category_l1) and _safe_col(invoices, c.amount):
        g = invoices.groupby([c.region, c.category_l1])[c.amount].sum().reset_index()
        g.columns = ["region", "category_level_1", "total"]
        region_category = g.to_dict(orient="records")

    return AnalyticsSummary(
        total_spend=total_spend,
        invoice_count=len(invoices),
        supplier_count=supplier_count,
        prediction_count=len(preds),
        review_count=len(reviews),
        predictions_by_source=predictions_by_source,
        spend_by_l1=spend_by_l1,
        spend_by_l2=spend_by_l2,
        monthly_trend=monthly_trend,
        top_suppliers=top_suppliers,
        region_category=region_category,
    )


@router.get(
    "/analytics/accuracy/{schema_name}",
    response_model=AccuracyResponse,
    operation_id="getAccuracy",
)
async def get_accuracy(
    request: Request, schema_name: str, source: str | None = None,
) -> AccuracyResponse:
    """Per-source accuracy for a schema. For three_level only — needs ground
    truth columns (`category_level_*`) on `invoices`. Other schemas have no
    ground truth, so we return an empty response.
    """
    cfg = request.app.state.project_config
    c = cfg.col
    invoices: pd.DataFrame = request.app.state.data.get("invoices", pd.DataFrame())
    preds = _predictions(request, schema_name)

    empty = AccuracyResponse(
        overall_accuracy=0, overall_precision=0, overall_recall=0, overall_f1=0,
        total_classified=0, total_correct=0, source=source or schema_name, categories=[],
    )

    if preds.empty or invoices.empty:
        return empty
    if source:
        preds = preds[preds["source"] == source]
        if preds.empty:
            return empty
    if schema_name != "three_level":
        # No ground truth for non-internal taxonomies.
        return empty

    # Pull predicted L2 from level_path[1] when present, fall back to label.
    def _pred_l2(row: pd.Series) -> str | None:
        path = row.get("level_path")
        if isinstance(path, list) and len(path) > 1 and path[1]:
            return path[1]
        return row.get("label")

    p = preds.copy()
    p["pred_l2"] = p.apply(_pred_l2, axis=1)
    p = p[["order_id", "pred_l2"]].dropna()
    inv_cols = [col for col in [c.id, c.category_l2, c.amount] if col in invoices.columns]
    df = p.merge(invoices[inv_cols], left_on="order_id", right_on=c.id, how="inner")

    df = df.dropna(subset=[c.category_l2, "pred_l2"])
    if df.empty:
        return empty
    df["_spend"] = pd.to_numeric(df.get(c.amount, 0), errors="coerce").fillna(0)

    total = len(df)
    correct = int((df[c.category_l2] == df["pred_l2"]).sum())
    overall_acc = correct / total if total else 0

    cat_rows: list[CategoryAccuracy] = []
    for cat in sorted(set(df[c.category_l2]) | set(df["pred_l2"])):
        tp = int(((df[c.category_l2] == cat) & (df["pred_l2"] == cat)).sum())
        fp = int(((df[c.category_l2] != cat) & (df["pred_l2"] == cat)).sum())
        fn = int(((df[c.category_l2] == cat) & (df["pred_l2"] != cat)).sum())
        prec = tp / (tp + fp) if (tp + fp) else 0
        rec = tp / (tp + fn) if (tp + fn) else 0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0
        cat_spend = float(df.loc[df[c.category_l2] == cat, "_spend"].sum())
        cat_count = int((df[c.category_l2] == cat).sum())
        cat_rows.append(CategoryAccuracy(
            category=cat, precision=round(prec, 2), recall=round(rec, 2),
            f1=round(f1, 2), spend=cat_spend, count=cat_count,
        ))
    cat_rows.sort(key=lambda r: r.spend, reverse=True)

    macro_prec = sum(r.precision for r in cat_rows) / len(cat_rows) if cat_rows else 0
    macro_rec = sum(r.recall for r in cat_rows) / len(cat_rows) if cat_rows else 0
    macro_f1 = sum(r.f1 for r in cat_rows) / len(cat_rows) if cat_rows else 0

    return AccuracyResponse(
        overall_accuracy=round(overall_acc, 4),
        overall_precision=round(macro_prec, 4),
        overall_recall=round(macro_rec, 4),
        overall_f1=round(macro_f1, 4),
        total_classified=total, total_correct=correct,
        source=source or schema_name,
        categories=cat_rows[:10],
    )


@router.get("/config/columns", response_model=dict, operation_id="getColumnConfig")
async def get_column_config(request: Request) -> dict:
    return request.app.state.project_config.col.model_dump()
