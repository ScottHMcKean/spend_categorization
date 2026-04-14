"""API routes for the Spend Categorization app."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Literal

import pandas as pd
from fastapi import Request

from .core import create_router
from .models import (
    AccuracyResponse,
    AnalyticsSummary,
    CategoryAccuracy,
    ReviewIn,
    StatusResponse,
    TableStatus,
    VersionOut,
)

_project_root = Path(__file__).resolve().parents[5]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

router = create_router()


def _df_to_dicts(df: pd.DataFrame, limit: int | None = None) -> list[dict]:
    if df.empty:
        return []
    out = df.head(limit) if limit else df
    return out.where(out.notna(), None).to_dict(orient="records")


def _safe_col(df: pd.DataFrame, col: str) -> bool:
    """True if col exists in df."""
    return col in df.columns


@router.get("/version", response_model=VersionOut, operation_id="version")
async def version():
    return VersionOut.from_metadata()


@router.get("/me", response_model=dict, operation_id="getMe")
async def get_me(request: Request):
    """Return the current user identity from Databricks headers or SDK."""
    from .core._headers import get_databricks_headers
    host = request.headers.get("x-forwarded-host")
    user_name = request.headers.get("x-forwarded-preferred-username")
    user_email = request.headers.get("x-forwarded-email")
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
async def get_status(request: Request):
    cfg = request.app.state.project_config
    table_status = request.app.state.table_status
    return StatusResponse(
        tables=[TableStatus(**t) for t in table_status],
        backend="Lakebase PostgreSQL",
        catalog=cfg.catalog,
        schema_name=cfg.schema_name,
    )


@router.get("/invoices", response_model=list[dict], operation_id="listInvoices")
async def list_invoices(request: Request, search: str = "", limit: int = 200):
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


@router.get("/invoices/flagged", response_model=list[dict], operation_id="listFlaggedInvoices")
async def list_flagged(request: Request, threshold: float = 3.5, limit: int = 50):
    cfg = request.app.state.project_config
    c = cfg.col
    invoices = request.app.state.data.get("invoices", pd.DataFrame())
    cat_key = f"cat_{cfg.categorization_source}"
    cat_df = request.app.state.data.get(cat_key, pd.DataFrame())

    if invoices.empty or cat_df.empty:
        return []

    pred_cols = [col for col in [c.id, c.pred_l1, c.pred_l2, c.pred_l3, c.confidence]
                 if _safe_col(cat_df, col)]
    result = invoices.merge(cat_df[pred_cols], on=c.id, how="left")
    if _safe_col(result, c.confidence):
        result[c.confidence] = pd.to_numeric(result[c.confidence], errors="coerce")
        result = result[(result[c.confidence] < threshold) | (result[c.confidence].isna())]
        result = result.sort_values(c.confidence, ascending=True)
        result[c.confidence] = result[c.confidence].round(0)
    return _df_to_dicts(result, limit)


@router.get("/categories", response_model=list[str], operation_id="listCategories")
async def list_categories(request: Request):
    cfg = request.app.state.project_config
    c = cfg.col
    df = request.app.state.data.get("categories", pd.DataFrame())
    if df.empty or not _safe_col(df, c.category_l2):
        return []
    return sorted(df[c.category_l2].dropna().unique().tolist())


@router.get(
    "/classifications/{source}",
    response_model=list[dict],
    operation_id="getClassifications",
)
async def get_classifications(
    request: Request,
    source: Literal["bootstrap", "catboost", "vectorsearch", "reviews"],
    limit: int = 500,
):
    cfg = request.app.state.project_config
    c = cfg.col
    key = f"cat_{source}" if source != "reviews" else "cat_reviews"
    df = request.app.state.data.get(key, pd.DataFrame()).copy()

    if not df.empty and source == "vectorsearch" and "llm_output" in df.columns:
        import json as _json
        for col, jkey in [(c.pred_l1, "level_1"), (c.pred_l2, "level_2"), (c.pred_l3, "level_3"), (c.confidence, "confidence")]:
            if not _safe_col(df, col) or df[col].isna().all():
                df[col] = df["llm_output"].apply(
                    lambda x: _json.loads(x).get(jkey) if isinstance(x, str) else None
                )

    if not df.empty and _safe_col(df, c.confidence):
        conf = pd.to_numeric(df[c.confidence], errors="coerce")
        if source == "catboost" and conf.notna().any() and conf.max() <= 1.0:
            conf = conf * 5.0
        df[c.confidence] = conf.round(0)

    return _df_to_dicts(df, limit)


@router.post("/reviews", response_model=dict, operation_id="submitReviews")
async def submit_reviews(request: Request, reviews: list[ReviewIn]):
    from src.app.reviews import write_reviews_batch

    cfg = request.app.state.project_config
    backend = request.app.state.backend
    formatted = [r.model_dump() for r in reviews]
    write_reviews_batch(cfg, formatted, backend=backend)

    try:
        table_path = cfg.app_table_path("cat_reviews")
        df = backend.execute_query(f"SELECT * FROM {table_path}")
        request.app.state.data["cat_reviews"] = df
    except Exception:
        pass

    return {"submitted": len(reviews)}


@router.post("/data/reload", response_model=StatusResponse, operation_id="reloadData")
async def reload_data(request: Request):
    cfg = request.app.state.project_config
    backend = request.app.state.backend
    new_status = []
    for label, tdef in cfg.app_tables.items():
        table_path = cfg.app_table_path(label)
        try:
            df = backend.execute_query(f"SELECT * FROM {table_path}")
            request.app.state.data[label] = df
            n = len(df.columns)
            col_display = ", ".join(df.columns[:6]) + ("..." if n > 6 else "")
            new_status.append({
                "table": label, "description": tdef.description,
                "rows": len(df), "columns": n, "column_names": col_display,
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


@router.get("/analytics/summary", response_model=AnalyticsSummary, operation_id="getAnalyticsSummary")
async def analytics_summary(request: Request):
    cfg = request.app.state.project_config
    c = cfg.col
    data = request.app.state.data
    invoices = data.get("invoices", pd.DataFrame())
    bootstrap = data.get("cat_bootstrap", pd.DataFrame())
    catboost = data.get("cat_catboost", pd.DataFrame())
    vectorsearch = data.get("cat_vectorsearch", pd.DataFrame())
    reviews = data.get("cat_reviews", pd.DataFrame())

    total_spend = float(invoices[c.amount].sum()) if not invoices.empty and _safe_col(invoices, c.amount) else 0
    supplier_count = int(invoices[c.supplier].nunique()) if not invoices.empty and _safe_col(invoices, c.supplier) else 0

    spend_by_l1 = []
    if not invoices.empty and _safe_col(invoices, c.category_l1) and _safe_col(invoices, c.amount):
        g = invoices.groupby(c.category_l1)[c.amount].sum().reset_index()
        g.columns = ["category_level_1", "total"]
        spend_by_l1 = g.to_dict(orient="records")

    spend_by_l2 = []
    if not invoices.empty and _safe_col(invoices, c.category_l2) and _safe_col(invoices, c.amount):
        g = invoices.groupby([c.category_l1, c.category_l2])[c.amount].sum().reset_index()
        g.columns = ["category_level_1", "category_level_2", "total"]
        spend_by_l2 = g.sort_values("total", ascending=False).head(20).to_dict(orient="records")

    monthly_trend = []
    if not invoices.empty and _safe_col(invoices, c.date) and _safe_col(invoices, c.amount):
        tmp = invoices.copy()
        tmp[c.date] = pd.to_datetime(tmp[c.date], errors="coerce")
        tmp["month"] = tmp[c.date].dt.to_period("M").astype(str)
        g = tmp.groupby("month")[c.amount].sum().reset_index()
        g.columns = ["month", "total"]
        monthly_trend = g.to_dict(orient="records")

    top_suppliers = []
    if not invoices.empty and _safe_col(invoices, c.supplier) and _safe_col(invoices, c.amount):
        g = invoices.groupby(c.supplier)[c.amount].sum().nlargest(15).reset_index()
        g.columns = ["supplier", "total"]
        top_suppliers = g.to_dict(orient="records")

    region_category = []
    if not invoices.empty and _safe_col(invoices, c.region) and _safe_col(invoices, c.category_l1) and _safe_col(invoices, c.amount):
        g = invoices.groupby([c.region, c.category_l1])[c.amount].sum().reset_index()
        g.columns = ["region", "category_level_1", "total"]
        region_category = g.to_dict(orient="records")

    return AnalyticsSummary(
        total_spend=total_spend,
        invoice_count=len(invoices),
        supplier_count=supplier_count,
        bootstrap_count=len(bootstrap),
        catboost_count=len(catboost),
        vectorsearch_count=len(vectorsearch),
        review_count=len(reviews),
        spend_by_l1=spend_by_l1,
        spend_by_l2=spend_by_l2,
        monthly_trend=monthly_trend,
        top_suppliers=top_suppliers,
        region_category=region_category,
    )


def _compute_accuracy(
    cat_df: pd.DataFrame, invoices: pd.DataFrame, source_label: str,
    col_actual: str, col_pred: str, col_id: str, col_amount: str,
    col_invoice_actual: str = "",
) -> AccuracyResponse:
    """Compute per-category precision, recall, F1 from actual vs predicted columns."""
    empty = AccuracyResponse(
        overall_accuracy=0, overall_precision=0, overall_recall=0, overall_f1=0,
        total_classified=0, total_correct=0, source=source_label, categories=[],
    )
    if cat_df.empty or col_pred not in cat_df.columns:
        return empty

    merged = cat_df.copy()
    if col_actual not in merged.columns and col_invoice_actual and not invoices.empty:
        inv_cols = [col_id, col_invoice_actual]
        if col_amount in invoices.columns:
            inv_cols.append(col_amount)
        merged = merged.merge(
            invoices[inv_cols].drop_duplicates(subset=[col_id]),
            on=col_id, how="left",
        )
        merged[col_actual] = merged[col_invoice_actual]

    if col_actual not in merged.columns:
        return empty

    df = merged.dropna(subset=[col_actual, col_pred]).copy()
    if not invoices.empty and col_id in invoices.columns and col_amount in invoices.columns:
        if col_amount not in df.columns:
            spend_map = invoices.set_index(col_id)[col_amount].to_dict()
            df["_spend"] = df[col_id].map(spend_map).fillna(0)
        else:
            df["_spend"] = pd.to_numeric(df[col_amount], errors="coerce").fillna(0)
    else:
        df["_spend"] = 0

    total = len(df)
    correct = int((df[col_actual] == df[col_pred]).sum())
    overall_acc = correct / total if total else 0

    all_cats = sorted(set(df[col_actual].unique()) | set(df[col_pred].unique()))
    cat_rows: list[CategoryAccuracy] = []

    for cat in all_cats:
        tp = int(((df[col_actual] == cat) & (df[col_pred] == cat)).sum())
        fp = int(((df[col_actual] != cat) & (df[col_pred] == cat)).sum())
        fn = int(((df[col_actual] == cat) & (df[col_pred] != cat)).sum())
        prec = tp / (tp + fp) if (tp + fp) else 0
        rec = tp / (tp + fn) if (tp + fn) else 0
        f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0
        cat_spend = float(df.loc[df[col_actual] == cat, "_spend"].sum())
        cat_count = int((df[col_actual] == cat).sum())
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
        total_classified=total,
        total_correct=correct,
        source=source_label,
        categories=cat_rows[:10],
    )


@router.get(
    "/analytics/accuracy/{source}",
    response_model=AccuracyResponse,
    operation_id="getAccuracy",
)
async def get_accuracy(
    request: Request,
    source: Literal["bootstrap", "catboost", "vectorsearch"],
):
    cfg = request.app.state.project_config
    c = cfg.col
    data = request.app.state.data
    invoices = data.get("invoices", pd.DataFrame())
    cat_df = data.get(f"cat_{source}", pd.DataFrame()).copy()
    if not cat_df.empty and source == "vectorsearch" and "llm_output" in cat_df.columns:
        import json as _json
        if not _safe_col(cat_df, c.pred_l2) or cat_df[c.pred_l2].isna().all():
            cat_df[c.pred_l2] = cat_df["llm_output"].apply(
                lambda x: _json.loads(x).get("level_2") if isinstance(x, str) else None
            )
    return _compute_accuracy(
        cat_df, invoices, source,
        col_actual=c.actual_l2, col_pred=c.pred_l2,
        col_id=c.id, col_amount=c.amount,
        col_invoice_actual=c.category_l2,
    )


@router.get("/config/columns", response_model=dict, operation_id="getColumnConfig")
async def get_column_config(request: Request):
    """Expose column mapping to the frontend for display labels."""
    cfg = request.app.state.project_config
    return cfg.col.model_dump()
