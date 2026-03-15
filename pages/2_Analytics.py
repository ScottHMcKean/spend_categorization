"""
Databricks Procurement Accelerator -- Analytics Dashboard
Spend analytics and model performance powered by Delta Lake.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

st.set_page_config(
    page_title="Analytics · Databricks Procurement Accelerator",
    page_icon="assets/databricks_logo.svg",
    layout="wide",
    initial_sidebar_state="collapsed",
)

from src.config import load_config
from src.app.database import init_backend, get_backend, create_backend

_config = load_config()

from src.app.sidebar import render_databricks_sidebar
render_databricks_sidebar(_config)

# ── Styles ────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;700&display=swap');
    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif !important;
        color: #0B2026;
    }
    .main { background-color: #F9FAFB; }
    .stButton > button {
        background-color: #FF3621 !important;
        color: white !important;
        border: none !important;
        border-radius: 6px !important;
        font-weight: 600 !important;
    }
    .kpi-card {
        background: white;
        border: 1px solid #E5EBF0;
        border-radius: 10px;
        padding: 20px 24px;
        text-align: center;
    }
    .kpi-value { font-size: 2rem; font-weight: 700; color: #0B2026; }
    .kpi-delta { font-size: 0.8rem; color: #22C55E; font-weight: 600; }
    .kpi-label { font-size: 0.78rem; color: #6B8899; margin-top: 4px; font-weight: 500; }
    .section-eyebrow {
        font-size: 0.7rem; font-weight: 700; letter-spacing: 0.15em;
        text-transform: uppercase; color: #FF3621; margin-bottom: 4px;
    }
    .section-title { font-size: 1.4rem; font-weight: 700; color: #0B2026; margin-bottom: 4px; }
    .perf-table {
        width: 100%; border-collapse: collapse; font-size: 0.85rem;
    }
    .perf-table th {
        background: #0B2026; color: white; padding: 10px 16px;
        text-align: left; font-weight: 600; font-size: 0.8rem;
    }
    .perf-table th:first-child { border-radius: 8px 0 0 0; }
    .perf-table th:last-child { border-radius: 0 8px 0 0; }
    .perf-table td {
        padding: 8px 16px; border-bottom: 1px solid #F0F4F7;
        color: #2D4452; vertical-align: middle;
    }
    .perf-table tr:hover td { background: #F7FBFC; }
    .perf-table tr:last-child td { border-bottom: none; }
    .accuracy-badge {
        display: inline-block; padding: 4px 14px; border-radius: 100px;
        font-weight: 700; font-size: 0.85rem;
    }
    .accuracy-high { background: #DCFCE7; color: #15803D; }
    .accuracy-mid { background: #FEF9C3; color: #854D0E; }
    .accuracy-low { background: #FEE2E2; color: #B91C1C; }
    </style>
    """,
    unsafe_allow_html=True,
)

COLORS = {
    "primary": "#FF3621",
    "dark": "#0B2026",
    "seq": ["#0B2026", "#1a3d4f", "#2e6e8a", "#3fa8c8", "#78cde0", "#b8e8f2"],
    "cat": px.colors.qualitative.Set2,
}


# ── Data loading ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_data_from_csv():
    base = Path(__file__).parent.parent / "assets"
    invoices = pd.read_csv(base / "invoices.csv", parse_dates=["date"])
    bootstrap = pd.read_csv(base / "cat_bootstrap.csv") if (base / "cat_bootstrap.csv").exists() else pd.DataFrame()
    reviews = pd.read_csv(base / "cat_reviews.csv") if (base / "cat_reviews.csv").exists() else pd.DataFrame()
    catboost = pd.read_csv(base / "cat_catboost.csv") if (base / "cat_catboost.csv").exists() else pd.DataFrame()
    vectorsearch = pd.read_csv(base / "cat_vectorsearch.csv") if (base / "cat_vectorsearch.csv").exists() else pd.DataFrame()
    return invoices, bootstrap, reviews, catboost, vectorsearch


def _safe_query(backend, query: str) -> pd.DataFrame:
    """Execute a query, returning an empty DataFrame on failure."""
    try:
        return backend.execute_query(query)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Query failed (table may not exist): {e}")
        return pd.DataFrame()


def _coerce_numeric(df: pd.DataFrame, columns: list[str]) -> None:
    """Coerce columns to numeric, handling Databricks Decimal/string returns."""
    for col in columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")


@st.cache_data(ttl=300)
def load_data_from_databricks(_config):
    backend = create_backend(_config)

    invoices = backend.execute_query(
        f"SELECT * FROM {_config.full_invoices_table_path}"
    )
    if "date" in invoices.columns:
        invoices["date"] = pd.to_datetime(invoices["date"], errors="coerce")
    _coerce_numeric(invoices, ["total", "unit_price", "amount"])

    bootstrap = _safe_query(backend, f"SELECT * FROM {_config.full_cat_bootstrap_table_path}")
    _coerce_numeric(bootstrap, ["confidence"])
    reviews = _safe_query(backend, f"SELECT * FROM {_config.full_cat_reviews_table_path}")
    catboost = _safe_query(backend, f"SELECT * FROM {_config.full_cat_catboost_table_path}")
    vectorsearch = _safe_query(backend, f"SELECT * FROM {_config.full_cat_vectorsearch_table_path}")
    return invoices, bootstrap, reviews, catboost, vectorsearch


def load_data(config):
    if config.app_mode == "test":
        return load_data_from_csv()
    return load_data_from_databricks(config)


invoices, bootstrap, reviews, catboost, vectorsearch = load_data(_config)


# ── Category hierarchy helpers ────────────────────────────────────────────────

@st.cache_data
def build_l2_to_l1_mapping(_config) -> dict:
    """Build mapping from L2 category names to their parent L1."""
    mapping = {}
    for l1, l2_dict in _config.categories.items():
        mapping[l1] = l1
        for l2 in l2_dict:
            mapping[l2] = l1
    return mapping


def derive_pred_l1(predictions_df: pd.DataFrame, l2_to_l1: dict) -> pd.DataFrame:
    """Add a derived pred_l1 column by mapping pred_level_1 through the hierarchy.

    In the bootstrap table, pred_level_1 often contains L2 names (e.g. 'Components')
    rather than L1 names (e.g. 'Direct'). This maps them back to L1.
    """
    df = predictions_df.copy()
    if "pred_level_1" in df.columns:
        df["pred_l1_derived"] = df["pred_level_1"].map(l2_to_l1).fillna(df["pred_level_1"])
    return df


# ── Precision / Recall helpers ────────────────────────────────────────────────

def compute_per_category_metrics(
    invoices_df: pd.DataFrame,
    predictions_df: pd.DataFrame,
    pred_col: str = "pred_level_1",
    truth_col: str = "category_level_1",
) -> pd.DataFrame:
    """Compute precision, recall, and spend per category (matching PDF tables)."""
    if predictions_df.empty or pred_col not in predictions_df.columns:
        return pd.DataFrame()

    inv_cols = ["order_id", truth_col, "total"]
    inv_cols = [c for c in inv_cols if c in invoices_df.columns]
    pred_cols = ["order_id", pred_col]
    pred_cols = [c for c in pred_cols if c in predictions_df.columns]

    merged = invoices_df[inv_cols].merge(
        predictions_df[pred_cols], on="order_id", how="inner"
    )
    if merged.empty or truth_col not in merged.columns or pred_col not in merged.columns:
        return pd.DataFrame()

    categories = sorted(merged[truth_col].dropna().unique())
    rows = []
    for cat in categories:
        tp = ((merged[pred_col] == cat) & (merged[truth_col] == cat)).sum()
        fp = ((merged[pred_col] == cat) & (merged[truth_col] != cat)).sum()
        fn = ((merged[pred_col] != cat) & (merged[truth_col] == cat)).sum()

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        spend = int(merged.loc[merged[truth_col] == cat, "total"].sum())

        rows.append({
            "Category": cat,
            "Precision": round(precision, 2),
            "Recall": round(recall, 2),
            "Spend": spend,
        })

    return pd.DataFrame(rows).sort_values("Spend", ascending=False)


def compute_multilevel_accuracy(
    invoices_df: pd.DataFrame,
    predictions_df: pd.DataFrame,
    l2_to_l1: dict = None,
) -> pd.DataFrame:
    """Compute accuracy at each category level.

    Uses direct comparison (pred_level_N vs category_level_N) with a
    fallback for older bootstrap data where pred_level_1 may contain L2 values.
    """
    rows = []

    level_pairs = [
        ("Level 1", "pred_level_1", "category_level_1"),
        ("Level 2", "pred_level_2", "category_level_2"),
        ("Level 3", "pred_level_3", "category_level_3"),
    ]

    for label, pred_col, truth_col in level_pairs:
        if pred_col not in predictions_df.columns or truth_col not in invoices_df.columns:
            continue
        merged = invoices_df[["order_id", truth_col]].merge(
            predictions_df[["order_id", pred_col]].dropna(subset=[pred_col]),
            on="order_id", how="inner",
        )
        if merged.empty:
            continue
        accuracy = (merged[pred_col] == merged[truth_col]).mean()

        # Fallback: if L1 accuracy is very low, the pred_level_1 column likely
        # contains L2 values (old bootstrap naming). Derive L1 from L2 mapping.
        if label == "Level 1" and accuracy < 0.3 and l2_to_l1:
            preds = derive_pred_l1(predictions_df, l2_to_l1)
            merged = invoices_df[["order_id", truth_col]].merge(
                preds[["order_id", "pred_l1_derived"]], on="order_id", how="inner"
            )
            if not merged.empty:
                accuracy = (merged["pred_l1_derived"] == merged[truth_col]).mean()

        rows.append({
            "Level": label,
            "Accuracy": round(accuracy, 4),
            "Error Rate": round(1 - accuracy, 4),
            "Count": len(merged),
        })

    return pd.DataFrame(rows)


def render_precision_recall_table(metrics_df: pd.DataFrame) -> str:
    """Render a precision/recall table as HTML matching PDF format."""
    if metrics_df.empty:
        return "<p>No data available.</p>"
    rows_html = ""
    for _, row in metrics_df.iterrows():
        p = row["Precision"]
        r = row["Recall"]
        p_cls = "accuracy-high" if p >= 0.9 else ("accuracy-mid" if p >= 0.7 else "accuracy-low")
        r_cls = "accuracy-high" if r >= 0.9 else ("accuracy-mid" if r >= 0.7 else "accuracy-low")
        rows_html += f"""
        <tr>
            <td style="font-weight:600;">{row['Category']}</td>
            <td><span class="accuracy-badge {p_cls}">{p:.2f}</span></td>
            <td><span class="accuracy-badge {r_cls}">{r:.2f}</span></td>
            <td style="text-align:right;">{row['Spend']:,}</td>
        </tr>
        """
    return f"""
    <table class="perf-table">
        <thead><tr>
            <th>Category</th><th>Precision</th><th>Recall</th><th style="text-align:right;">Spend</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
    </table>
    """


# ── Header ────────────────────────────────────────────────────────────────────
hc1, hc2 = st.columns([3, 1])
with hc1:
    st.markdown(
        """
        <div class="section-eyebrow">Analytics Dashboard</div>
        <div class="section-title">Procurement Spend Intelligence</div>
        """,
        unsafe_allow_html=True,
    )
with hc2:
    st.markdown("<div style='margin-top:20px;'></div>", unsafe_allow_html=True)
    if st.button("Back to Home"):
        st.switch_page("home.py")

_mode_labels = {"prod": "SQL", "lakebase": "Lakebase", "test": "CSV (test)"}
mode_label = _mode_labels.get(_config.app_mode, _config.app_mode)
st.caption(f"Data source: **{_config.catalog}.{_config.schema_name}** | Mode: **{mode_label}**")
st.divider()

# ── Sidebar filters ───────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Filters")
    all_l1 = ["All"] + sorted(invoices["category_level_1"].dropna().unique().tolist())
    sel_l1 = st.selectbox("Category Level 1", all_l1)

    all_regions = ["All"] + sorted(invoices["region"].dropna().unique().tolist())
    sel_region = st.selectbox("Region", all_regions)

    date_min = invoices["date"].min()
    date_max = invoices["date"].max()
    if pd.notna(date_min) and pd.notna(date_max):
        date_range = st.date_input(
            "Date Range",
            value=(date_min.date(), date_max.date()),
            min_value=date_min.date(),
            max_value=date_max.date(),
        )
    else:
        date_range = ()

df = invoices.copy()
if sel_l1 != "All":
    df = df[df["category_level_1"] == sel_l1]
if sel_region != "All":
    df = df[df["region"] == sel_region]
if len(date_range) == 2:
    df = df[(df["date"].dt.date >= date_range[0]) & (df["date"].dt.date <= date_range[1])]

# ── KPI row ───────────────────────────────────────────────────────────────────
total_spend = df["total"].sum()
n_invoices = len(df)
n_suppliers = df["supplier"].nunique()
avg_confidence = float(bootstrap["confidence"].mean()) if (not bootstrap.empty and "confidence" in bootstrap.columns) else 0.0
avg_confidence = avg_confidence if pd.notna(avg_confidence) else 0.0
n_reviewed = len(reviews) if not reviews.empty else 0

k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    st.metric("Total Spend", f"${total_spend/1e6:.1f}M")
with k2:
    st.metric("Invoices", f"{n_invoices:,}")
with k3:
    st.metric("Unique Suppliers", f"{n_suppliers:,}")
with k4:
    st.metric("Avg. AI Confidence", f"{avg_confidence:.1f} / 5")
with k5:
    st.metric("Human Reviews", f"{n_reviewed:,}")

st.markdown("<div style='margin-bottom:24px;'></div>", unsafe_allow_html=True)

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_spend, tab_models = st.tabs(["Spend Analytics", "Model Performance"])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: SPEND ANALYTICS (existing charts)
# ══════════════════════════════════════════════════════════════════════════════
with tab_spend:

    # Row 1: Treemap + Trend
    rc1, rc2 = st.columns([1, 1])

    with rc1:
        st.markdown("**Spend by Category (Treemap)**")
        tree_df = df.groupby(["category_level_1", "category_level_2"])["total"].sum().reset_index()
        if not tree_df.empty:
            fig_tree = px.treemap(
                tree_df,
                path=["category_level_1", "category_level_2"],
                values="total",
                color="category_level_1",
                color_discrete_sequence=COLORS["seq"],
                title="",
            )
            fig_tree.update_layout(
                margin=dict(t=0, b=0, l=0, r=0),
                font_family="DM Sans",
                paper_bgcolor="white",
                height=320,
            )
            fig_tree.update_traces(textfont_size=12)
            st.plotly_chart(fig_tree, use_container_width=True)

    with rc2:
        st.markdown("**Monthly Spend Trend**")
        trend = df.copy()
        if not trend.empty:
            trend["month"] = trend["date"].dt.to_period("M").dt.to_timestamp()
            trend_agg = trend.groupby(["month", "category_level_1"])["total"].sum().reset_index()
            fig_trend = px.area(
                trend_agg,
                x="month",
                y="total",
                color="category_level_1",
                color_discrete_sequence=COLORS["seq"],
                labels={"total": "Spend ($)", "month": "", "category_level_1": "Category"},
            )
            fig_trend.update_layout(
                margin=dict(t=0, b=0, l=0, r=0),
                font_family="DM Sans",
                paper_bgcolor="white",
                plot_bgcolor="white",
                height=320,
                legend=dict(orientation="h", y=-0.15),
                yaxis=dict(gridcolor="#F0F4F7"),
                xaxis=dict(gridcolor="#F0F4F7"),
            )
            st.plotly_chart(fig_trend, use_container_width=True)

    # Row 2: Top suppliers + Regional heatmap
    st.divider()
    rc3, rc4 = st.columns([1, 1])

    with rc3:
        st.markdown("**Top 15 Suppliers by Spend**")
        if not df.empty:
            top_sup = df.groupby("supplier")["total"].sum().nlargest(15).reset_index()
            fig_sup = px.bar(
                top_sup.sort_values("total"),
                x="total",
                y="supplier",
                orientation="h",
                color="total",
                color_continuous_scale=["#b8e8f2", "#0B2026"],
                labels={"total": "Spend ($)", "supplier": ""},
            )
            fig_sup.update_layout(
                margin=dict(t=0, b=0, l=0, r=0),
                font_family="DM Sans",
                paper_bgcolor="white",
                plot_bgcolor="white",
                height=360,
                coloraxis_showscale=False,
                yaxis=dict(tickfont=dict(size=11)),
                xaxis=dict(gridcolor="#F0F4F7"),
            )
            st.plotly_chart(fig_sup, use_container_width=True)

    with rc4:
        st.markdown("**Spend by Region & Category**")
        if not df.empty:
            region_cat = df.groupby(["region", "category_level_1"])["total"].sum().reset_index()
            pivot = region_cat.pivot(index="region", columns="category_level_1", values="total").fillna(0)
            fig_heat = px.imshow(
                pivot / 1e6,
                color_continuous_scale=["#FFFFFF", "#0B2026"],
                labels=dict(color="$M"),
                aspect="auto",
                text_auto=".1f",
            )
            fig_heat.update_layout(
                margin=dict(t=0, b=0, l=0, r=0),
                font_family="DM Sans",
                paper_bgcolor="white",
                height=360,
                xaxis_title="",
                yaxis_title="",
                coloraxis_showscale=False,
            )
            fig_heat.update_traces(textfont=dict(size=10))
            st.plotly_chart(fig_heat, use_container_width=True)

    # Row 3: Spend cube
    st.divider()
    st.markdown("**Spend Cube -- Category Detail**")
    if not df.empty:
        cube = df.groupby(["category_level_1", "category_level_2", "category_level_3"]).agg(
            invoices_count=("order_id", "count"),
            total_spend=("total", "sum"),
            avg_unit_price=("unit_price", "mean"),
            suppliers=("supplier", "nunique"),
        ).reset_index()
        cube["total_spend_M"] = cube["total_spend"].apply(lambda x: f"${x/1e6:.2f}M")
        cube["avg_unit_price_fmt"] = cube["avg_unit_price"].apply(lambda x: f"${x:,.0f}")
        cube = cube.rename(columns={
            "category_level_1": "L1",
            "category_level_2": "L2",
            "category_level_3": "L3",
            "invoices_count": "# Invoices",
            "total_spend_M": "Total Spend",
            "avg_unit_price_fmt": "Avg Unit Price",
            "suppliers": "# Suppliers",
        })
        cube = cube.sort_values("total_spend", ascending=False).drop(columns="total_spend")
        st.dataframe(
            cube[["L1", "L2", "L3", "# Invoices", "Total Spend", "Avg Unit Price", "# Suppliers"]],
            use_container_width=True,
            height=320,
            hide_index=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: MODEL PERFORMANCE (precision/recall from PDF)
# ══════════════════════════════════════════════════════════════════════════════
with tab_models:

    # ── Problem summary (PDF slide 2) ─────────────────────────────────────────
    st.markdown(
        """
        <div class="section-eyebrow">Problem Summary</div>
        <div class="section-title">Spend Categorization Accuracy</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div style='margin-bottom:16px;'></div>", unsafe_allow_html=True)

    # Spend by category L1 (Direct / Indirect / Non-Procureable / Uncategorized)
    if not invoices.empty:
        spend_by_l1 = invoices.groupby("category_level_1")["total"].sum().reset_index()
        spend_by_l1 = spend_by_l1.sort_values("total", ascending=False)

        ps1, ps2 = st.columns([1, 2])
        with ps1:
            fig_donut = px.pie(
                spend_by_l1,
                values="total",
                names="category_level_1",
                hole=0.45,
                color_discrete_sequence=COLORS["seq"],
            )
            fig_donut.update_layout(
                margin=dict(t=0, b=0, l=0, r=0),
                font_family="DM Sans",
                paper_bgcolor="white",
                height=260,
                showlegend=True,
                legend=dict(orientation="h", y=-0.1),
            )
            fig_donut.update_traces(textinfo="percent+label", textfont_size=11)
            st.plotly_chart(fig_donut, use_container_width=True)
        with ps2:
            st.markdown("**Key Questions (from the PDF)**")
            st.markdown("- Can we automate the categorization solution?")
            st.markdown("- Can it beat the existing rule-based system?")
            st.markdown("- How much will it cost?")
            st.markdown("")
            st.markdown("**Approach**: Two AI-driven solutions evaluated below -- "
                        "Retrieval Augmented Generation (LLM + Vector Search) and "
                        "gradient-boosted Machine Learning (CatBoost).")

    st.divider()

    # ── Solution 1: RAG / Bootstrap (PDF slides 5-6) ──────────────────────────
    st.markdown(
        """
        <div class="section-eyebrow">Solution 1: Retrieval Augmented Generation</div>
        <div class="section-title">LLM Bootstrap Categorization</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div style='margin-bottom:12px;'></div>", unsafe_allow_html=True)

    l2_to_l1 = build_l2_to_l1_mapping(_config)
    bootstrap_with_l1 = derive_pred_l1(bootstrap, l2_to_l1) if not bootstrap.empty else bootstrap

    # L2 precision/recall (pred_level_1 contains L2 predictions)
    bootstrap_metrics = compute_per_category_metrics(
        invoices, bootstrap, pred_col="pred_level_1", truth_col="category_level_2"
    )
    # L1 precision/recall (derived from L2 predictions)
    bootstrap_l1_metrics = compute_per_category_metrics(
        invoices, bootstrap_with_l1, pred_col="pred_l1_derived", truth_col="category_level_1"
    )
    bootstrap_multilevel = compute_multilevel_accuracy(invoices, bootstrap, l2_to_l1=l2_to_l1)

    has_bootstrap = not bootstrap_metrics.empty or not bootstrap_l1_metrics.empty
    if has_bootstrap:
        overall_l1_row = bootstrap_multilevel.loc[bootstrap_multilevel["Level"] == "Level 1", "Accuracy"]
        overall_l1_acc = overall_l1_row.iloc[0] if not overall_l1_row.empty else 0
        overall_l2_row = bootstrap_multilevel.loc[bootstrap_multilevel["Level"] == "Level 2", "Accuracy"]
        overall_l2_acc = overall_l2_row.iloc[0] if not overall_l2_row.empty else 0

        bc1, bc2, bc3, bc4 = st.columns(4)
        with bc1:
            badge_cls = "accuracy-high" if overall_l1_acc >= 0.9 else "accuracy-mid"
            st.markdown(
                f'<div style="text-align:center;"><span class="accuracy-badge {badge_cls}" '
                f'style="font-size:1.4rem;padding:8px 24px;">L1: {overall_l1_acc:.0%}</span></div>',
                unsafe_allow_html=True,
            )
        with bc2:
            badge_cls = "accuracy-high" if overall_l2_acc >= 0.9 else "accuracy-mid"
            st.markdown(
                f'<div style="text-align:center;"><span class="accuracy-badge {badge_cls}" '
                f'style="font-size:1.4rem;padding:8px 24px;">L2: {overall_l2_acc:.0%}</span></div>',
                unsafe_allow_html=True,
            )
        with bc3:
            st.metric("Invoices Classified", f"{len(bootstrap):,}")
        with bc4:
            total_spend_correct = int(overall_l2_acc * invoices["total"].sum()) if overall_l2_acc else 0
            st.metric("Spend Correctly Categorized", f"${total_spend_correct/1e6:.1f}M")

        st.markdown("<div style='margin-bottom:16px;'></div>", unsafe_allow_html=True)

        # Architecture diagram for RAG (PDF slide 5)
        rag1, rag2 = st.columns([1, 1])
        with rag1:
            st.markdown("**RAG Pipeline**")
            st.markdown(
                """
                <div style="background:#F7F9FA;border:1px solid #E5EBF0;border-radius:10px;padding:20px;">
                    <div style="display:flex;align-items:center;gap:12px;margin-bottom:12px;">
                        <div style="background:#0B2026;color:white;padding:6px 14px;border-radius:6px;font-size:0.8rem;font-weight:600;">Orders</div>
                        <div style="color:#8CA0AC;">+</div>
                        <div style="background:#0B2026;color:white;padding:6px 14px;border-radius:6px;font-size:0.8rem;font-weight:600;">Similar Entries (Vector Search)</div>
                        <div style="color:#8CA0AC;">+</div>
                        <div style="background:#0B2026;color:white;padding:6px 14px;border-radius:6px;font-size:0.8rem;font-weight:600;">Category Tree</div>
                    </div>
                    <div style="text-align:center;color:#8CA0AC;font-size:1.2rem;margin:8px 0;">&#8595;</div>
                    <div style="background:#FF3621;color:white;padding:8px 14px;border-radius:6px;font-size:0.85rem;font-weight:600;text-align:center;">
                        Foundation Model &rarr; Predict All Levels
                    </div>
                    <div style="text-align:center;color:#8CA0AC;font-size:1.2rem;margin:8px 0;">&#8595;</div>
                    <div style="background:#22C55E;color:white;padding:8px 14px;border-radius:6px;font-size:0.85rem;font-weight:600;text-align:center;">
                        Structured Output: Level 1 / Level 2 / Level 3 + Confidence + Rationale
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        with rag2:
            # Show L1 precision/recall table (matches PDF format)
            st.markdown("**Per-Category Precision & Recall (Level 1)**")
            if not bootstrap_l1_metrics.empty:
                st.markdown(render_precision_recall_table(bootstrap_l1_metrics), unsafe_allow_html=True)
            elif not bootstrap_metrics.empty:
                st.markdown(render_precision_recall_table(bootstrap_metrics), unsafe_allow_html=True)

        # Multilevel accuracy breakdown
        if not bootstrap_multilevel.empty:
            st.markdown("<div style='margin-bottom:12px;'></div>", unsafe_allow_html=True)
            st.markdown("**Accuracy by Category Level**")
            ml_cols = st.columns(len(bootstrap_multilevel))
            for col, (_, row) in zip(ml_cols, bootstrap_multilevel.iterrows()):
                with col:
                    acc = row["Accuracy"]
                    cls = "accuracy-high" if acc >= 0.9 else ("accuracy-mid" if acc >= 0.7 else "accuracy-low")
                    st.markdown(
                        f"""
                        <div style="background:white;border:1px solid #E5EBF0;border-radius:8px;padding:16px;text-align:center;">
                            <div style="font-size:0.75rem;color:#8CA0AC;font-weight:600;margin-bottom:4px;">{row['Level']}</div>
                            <span class="accuracy-badge {cls}" style="font-size:1.2rem;">{acc:.1%}</span>
                            <div style="font-size:0.72rem;color:#8CA0AC;margin-top:4px;">Error: {row['Error Rate']:.1%}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
    else:
        st.info("No bootstrap categorization data available. Run notebook 2_bootstrap.ipynb first.")

    st.divider()

    # ── Solution 2: CatBoost (PDF slides 7-8) ────────────────────────────────
    st.markdown(
        """
        <div class="section-eyebrow">Solution 2: Machine Learning</div>
        <div class="section-title">CatBoost Gradient Boosting</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div style='margin-bottom:12px;'></div>", unsafe_allow_html=True)

    catboost_with_l1 = derive_pred_l1(catboost, l2_to_l1) if not catboost.empty else catboost
    catboost_metrics = compute_per_category_metrics(
        invoices, catboost_with_l1, pred_col="pred_l1_derived", truth_col="category_level_1"
    ) if not catboost.empty and "pred_level_1" in catboost.columns else pd.DataFrame()
    catboost_multilevel = compute_multilevel_accuracy(invoices, catboost, l2_to_l1=l2_to_l1)

    if not catboost_metrics.empty:
        overall_cb_l1 = catboost_multilevel.loc[catboost_multilevel["Level"] == "Level 1", "Accuracy"]
        overall_cb_acc = overall_cb_l1.iloc[0] if not overall_cb_l1.empty else 0

        cb1, cb2, cb3 = st.columns(3)
        with cb1:
            badge_cls = "accuracy-high" if overall_cb_acc >= 0.9 else "accuracy-mid"
            st.markdown(
                f'<div style="text-align:center;"><span class="accuracy-badge {badge_cls}" '
                f'style="font-size:1.4rem;padding:8px 24px;">L1 Accuracy: {overall_cb_acc:.0%}</span></div>',
                unsafe_allow_html=True,
            )
        with cb2:
            st.metric("Invoices Classified", f"{len(catboost):,}")
        with cb3:
            coverage = len(catboost) / max(len(invoices), 1)
            st.metric("Coverage", f"{coverage:.0%}")

        st.markdown("<div style='margin-bottom:16px;'></div>", unsafe_allow_html=True)

        cb_col1, cb_col2 = st.columns([1, 1])
        with cb_col1:
            st.markdown("**Multilevel Modeling (PDF Slide 7)**")
            st.markdown(
                """
                <div style="background:#F7F9FA;border:1px solid #E5EBF0;border-radius:10px;padding:20px;">
                    <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;">
                        <div style="background:#0B2026;color:white;padding:6px 14px;border-radius:6px;font-size:0.8rem;font-weight:600;">Orders: Features + Targets</div>
                    </div>
                    <div style="text-align:center;color:#8CA0AC;font-size:1.2rem;">&#8595;</div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if not catboost_multilevel.empty:
                for _, row in catboost_multilevel.iterrows():
                    acc = row["Accuracy"]
                    err = row["Error Rate"]
                    cls = "accuracy-high" if acc >= 0.9 else ("accuracy-mid" if acc >= 0.7 else "accuracy-low")
                    st.markdown(
                        f"""
                        <div style="background:white;border:1px solid #E5EBF0;border-radius:8px;padding:10px 16px;margin:4px 0;display:flex;justify-content:space-between;align-items:center;">
                            <span style="font-weight:600;font-size:0.85rem;">{row['Level']} Predict</span>
                            <span class="accuracy-badge {cls}">{err:.0%} error</span>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
            st.caption("Tree models naturally enforce category ontology. Errors propagate through levels.")

        with cb_col2:
            st.markdown("**Per-Category Precision & Recall**")
            st.markdown(render_precision_recall_table(catboost_metrics), unsafe_allow_html=True)

        # Production retraining diagram (PDF slide 8)
        st.markdown("<div style='margin-bottom:16px;'></div>", unsafe_allow_html=True)
        st.markdown("**Production Retraining & Versioning (PDF Slide 8)**")
        retrain_cols = st.columns(6)
        retrain_steps = [
            ("1", "Ingest New Data", "#0B2026"),
            ("2", "Train L1 Model", "#0B2026"),
            ("3", "Train L2 Model", "#0B2026"),
            ("4", "Serve & Predict", "#FF3621"),
            ("5", "Correct Categories", "#22C55E"),
            ("6", "Write Predictions", "#0B2026"),
        ]
        for col, (num, label, bg) in zip(retrain_cols, retrain_steps):
            with col:
                st.markdown(
                    f"""
                    <div style="text-align:center;">
                        <div style="width:36px;height:36px;background:{bg};border-radius:50%;
                                    display:flex;align-items:center;justify-content:center;
                                    font-size:1rem;font-weight:700;color:white;margin:0 auto 8px;">
                            {num}
                        </div>
                        <div style="font-size:0.75rem;font-weight:600;color:#0B2026;">{label}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
    else:
        st.info("No CatBoost categorization data available. Run notebook 4_catboost.ipynb first.")

    st.divider()

    # ── Vector Search results ─────────────────────────────────────────────────
    st.markdown(
        """
        <div class="section-eyebrow">Solution 1b: Vector Search</div>
        <div class="section-title">Embedding-Based RAG Categorization</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div style='margin-bottom:12px;'></div>", unsafe_allow_html=True)

    vs_with_l1 = derive_pred_l1(vectorsearch, l2_to_l1) if not vectorsearch.empty else vectorsearch
    vs_metrics = compute_per_category_metrics(
        invoices, vs_with_l1, pred_col="pred_l1_derived", truth_col="category_level_1"
    ) if not vectorsearch.empty and "pred_level_1" in vectorsearch.columns else pd.DataFrame()
    if not vs_metrics.empty:
        vs_multilevel = compute_multilevel_accuracy(invoices, vectorsearch, l2_to_l1=l2_to_l1)
        vs_l1 = vs_multilevel.loc[vs_multilevel["Level"] == "Level 1", "Accuracy"]
        vs_acc = vs_l1.iloc[0] if not vs_l1.empty else 0

        vs1, vs2 = st.columns(3)[:2]
        with vs1:
            badge_cls = "accuracy-high" if vs_acc >= 0.9 else "accuracy-mid"
            st.markdown(
                f'<div style="text-align:center;"><span class="accuracy-badge {badge_cls}" '
                f'style="font-size:1.4rem;padding:8px 24px;">L1 Accuracy: {vs_acc:.0%}</span></div>',
                unsafe_allow_html=True,
            )
        with vs2:
            st.metric("Invoices Classified", f"{len(vectorsearch):,}")

        st.markdown("<div style='margin-bottom:12px;'></div>", unsafe_allow_html=True)
        st.markdown("**Per-Category Precision & Recall**")
        st.markdown(render_precision_recall_table(vs_metrics), unsafe_allow_html=True)
    else:
        st.info("No Vector Search categorization data available. Run notebook 5_vectorsearch.ipynb first.")

    st.divider()

    # ── Model comparison summary ──────────────────────────────────────────────
    st.markdown(
        """
        <div class="section-eyebrow">Comparison</div>
        <div class="section-title">Categorization Model Accuracy</div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div style='margin-bottom:12px;'></div>", unsafe_allow_html=True)

    l2_to_l1_cmp = build_l2_to_l1_mapping(_config)
    model_data = []
    if not bootstrap.empty and "pred_level_1" in bootstrap.columns:
        bs_derived = derive_pred_l1(bootstrap, l2_to_l1_cmp)
        merged_bs = invoices[["order_id", "category_level_1"]].merge(
            bs_derived[["order_id", "pred_l1_derived"]], on="order_id"
        )
        if not merged_bs.empty:
            acc = (merged_bs["pred_l1_derived"] == merged_bs["category_level_1"]).mean()
            model_data.append({"Model": "LLM Bootstrap", "Accuracy": acc, "Coverage": len(bootstrap) / max(len(invoices), 1)})

    if not catboost.empty and "pred_level_1" in catboost.columns:
        merged_cb = invoices[["order_id", "category_level_1"]].merge(
            catboost[["order_id", "pred_level_1"]], on="order_id"
        )
        if not merged_cb.empty:
            acc_cb = (merged_cb["pred_level_1"] == merged_cb["category_level_1"]).mean()
            model_data.append({"Model": "CatBoost", "Accuracy": acc_cb, "Coverage": len(catboost) / max(len(invoices), 1)})

    if not vectorsearch.empty and "pred_level_1" in vectorsearch.columns:
        merged_vs = invoices[["order_id", "category_level_1"]].merge(
            vectorsearch[["order_id", "pred_level_1"]], on="order_id"
        )
        if not merged_vs.empty:
            acc_vs = (merged_vs["pred_level_1"] == merged_vs["category_level_1"]).mean()
            model_data.append({"Model": "Vector Search", "Accuracy": acc_vs, "Coverage": len(vectorsearch) / max(len(invoices), 1)})

    if not reviews.empty:
        model_data.append({"Model": "Human Review", "Accuracy": 1.0, "Coverage": len(reviews) / max(len(invoices), 1)})

    if model_data:
        model_df = pd.DataFrame(model_data)
        fig_model = go.Figure()
        fig_model.add_trace(go.Bar(
            x=model_df["Model"],
            y=model_df["Accuracy"],
            name="Level-1 Accuracy",
            marker_color=COLORS["primary"],
            text=[f"{v:.1%}" for v in model_df["Accuracy"]],
            textposition="outside",
        ))
        fig_model.add_trace(go.Scatter(
            x=model_df["Model"],
            y=model_df["Coverage"],
            name="Coverage",
            mode="lines+markers",
            yaxis="y2",
            line=dict(color=COLORS["dark"], width=2),
            marker=dict(size=8),
        ))
        fig_model.update_layout(
            margin=dict(t=0, b=0, l=0, r=0),
            font_family="DM Sans",
            paper_bgcolor="white",
            plot_bgcolor="white",
            height=320,
            legend=dict(orientation="h", y=-0.2),
            yaxis=dict(tickformat=".0%", gridcolor="#F0F4F7", range=[0, 1.15]),
            yaxis2=dict(tickformat=".0%", overlaying="y", side="right", range=[0, 1.4], showgrid=False),
            xaxis=dict(gridcolor="#F0F4F7"),
            barmode="group",
        )
        st.plotly_chart(fig_model, use_container_width=True)
        st.caption("Coverage = share of invoices classified by each model. Human Review = ground truth.")
    else:
        st.info("No model data available. Run the categorization notebooks first.")


# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("<div style='margin-top:40px;'></div>", unsafe_allow_html=True)
st.markdown(
    "<div style='text-align:center;font-size:0.78rem;color:#8CA0AC;'>"
    "Databricks Procurement Accelerator -- Analytics powered by Delta Lake + Plotly"
    "</div>",
    unsafe_allow_html=True,
)
