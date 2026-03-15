"""
Databricks Procurement Accelerator
Landing page -- the open-source alternative to management consulting spend solutions.
"""

import streamlit as st

st.set_page_config(
    page_title="Databricks Procurement Accelerator",
    page_icon="assets/databricks_logo.svg",
    layout="wide",
    initial_sidebar_state="collapsed",
)

from src.config import load_config
from src.app.sidebar import render_databricks_sidebar
render_databricks_sidebar(load_config())

# ── Styles ──────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,300;0,400;0,500;0,700;1,400&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif !important;
        color: #0B2026;
    }

    .main { background-color: #FFFFFF; }

    /* Hero */
    .hero {
        background: linear-gradient(135deg, #0B2026 0%, #1a3d4f 60%, #0B2026 100%);
        color: white;
        padding: 80px 60px 60px 60px;
        border-radius: 16px;
        margin-bottom: 48px;
        position: relative;
        overflow: hidden;
    }
    .hero::before {
        content: '';
        position: absolute;
        top: -50%;
        right: -10%;
        width: 500px;
        height: 500px;
        background: radial-gradient(circle, rgba(255,54,33,0.15) 0%, transparent 70%);
        border-radius: 50%;
    }
    .hero-eyebrow {
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        color: #FF3621;
        margin-bottom: 16px;
    }
    .hero-title {
        font-size: 3.2rem;
        font-weight: 700;
        line-height: 1.1;
        color: white !important;
        margin-bottom: 24px;
    }
    .hero-title span { color: #FF3621; }
    .hero-subtitle {
        font-size: 1.2rem;
        font-weight: 400;
        line-height: 1.6;
        color: rgba(255,255,255,0.8);
        max-width: 600px;
        margin-bottom: 40px;
    }
    .hero-badge {
        display: inline-block;
        background: rgba(255,54,33,0.2);
        border: 1px solid rgba(255,54,33,0.4);
        color: #FF6B5B;
        padding: 6px 16px;
        border-radius: 100px;
        font-size: 0.8rem;
        font-weight: 600;
        margin-right: 8px;
        margin-bottom: 8px;
    }

    /* Section headers */
    .section-eyebrow {
        font-size: 0.7rem;
        font-weight: 700;
        letter-spacing: 0.15em;
        text-transform: uppercase;
        color: #FF3621;
        margin-bottom: 8px;
    }
    .section-title {
        font-size: 2rem;
        font-weight: 700;
        color: #0B2026;
        margin-bottom: 8px;
        line-height: 1.2;
    }
    .section-subtitle {
        font-size: 1rem;
        color: #4a6170;
        line-height: 1.6;
        max-width: 640px;
    }

    /* Pillar cards */
    .pillar-card {
        background: #F7F9FA;
        border: 1px solid #E5EBF0;
        border-radius: 12px;
        padding: 32px 28px;
        height: 100%;
        transition: box-shadow 0.2s;
    }
    .pillar-card:hover { box-shadow: 0 8px 30px rgba(11,32,38,0.08); }
    .pillar-icon {
        font-size: 2rem;
        margin-bottom: 16px;
    }
    .pillar-title {
        font-size: 1.1rem;
        font-weight: 700;
        color: #0B2026;
        margin-bottom: 12px;
    }
    .pillar-desc {
        font-size: 0.9rem;
        color: #4a6170;
        line-height: 1.6;
        margin-bottom: 16px;
    }
    .pillar-tag {
        display: inline-block;
        background: #E8F4F8;
        color: #0B6B8A;
        padding: 3px 10px;
        border-radius: 6px;
        font-size: 0.72rem;
        font-weight: 600;
        margin: 2px;
    }

    /* Feature row */
    .feature-row {
        display: flex;
        align-items: flex-start;
        gap: 16px;
        padding: 20px 0;
        border-bottom: 1px solid #F0F4F7;
    }
    .feature-check { color: #22C55E; font-size: 1.2rem; flex-shrink: 0; }
    .feature-x { color: #EF4444; font-size: 1.2rem; flex-shrink: 0; }

    /* Comparison table */
    .comparison-table { width: 100%; border-collapse: collapse; }
    .comparison-table th {
        background: #0B2026;
        color: white;
        padding: 14px 20px;
        text-align: left;
        font-size: 0.85rem;
        font-weight: 600;
    }
    .comparison-table th:first-child { border-radius: 8px 0 0 0; }
    .comparison-table th:last-child { border-radius: 0 8px 0 0; }
    .comparison-table td {
        padding: 12px 20px;
        border-bottom: 1px solid #F0F4F7;
        font-size: 0.88rem;
        color: #2D4452;
        vertical-align: top;
    }
    .comparison-table tr:hover td { background: #F7FBFC; }
    .comparison-table tr:last-child td { border-bottom: none; }
    .comparison-table td:nth-child(1) { font-weight: 600; color: #0B2026; }
    .tag-yes {
        display: inline-block;
        background: #DCFCE7;
        color: #15803D;
        padding: 2px 10px;
        border-radius: 100px;
        font-size: 0.75rem;
        font-weight: 700;
    }
    .tag-no {
        display: inline-block;
        background: #FEE2E2;
        color: #B91C1C;
        padding: 2px 10px;
        border-radius: 100px;
        font-size: 0.75rem;
        font-weight: 700;
    }
    .tag-partial {
        display: inline-block;
        background: #FEF9C3;
        color: #854D0E;
        padding: 2px 10px;
        border-radius: 100px;
        font-size: 0.75rem;
        font-weight: 700;
    }

    /* Nav cards */
    .nav-card {
        background: white;
        border: 1.5px solid #E5EBF0;
        border-radius: 12px;
        padding: 28px 24px;
        cursor: pointer;
        transition: all 0.2s;
        height: 100%;
    }
    .nav-card:hover {
        border-color: #FF3621;
        box-shadow: 0 4px 20px rgba(255,54,33,0.1);
    }
    .nav-card-icon { font-size: 2rem; margin-bottom: 12px; }
    .nav-card-title { font-size: 1rem; font-weight: 700; color: #0B2026; margin-bottom: 8px; }
    .nav-card-desc { font-size: 0.85rem; color: #4a6170; line-height: 1.5; }

    /* Cost comparison */
    .cost-card {
        border-radius: 12px;
        padding: 32px;
        text-align: center;
    }
    .cost-card-competitor {
        background: #F7F9FA;
        border: 1px solid #E5EBF0;
    }
    .cost-card-databricks {
        background: #0B2026;
        border: 1px solid #0B2026;
        color: white;
    }
    .cost-amount {
        font-size: 2.5rem;
        font-weight: 700;
        margin: 12px 0;
    }
    .cost-label {
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        font-weight: 600;
        opacity: 0.6;
    }
    .cost-desc { font-size: 0.85rem; opacity: 0.7; line-height: 1.5; }

    /* Architecture */
    .arch-box {
        background: white;
        border: 1.5px solid #E5EBF0;
        border-radius: 8px;
        padding: 16px 20px;
        margin-bottom: 8px;
        font-size: 0.85rem;
        font-weight: 600;
        color: #0B2026;
    }
    .arch-box-active {
        background: #FF3621;
        border-color: #FF3621;
        color: white;
    }
    .arch-layer-label {
        font-size: 0.68rem;
        font-weight: 700;
        letter-spacing: 0.1em;
        text-transform: uppercase;
        color: #8CA0AC;
        margin-bottom: 8px;
    }

    /* Notebook guide rows */
    .notebook-row {
        background: white;
        border: 1px solid #E5EBF0;
        border-radius: 8px;
        padding: 16px 20px;
        margin-bottom: 8px;
        display: flex;
        align-items: flex-start;
        gap: 16px;
    }
    .nb-num {
        width: 32px; height: 32px; border-radius: 50%; background: #0B2026;
        color: white; font-weight: 700; font-size: 0.85rem;
        display: flex; align-items: center; justify-content: center; flex-shrink: 0;
    }
    .nb-title { font-weight: 700; font-size: 0.9rem; color: #0B2026; }
    .nb-desc { font-size: 0.82rem; color: #4a6170; margin-top: 2px; line-height: 1.5; }

    /* Table schema */
    .table-schema {
        width: 100%; border-collapse: collapse; font-size: 0.83rem;
    }
    .table-schema th {
        background: #F0F4F7; color: #0B2026; padding: 8px 12px;
        text-align: left; font-weight: 700; border-bottom: 2px solid #E5EBF0;
    }
    .table-schema td {
        padding: 8px 12px; border-bottom: 1px solid #F5F7FA; color: #2D4452; vertical-align: top;
    }
    .table-schema tr:last-child td { border-bottom: none; }
    .table-schema tr:hover td { background: #F7FBFC; }

    /* Divider */
    .section-divider {
        border: none;
        border-top: 1px solid #F0F4F7;
        margin: 56px 0;
    }

    /* Stacked metrics */
    .metric-box {
        background: #F7F9FA;
        border: 1px solid #E5EBF0;
        border-radius: 10px;
        padding: 20px 24px;
        text-align: center;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #FF3621;
    }
    .metric-label {
        font-size: 0.8rem;
        color: #4a6170;
        font-weight: 500;
        margin-top: 4px;
    }

    /* Override streamlit button */
    .stButton > button {
        background-color: #FF3621 !important;
        color: white !important;
        border: none !important;
        border-radius: 6px !important;
        font-family: 'DM Sans', sans-serif !important;
        font-weight: 600 !important;
        padding: 10px 24px !important;
    }
    .stButton > button:hover {
        background-color: #E62E1C !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Header nav ───────────────────────────────────────────────────────────────
col_logo, col_nav = st.columns([1, 3])
with col_logo:
    st.image("assets/databricks_logo.svg", width=180)
with col_nav:
    st.markdown(
        """
        <div style="display:flex; gap:32px; align-items:center; padding-top:8px; justify-content:flex-end;">
            <a href="/" style="color:#0B2026;font-weight:600;font-size:0.9rem;text-decoration:none;">Overview</a>
            <a href="/Spend_Review" target="_self" style="color:#0B2026;font-weight:600;font-size:0.9rem;text-decoration:none;">Spend Review</a>
            <a href="/Analytics" target="_self" style="color:#0B2026;font-weight:600;font-size:0.9rem;text-decoration:none;">Analytics</a>
            <a href="/Comparison" target="_self" style="color:#0B2026;font-weight:600;font-size:0.9rem;text-decoration:none;">vs Spend Solutions</a>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<div style='margin-bottom:24px;'></div>", unsafe_allow_html=True)

# ── Hero ─────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="hero">
        <div class="hero-eyebrow">Databricks Procurement Accelerator</div>
        <div class="hero-title">Transparency.<br>Insights. <span>Impact.</span></div>
        <div class="hero-subtitle">
            Transform your procurement spend data into opportunity — with the full power of Databricks,
            open-source AI, and a human-in-the-loop review workflow. A fraction of the cost of proprietary SaaS.
        </div>
        <div>
            <span class="hero-badge">Delta Lake</span>
            <span class="hero-badge">LLM Categorization</span>
            <span class="hero-badge">CatBoost + Vector Search</span>
            <span class="hero-badge">MLflow</span>
            <span class="hero-badge">Lakebase PostgreSQL</span>
            <span class="hero-badge">Open Data</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ── Key metrics ───────────────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
for col, val, lbl in [
    (m1, "5,000+", "Invoices processed"),
    (m2, "3", "AI categorization models"),
    (m3, "95%+", "Bootstrap accuracy"),
    (m4, "~80%", "Cost vs. proprietary SaaS"),
]:
    with col:
        st.markdown(
            f"""<div class="metric-box">
                <div class="metric-value">{val}</div>
                <div class="metric-label">{lbl}</div>
            </div>""",
            unsafe_allow_html=True,
        )

st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

# ── 3 Pillars ─────────────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="section-eyebrow">Our Approach</div>
    <div class="section-title">Built on three core pillars</div>
    <div class="section-subtitle">
        The same framework as enterprise spend analytics — delivered on open infrastructure you own and control.
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown("<div style='margin-bottom:28px;'></div>", unsafe_allow_html=True)

p1, p2, p3 = st.columns(3)
pillars = [
    {
        "icon": "🔍",
        "title": "Transparency",
        "desc": "A reliable single source of truth powered by Delta Lake. AI-cleansed and AI-categorized spend data at line-item level — refreshable in minutes, not months.",
        "tags": ["Delta Lake ACID", "LLM Categorization", "Supplier Harmonization", "3-Level Taxonomy"],
        "vs": "Equivalent to traditional spend solutions' Single Source of Truth + Data Cleansing & Categorization",
    },
    {
        "icon": "💡",
        "title": "Insights",
        "desc": "Value-driven spend analytics across categories, suppliers, regions, and cost centres. From footprint and opportunity analysis to category strategy — powered by Databricks SQL and open AI.",
        "tags": ["Spend Analytics", "Category Insights", "Supplier Analysis", "AI-Generated Summaries"],
        "vs": "Equivalent to traditional spend solutions' Spend & Opportunity + Strategy & Negotiation",
    },
    {
        "icon": "🎯",
        "title": "Impact",
        "desc": "Turn insights into action with a human-in-the-loop review workflow. Track categorization accuracy, measure model improvement, and build a growing training dataset from every review.",
        "tags": ["Human-in-the-Loop", "Continuous Learning", "MLflow Tracking", "Audit Trail"],
        "vs": "Equivalent to traditional spend solutions' Integrated Impact Management",
    },
]
for col, pillar in zip([p1, p2, p3], pillars):
    with col:
        tags_html = "".join(f'<span class="pillar-tag">{t}</span>' for t in pillar["tags"])
        st.markdown(
            f"""
            <div class="pillar-card">
                <div class="pillar-icon">{pillar['icon']}</div>
                <div class="pillar-title">{pillar['title']}</div>
                <div class="pillar-desc">{pillar['desc']}</div>
                <div style="margin-bottom:12px;">{tags_html}</div>
                <div style="font-size:0.75rem;color:#8CA0AC;font-style:italic;">{pillar['vs']}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

# ── Offerings deep-dive ───────────────────────────────────────────────────────
st.markdown(
    """
    <div class="section-eyebrow">How We Cover Every Spend Solution Offering</div>
    <div class="section-title">Feature-by-feature: Spend Solutions vs. Databricks PA</div>
    <div class="section-subtitle">
        Every management consulting spend solution capability mapped to its open-source equivalent on Databricks.
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown("<div style='margin-bottom:28px;'></div>", unsafe_allow_html=True)

OFFERINGS = [
    {
        "competitor": "Single Source of Truth",
        "category": "Transparency",
        "competitor_desc": "ETL pipeline across ERP systems, refreshable data model, harmonised master & transactional data.",
        "databricks_equiv": "Delta Lake as the unified, ACID-compliant data platform. Batch ETL via Databricks Workflows + AI_QUERY enrichment. MERGE INTO for idempotent, refreshable pipelines.",
        "components": ["Delta Lake", "Databricks Workflows", "AI_QUERY", "Apache Spark"],
        "status": "full",
    },
    {
        "competitor": "Data Cleansing & Categorization",
        "category": "Transparency",
        "competitor_desc": "ML-powered categorization engine, global supplier knowledge base (200M+ entries), UNSPSC taxonomy, supervised feedback learning.",
        "databricks_equiv": "3-model ensemble: Claude Sonnet LLM bootstrap → CatBoost gradient boosting → Vector Search RAG. Human-in-the-loop corrections feed continuous retraining. Confidence scoring (1–5 scale). 3-level taxonomy with cost-centre mapping.",
        "components": ["Claude Sonnet 4.5", "CatBoost", "Databricks Vector Search", "MLflow", "Human Review App"],
        "status": "full",
    },
    {
        "competitor": "Spend & Opportunity Analytics",
        "category": "Insights",
        "competitor_desc": "Procurement cockpit, supplier negotiation fact base, category insights, working capital optimisation, synergy analytics, opportunity scan.",
        "databricks_equiv": "Interactive analytics dashboard on top of Delta Lake tables. Spend by category/supplier/region/cost-centre. Confidence distribution, model accuracy trends, and top-opportunity surfacing.",
        "components": ["Databricks SQL", "Delta Lake", "Plotly/Altair", "Streamlit"],
        "status": "full",
    },
    {
        "competitor": "Strategy & Negotiation",
        "category": "Insights",
        "competitor_desc": "Curated analytics journeys for procurement managers preparing supplier negotiations and category strategies.",
        "databricks_equiv": "LLM-generated category summaries and negotiation prep using Claude on Databricks. Category spend cube with supplier consolidation insights from the review workflow.",
        "components": ["Claude Sonnet 4.5", "Databricks SQL", "MLflow Prompt Registry"],
        "status": "partial",
    },
    {
        "competitor": "Carbon & Sustainability",
        "category": "Insights",
        "competitor_desc": "Scope 3 emissions reporting, carbon factor enrichment, decarbonisation journey tracking.",
        "databricks_equiv": "Open emissions factor datasets (EXIOBASE, EPA, Climatiq) joinable to spend via Delta Sharing. Spend × emissions intensity = Scope 3 estimate. Roadmap feature.",
        "components": ["Delta Sharing", "EXIOBASE Open Data", "Databricks SQL"],
        "status": "partial",
    },
    {
        "competitor": "Input Cost & Resilience",
        "category": "Insights",
        "competitor_desc": "Market data integration, supply chain disruption signals, alignment with corporate sustainability goals.",
        "databricks_equiv": "Open commodity price feeds and supplier risk data via Delta Sharing Marketplace. Join to spend cube for cost pressure analysis.",
        "components": ["Delta Sharing Marketplace", "Open Commodity Data", "Databricks SQL"],
        "status": "partial",
    },
    {
        "competitor": "Integrated Impact Management",
        "category": "Impact",
        "competitor_desc": "Project management for savings initiatives, real-time savings tracking, P&L reconciliation, program management platform.",
        "databricks_equiv": "MLflow experiment tracking for model improvement ROI. Append-only review audit trail in cat_reviews as savings baseline. Human review workflow surfaces high-value correction opportunities.",
        "components": ["MLflow", "Lakebase PostgreSQL", "Streamlit Review App"],
        "status": "partial",
    },
    {
        "competitor": "Managed Insights Services",
        "category": "Impact",
        "competitor_desc": "Management consulting expertise, capability building workshops, data-driven performance dialogues.",
        "databricks_equiv": "Open-source community, Databricks Academy, and the accelerator as a self-service starting point. No locked-in consulting fees.",
        "components": ["Open Source", "Databricks Academy", "Community"],
        "status": "partial",
    },
    {
        "competitor": "Spend Platform (SaaS)",
        "category": "Digital Backbone",
        "competitor_desc": "Fully scalable SaaS, highly flexible, different deployment scenarios.",
        "databricks_equiv": "Databricks serverless compute + Lakebase PostgreSQL. Deploy on any cloud (AWS, Azure, GCP). Your data stays in your own lakehouse — no vendor lock-in.",
        "components": ["Databricks Serverless", "Lakebase PostgreSQL", "Multi-cloud"],
        "status": "full",
    },
    {
        "competitor": "Category Management (Source AI)",
        "category": "Source AI",
        "competitor_desc": "Generative AI for 360 market intelligence, contract review, idea generation, negotiation prep.",
        "databricks_equiv": "MLflow Prompt Registry with versioned prompts. Claude Sonnet 4.5 for category analysis, rationale generation, and corrective feedback. Prompt optimisation loop driven by human reviews.",
        "components": ["Claude Sonnet 4.5", "MLflow Prompt Registry", "3_optimize.ipynb"],
        "status": "full",
    },
]

# Build HTML table
rows_html = ""
status_badge = {
    "full": '<span class="tag-yes">Full</span>',
    "partial": '<span class="tag-partial">Roadmap</span>',
    "none": '<span class="tag-no">Not yet</span>',
}
cat_color = {
    "Transparency": "#E8F4F8",
    "Insights": "#F0F4FF",
    "Impact": "#F0FDF4",
    "Digital Backbone": "#FFF7ED",
    "Source AI": "#FAF5FF",
}
for o in OFFERINGS:
    tags_html = " ".join(f'<code style="background:#F0F4F7;padding:1px 6px;border-radius:4px;font-size:0.75rem;">{c}</code>' for c in o["components"])
    bg = cat_color.get(o["category"], "#FFF")
    rows_html += f"""
    <tr>
        <td style="background:{bg};">{o['competitor']}<br><span style="font-size:0.72rem;color:#8CA0AC;font-weight:400;">{o['category']}</span></td>
        <td style="font-size:0.82rem;color:#4a6170;">{o['competitor_desc']}</td>
        <td style="font-size:0.82rem;">{o['databricks_equiv']}<br><div style="margin-top:6px;">{tags_html}</div></td>
        <td style="text-align:center;">{status_badge[o['status']]}</td>
    </tr>
    """

st.markdown(
    f"""
    <table class="comparison-table">
        <thead>
            <tr>
                <th style="width:18%;">Spend Solution Feature</th>
                <th style="width:26%;">What Competitors Do</th>
                <th style="width:46%;">How Databricks PA Answers It</th>
                <th style="width:10%;text-align:center;">Coverage</th>
            </tr>
        </thead>
        <tbody>
            {rows_html}
        </tbody>
    </table>
    """,
    unsafe_allow_html=True,
)

st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

# ── Cost comparison ───────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="section-eyebrow">Total Cost of Ownership</div>
    <div class="section-title">A fraction of the cost</div>
    <div class="section-subtitle">
        Management consulting spend solutions charge enterprise SaaS fees plus consulting rates. The Databricks Procurement Accelerator
        runs on your existing Databricks subscription -- open-source, no vendor lock-in.
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown("<div style='margin-bottom:28px;'></div>", unsafe_allow_html=True)

cc1, cc2, cc3 = st.columns([2, 1, 2])
with cc1:
    st.markdown(
        """
        <div class="cost-card cost-card-competitor">
            <div class="cost-label">Management Consulting Spend Solutions</div>
            <div class="cost-amount" style="color:#0B2026;">$$$$$</div>
            <div class="cost-desc">
                Enterprise SaaS subscription + consulting engagement fees +
                implementation costs + ongoing managed services.
                Pricing not published — "request a demo" only.
            </div>
            <br>
            <div style="text-align:left;">
                <div style="font-size:0.8rem;margin:4px 0;color:#4a6170;">✗ Proprietary black-box algorithms</div>
                <div style="font-size:0.8rem;margin:4px 0;color:#4a6170;">✗ Vendor lock-in</div>
                <div style="font-size:0.8rem;margin:4px 0;color:#4a6170;">✗ Data leaves your environment</div>
                <div style="font-size:0.8rem;margin:4px 0;color:#4a6170;">✗ Consulting dependency</div>
                <div style="font-size:0.8rem;margin:4px 0;color:#4a6170;">✗ SAP BTP / Celonis add-on costs</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
with cc2:
    st.markdown(
        """
        <div style="display:flex;align-items:center;justify-content:center;height:100%;font-size:2rem;color:#8CA0AC;">vs</div>
        """,
        unsafe_allow_html=True,
    )
with cc3:
    st.markdown(
        """
        <div class="cost-card cost-card-databricks">
            <div class="cost-label" style="color:rgba(255,255,255,0.5);">Databricks Procurement Accelerator</div>
            <div class="cost-amount" style="color:#FF3621;">$</div>
            <div class="cost-desc" style="color:rgba(255,255,255,0.7);">
                Databricks compute costs only (serverless, pay-as-you-go).
                Open-source code you own and extend. No consulting lock-in.
                Estimated 70–85% lower TCO vs. enterprise alternatives.
            </div>
            <br>
            <div style="text-align:left;">
                <div style="font-size:0.8rem;margin:4px 0;color:rgba(255,255,255,0.8);">✓ Fully open-source &amp; auditable</div>
                <div style="font-size:0.8rem;margin:4px 0;color:rgba(255,255,255,0.8);">✓ Your data stays in your lakehouse</div>
                <div style="font-size:0.8rem;margin:4px 0;color:rgba(255,255,255,0.8);">✓ Multi-cloud (AWS / Azure / GCP)</div>
                <div style="font-size:0.8rem;margin:4px 0;color:rgba(255,255,255,0.8);">✓ Extend with any open dataset</div>
                <div style="font-size:0.8rem;margin:4px 0;color:rgba(255,255,255,0.8);">✓ Built-in model governance via MLflow</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

# ── Guide: Notebook pipeline ─────────────────────────────────────────────────
st.markdown(
    """
    <div class="section-eyebrow">Guide</div>
    <div class="section-title">Run the full pipeline in 6 steps</div>
    <div class="section-subtitle">Each notebook is self-contained and idempotent -- safe to re-run at any time.</div>
    """,
    unsafe_allow_html=True,
)
st.markdown("<div style='margin-bottom:20px;'></div>", unsafe_allow_html=True)

NOTEBOOKS = [
    {
        "num": "0", "file": "0_generate.ipynb", "title": "Data Generation",
        "desc": "Generate 5,000+ realistic synthetic invoices. Uses AI_QUERY (GPT-OSS 20B) to enrich rows with realistic supplier names, descriptions, and countries. Stores to invoices and categories Delta tables via MERGE INTO.",
        "output": "invoices, invoices_raw, categories", "time": "~10 min",
        "component": "AI_QUERY, Delta Lake, PySpark",
    },
    {
        "num": "1", "file": "1_eda.ipynb", "title": "Exploratory Data Analysis",
        "desc": "Quality checks on generated data. Distribution analysis across category levels, cost centres, plants, and regions. Validates that the data generation distribution matches config.",
        "output": "EDA charts and quality report", "time": "~3 min",
        "component": "PySpark, Pandas, Databricks SQL",
    },
    {
        "num": "2", "file": "2_bootstrap.ipynb", "title": "LLM Bootstrap Categorization",
        "desc": "Classify every invoice using Claude Sonnet 4.5 via AI_QUERY. Builds a versioned prompt in MLflow Prompt Registry. Assigns confidence scores (1-5 scale) and rationale. Evaluates accuracy against ground truth.",
        "output": "cat_bootstrap, prompts", "time": "~20 min (5K rows)",
        "component": "Claude Sonnet 4.5, MLflow, AI_QUERY",
    },
    {
        "num": "3", "file": "3_optimize.ipynb", "title": "Prompt Optimisation",
        "desc": "Uses a sample of human reviews from cat_reviews to iteratively improve the classification prompt. Runs structured evaluation loops and updates the MLflow Prompt Registry.",
        "output": "Updated prompt version in MLflow", "time": "~15 min",
        "component": "Claude Sonnet 4.5, MLflow Prompt Registry",
    },
    {
        "num": "4", "file": "4_catboost.ipynb", "title": "CatBoost Model Training",
        "desc": "Train a CatBoost gradient boosting classifier. Feature engineering from invoice text, supplier, and category fields. Registers model in MLflow. Produces batch predictions.",
        "output": "cat_catboost, MLflow model", "time": "~10 min",
        "component": "CatBoost, MLflow, PySpark",
    },
    {
        "num": "5", "file": "5_vectorsearch.ipynb", "title": "Vector Search RAG Model",
        "desc": "Creates text embeddings from invoice descriptions. Uses Databricks Vector Search for nearest-neighbor lookup. Each prediction is explainable via similar reviewed invoices.",
        "output": "cat_vectorsearch, vector index", "time": "~15 min",
        "component": "Databricks Vector Search, Embeddings",
    },
    {
        "num": "6", "file": "6_sync_to_lakebase.ipynb", "title": "Sync to Lakebase",
        "desc": "Mirrors Delta classification tables to Lakebase PostgreSQL as synced tables. Creates the native cat_reviews table for writable human corrections.",
        "output": "Lakebase synced tables", "time": "~2 min",
        "component": "Lakebase, Databricks SDK, psycopg2",
    },
]
for nb in NOTEBOOKS:
    st.markdown(
        f"""
        <div class="notebook-row">
            <div class="nb-num">{nb['num']}</div>
            <div style="flex:1;">
                <div class="nb-title"><code>{nb['file']}</code> -- {nb['title']}</div>
                <div class="nb-desc">{nb['desc']}</div>
                <div style="margin-top:8px;display:flex;gap:16px;flex-wrap:wrap;">
                    <span style="font-size:0.75rem;color:#4a6170;"><strong>Output:</strong> {nb['output']}</span>
                    <span style="font-size:0.75rem;color:#4a6170;"><strong>Runtime:</strong> {nb['time']}</span>
                    <span style="font-size:0.75rem;color:#4a6170;"><strong>Components:</strong> {nb['component']}</span>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.divider()

# ── Guide: Data model ────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="section-eyebrow">Data Model</div>
    <div class="section-title">Delta Lake table schemas</div>
    <div class="section-subtitle">All tables live in a single catalog.schema -- configurable in config.yaml.</div>
    """,
    unsafe_allow_html=True,
)
st.markdown("<div style='margin-bottom:20px;'></div>", unsafe_allow_html=True)

tab_inv, tab_cat, tab_boot, tab_rev = st.tabs(
    ["invoices", "categories", "cat_bootstrap / catboost / vectorsearch", "cat_reviews"]
)

with tab_inv:
    st.markdown(
        """
        <table class="table-schema">
            <thead><tr><th>Column</th><th>Type</th><th>Description</th></tr></thead>
            <tbody>
                <tr><td><code>order_id</code></td><td>STRING (PK)</td><td>Unique invoice identifier</td></tr>
                <tr><td><code>date</code></td><td>DATE</td><td>Invoice date</td></tr>
                <tr><td><code>description</code></td><td>STRING</td><td>Line-item description</td></tr>
                <tr><td><code>supplier</code></td><td>STRING</td><td>Supplier name</td></tr>
                <tr><td><code>supplier_country</code></td><td>STRING</td><td>ISO-2 country code</td></tr>
                <tr><td><code>amount</code></td><td>INT</td><td>Quantity</td></tr>
                <tr><td><code>unit_price</code></td><td>DOUBLE</td><td>Price per unit</td></tr>
                <tr><td><code>total</code></td><td>DOUBLE</td><td>amount x unit_price</td></tr>
                <tr><td><code>category_level_1/2/3</code></td><td>STRING</td><td>Ground-truth category hierarchy</td></tr>
                <tr><td><code>cost_centre</code></td><td>STRING</td><td>Cost centre code</td></tr>
                <tr><td><code>plant</code></td><td>STRING</td><td>Plant name</td></tr>
                <tr><td><code>region</code></td><td>STRING</td><td>Geographic region</td></tr>
            </tbody>
        </table>
        """,
        unsafe_allow_html=True,
    )

with tab_cat:
    st.markdown(
        """
        <table class="table-schema">
            <thead><tr><th>Column</th><th>Type</th><th>Description</th></tr></thead>
            <tbody>
                <tr><td><code>category_level_1</code></td><td>STRING</td><td>Direct / Indirect / Non-Procureable</td></tr>
                <tr><td><code>category_level_2</code></td><td>STRING</td><td>Sub-category</td></tr>
                <tr><td><code>category_level_3</code></td><td>STRING</td><td>Leaf category</td></tr>
                <tr><td><code>category_level_3_description</code></td><td>STRING</td><td>Description used in LLM prompts</td></tr>
                <tr><td><code>cost_centre</code></td><td>STRING</td><td>Mapped cost centre for this L2</td></tr>
            </tbody>
        </table>
        """,
        unsafe_allow_html=True,
    )

with tab_boot:
    st.markdown(
        """
        <p style="font-size:0.85rem;color:#4a6170;">All three categorization tables share the same core schema. <code>source</code> distinguishes them.</p>
        <table class="table-schema">
            <thead><tr><th>Column</th><th>Type</th><th>Description</th></tr></thead>
            <tbody>
                <tr><td><code>order_id</code></td><td>STRING (FK)</td><td>Links to invoices table</td></tr>
                <tr><td><code>pred_level_1</code></td><td>STRING</td><td>Predicted L1 category</td></tr>
                <tr><td><code>pred_level_2</code></td><td>STRING</td><td>Predicted L2 category</td></tr>
                <tr><td><code>pred_level_3</code></td><td>STRING</td><td>Predicted L3 category</td></tr>
                <tr><td><code>confidence</code></td><td>DOUBLE</td><td>1-5 scale</td></tr>
                <tr><td><code>source</code></td><td>STRING</td><td>bootstrap / catboost / vectorsearch</td></tr>
                <tr><td><code>classified_at</code></td><td>TIMESTAMP</td><td>When the prediction was made</td></tr>
            </tbody>
        </table>
        """,
        unsafe_allow_html=True,
    )

with tab_rev:
    st.markdown(
        """
        <p style="font-size:0.85rem;color:#4a6170;">
            <strong>Append-only.</strong> Every row is an immutable correction record --
            the growing ground-truth dataset that trains all downstream models.
        </p>
        <table class="table-schema">
            <thead><tr><th>Column</th><th>Type</th><th>Description</th></tr></thead>
            <tbody>
                <tr><td><code>order_id</code></td><td>STRING</td><td>Invoice being reviewed</td></tr>
                <tr><td><code>source</code></td><td>STRING</td><td>Which model produced the prediction</td></tr>
                <tr><td><code>reviewer</code></td><td>STRING</td><td>User ID of the reviewer</td></tr>
                <tr><td><code>original_level_1/2/3</code></td><td>STRING</td><td>Original prediction</td></tr>
                <tr><td><code>reviewed_level_1/2/3</code></td><td>STRING</td><td>Human-corrected category</td></tr>
                <tr><td><code>review_status</code></td><td>STRING</td><td>approved / corrected / rejected</td></tr>
                <tr><td><code>comments</code></td><td>STRING</td><td>Optional rationale from reviewer</td></tr>
                <tr><td><code>created_at</code></td><td>TIMESTAMP</td><td>Record creation timestamp</td></tr>
            </tbody>
        </table>
        """,
        unsafe_allow_html=True,
    )

st.divider()

# ── Guide: Configuration ─────────────────────────────────────────────────────
st.markdown(
    """
    <div class="section-eyebrow">Configuration</div>
    <div class="section-title">Single config.yaml drives everything</div>
    """,
    unsafe_allow_html=True,
)

st.code(
    """# config.yaml -- top-level structure
generate:
  company:
    name: Borealis Wind Systems
    categories_file: assets/categories.csv
    cost_centres: [CC-100-Production, CC-200-Maintenance, ...]
  data:
    catalog: shm
    schema: spend
    small_llm_endpoint: databricks-gpt-oss-20b
    large_llm_endpoint: databricks-claude-sonnet-4-5
    rows: 5000

categorize:
  categorization_source: bootstrap   # bootstrap | catboost | vectorsearch
  tables:
    cat_bootstrap: cat_bootstrap
    cat_catboost: cat_catboost
    cat_vectorsearch: cat_vectorsearch
    cat_reviews: cat_reviews

app:
  mode: lakebase                     # test (CSV) | prod (SDK) | lakebase (PG)
  lakebase_instance: spend-categorization
  flagging:
    low_confidence_threshold: 3.5
    critical_confidence_threshold: 2.5
""",
    language="yaml",
)

st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

# ── Workflow ──────────────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="section-eyebrow">How It Works</div>
    <div class="section-title">The continuous improvement loop</div>
    """,
    unsafe_allow_html=True,
)
st.markdown("<div style='margin-bottom:28px;'></div>", unsafe_allow_html=True)

wf_cols = st.columns(5)
steps = [
    ("1", "Generate", "Synthetic or real invoice data ingested into Delta Lake via AI_QUERY enrichment"),
    ("2", "Bootstrap", "Claude Sonnet LLM classifies every invoice with confidence score and rationale"),
    ("3", "Review", "Procurement team reviews low-confidence items in the human-in-the-loop app"),
    ("4", "Train", "CatBoost + Vector Search models retrain on human-corrected data via MLflow"),
    ("5", "Improve", "Each review cycle raises accuracy — compounding returns on every correction"),
]
for col, (num, title, desc) in zip(wf_cols, steps):
    with col:
        st.markdown(
            f"""
            <div style="text-align:center;padding:20px 12px;">
                <div style="width:48px;height:48px;background:#FF3621;border-radius:50%;
                            display:flex;align-items:center;justify-content:center;
                            font-size:1.3rem;font-weight:700;color:white;margin:0 auto 12px;">
                    {num}
                </div>
                <div style="font-size:0.95rem;font-weight:700;color:#0B2026;margin-bottom:8px;">{title}</div>
                <div style="font-size:0.8rem;color:#4a6170;line-height:1.5;">{desc}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

# ── Navigation cards ──────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="section-eyebrow">Explore the Platform</div>
    <div class="section-title">Get started</div>
    """,
    unsafe_allow_html=True,
)
st.markdown("<div style='margin-bottom:24px;'></div>", unsafe_allow_html=True)

nav1, nav2, nav3 = st.columns(3)
nav_items = [
    ("🔎", "Spend Review", "Search invoices, flag low-confidence items, and submit human corrections.", "/Spend_Review"),
    ("📊", "Analytics Dashboard", "Spend by category, supplier, region. Model accuracy trends.", "/Analytics"),
    ("⚔️", "vs Spend Solutions", "Full feature-by-feature comparison with management consulting spend solutions.", "/Comparison"),
]
for col, (icon, title, desc, href) in zip([nav1, nav2, nav3], nav_items):
    with col:
        st.markdown(
            f"""
            <a href="{href}" target="_self" style="text-decoration:none;">
                <div class="nav-card">
                    <div class="nav-card-icon">{icon}</div>
                    <div class="nav-card-title">{title}</div>
                    <div class="nav-card-desc">{desc}</div>
                </div>
            </a>
            """,
            unsafe_allow_html=True,
        )

st.markdown("<hr class='section-divider'>", unsafe_allow_html=True)

# ── What's included ───────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="section-eyebrow">What's Included</div>
    <div class="section-title">Everything in this accelerator</div>
    <div class="section-subtitle">Five pages, one unified platform — run with <code>streamlit run home.py</code></div>
    """,
    unsafe_allow_html=True,
)
st.markdown("<div style='margin-bottom:20px;'></div>", unsafe_allow_html=True)

PAGES_TABLE = [
    ("🏠", "Home + Guide", "/", "Landing page, pillars, feature comparison, cost analysis, notebook guide, data model, config reference"),
    ("🔎", "Spend Review", "/Spend_Review", "Human-in-the-loop invoice review: search, flag low-confidence items, submit corrections that train future models"),
    ("📊", "Analytics", "/Analytics", "Spend treemap, monthly trend, top 15 suppliers, AI confidence distribution, model accuracy comparison"),
    ("⚔️", "vs Spend Solutions", "/Comparison", "Deep side-by-side for all 10 spend solution offerings -- what they do, how this accelerator answers it, components, build notes"),
]

pages_rows = ""
for icon, name, url, desc in PAGES_TABLE:
    pages_rows += f"""
    <tr>
        <td style="width:32px;font-size:1.2rem;padding:12px 8px 12px 16px;">{icon}</td>
        <td style="font-weight:700;color:#0B2026;white-space:nowrap;padding:12px 16px 12px 4px;">
            <a href="{url}" target="_self" style="color:#0B2026;text-decoration:none;">{name}</a>
        </td>
        <td style="font-size:0.83rem;color:#4a6170;padding:12px 16px;">{desc}</td>
    </tr>
    """

st.markdown(
    f"""
    <table style="width:100%;border-collapse:collapse;background:white;border:1px solid #E5EBF0;border-radius:10px;overflow:hidden;">
        <thead>
            <tr style="background:#F7F9FA;border-bottom:2px solid #E5EBF0;">
                <th style="padding:10px 8px 10px 16px;text-align:left;font-size:0.75rem;color:#8CA0AC;font-weight:700;letter-spacing:0.05em;"></th>
                <th style="padding:10px 4px;text-align:left;font-size:0.75rem;color:#8CA0AC;font-weight:700;letter-spacing:0.05em;">PAGE</th>
                <th style="padding:10px 16px;text-align:left;font-size:0.75rem;color:#8CA0AC;font-weight:700;letter-spacing:0.05em;">WHAT IT CONTAINS</th>
            </tr>
        </thead>
        <tbody>{pages_rows}</tbody>
    </table>
    """,
    unsafe_allow_html=True,
)

st.markdown("<div style='margin-bottom:28px;'></div>", unsafe_allow_html=True)

# Coverage scorecard
st.markdown("**Spend solutions coverage summary**")
cov1, cov2, cov3, cov4 = st.columns(4)
coverage = [
    (cov1, "4 / 10", "Full parity", "#DCFCE7", "#15803D"),
    (cov2, "2 / 10", "Databricks advantage", "#DBEAFE", "#1D4ED8"),
    (cov3, "4 / 10", "Roadmap / partial", "#FEF9C3", "#854D0E"),
    (cov4, "0 / 10", "Competitor-only gaps", "#FEE2E2", "#B91C1C"),
]
for col, val, lbl, bg, fg in coverage:
    with col:
        st.markdown(
            f"""
            <div style="background:{bg};border-radius:8px;padding:16px 12px;text-align:center;">
                <div style="font-size:1.8rem;font-weight:700;color:{fg};">{val}</div>
                <div style="font-size:0.75rem;font-weight:600;color:{fg};margin-top:4px;">{lbl}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown(
    """
    <div style="margin-top:10px;font-size:0.78rem;color:#8CA0AC;">
    Coverage across 10 spend solution offerings: Transparency x2, Insights x4, Impact x2, Digital Backbone x1, Source AI x1.
    <a href="/Comparison" target="_self" style="color:#FF3621;font-weight:600;">See full breakdown →</a>
    </div>
    """,
    unsafe_allow_html=True,
)

# ── Footer ────────────────────────────────────────────────────────────────────
st.markdown("<div style='margin-top:64px;'></div>", unsafe_allow_html=True)
st.markdown(
    """
    <div style="background:#F7F9FA;border-radius:12px;padding:32px 40px;display:flex;
                justify-content:space-between;align-items:center;flex-wrap:wrap;gap:16px;">
        <div>
            <div style="font-weight:700;color:#0B2026;font-size:1rem;">Databricks Procurement Accelerator</div>
            <div style="font-size:0.8rem;color:#8CA0AC;margin-top:4px;">
                Open-source spend analytics on the Databricks Lakehouse
            </div>
        </div>
        <div style="font-size:0.8rem;color:#8CA0AC;">
            Built with Delta Lake · MLflow · Claude Sonnet · Streamlit
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)
