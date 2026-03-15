"""
Databricks Procurement Accelerator -- vs Spend Solutions
Deep feature comparison with management consulting spend solutions.
"""

import streamlit as st

st.set_page_config(
    page_title="vs Spend Solutions · Databricks Procurement Accelerator",
    page_icon="⚔️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

from src.config import load_config
from src.app.sidebar import render_databricks_sidebar
render_databricks_sidebar(load_config())

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;700&display=swap');
    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif !important; color: #0B2026;
    }
    .main { background-color: #F9FAFB; }
    .stButton > button {
        background-color: #FF3621 !important; color: white !important;
        border: none !important; border-radius: 6px !important; font-weight: 600 !important;
    }
    .section-eyebrow {
        font-size: 0.7rem; font-weight: 700; letter-spacing: 0.15em;
        text-transform: uppercase; color: #FF3621; margin-bottom: 4px;
    }
    .section-title { font-size: 1.4rem; font-weight: 700; color: #0B2026; margin-bottom: 4px; }
    .section-subtitle { font-size: 0.95rem; color: #4a6170; line-height: 1.6; max-width: 700px; }

    .offering-header {
        background: #0B2026;
        color: white;
        padding: 20px 28px;
        border-radius: 10px 10px 0 0;
        font-size: 1.1rem;
        font-weight: 700;
    }
    .offering-body {
        background: white;
        border: 1px solid #E5EBF0;
        border-top: none;
        border-radius: 0 0 10px 10px;
        padding: 24px 28px;
        margin-bottom: 20px;
    }
    .offering-competitor {
        background: #F7F9FA;
        border-left: 3px solid #CBD5E1;
        padding: 14px 16px;
        border-radius: 0 6px 6px 0;
        margin-bottom: 12px;
    }
    .offering-databricks {
        background: #F0FDF4;
        border-left: 3px solid #22C55E;
        padding: 14px 16px;
        border-radius: 0 6px 6px 0;
        margin-bottom: 12px;
    }
    .offering-label {
        font-size: 0.68rem; font-weight: 700; letter-spacing: 0.1em;
        text-transform: uppercase; margin-bottom: 6px;
    }
    .offering-label-sp { color: #6B8899; }
    .offering-label-db { color: #15803D; }
    .offering-text { font-size: 0.85rem; color: #2D4452; line-height: 1.6; }
    .feature-bullet { font-size: 0.82rem; color: #2D4452; margin: 3px 0; }

    .chip {
        display: inline-block; padding: 3px 10px; border-radius: 100px;
        font-size: 0.73rem; font-weight: 700; margin: 2px;
    }
    .chip-green { background: #DCFCE7; color: #15803D; }
    .chip-blue { background: #DBEAFE; color: #1D4ED8; }
    .chip-orange { background: #FEF3C7; color: #92400E; }
    .chip-red { background: #FEE2E2; color: #B91C1C; }
    .chip-gray { background: #F1F5F9; color: #475569; }

    .verdict-card {
        border-radius: 10px;
        padding: 24px 28px;
        margin-bottom: 12px;
    }
    .verdict-win { background: #F0FDF4; border: 1.5px solid #86EFAC; }
    .verdict-tie { background: #FEF9C3; border: 1.5px solid #FDE047; }
    .verdict-lose { background: #FFF7ED; border: 1.5px solid #FED7AA; }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Header ────────────────────────────────────────────────────────────────────
hc1, hc2 = st.columns([3, 1])
with hc1:
    st.markdown(
        """
        <div class="section-eyebrow">Competitive Analysis</div>
        <div class="section-title">Databricks Procurement Accelerator vs. Management Consulting Spend Solutions</div>
        <div class="section-subtitle">
            A feature-by-feature comparison of every spend solution offering against what the open-source
            Databricks Procurement Accelerator delivers -- and how you'd build the rest.
        </div>
        """,
        unsafe_allow_html=True,
    )
with hc2:
    st.markdown("<div style='margin-top:20px;'></div>", unsafe_allow_html=True)
    if st.button("← Back to Home"):
        st.switch_page("home.py")

st.divider()

# ── Summary scorecard ─────────────────────────────────────────────────────────
st.markdown("**Summary Scorecard**")
sc1, sc2, sc3, sc4 = st.columns(4)
for col, lbl, val, chip_cls in [
    (sc1, "Full parity", "4 / 10", "chip-green"),
    (sc2, "Databricks advantage", "2 / 10", "chip-blue"),
    (sc3, "Roadmap / partial", "4 / 10", "chip-orange"),
    (sc4, "Competitor-only", "0 / 10", "chip-red"),
]:
    with col:
        st.markdown(
            f"""
            <div style="background:white;border:1px solid #E5EBF0;border-radius:8px;
                        padding:16px;text-align:center;">
                <div style="font-size:1.6rem;font-weight:700;color:#0B2026;">{val}</div>
                <span class="chip {chip_cls}">{lbl}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )
st.caption("Coverage assessed against 10 spend solution offerings (Transparency x 2, Insights x 4, Impact x 2, Digital Backbone x 1, Source AI x 1)")

st.divider()

# ── Detailed offering comparisons ─────────────────────────────────────────────
OFFERINGS = [
    {
        "title": "Transparency · Single Source of Truth",
        "category": "Transparency",
        "verdict": "win",
        "verdict_label": "Full parity + Databricks advantage",
        "competitor_bullets": [
            "ETL pipeline across ERP systems (SAP, Oracle, etc.)",
            "Refreshable data model on line-item level",
            "Intelligent combination of master + transactional data",
            "Flexible data model for combinatorial analytics",
            "Data Extraction & Exchange Suite",
            "Data Transformation Engine",
            "Supplier Knowledge Base (200M+ entries)",
        ],
        "databricks_bullets": [
            "Delta Lake as the ACID-compliant single source of truth — all tables in one catalog.schema",
            "MERGE INTO pattern: idempotent, refreshable pipelines — re-run safely at any time",
            "AI_QUERY() for inline LLM enrichment of raw data at scale (thousands of rows per batch)",
            "Flexible schema via Delta's schema evolution — add columns without pipeline rewrites",
            "PySpark + Databricks Workflows for ERP connector integration",
            "Open supplier data enrichment via Delta Sharing Marketplace",
            "Time travel for audit & rollback — every version of every table preserved",
        ],
        "components": ["Delta Lake", "Databricks Workflows", "AI_QUERY", "PySpark", "Delta Sharing"],
        "build_note": "Replace legacy ETL with Databricks Workflows + a connector to your ERP (SAP, Oracle, Coupa). Delta Lake handles the rest natively.",
    },
    {
        "title": "Transparency · Data Cleansing & Categorization",
        "category": "Transparency",
        "verdict": "win",
        "verdict_label": "Full parity — this is the core of the accelerator",
        "competitor_bullets": [
            "ML-powered automatic categorization at line-item level",
            "Global supplier knowledge base (hundreds of millions of entries)",
            "UNSPSC taxonomy + client-specific taxonomy mapping",
            "Supplier name harmonization & deduplication",
            "Multi-step matching algorithm (multi-language)",
            "Confidence scores + prediction of future categorizations",
            "User-friendly UI for review, validation, and browsing",
        ],
        "databricks_bullets": [
            "3-model ensemble: Claude Sonnet LLM → CatBoost → Vector Search RAG",
            "LLM bootstrap produces Level 1/2/3 predictions + confidence (1–5 scale) + rationale",
            "Human-in-the-loop Streamlit review app — submit corrections that train future models",
            "Append-only cat_reviews table = growing ground truth dataset, full audit trail",
            "Configurable 3-level taxonomy via categories.csv — swap in UNSPSC or any standard",
            "Prompt versioning and optimisation loop via MLflow Prompt Registry",
            "Vector Search finds 'most similar previously reviewed invoice' — explainable AI",
            "CatBoost retrains automatically on accumulated human reviews",
        ],
        "components": ["Claude Sonnet 4.5", "CatBoost", "Databricks Vector Search", "MLflow", "Streamlit"],
        "build_note": "The full categorization pipeline (notebooks 2–5) is already built. Swap the categories.csv taxonomy for your own or for UNSPSC.",
    },
    {
        "title": "Insights · Spend & Opportunity Analytics",
        "category": "Insights",
        "verdict": "win",
        "verdict_label": "Full parity — analytics on your own Delta Lake",
        "competitor_bullets": [
            "Procurement cockpit — opportunity identification",
            "Supplier negotiation fact base",
            "Category insights across all dimensions",
            "Working capital optimisation (A/P focus)",
            "Synergy analytics for M&A",
            "Automated opportunity scan vs. ideal scenarios",
            "Guided top-down navigation design",
        ],
        "databricks_bullets": [
            "Spend & opportunity analytics on Delta Lake via Databricks SQL — no data export needed",
            "Spend by category (treemap), supplier (ranked bar), region (heatmap), cost centre",
            "Monthly trend analysis to surface spend growth and anomalies",
            "Supplier concentration analysis — identify negotiation leverage",
            "Category drill-down from L1 → L2 → L3 with unit price benchmarking",
            "Databricks Genie / AI/BI for natural language querying of spend data",
            "Opportunity scan: flag categories with high variance in unit price across suppliers",
        ],
        "components": ["Databricks SQL", "Databricks Genie", "Delta Lake", "Plotly", "Streamlit"],
        "build_note": "The Analytics page covers the core. Extend with Databricks AI/BI Dashboards for executive-facing views without the Streamlit layer.",
    },
    {
        "title": "Insights · Strategy & Negotiation",
        "category": "Insights",
        "verdict": "tie",
        "verdict_label": "Roadmap — LLM building blocks are already in place",
        "competitor_bullets": [
            "Curated analytics journeys for procurement managers",
            "Preparation for supplier negotiations",
            "Category strategy review across all dimensions",
            "Cross-functional team support",
            "Consulting procurement expertise embedded",
        ],
        "databricks_bullets": [
            "Claude Sonnet 4.5 can generate category summaries and negotiation prep from spend data",
            "MLflow Prompt Registry + prompt optimisation loop is the foundation",
            "Extend 3_optimize.ipynb to generate category strategy reports per L2",
            "Supplier consolidation signals surfaced by the review app (duplicate detection)",
            "Open data (commodity prices via Delta Sharing) enriches negotiation prep context",
        ],
        "components": ["Claude Sonnet 4.5", "MLflow Prompt Registry", "Delta Sharing", "Databricks SQL"],
        "build_note": "Build a 'Category Report' notebook that runs Claude over the spend cube for each category and outputs a structured negotiation brief.",
    },
    {
        "title": "Insights · Carbon & Sustainability",
        "category": "Insights",
        "verdict": "tie",
        "verdict_label": "Roadmap — open emissions data available today",
        "competitor_bullets": [
            "Scope 3 emissions reporting (supply chain)",
            "Carbon emission factor enrichment per category",
            "Proprietary carbon tracking integration",
            "Decarbonisation journey tracking",
            "Carbon factor data constantly updated",
        ],
        "databricks_bullets": [
            "EXIOBASE and EPA open emissions factor datasets — joinable to your spend cube",
            "Spend × emissions intensity factor = Scope 3 estimate by category and supplier",
            "Climatiq API available for real-time emission factors via Delta Live Tables",
            "Delta Sharing Marketplace: subscribe to open carbon datasets",
            "No proprietary carbon data license required",
        ],
        "components": ["Delta Sharing", "EXIOBASE Open Data", "EPA Emissions", "Climatiq API", "Databricks SQL"],
        "build_note": "Join categories.csv to an open EEIO/EXIOBASE table by UNSPSC code. One SQL query gives you Scope 3 estimates — no Catalyst Zero subscription needed.",
    },
    {
        "title": "Insights · Input Cost & Resilience",
        "category": "Insights",
        "verdict": "tie",
        "verdict_label": "Roadmap — open commodity data available via Delta Sharing",
        "competitor_bullets": [
            "Real-time market data integration",
            "Supply chain disruption signals",
            "Price variance alerts",
            "Alignment with corporate sustainability goals",
            "Respond to market trends in a timely manner",
        ],
        "databricks_bullets": [
            "Databricks Marketplace: subscribe to commodity price feeds (LME metals, energy)",
            "Join commodity price time series to spend cube by category for cost pressure analysis",
            "Supplier country risk scores (World Bank open data) via Delta Sharing",
            "Alert logic via Databricks SQL alerts on materialised views",
        ],
        "components": ["Databricks Marketplace", "Delta Sharing", "Databricks SQL Alerts", "Open Commodity Data"],
        "build_note": "Extend Analytics page with a 'Risk & Resilience' tab. Subscribe to commodity price datasets from the Databricks Marketplace (free tier available).",
    },
    {
        "title": "Impact · Integrated Impact Management",
        "category": "Impact",
        "verdict": "tie",
        "verdict_label": "Roadmap — foundation in place, project tracking to add",
        "competitor_bullets": [
            "Project management for savings initiatives",
            "Real-time savings tracking",
            "P&L reconciliation with spend cube link",
            "Collaborative features for cross-team use",
            "Wave program management platform",
            "Best-in-class savings guideline built-in",
        ],
        "databricks_bullets": [
            "cat_reviews as the savings baseline — every corrected categorization is a measurable improvement",
            "MLflow experiment tracking: measures model accuracy improvement per review cycle",
            "Add a savings_initiatives Delta table: link spend deltas to procurement projects",
            "Lakebase PostgreSQL: collaborative review workflow already built",
            "Databricks Workflows: schedule accuracy reports and savings reconciliation",
        ],
        "components": ["MLflow", "Lakebase PostgreSQL", "Delta Lake", "Databricks Workflows"],
        "build_note": "Add a savings_initiatives table to track projects. Build a notebook that computes delta between baseline spend and post-categorization-corrected spend per category.",
    },
    {
        "title": "Impact · Managed Insights Services",
        "category": "Impact",
        "verdict": "win",
        "verdict_label": "Databricks advantage — no consulting lock-in",
        "competitor_bullets": [
            "Consulting expert team access",
            "Insights workshops",
            "Capability building programs",
            "Data-driven performance dialogues",
            "Ongoing managed analytics services",
        ],
        "databricks_bullets": [
            "Fully open-source -- no consulting retainer required",
            "Databricks Academy: free training on all platform components",
            "Community support + GitHub issues for the accelerator",
            "Every algorithm is auditable — no black-box procurement models",
            "Extend yourself or hire any data engineering team (not locked to one vendor)",
            "Lower TCO: estimated 70-85% lower than traditional spend solutions + consulting",
        ],
        "components": ["Open Source", "Databricks Academy", "GitHub"],
        "build_note": "The accelerator IS the capability-building tool. Run it with your procurement team — each notebook is a learning exercise as well as a production pipeline.",
    },
    {
        "title": "Digital Backbone · Spend Platform (SaaS)",
        "category": "Digital Backbone",
        "verdict": "win",
        "verdict_label": "Full parity — Databricks is the platform",
        "competitor_bullets": [
            "Fully scalable SaaS deployment",
            "Regular product updates",
            "Different deployment scenarios",
            "Flexible ERP integration depth",
            "SAP BTP integration option",
            "Celonis integration option",
        ],
        "databricks_bullets": [
            "Databricks serverless: scale from 0 to petabytes — pay only for what you use",
            "Multi-cloud: deploy on AWS, Azure, or GCP in your own account",
            "Databricks Apps: host the Streamlit review app natively with SSO",
            "Native Unity Catalog governance: row-level security, column masking, lineage",
            "Integrates with SAP, Oracle, Coupa via Databricks Partner Connect",
            "Your data never leaves your cloud — no SaaS data residency issues",
        ],
        "components": ["Databricks Serverless", "Unity Catalog", "Databricks Apps", "Partner Connect", "Multi-cloud"],
        "build_note": "Deploy to production: enable Databricks Serverless SQL, host the app via Databricks Apps, and set up Unity Catalog for governance.",
    },
    {
        "title": "Source AI · Category Management",
        "category": "Source AI",
        "verdict": "win",
        "verdict_label": "Full parity — same generative AI, open model choice",
        "competitor_bullets": [
            "360° market intelligence engine",
            "Real-time internal + external category data",
            "Contract review and summarisation",
            "Automated idea generation for savings",
            "Negotiation argument structuring",
            "Category analytics and spend cube",
        ],
        "databricks_bullets": [
            "Claude Sonnet 4.5 for category analysis — same generation of LLM as Source AI",
            "MLflow Prompt Registry: versioned, optimised prompts for each category type",
            "3_optimize.ipynb: automated prompt improvement loop driven by human feedback",
            "Open model choice: swap Claude for Llama 3, Mistral, or any DBRX model",
            "Category spend cube built from Delta Lake — no separate data extraction",
            "Add web search grounding (Databricks AI/BI) for real-time market intelligence",
        ],
        "components": ["Claude Sonnet 4.5", "MLflow Prompt Registry", "Databricks AI/BI", "Open Model Choice"],
        "build_note": "Source AI = prompt engineering on top of spend data. You already have both. Extend 3_optimize.ipynb to produce per-category strategy documents automatically.",
    },
]

for offering in OFFERINGS:
    verdict_class = {
        "win": "verdict-win",
        "tie": "verdict-tie",
        "lose": "verdict-lose",
    }[offering["verdict"]]
    verdict_icon = {"win": "✅", "tie": "🟡", "lose": "❌"}[offering["verdict"]]

    sp_bullets = "".join(f'<div class="feature-bullet">• {b}</div>' for b in offering["competitor_bullets"])
    db_bullets = "".join(f'<div class="feature-bullet">• {b}</div>' for b in offering["databricks_bullets"])
    chips = "".join(f'<span class="chip chip-blue">{c}</span>' for c in offering["components"])

    st.markdown(
        f"""
        <div class="offering-header">
            {verdict_icon} {offering['title']}
            <span style="font-size:0.75rem;font-weight:400;opacity:0.7;margin-left:12px;">{offering['category']}</span>
        </div>
        <div class="offering-body">
            <div class="verdict-card {verdict_class}" style="margin-bottom:16px;">
                <strong>Verdict:</strong> {offering['verdict_label']}
            </div>
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px;">
                <div>
                    <div class="offering-competitor">
                        <div class="offering-label offering-label-sp">Management Consulting</div>
                        <div class="offering-text">{sp_bullets}</div>
                    </div>
                </div>
                <div>
                    <div class="offering-databricks">
                        <div class="offering-label offering-label-db">Databricks Procurement Accelerator</div>
                        <div class="offering-text">{db_bullets}</div>
                    </div>
                </div>
            </div>
            <div style="margin-top:4px;">
                <span style="font-size:0.75rem;font-weight:700;color:#6B8899;">COMPONENTS: </span>{chips}
            </div>
            <div style="margin-top:12px;background:#F7F9FA;border-radius:6px;padding:10px 14px;">
                <span style="font-size:0.75rem;font-weight:700;color:#0B2026;">HOW TO BUILD IT: </span>
                <span style="font-size:0.82rem;color:#4a6170;">{offering['build_note']}</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.divider()

# ── Final verdict ─────────────────────────────────────────────────────────────
st.markdown(
    """
    <div class="section-eyebrow">Bottom Line</div>
    <div class="section-title">Why choose the Databricks Procurement Accelerator?</div>
    """,
    unsafe_allow_html=True,
)
st.markdown("<div style='margin-bottom:20px;'></div>", unsafe_allow_html=True)

b1, b2, b3 = st.columns(3)
for col, icon, title, body in [
    (
        b1, "💰", "Dramatically lower cost",
        "No SaaS subscription fees. No consulting retainer. Pay only for Databricks compute -- which you likely already have. Estimated 70-85% lower TCO.",
    ),
    (
        b2, "🔓", "No vendor lock-in",
        "Your data stays in your own lakehouse. Your code is open-source. Swap LLM providers, change taxonomies, add new models — full control at every layer.",
    ),
    (
        b3, "🚀", "Built on the same AI",
        "Claude Sonnet 4.5 powers both Source AI and this accelerator. You get the same generation of generative AI without the consulting markup.",
    ),
]:
    with col:
        st.markdown(
            f"""
            <div style="background:white;border:1.5px solid #E5EBF0;border-radius:10px;padding:24px;height:100%;">
                <div style="font-size:2rem;margin-bottom:12px;">{icon}</div>
                <div style="font-size:0.95rem;font-weight:700;color:#0B2026;margin-bottom:8px;">{title}</div>
                <div style="font-size:0.85rem;color:#4a6170;line-height:1.6;">{body}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown("<div style='margin-top:40px;'></div>", unsafe_allow_html=True)
st.markdown(
    "<div style='text-align:center;font-size:0.78rem;color:#8CA0AC;'>"
    "Databricks Procurement Accelerator · Competitive Analysis vs. Management Consulting Spend Solutions"
    "</div>",
    unsafe_allow_html=True,
)
