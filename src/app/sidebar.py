"""Shared sidebar with Databricks asset links and debug info.

Collapsed by default. Contains links to UC tables, MLflow experiments,
Databricks jobs, and a debug section with table counts.
"""

import os
import logging
from typing import Optional

import streamlit as st

from src.config import Config

logger = logging.getLogger(__name__)


def _workspace_url() -> str:
    return os.getenv("DATABRICKS_HOST", "").rstrip("/")


def render_databricks_sidebar(config: Config) -> None:
    """Render the shared Databricks assets sidebar."""
    host = _workspace_url()
    catalog = config.catalog
    schema = config.schema_name

    with st.sidebar:
        st.image("assets/databricks_logo.svg", width=140)
        st.caption("Procurement Accelerator")

        if host:
            uc_base = f"{host}/#/explore/data/{catalog}/{schema}"
            st.markdown(
                f'<div style="font-size:0.78rem;line-height:2;">'
                f'<a href="{uc_base}" target="_blank" style="color:#0B6B8A;">Unity Catalog</a> · '
                f'<a href="{host}/#/mlflow" target="_blank" style="color:#0B6B8A;">MLflow</a> · '
                f'<a href="{host}/#/jobs" target="_blank" style="color:#0B6B8A;">Jobs</a>'
                f"</div>",
                unsafe_allow_html=True,
            )
        else:
            st.caption(f"{catalog}.{schema}")

        with st.expander("Debug", expanded=False):
            st.caption(f"Mode: {config.app_mode} | Source: {config.categorization_source}")

            if config.app_mode == "lakebase":
                st.caption(f"Instance: {config.lakebase_instance}")

            try:
                from src.app.database import get_backend
                backend = get_backend()
                connected = backend.is_connected()
                st.caption(f"{type(backend).__name__} | Connected: {connected}")

                if connected:
                    tables = [
                        ("invoices", config.full_invoices_table_path),
                        ("categories", config.full_categories_table_path),
                        ("bootstrap", config.full_cat_bootstrap_table_path),
                        ("catboost", config.full_cat_catboost_table_path),
                        ("vectorsearch", config.full_cat_vectorsearch_table_path),
                        ("reviews", config.full_cat_reviews_table_path),
                    ]
                    counts = []
                    for label, table_path in tables:
                        try:
                            df = backend.execute_query(
                                f"SELECT count(*) as cnt FROM {table_path}"
                            )
                            cnt = df["cnt"].iloc[0] if not df.empty else "?"
                            counts.append(f"{label}: {cnt}")
                        except Exception:
                            counts.append(f"{label}: --")
                    st.caption(" | ".join(counts[:3]))
                    st.caption(" | ".join(counts[3:]))
            except Exception:
                st.caption("Backend: not initialized")
