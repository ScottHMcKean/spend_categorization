"""
Invoice Classification Review Tool

Streamlit application for reviewing invoice classifications.
Supports invoice search, flagged review, and submitting reviews.

Modes:
- test: Uses in-memory mock data
- prod: Uses Lakebase PostgreSQL on Databricks
"""

import streamlit as st
import pandas as pd
from typing import List, Dict

from src.config import load_config
from src.app.database import init_backend, get_backend
from src.app import (
    search_invoices,
    get_flagged_invoices,
    get_invoices_by_ids,
    get_available_categories,
    write_reviews_batch,
)
from src.app.ui_components import (
    render_invoice_table,
    render_search_pane,
    render_flagged_pane,
    render_review_pane,
    show_success_message,
    show_error_message,
    show_info_message,
)


# Load configuration
try:
    _config = load_config()
except Exception as e:
    st.error(f"Failed to load configuration: {e}")
    st.stop()

st.set_page_config(
    page_title=_config.ui_title,
    page_icon="databricks",
    layout="wide",
    initial_sidebar_state="expanded",
)


def apply_databricks_theme():
    """Apply Databricks branding."""
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
        
        html, body, [class*="css"] {
            font-family: 'DM Sans', sans-serif !important;
            color: #0B2026 !important;
        }
        
        .main { background-color: #F9F7F4; }
        [data-testid="stSidebar"] { background-color: #EEEDE9; }
        
        h1, h2, h3, h4, h5, h6 {
            color: #0B2026 !important;
            font-family: 'DM Sans', sans-serif !important;
            font-weight: 700 !important;
        }
        
        .stButton > button {
            background-color: #FF3621;
            color: white !important;
            font-family: 'DM Sans', sans-serif !important;
            font-weight: 500;
            border: none;
            border-radius: 4px;
        }
        
        .stButton > button:hover {
            background-color: #E62E1C;
            color: white !important;
        }
        
        .stTabs [data-baseweb="tab"] {
            color: #0B2026 !important;
            font-family: 'DM Sans', sans-serif !important;
            font-weight: 500;
            font-size: 1.3rem !important;
        }
        
        .stTabs [aria-selected="true"] {
            color: #FF3621 !important;
            border-bottom-color: #FF3621 !important;
            font-weight: 700 !important;
        }
        </style>
    """,
        unsafe_allow_html=True,
    )


def initialize_session_state():
    """Initialize session state variables."""
    defaults = {
        "search_results": pd.DataFrame(),
        "flagged_results": pd.DataFrame(),
        "selected_for_review": set(),
        "review_invoices": pd.DataFrame(),
        "available_categories": [],
        "config": None,
        "backend_initialized": False,
        "total_invoices_in_review": 0,
        "completed_invoices": 0,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def load_configurations():
    """Load and initialize backend."""
    try:
        config = load_config()
        st.session_state.config = config

        if not st.session_state.backend_initialized:
            init_backend(config)
            st.session_state.backend_initialized = True

        return True
    except Exception as e:
        show_error_message(f"Configuration error: {str(e)}")
        return False


def load_available_categories():
    """Load categories from database."""
    try:
        categories = get_available_categories(st.session_state.config)
        st.session_state.available_categories = categories
    except Exception as e:
        show_error_message(f"Failed to load categories: {str(e)}")


def handle_search(search_term: str, category_filter: str = None, limit: int = 50):
    """Handle invoice search."""
    try:
        backend = get_backend()
        config = st.session_state.config

        if not search_term:
            if hasattr(backend, "_invoices"):
                results = backend._invoices.head(limit)
            else:
                results = search_invoices(config, search_term="", limit=limit)
        else:
            results = search_invoices(config, search_term=search_term, limit=limit)

        if category_filter and not results.empty:
            results = results[results["category"] == category_filter]

        st.session_state.search_results = results

        if len(results) > 0:
            show_info_message(f"Found {len(results)} invoice(s)")
        else:
            show_info_message("No invoices found")

    except Exception as e:
        show_error_message(f"Search failed: {str(e)}")


def handle_load_flagged():
    """Load flagged invoices."""
    try:
        config = st.session_state.config
        results = get_flagged_invoices(config, limit=config.page_size)
        st.session_state.flagged_results = results

        if len(results) > 0:
            show_info_message(f"Loaded {len(results)} flagged invoice(s)")
        else:
            show_info_message("No flagged invoices found")

    except Exception as e:
        show_error_message(f"Failed to load flagged invoices: {str(e)}")


def add_to_review_queue(invoice_ids: set):
    """Add invoices to review queue."""
    if not invoice_ids:
        return

    st.session_state.selected_for_review.update(invoice_ids)

    try:
        review_df = get_invoices_by_ids(
            st.session_state.config,
            list(st.session_state.selected_for_review),
        )
        st.session_state.review_invoices = review_df
        st.session_state.total_invoices_in_review = len(review_df)

    except Exception as e:
        show_error_message(f"Failed to load review invoices: {str(e)}")


def handle_submit_reviews(reviews: List[Dict]):
    """Submit invoice reviews."""
    try:
        config = st.session_state.config

        # Convert corrections format to reviews format
        formatted_reviews = []
        for r in reviews:
            formatted_reviews.append(
                {
                    "invoice_id": r["invoice_id"],
                    "category": r["corrected_category"],
                    "schema_id": "default",
                    "prompt_id": "default",
                    "labeler_id": r.get("corrected_by", config.default_user),
                    "source": "human",
                }
            )

        write_reviews_batch(config, formatted_reviews)

        if config.is_test_mode:
            show_success_message(f"[TEST MODE] Submitted {len(reviews)} review(s)")
        else:
            show_success_message(f"Submitted {len(reviews)} review(s)")

        st.session_state.completed_invoices += len(reviews)

        # Remove from queue
        for r in reviews:
            invoice_id = r["invoice_id"]
            if invoice_id in st.session_state.selected_for_review:
                st.session_state.selected_for_review.remove(invoice_id)

        # Update review invoices
        if st.session_state.selected_for_review:
            review_df = get_invoices_by_ids(
                config,
                list(st.session_state.selected_for_review),
            )
            st.session_state.review_invoices = review_df
        else:
            st.session_state.review_invoices = pd.DataFrame()

    except Exception as e:
        show_error_message(f"Failed to submit reviews: {str(e)}")


def handle_clear_review():
    """Clear review queue."""
    st.session_state.selected_for_review = set()
    st.session_state.review_invoices = pd.DataFrame()
    st.session_state.total_invoices_in_review = 0
    st.session_state.completed_invoices = 0


def render_sidebar():
    """Render sidebar."""
    config = st.session_state.config

    with st.sidebar:
        st.image("assets/databricks_logo.svg", width=200)
        st.title("Spend Categorization")
        st.divider()

        mode = config.mode.upper()
        if config.is_test_mode:
            st.caption(f"Mode: **{mode}** (Mock Data)")
        else:
            st.caption(f"Mode: **{mode}** (Lakebase)")

        st.divider()

        st.subheader("Session Stats")
        st.metric("Search Results", len(st.session_state.search_results))
        st.metric("Flagged Invoices", len(st.session_state.flagged_results))

        if st.session_state.total_invoices_in_review > 0:
            st.divider()
            st.subheader("Review Progress")

            done = st.session_state.completed_invoices
            remaining = len(st.session_state.review_invoices)
            total = st.session_state.total_invoices_in_review

            st.write(f"**Done:** {done}")
            st.write(f"**Remaining:** {remaining}")

            if total > 0:
                st.progress(done / total)
                st.caption(f"{done} of {total} invoices labeled")

        st.divider()

        if st.button("Refresh Categories", use_container_width=True):
            load_available_categories()
            st.rerun()


def main():
    """Main application."""
    apply_databricks_theme()
    initialize_session_state()

    if st.session_state.config is None:
        if not load_configurations():
            st.stop()

    config = st.session_state.config

    if not st.session_state.available_categories:
        load_available_categories()

    render_sidebar()

    if config.is_test_mode:
        st.info("**Test Mode**: Using sample data. No database changes will be made.")

    tab1, tab2, tab3 = st.tabs(["Search", "Flagged Invoices", "Review & Correct"])

    with tab1:
        render_search_pane(
            on_search=handle_search,
            available_categories=st.session_state.available_categories,
        )
        st.divider()

        if not st.session_state.search_results.empty:
            st.subheader("Search Results")
            selected_ids = render_invoice_table(
                st.session_state.search_results,
                key_prefix="search",
                selectable=True,
            )

            if selected_ids:
                if st.button("Add Selected to Review Queue", type="primary"):
                    add_to_review_queue(selected_ids)
                    show_success_message(
                        f"Added {len(selected_ids)} invoice(s) to review queue"
                    )
                    st.rerun()

    with tab2:
        render_flagged_pane(on_load_flagged=handle_load_flagged)
        st.divider()

        if not st.session_state.flagged_results.empty:
            st.subheader("Flagged Invoices")
            selected_ids = render_invoice_table(
                st.session_state.flagged_results,
                key_prefix="flagged",
                selectable=True,
            )

            if selected_ids:
                if st.button(
                    "Add Selected to Review Queue", type="primary", key="add_flagged"
                ):
                    add_to_review_queue(selected_ids)
                    show_success_message(
                        f"Added {len(selected_ids)} invoice(s) to review queue"
                    )
                    st.rerun()

    with tab3:
        render_review_pane(
            review_invoices=st.session_state.review_invoices,
            available_categories=st.session_state.available_categories,
            on_submit_corrections=handle_submit_reviews,
            on_clear_review=handle_clear_review,
        )


if __name__ == "__main__":
    main()
