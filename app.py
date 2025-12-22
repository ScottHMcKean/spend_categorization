"""
Invoice Classification Correction Tool

A Streamlit application for correcting invoice classifications with Databricks integration.
Supports invoice search, flagged invoice review, and batch corrections with Type 2 SCD tracking.

Modes:
- test: Uses in-memory mock data (no database required)
- prod: Uses Lakebase PostgreSQL on Databricks
"""

import streamlit as st
import pandas as pd
from typing import List, Dict

from invoice_app.config import load_config, LakebaseConfig, AppConfig
from invoice_app.database import init_backend, get_backend
from invoice_app import (
    search_invoices,
    get_flagged_invoices,
    get_invoices_by_ids,
    get_available_categories,
    write_corrections_batch,
)
from invoice_app.ui_components import (
    render_invoice_table,
    render_search_pane,
    render_flagged_pane,
    render_review_pane,
    show_success_message,
    show_error_message,
    show_info_message,
)


# Load configuration first (before page config)
try:
    _config = load_config()
    _app_config_temp = AppConfig.from_dict(_config)
except Exception as e:
    st.error(f"Failed to load configuration: {e}")
    st.stop()

# Page configuration
st.set_page_config(
    page_title=_app_config_temp.ui_title,
    page_icon="🧱",
    layout="wide",
    initial_sidebar_state="expanded",
)


def apply_databricks_theme():
    """Apply Databricks branding and typography."""
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
        
        /* Global styles */
        html, body, [class*="css"] {
            font-family: 'DM Sans', sans-serif !important;
            color: #0B2026 !important;
        }
        
        /* Main background */
        .main {
            background-color: #F9F7F4;
        }
        
        /* Sidebar */
        [data-testid="stSidebar"] {
            background-color: #EEEDE9;
        }
        
        /* Headers */
        h1, h2, h3, h4, h5, h6 {
            color: #0B2026 !important;
            font-family: 'DM Sans', sans-serif !important;
            font-weight: 700 !important;
        }
        
        /* Buttons */
        .stButton > button {
            background-color: #FF3621;
            color: white !important;
            font-family: 'DM Sans', sans-serif !important;
            font-weight: 500;
            border: none;
            border-radius: 4px;
            padding: 0.5rem 1rem;
        }
        
        .stButton > button:hover {
            background-color: #E62E1C;
            color: white !important;
        }
        
        /* Text inputs */
        .stTextInput > div > div > input {
            font-family: 'DM Sans', sans-serif !important;
            color: #0B2026 !important;
            background-color: white;
            border: 1px solid #EEEDE9;
        }
        
        /* Select boxes */
        .stSelectbox > div > div > select {
            font-family: 'DM Sans', sans-serif !important;
            color: #0B2026 !important;
        }
        
        /* Expanders */
        .streamlit-expanderHeader {
            font-family: 'DM Sans', sans-serif !important;
            color: #0B2026 !important;
            background-color: #EEEDE9;
            padding: 0.5rem !important;
        }
        
        .streamlit-expanderContent {
            padding: 0.75rem !important;
        }
        
        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {
            background-color: transparent;
        }
        
        .stTabs [data-baseweb="tab"] {
            color: #0B2026 !important;
            font-family: 'DM Sans', sans-serif !important;
            font-weight: 500;
            background-color: transparent;
            font-size: 1.3rem !important;
            padding: 0.75rem 1.5rem !important;
        }
        
        .stTabs [aria-selected="true"] {
            color: #FF3621 !important;
            border-bottom-color: #FF3621 !important;
            background-color: transparent;
            font-weight: 700 !important;
        }
        
        /* Info/Warning boxes */
        .stAlert {
            font-family: 'DM Sans', sans-serif !important;
        }
        
        /* Metrics */
        [data-testid="stMetricValue"] {
            color: #0B2026 !important;
            font-family: 'DM Sans', sans-serif !important;
        }
        
        /* Dividers */
        hr {
            border-color: #EEEDE9 !important;
        }
        
        /* Logo container */
        .logo-container {
            display: flex;
            align-items: center;
            margin-bottom: 2rem;
            padding: 1rem;
            background-color: white;
            border-radius: 8px;
        }
        
        /* Checkbox */
        .stCheckbox {
            color: #0B2026 !important;
        }
        </style>
    """,
        unsafe_allow_html=True,
    )


def initialize_session_state():
    """Initialize Streamlit session state variables."""
    if "search_results" not in st.session_state:
        st.session_state.search_results = pd.DataFrame()

    if "flagged_results" not in st.session_state:
        st.session_state.flagged_results = pd.DataFrame()

    if "selected_for_review" not in st.session_state:
        st.session_state.selected_for_review = set()

    if "review_invoices" not in st.session_state:
        st.session_state.review_invoices = pd.DataFrame()

    if "available_categories" not in st.session_state:
        st.session_state.available_categories = []

    if "app_config" not in st.session_state:
        st.session_state.app_config = None

    if "lakebase_config" not in st.session_state:
        st.session_state.lakebase_config = None

    if "backend_initialized" not in st.session_state:
        st.session_state.backend_initialized = False

    if "total_invoices_in_review" not in st.session_state:
        st.session_state.total_invoices_in_review = 0

    if "completed_invoices" not in st.session_state:
        st.session_state.completed_invoices = 0


def load_configurations():
    """Load database and application configurations."""
    try:
        config = load_config()
        app_config = AppConfig.from_dict(config)
        st.session_state.app_config = app_config

        # Load Lakebase config for prod mode
        if app_config.is_prod_mode:
            lakebase_config = LakebaseConfig.from_dict(config)
            lakebase_config.validate()
            st.session_state.lakebase_config = lakebase_config
        else:
            st.session_state.lakebase_config = None

        # Initialize the database backend
        if not st.session_state.backend_initialized:
            init_backend(app_config, st.session_state.lakebase_config)
            st.session_state.backend_initialized = True

        return True
    except Exception as e:
        show_error_message(f"Configuration error: {str(e)}")
        return False


def load_available_categories():
    """Load available categories from the database or demo data."""
    try:
        categories = get_available_categories(st.session_state.app_config)
        st.session_state.available_categories = categories
    except Exception as e:
        show_error_message(f"Failed to load categories: {str(e)}")


def handle_search(search_term: str, category_filter: str = None, limit: int = 50):
    """Handle invoice search."""
    try:
        # Get backend for queries
        backend = get_backend()
        
        if not search_term:
            # Return first N invoices when no search term
            results = search_invoices(
                st.session_state.app_config,
                search_term="",
                limit=limit,
            )
            # For empty search, just get all (the query handles it)
            if hasattr(backend, '_invoices'):
                # MockBackend - get all invoices
                results = backend._invoices.head(limit)
        else:
            results = search_invoices(
                st.session_state.app_config,
                search_term=search_term,
                limit=limit,
            )

        # Apply category filter if specified
        if category_filter and not results.empty:
            results = results[results["category"] == category_filter]

        st.session_state.search_results = results

        if len(results) > 0:
            show_info_message(f"Found {len(results)} invoice(s)")
        else:
            show_info_message("No invoices found matching your search")

    except Exception as e:
        show_error_message(f"Search failed: {str(e)}")


def handle_load_flagged():
    """Handle loading flagged invoices."""
    try:
        results = get_flagged_invoices(
            st.session_state.app_config,
            limit=st.session_state.app_config.page_size,
        )

        st.session_state.flagged_results = results

        if len(results) > 0:
            show_info_message(f"Loaded {len(results)} flagged invoice(s)")
        else:
            show_info_message("No flagged invoices found")

    except Exception as e:
        show_error_message(f"Failed to load flagged invoices: {str(e)}")


def add_to_review_queue(invoice_ids: set):
    """Add selected invoices to the review queue."""
    if not invoice_ids:
        return

    # Combine IDs from both search and flagged results
    st.session_state.selected_for_review.update(invoice_ids)

    # Fetch full invoice details
    try:
        review_df = get_invoices_by_ids(
            st.session_state.app_config,
            list(st.session_state.selected_for_review),
        )
        st.session_state.review_invoices = review_df

        # Update total count when adding new invoices
        st.session_state.total_invoices_in_review = len(review_df)

    except Exception as e:
        show_error_message(f"Failed to load review invoices: {str(e)}")


def handle_submit_corrections(corrections: List[Dict]):
    """Handle submission of invoice corrections."""
    try:
        if st.session_state.app_config.is_test_mode:
            # In test mode, show success message but still process through backend
            write_corrections_batch(st.session_state.app_config, corrections)
            show_success_message(
                f"[TEST MODE] Successfully submitted {len(corrections)} correction(s)!"
            )
        else:
            write_corrections_batch(st.session_state.app_config, corrections)
            show_success_message(
                f"Successfully submitted {len(corrections)} correction(s)!"
            )

        # Increment completed count
        st.session_state.completed_invoices += len(corrections)

        # Remove submitted invoices from review queue
        for correction in corrections:
            invoice_id = correction["invoice_id"]
            if invoice_id in st.session_state.selected_for_review:
                st.session_state.selected_for_review.remove(invoice_id)

        # Update review_invoices DataFrame
        if st.session_state.selected_for_review:
            review_df = get_invoices_by_ids(
                st.session_state.app_config,
                list(st.session_state.selected_for_review),
            )
            st.session_state.review_invoices = review_df
        else:
            # All invoices processed, clear everything
            st.session_state.review_invoices = pd.DataFrame()
            st.session_state.current_invoice_idx = 0

    except Exception as e:
        show_error_message(f"Failed to submit corrections: {str(e)}")


def handle_clear_review():
    """Clear the review queue."""
    st.session_state.selected_for_review = set()
    st.session_state.review_invoices = pd.DataFrame()
    st.session_state.total_invoices_in_review = 0
    st.session_state.completed_invoices = 0


def render_sidebar():
    """Render the sidebar with configuration and stats."""
    with st.sidebar:
        # Databricks logo at the top
        st.image("assets/databricks_logo.svg", width=200)

        st.title("Spend Categorization")

        st.divider()

        # Mode indicator
        mode = st.session_state.app_config.mode.upper()
        if st.session_state.app_config.is_test_mode:
            st.caption(f"🧪 Mode: **{mode}** (Mock Data)")
        else:
            st.caption(f"🏭 Mode: **{mode}** (Lakebase)")

        st.divider()

        # Stats
        st.subheader("Session Stats")
        st.metric("Search Results", len(st.session_state.search_results))
        st.metric("Flagged Invoices", len(st.session_state.flagged_results))

        # Review progress
        if st.session_state.total_invoices_in_review > 0:
            st.divider()
            st.subheader("Review Progress")

            done = st.session_state.completed_invoices
            remaining = len(st.session_state.review_invoices)
            total = st.session_state.total_invoices_in_review

            st.write(f"**Done:** {done}")
            st.write(f"**Remaining:** {remaining}")

            # Progress bar showing done / total
            if total > 0:
                progress = done / total
                st.progress(progress)
                st.caption(f"{done} of {total} invoices labeled")

        st.divider()

        # Refresh data
        if st.button("Refresh Categories", use_container_width=True):
            load_available_categories()
            st.rerun()


def main():
    """Main application logic."""
    # Apply Databricks theme
    apply_databricks_theme()

    initialize_session_state()

    # Load configurations and initialize backend
    if st.session_state.app_config is None:
        if not load_configurations():
            st.stop()

    # Load categories if not already loaded
    if not st.session_state.available_categories:
        load_available_categories()

    # Render sidebar
    render_sidebar()

    # Main content area
    if st.session_state.app_config.is_test_mode:
        st.info(
            "**Test Mode**: This is a demonstration using sample data. No actual database changes will be made."
        )

    # Create tabs for different views
    tab1, tab2, tab3 = st.tabs(["Search", "Flagged Invoices", "Review & Correct"])

    # Tab 1: Search
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

    # Tab 2: Flagged Invoices
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

    # Tab 3: Review & Correct
    with tab3:
        render_review_pane(
            review_invoices=st.session_state.review_invoices,
            available_categories=st.session_state.available_categories,
            on_submit_corrections=handle_submit_corrections,
            on_clear_review=handle_clear_review,
        )


if __name__ == "__main__":
    main()
