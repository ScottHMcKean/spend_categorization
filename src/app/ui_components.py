"""Streamlit UI components for the invoice correction app."""

import streamlit as st
import pandas as pd
from typing import List, Dict, Set, Optional


def render_invoice_table(
    df: pd.DataFrame,
    key_prefix: str,
    selectable: bool = True,
    selected_ids: Optional[Set[str]] = None,
) -> Set[str]:
    """
    Render a table of invoices with optional checkboxes for selection.

    Args:
        df: DataFrame of invoices to display
        key_prefix: Unique prefix for Streamlit widget keys
        selectable: Whether to show checkboxes for selection
        selected_ids: Set of currently selected invoice IDs

    Returns:
        Set of selected invoice IDs
    """
    if df.empty:
        st.info("No invoices found.")
        return set()

    if selected_ids is None:
        selected_ids = set()

    newly_selected = set()

    # Add "Select All" checkbox at the top
    if selectable:
        col_sel, col_label = st.columns([0.5, 9.5])
        with col_sel:
            select_all = st.checkbox(
                "All",
                key=f"{key_prefix}_select_all",
                value=False,
                label_visibility="visible",
            )
        with col_label:
            st.markdown("**Select All**")

        if select_all:
            newly_selected = set(df["invoice_id"].tolist())
            return newly_selected

    # Display table with checkboxes
    for idx, row in df.iterrows():
        invoice_id = row["invoice_id"]

        col1, col2 = st.columns([0.5, 9.5])

        with col1:
            if selectable:
                is_checked = st.checkbox(
                    "Select",
                    key=f"{key_prefix}_checkbox_{invoice_id}_{idx}",
                    value=invoice_id in selected_ids,
                    label_visibility="collapsed",
                )
                if is_checked:
                    newly_selected.add(invoice_id)

        with col2:
            # Display invoice details in an expander
            with st.expander(
                f"Invoice #{row.get('invoice_number', invoice_id)} - {row.get('vendor_name', 'N/A')}"
            ):
                # Create two columns for details
                detail_col1, detail_col2 = st.columns(2)

                with detail_col1:
                    st.write(f"**Invoice ID:** {invoice_id}")
                    st.write(f"**Date:** {row.get('invoice_date', 'N/A')}")
                    st.write(f"**Amount:** ${row.get('amount', 0):,.2f}")

                with detail_col2:
                    st.write(f"**Category:** {row.get('category', 'N/A')}")
                    confidence = row.get("confidence_score", 0)
                    st.write(f"**Confidence:** {confidence:.2%}")
                    st.write(f"**Description:** {row.get('description', 'N/A')}")

    return newly_selected


def render_search_pane(
    on_search: callable,
    available_categories: List[str],
) -> None:
    """
    Render the invoice search interface.

    Args:
        on_search: Callback function to execute search
        available_categories: List of available categories for filtering
    """
    st.subheader("Search Invoices")

    with st.form("search_form"):
        search_term = st.text_input(
            "Search by invoice number, vendor, or description",
            placeholder="Enter search term (leave blank to load 50 invoices)...",
        )

        col1, col2 = st.columns(2)
        with col1:
            category_filter = st.selectbox(
                "Filter by Category",
                options=["All"] + available_categories,
            )

        with col2:
            limit = st.number_input(
                "Max Results",
                min_value=10,
                max_value=500,
                value=50,
                step=10,
            )

        submit = st.form_submit_button("Search", type="primary")

        if submit:
            on_search(
                search_term=search_term,
                category_filter=None if category_filter == "All" else category_filter,
                limit=limit,
            )


def render_flagged_pane(
    on_load_flagged: callable,
) -> None:
    """
    Render the flagged invoices interface.

    Args:
        on_load_flagged: Callback function to load flagged invoices
    """
    st.subheader("Flagged Invoices")

    col1, col2 = st.columns([3, 1])

    with col1:
        st.write("Review invoices flagged for low confidence or missing categories.")

    with col2:
        if st.button("Load Flagged", type="primary"):
            on_load_flagged()


def render_review_pane(
    review_invoices: pd.DataFrame,
    available_categories: List[str],
    on_submit_corrections: callable,
    on_clear_review: callable,
) -> None:
    """
    Render the review and correction interface.

    Args:
        review_invoices: DataFrame of invoices selected for review
        available_categories: List of available categories
        on_submit_corrections: Callback to submit corrections
        on_clear_review: Callback to clear the review queue
    """
    if review_invoices.empty:
        st.info(
            "No invoices selected for review. Select invoices from search or flagged panes above."
        )
        return

    # Initialize session state for current invoice index if not exists
    if "current_invoice_idx" not in st.session_state:
        st.session_state.current_invoice_idx = 0

    # Ensure index is within bounds
    if st.session_state.current_invoice_idx >= len(review_invoices):
        st.session_state.current_invoice_idx = 0

    # Get current invoice
    current_idx = st.session_state.current_invoice_idx
    row = review_invoices.iloc[current_idx]
    invoice_id = row["invoice_id"]

    # Display invoice details - compact layout
    col1, col2, col3 = st.columns([2, 2, 1])

    with col1:
        st.markdown(f"### Invoice #{row.get('invoice_number', invoice_id)}")
        st.write(f"**Vendor:** {row.get('vendor_name', 'N/A')}")

    with col2:
        st.write(f"**Amount:** ${row.get('amount', 0):,.2f}")
        st.write(f"**Date:** {row.get('invoice_date', 'N/A')}")

    with col3:
        st.write(f"**Confidence:** {row.get('confidence_score', 0):.1%}")
        if st.button("Clear Queue", use_container_width=True, key="clear_mini"):
            on_clear_review()
            st.rerun()

    st.write(f"**Description:** {row.get('description', 'N/A')}")

    st.markdown("---")

    # Correction inputs - Two categories
    st.markdown("#### Classification")

    col1, col2 = st.columns(2)

    current_category = row.get("category")
    # Find the index of current category, default to 0 if not found
    default_idx = 0
    if current_category and current_category in available_categories:
        default_idx = available_categories.index(current_category)

    with col1:
        primary_category = st.selectbox(
            "Primary Category",
            options=available_categories,
            index=default_idx,
            key=f"primary_cat_{invoice_id}_{current_idx}",
            help=f"Current: {current_category if current_category else 'None'}",
        )

    with col2:
        secondary_category = st.selectbox(
            "Secondary Category",
            options=["None"] + available_categories,
            index=0,
            key=f"secondary_cat_{invoice_id}_{current_idx}",
            help="Optional secondary classification",
        )

    comment = st.text_area(
        "Comment (optional)",
        key=f"comment_{invoice_id}_{current_idx}",
        placeholder="Explain the correction...",
        height=70,
    )

    st.markdown("---")

    # Navigation and submission buttons
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button(
            "⬅ Previous",
            disabled=(current_idx == 0),
            use_container_width=True,
        ):
            st.session_state.current_invoice_idx -= 1
            st.rerun()

    with col2:
        if st.button(
            "Skip ➡",
            disabled=(current_idx >= len(review_invoices) - 1),
            use_container_width=True,
        ):
            st.session_state.current_invoice_idx += 1
            st.rerun()

    with col3:
        if st.button("Submit", type="primary", use_container_width=True):
            # Combine categories
            combined_category = primary_category
            if secondary_category != "None":
                combined_category = f"{primary_category} | {secondary_category}"

            correction = {
                "invoice_id": invoice_id,
                "transaction_ids": [row.get("transaction_id", invoice_id)],
                "corrected_category": combined_category,
                "comment": comment if comment else None,
            }
            on_submit_corrections([correction])

            # Adjust index if needed after submission
            if st.session_state.current_invoice_idx >= len(review_invoices) - 1:
                st.session_state.current_invoice_idx = max(0, len(review_invoices) - 2)

            st.rerun()

    with col4:
        if st.button("Finish", use_container_width=True):
            st.session_state.current_invoice_idx = 0
            on_clear_review()
            st.rerun()


def show_success_message(message: str) -> None:
    """Display a success message."""
    st.success(message)


def show_error_message(message: str) -> None:
    """Display an error message."""
    st.error(message)


def show_info_message(message: str) -> None:
    """Display an info message."""
    st.info(message)

