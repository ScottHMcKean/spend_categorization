"""Test fixtures and utilities."""

import pytest
import pandas as pd
from invoice_app.config import DatabricksConfig, AppConfig


@pytest.fixture
def sample_db_config():
    """Provide a sample DatabricksConfig for testing."""
    return DatabricksConfig(
        server_hostname="test.databricks.com",
        http_path="/sql/1.0/warehouses/test",
        access_token="test_token",
        catalog="test_catalog",
        schema="test_schema",
    )


@pytest.fixture
def sample_app_config():
    """Provide a sample AppConfig for testing."""
    return AppConfig(
        invoices_table="test_invoices",
        corrections_table="test_corrections",
        flagged_invoices_view="test_flagged",
        page_size=50,
    )


@pytest.fixture
def sample_invoice_data():
    """Provide sample invoice data for testing."""
    return pd.DataFrame({
        "invoice_id": ["INV001", "INV002", "INV003"],
        "invoice_number": ["2024-001", "2024-002", "2024-003"],
        "transaction_id": ["TXN001", "TXN002", "TXN003"],
        "vendor_name": ["Vendor A", "Vendor B", "Vendor C"],
        "invoice_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "amount": [100.00, 200.00, 300.00],
        "category": ["Office Supplies", "Software", "Hardware"],
        "confidence_score": [0.95, 0.65, 0.45],
        "description": ["Pens and paper", "Software license", "Computer equipment"],
    })


@pytest.fixture
def sample_corrections():
    """Provide sample correction data for testing."""
    return [
        {
            "invoice_id": "INV001",
            "transaction_ids": ["TXN001"],
            "corrected_category": "Stationery",
            "comment": "More specific category",
            "corrected_by": "test_user",
        },
        {
            "invoice_id": "INV002",
            "transaction_ids": ["TXN002"],
            "corrected_category": "IT Services",
            "comment": "Reclassified",
            "corrected_by": "test_user",
        },
    ]

