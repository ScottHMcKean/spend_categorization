"""Test fixtures."""

import pytest
import pandas as pd
from src.app.config import AppConfig


@pytest.fixture
def sample_config():
    """Provide a sample AppConfig for testing."""
    return AppConfig(
        mode="test",
        page_size=50,
        invoices_sync="test_invoices_sync",
        categorization_sync="test_categorization_sync",
        reviews_table="test_reviews",
    )


@pytest.fixture
def sample_invoice_data():
    """Provide sample invoice data for testing."""
    return pd.DataFrame(
        {
            "invoice_id": ["INV001", "INV002", "INV003"],
            "invoice_number": ["2024-001", "2024-002", "2024-003"],
            "transaction_id": ["TXN001", "TXN002", "TXN003"],
            "vendor_name": ["Vendor A", "Vendor B", "Vendor C"],
            "invoice_date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "amount": [100.00, 200.00, 300.00],
            "category": ["Office Supplies", "Software", "Hardware"],
            "confidence_score": [0.95, 0.65, 0.45],
            "description": ["Pens and paper", "Software license", "Computer equipment"],
        }
    )


@pytest.fixture
def sample_reviews():
    """Provide sample review data for testing."""
    return [
        {
            "invoice_id": "INV001",
            "category": "Stationery",
            "schema_id": "v1",
            "prompt_id": "p1",
            "labeler_id": "test_user",
        },
        {
            "invoice_id": "INV002",
            "category": "IT Services",
            "schema_id": "v1",
            "prompt_id": "p1",
            "labeler_id": "test_user",
        },
    ]
