"""Test fixtures."""

import pytest
import pandas as pd
from src.config import Config


@pytest.fixture
def sample_config():
    """Provide a sample Config for testing."""
    return Config.from_yaml()


@pytest.fixture
def sample_invoice_data():
    """Provide sample invoice data for testing."""
    return pd.DataFrame(
        {
            "order_id": ["ORD-001", "ORD-002", "ORD-003"],
            "date": ["2024-03-15", "2024-06-22", "2024-09-10"],
            "description": [
                "Spherical roller bearing",
                "Cloud storage subscription",
                "Fiber cement roofing panels",
            ],
            "supplier": ["SKF Group", "Amazon Web Services", "James Hardie"],
            "supplier_country": ["SE", "US", "AU"],
            "amount": [12, 1, 200],
            "unit_price": [485.50, 24000.00, 45.00],
            "total": [5826.00, 24000.00, 9000.00],
            "category_level_1": ["Direct", "Indirect", "Indirect"],
            "category_level_2": [
                "Bearings & Seals",
                "IT & Software",
                "Facilities & Utilities",
            ],
            "category_level_3": [
                "Spherical roller bearing",
                "SaaS subscription",
                "Building materials",
            ],
            "cost_centre": ["CC-100-Production", "CC-400-IT", "CC-200-Maintenance"],
            "plant": ["US-East Plant", "Germany-North", "US-West Plant"],
            "region": ["North America", "Europe", "North America"],
        }
    )


@pytest.fixture
def sample_reviews():
    """Provide sample review data for testing."""
    return [
        {
            "order_id": "ORD-001",
            "source": "bootstrap",
            "original_level_1": "Direct",
            "original_level_2": "Bearings & Seals",
            "original_level_3": "Spherical roller bearing",
            "reviewed_level_1": "Direct",
            "reviewed_level_2": "Bearings & Seals",
            "reviewed_level_3": "Spherical roller bearing",
            "reviewer": "test_user",
            "review_status": "approved",
            "comments": "",
        },
        {
            "order_id": "ORD-002",
            "source": "bootstrap",
            "original_level_1": "Indirect",
            "original_level_2": "IT & Software",
            "original_level_3": "SaaS subscription",
            "reviewed_level_1": "Indirect",
            "reviewed_level_2": "IT & Software",
            "reviewed_level_3": "Cloud storage",
            "reviewer": "test_user",
            "review_status": "corrected",
            "comments": "More specific subcategory",
        },
    ]
