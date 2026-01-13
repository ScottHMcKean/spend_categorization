"""Test data generation for mock backend."""

import pandas as pd
from datetime import datetime, timedelta
import random


def generate_sample_invoices(n: int = 100) -> pd.DataFrame:
    """Generate sample invoice data for testing."""
    categories = get_sample_categories()
    
    invoices = []
    for i in range(n):
        invoice_date = datetime.now() - timedelta(days=random.randint(0, 365))
        invoices.append({
            "invoice_id": f"INV-{i+1:06d}",
            "invoice_number": f"INV{i+1:06d}",
            "vendor_name": random.choice([
                "ABC Corp", "XYZ Industries", "Tech Solutions", 
                "Global Suppliers", "Local Vendor Co"
            ]),
            "description": random.choice([
                "Office supplies", "IT equipment", "Professional services",
                "Raw materials", "Transportation services"
            ]),
            "amount": round(random.uniform(100, 10000), 2),
            "invoice_date": invoice_date.strftime("%Y-%m-%d"),
            "category": random.choice(categories),
            "confidence_score": round(random.uniform(0.5, 1.0), 2),
            "cost_centre": random.choice(["CC-001", "CC-002", "CC-003"]),
        })
    
    return pd.DataFrame(invoices)


def get_sample_categories() -> list:
    """Get sample category list for testing."""
    return [
        "Office Supplies",
        "IT Equipment",
        "Professional Services",
        "Raw Materials",
        "Transportation",
        "Utilities",
        "Maintenance",
        "Marketing",
        "Travel",
        "Training",
    ]
