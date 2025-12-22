"""Demo data generator for testing the UI without Databricks.

Note: This module is kept for backward compatibility.
The new MockBackend in database.py provides the same functionality
through the database abstraction layer.

For new code, use:
    from invoice_app.database import init_backend, get_backend
    from invoice_app.config import AppConfig
    
    app_config = AppConfig(mode="test", ...)
    init_backend(app_config)
    backend = get_backend()
"""

import pandas as pd
from datetime import datetime, timedelta
import random


def generate_demo_invoices(count: int = 50) -> pd.DataFrame:
    """Generate demo invoice data for UI testing."""
    vendors = [
        "Acme Office Supplies", "TechCorp Software", "Global Hardware Inc",
        "CloudServices Ltd", "Office Depot", "Amazon Business", "Staples",
        "Dell Technologies", "Microsoft Corporation", "Adobe Systems",
    ]
    
    categories = [
        "Office Supplies", "Software", "Hardware", "Cloud Services",
        "Consulting", "Marketing", "Travel", "Utilities",
    ]
    
    descriptions = [
        "Printer paper and toner", "Annual software license renewal",
        "Laptop computers for staff", "Cloud storage subscription",
        "Professional services consulting", "Digital marketing campaign",
        "Business travel expenses", "Office internet service",
    ]
    
    invoices = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(count):
        invoice_date = base_date + timedelta(days=random.randint(0, 365))
        vendor = random.choice(vendors)
        category = random.choice(categories)
        description = random.choice(descriptions)
        amount = round(random.uniform(50, 5000), 2)
        
        # Simulate low confidence for some invoices
        if random.random() < 0.3:  # 30% low confidence
            confidence = round(random.uniform(0.3, 0.69), 2)
        else:
            confidence = round(random.uniform(0.7, 0.99), 2)
        
        # 10% chance of null category
        if random.random() < 0.1:
            category = None
            confidence = 0.0
        
        invoices.append({
            "invoice_id": f"INV{i+1:06d}",
            "invoice_number": f"2024-{i+1:04d}",
            "transaction_id": f"TXN{i+1:06d}",
            "vendor_name": vendor,
            "invoice_date": invoice_date.strftime("%Y-%m-%d"),
            "amount": amount,
            "category": category,
            "confidence_score": confidence,
            "description": description,
        })
    
    return pd.DataFrame(invoices)


# Global demo data
_demo_data = None


def get_demo_data() -> pd.DataFrame:
    """Get or create demo data."""
    global _demo_data
    if _demo_data is None:
        _demo_data = generate_demo_invoices(100)
    return _demo_data.copy()


def search_demo_invoices(search_term: str, limit: int = 50) -> pd.DataFrame:
    """Search demo invoices."""
    df = get_demo_data()
    
    # Filter by search term
    mask = (
        df["invoice_number"].str.contains(search_term, case=False, na=False) |
        df["vendor_name"].str.contains(search_term, case=False, na=False) |
        df["description"].str.contains(search_term, case=False, na=False)
    )
    
    return df[mask].head(limit)


def get_demo_flagged_invoices(limit: int = 50) -> pd.DataFrame:
    """Get flagged demo invoices."""
    df = get_demo_data()
    
    # Flag low confidence or null category
    mask = (df["confidence_score"] < 0.7) | (df["category"].isna())
    
    return df[mask].sort_values("confidence_score").head(limit)


def get_demo_invoices_by_ids(invoice_ids: list) -> pd.DataFrame:
    """Get demo invoices by IDs."""
    df = get_demo_data()
    return df[df["invoice_id"].isin(invoice_ids)]


def get_demo_categories() -> list:
    """Get list of demo categories."""
    return [
        "Office Supplies", "Software", "Hardware", "Cloud Services",
        "Consulting", "Marketing", "Travel", "Utilities",
        "IT Services", "Professional Services", "Training",
        "Raw Materials", "Components", "MRO", "Logistics & Freight",
    ]
