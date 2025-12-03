"""Example usage of the invoice app modules."""

from invoice_app.config import load_config, DatabricksConfig, AppConfig
from invoice_app import (
    search_invoices,
    get_flagged_invoices,
    get_available_categories,
    write_correction,
)


def example_search():
    """Example: Search for invoices."""
    # Load configuration
    config = load_config()
    db_config = DatabricksConfig.from_dict(config)
    app_config = AppConfig.from_dict(config)
    
    # Search for invoices
    results = search_invoices(
        db_config=db_config,
        app_config=app_config,
        search_term="software",
        limit=10,
    )
    
    print(f"Found {len(results)} invoices:")
    print(results[["invoice_number", "vendor_name", "category", "confidence_score"]])


def example_flagged():
    """Example: Get flagged invoices."""
    config = load_config()
    db_config = DatabricksConfig.from_dict(config)
    app_config = AppConfig.from_dict(config)
    
    # Get flagged invoices
    results = get_flagged_invoices(
        db_config=db_config,
        app_config=app_config,
        limit=10,
    )
    
    print(f"Found {len(results)} flagged invoices:")
    print(results[["invoice_number", "category", "confidence_score"]])


def example_correct():
    """Example: Submit a correction."""
    config = load_config()
    db_config = DatabricksConfig.from_dict(config)
    app_config = AppConfig.from_dict(config)
    
    # Submit a correction
    write_correction(
        db_config=db_config,
        app_config=app_config,
        invoice_id="INV001",
        transaction_ids=["TXN001"],
        corrected_category="IT Services",
        comment="Reclassified from Software to IT Services",
        corrected_by="example_user",
    )
    
    print("✅ Correction submitted")


def example_categories():
    """Example: Get available categories."""
    config = load_config()
    db_config = DatabricksConfig.from_dict(config)
    app_config = AppConfig.from_dict(config)
    
    categories = get_available_categories(
        db_config=db_config,
        app_config=app_config,
    )
    
    print(f"Available categories ({len(categories)}):")
    for category in categories:
        print(f"  - {category}")


if __name__ == "__main__":
    print("Invoice App Examples\n" + "="*50 + "\n")
    
    config = load_config()
    app_config = AppConfig.from_dict(config)
    
    if app_config.demo_mode:
        print("❌ Examples require a real Databricks connection.")
        print("   Set demo_mode: false in config.yaml and configure Databricks settings.")
    else:
        print("1. Searching for invoices...")
        example_search()
        
        print("\n2. Getting flagged invoices...")
        example_flagged()
        
        print("\n3. Getting available categories...")
        example_categories()
        
        # Uncomment to test corrections
        # print("\n4. Submitting a correction...")
        # example_correct()


