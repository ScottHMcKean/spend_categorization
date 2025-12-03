"""
Sample script to generate mock invoice data for testing.

This creates a sample invoices table in Databricks with test data.
Useful for development and testing without production data.
"""

from datetime import datetime, timedelta
import random
from invoice_app.config import load_config, DatabricksConfig, AppConfig
from invoice_app import execute_write


# Sample data
VENDORS = [
    "Acme Office Supplies",
    "TechCorp Software",
    "Global Hardware Inc",
    "CloudServices Ltd",
    "Office Depot",
    "Amazon Business",
    "Staples",
    "Dell Technologies",
    "Microsoft Corporation",
    "Adobe Systems",
]

CATEGORIES = [
    "Office Supplies",
    "Software",
    "Hardware",
    "Cloud Services",
    "Consulting",
    "Marketing",
    "Travel",
    "Utilities",
]

DESCRIPTIONS = [
    "Printer paper and toner",
    "Annual software license renewal",
    "Laptop computers for staff",
    "Cloud storage subscription",
    "Professional services consulting",
    "Digital marketing campaign",
    "Business travel expenses",
    "Office internet service",
    "Employee training materials",
    "Conference registration fees",
]


def generate_sample_invoices(count: int = 100):
    """Generate sample invoice data."""
    invoices = []
    base_date = datetime(2024, 1, 1)
    
    for i in range(count):
        invoice_date = base_date + timedelta(days=random.randint(0, 365))
        vendor = random.choice(VENDORS)
        category = random.choice(CATEGORIES)
        description = random.choice(DESCRIPTIONS)
        amount = round(random.uniform(50, 5000), 2)
        
        # Simulate low confidence for some invoices
        if random.random() < 0.2:  # 20% low confidence
            confidence = round(random.uniform(0.3, 0.69), 2)
        else:
            confidence = round(random.uniform(0.7, 0.99), 2)
        
        # 5% chance of null category
        if random.random() < 0.05:
            category = None
            confidence = 0.0
        
        invoices.append({
            "invoice_id": f"INV{i+1:04d}",
            "invoice_number": f"2024-{i+1:04d}",
            "transaction_id": f"TXN{i+1:04d}",
            "vendor_name": vendor,
            "invoice_date": invoice_date.strftime("%Y-%m-%d"),
            "amount": amount,
            "category": category,
            "confidence_score": confidence,
            "description": description,
        })
    
    return invoices


def create_sample_table(db_config: DatabricksConfig, table_name: str):
    """Create a sample invoices table with test data."""
    
    # Drop existing table
    drop_query = f"""
        DROP TABLE IF EXISTS {db_config.catalog}.{db_config.schema}.{table_name}
    """
    execute_write(db_config, drop_query)
    
    # Create table
    create_query = f"""
        CREATE TABLE {db_config.catalog}.{db_config.schema}.{table_name} (
            invoice_id STRING NOT NULL,
            invoice_number STRING,
            transaction_id STRING,
            vendor_name STRING,
            invoice_date DATE,
            amount DECIMAL(18,2),
            category STRING,
            confidence_score DOUBLE,
            description STRING
        )
        USING DELTA
    """
    execute_write(db_config, create_query)
    
    # Insert sample data
    invoices = generate_sample_invoices(100)
    
    for invoice in invoices:
        insert_query = f"""
            INSERT INTO {db_config.catalog}.{db_config.schema}.{table_name}
            VALUES (
                '{invoice["invoice_id"]}',
                '{invoice["invoice_number"]}',
                '{invoice["transaction_id"]}',
                '{invoice["vendor_name"]}',
                '{invoice["invoice_date"]}',
                {invoice["amount"]},
                {'NULL' if invoice["category"] is None else f"'{invoice['category']}'"},
                {invoice["confidence_score"]},
                '{invoice["description"]}'
            )
        """
        execute_write(db_config, insert_query)
    
    print(f"✅ Created sample table with {len(invoices)} invoices")


def main():
    """Generate sample data."""
    print("🎲 Generating sample invoice data...\n")
    
    # Load configuration
    config = load_config()
    db_config = DatabricksConfig.from_dict(config)
    app_config = AppConfig.from_dict(config)
    
    if app_config.demo_mode:
        print("❌ Cannot run sample data generator in demo mode.")
        print("   Set demo_mode: false in config.yaml to use Databricks")
        return
    
    db_config.validate()
    
    table_name = app_config.invoices_table
    
    print(f"📊 Creating table: {db_config.catalog}.{db_config.schema}.{table_name}\n")
    print("⚠️  WARNING: This will drop and recreate the table!\n")
    
    response = input("Continue? (y/N): ")
    
    if response.lower() == 'y':
        create_sample_table(db_config, table_name)
        print("\n✅ Sample data generated successfully!")
        print(f"Table: {db_config.catalog}.{db_config.schema}.{table_name}")
    else:
        print("Cancelled.")


if __name__ == "__main__":
    main()


