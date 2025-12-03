"""
Database setup script for the invoice correction application.

This script creates the necessary tables and views in Databricks.
Run this once before using the application.
"""

from invoice_app.config import load_config, DatabricksConfig, AppConfig
from invoice_app import execute_write


def create_corrections_table(db_config: DatabricksConfig, table_name: str):
    """Create the Type 2 SCD corrections table."""
    query = f"""
        CREATE TABLE IF NOT EXISTS {db_config.catalog}.{db_config.schema}.{table_name} (
            transaction_id STRING NOT NULL,
            invoice_id STRING NOT NULL,
            category STRING NOT NULL,
            start_date TIMESTAMP NOT NULL,
            end_date TIMESTAMP,
            is_current BOOLEAN NOT NULL DEFAULT TRUE,
            comment STRING,
            corrected_by STRING,
            correction_timestamp TIMESTAMP NOT NULL,
            CONSTRAINT pk_corrections PRIMARY KEY (transaction_id, start_date)
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
    """
    execute_write(db_config, query)
    print(f"✅ Created table: {db_config.catalog}.{db_config.schema}.{table_name}")


def create_flagged_invoices_view(
    db_config: DatabricksConfig,
    app_config: AppConfig,
):
    """Create a view for flagged invoices."""
    if not app_config.flagged_invoices_view:
        print("⏭️  Skipping flagged invoices view (not configured)")
        return
    
    query = f"""
        CREATE OR REPLACE VIEW {db_config.catalog}.{db_config.schema}.{app_config.flagged_invoices_view} AS
        SELECT *, 
            CASE 
                WHEN confidence_score < {app_config.critical_confidence_threshold} THEN 1
                WHEN confidence_score < {app_config.low_confidence_threshold} THEN 2
                WHEN category IS NULL THEN 1
                ELSE 3
            END as flag_priority
        FROM {db_config.catalog}.{db_config.schema}.{app_config.invoices_table}
        WHERE confidence_score < {app_config.low_confidence_threshold} OR category IS NULL
    """
    execute_write(db_config, query)
    print(f"✅ Created view: {db_config.catalog}.{db_config.schema}.{app_config.flagged_invoices_view}")


def initialize_corrections_from_invoices(
    db_config: DatabricksConfig,
    app_config: AppConfig,
):
    """
    Populate the corrections table with initial data from the invoices table.
    Only run this once to seed the corrections table.
    """
    query = f"""
        INSERT INTO {db_config.catalog}.{db_config.schema}.{app_config.corrections_table}
        SELECT 
            COALESCE(transaction_id, invoice_id) as transaction_id,
            invoice_id,
            category,
            CURRENT_TIMESTAMP() as start_date,
            NULL as end_date,
            TRUE as is_current,
            'Initial load' as comment,
            'system' as corrected_by,
            CURRENT_TIMESTAMP() as correction_timestamp
        FROM {db_config.catalog}.{db_config.schema}.{app_config.invoices_table}
        WHERE NOT EXISTS (
            SELECT 1 
            FROM {db_config.catalog}.{db_config.schema}.{app_config.corrections_table} c
            WHERE c.transaction_id = COALESCE(transaction_id, invoice_id)
            AND c.is_current = TRUE
        )
    """
    execute_write(db_config, query)
    print(f"✅ Initialized corrections table from invoices")


def main():
    """Main setup routine."""
    print("🔧 Setting up Invoice Correction Application Database...\n")
    
    # Load configuration
    config = load_config()
    db_config = DatabricksConfig.from_dict(config)
    app_config = AppConfig.from_dict(config)
    
    if app_config.demo_mode:
        print("❌ Cannot run setup in demo mode. Set demo_mode: false in config.yaml")
        return
    
    db_config.validate()
    
    print(f"📊 Target: {db_config.catalog}.{db_config.schema}\n")
    
    # Create corrections table
    print("Creating corrections table...")
    create_corrections_table(db_config, app_config.corrections_table)
    
    # Create flagged invoices view
    print("\nCreating flagged invoices view...")
    create_flagged_invoices_view(db_config, app_config)
    
    # Ask if user wants to initialize corrections
    print("\n" + "="*60)
    response = input("Initialize corrections table from invoices? (y/N): ")
    
    if response.lower() == 'y':
        print("\nInitializing corrections table...")
        initialize_corrections_from_invoices(db_config, app_config)
    
    print("\n" + "="*60)
    print("✅ Setup complete! You can now run the Streamlit app:")
    print("   uv run streamlit run app.py")


if __name__ == "__main__":
    main()


