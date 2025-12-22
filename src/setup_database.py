#!/usr/bin/env python3
"""
Database Setup Script for Spend Categorization Application

This script initializes the Lakebase PostgreSQL database with the required tables
and optionally loads sample data for testing.

Usage:
    # Initialize tables only (prod mode)
    uv run python src/setup_database.py --init-tables

    # Initialize tables and load sample data
    uv run python src/setup_database.py --init-tables --load-sample-data

    # Check connection only
    uv run python src/setup_database.py --check-connection
"""

import argparse
import logging
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from invoice_app.config import load_config, AppConfig, LakebaseConfig
from invoice_app.database import (
    init_backend,
    get_backend,
    LakebaseBackend,
    MockBackend,
)
from invoice_app.corrections import (
    initialize_corrections_table,
    initialize_invoices_table,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def check_connection(app_config: AppConfig, lakebase_config: LakebaseConfig) -> bool:
    """
    Check if we can connect to the Lakebase database.

    Args:
        app_config: Application configuration
        lakebase_config: Lakebase configuration

    Returns:
        True if connection successful, False otherwise
    """
    logger.info(
        f"Checking connection to Lakebase instance: {lakebase_config.instance_name}"
    )

    try:
        backend = init_backend(app_config, lakebase_config)

        if backend.is_connected():
            logger.info("✅ Successfully connected to Lakebase database")
            return True
        else:
            logger.error("❌ Failed to connect to Lakebase database")
            return False

    except Exception as e:
        logger.error(f"❌ Connection failed: {e}")
        return False


def init_tables(app_config: AppConfig) -> bool:
    """
    Initialize database tables.

    Args:
        app_config: Application configuration

    Returns:
        True if successful, False otherwise
    """
    logger.info("Initializing database tables...")

    try:
        backend = get_backend()

        # Create invoices table
        logger.info(f"Creating table: {app_config.invoices_table}")
        initialize_invoices_table(app_config, backend)
        logger.info(f"✅ Table {app_config.invoices_table} created/verified")

        # Create corrections table
        logger.info(f"Creating table: {app_config.corrections_table}")
        initialize_corrections_table(app_config, backend)
        logger.info(f"✅ Table {app_config.corrections_table} created/verified")

        logger.info("✅ All tables initialized successfully")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to initialize tables: {e}")
        return False


def load_sample_data(app_config: AppConfig, count: int = 100) -> bool:
    """
    Load sample invoice data into the database.

    Args:
        app_config: Application configuration
        count: Number of sample invoices to generate

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Loading {count} sample invoices...")

    try:
        import random
        from datetime import datetime, timedelta

        backend = get_backend()

        vendors = [
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
            "Steel Supply Co",
            "Industrial Components Ltd",
            "FastFreight Logistics",
            "Precision Bearings Inc",
            "ElectroParts USA",
            "SafetyFirst Equipment",
        ]

        categories = [
            "Office Supplies",
            "Software",
            "Hardware",
            "Cloud Services",
            "Consulting",
            "Marketing",
            "Travel",
            "Utilities",
            "Raw Materials",
            "Components",
            "MRO",
            "Logistics & Freight",
            "Bearings & Seals",
            "Electrical Assemblies",
            "Safety & PPE",
        ]

        descriptions = [
            "Printer paper and toner supplies",
            "Annual software license renewal",
            "Laptop computers for engineering team",
            "Cloud storage subscription - annual",
            "Professional services consulting",
            "Digital marketing campaign Q4",
            "Business travel expenses - conference",
            "Office internet service monthly",
            "Steel plates for manufacturing line",
            "Electronic components batch order",
            "Maintenance supplies and tools",
            "Freight shipping services - international",
            "Roller bearings for turbine assembly",
            "Control cabinet components",
            "Safety equipment and PPE supplies",
        ]

        base_date = datetime(2024, 1, 1)

        for i in range(count):
            invoice_date = base_date + timedelta(days=random.randint(0, 365))
            vendor = random.choice(vendors)
            category = random.choice(categories)
            description = random.choice(descriptions)
            amount = round(random.uniform(50, 5000), 2)

            # Simulate low confidence for some invoices
            if random.random() < 0.3:
                confidence = round(random.uniform(0.3, 0.69), 4)
            else:
                confidence = round(random.uniform(0.7, 0.99), 4)

            # 10% chance of null category
            if random.random() < 0.1:
                category = None
                confidence = 0.0

            invoice_id = f"INV{i+1:06d}"
            invoice_number = f"2024-{i+1:04d}"
            transaction_id = f"TXN{i+1:06d}"

            # Insert into database
            insert_query = f"""
                INSERT INTO {app_config.invoices_table}
                (invoice_id, invoice_number, transaction_id, vendor_name, 
                 invoice_date, amount, category, confidence_score, description)
                VALUES (:invoice_id, :invoice_number, :transaction_id, :vendor_name,
                        :invoice_date, :amount, :category, :confidence_score, :description)
                ON CONFLICT (invoice_id) DO NOTHING
            """

            params = {
                "invoice_id": invoice_id,
                "invoice_number": invoice_number,
                "transaction_id": transaction_id,
                "vendor_name": vendor,
                "invoice_date": invoice_date.strftime("%Y-%m-%d"),
                "amount": amount,
                "category": category,
                "confidence_score": confidence,
                "description": description,
            }

            backend.execute_write(insert_query, params)

            if (i + 1) % 20 == 0:
                logger.info(f"  Inserted {i + 1}/{count} invoices...")

        logger.info(f"✅ Successfully loaded {count} sample invoices")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to load sample data: {e}")
        return False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Database setup script for Spend Categorization application"
    )
    parser.add_argument(
        "--check-connection",
        action="store_true",
        help="Check database connection only",
    )
    parser.add_argument(
        "--init-tables",
        action="store_true",
        help="Initialize database tables",
    )
    parser.add_argument(
        "--load-sample-data",
        action="store_true",
        help="Load sample invoice data",
    )
    parser.add_argument(
        "--sample-count",
        type=int,
        default=100,
        help="Number of sample invoices to generate (default: 100)",
    )
    parser.add_argument(
        "--mode",
        choices=["test", "prod"],
        default=None,
        help="Override mode from config (test uses mock, prod uses Lakebase)",
    )

    args = parser.parse_args()

    # Load configuration
    try:
        config = load_config()
        app_config = AppConfig.from_dict(config)

        # Override mode if specified
        if args.mode:
            app_config.mode = args.mode
            logger.info(f"Mode overridden to: {args.mode}")

        lakebase_config = None
        if app_config.is_prod_mode:
            lakebase_config = LakebaseConfig.from_dict(config)
            lakebase_config.validate()

    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        sys.exit(1)

    # Check connection (required for prod mode operations)
    if args.check_connection or (
        app_config.is_prod_mode and (args.init_tables or args.load_sample_data)
    ):
        if app_config.is_test_mode:
            logger.info("🧪 Test mode: Using MockBackend (no real connection needed)")
            init_backend(app_config, None)
        else:
            if not check_connection(app_config, lakebase_config):
                sys.exit(1)
    else:
        # Initialize backend even without explicit connection check
        try:
            init_backend(app_config, lakebase_config)
        except Exception as e:
            logger.error(f"Failed to initialize backend: {e}")
            sys.exit(1)

    # Only check connection requested
    if args.check_connection and not args.init_tables and not args.load_sample_data:
        logger.info("Connection check completed.")
        sys.exit(0)

    # Initialize tables
    if args.init_tables:
        if not init_tables(app_config):
            sys.exit(1)

    # Load sample data
    if args.load_sample_data:
        if not args.init_tables:
            logger.warning(
                "--load-sample-data without --init-tables: tables must already exist"
            )
        if not load_sample_data(app_config, args.sample_count):
            sys.exit(1)

    if not args.check_connection and not args.init_tables and not args.load_sample_data:
        parser.print_help()
        sys.exit(0)

    logger.info("✅ Database setup completed successfully!")


if __name__ == "__main__":
    main()
