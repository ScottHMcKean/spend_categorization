import logging
import uuid
import psycopg2
import psycopg2.extras
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager
from databricks.sdk import WorkspaceClient
from datetime import datetime, timezone

# SQLAlchemy imports for schema-based operations
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .schema import Base, User, Document, DocumentChunk, Conversation, Message

logger = logging.getLogger(__name__)


class DatabaseService:
    """Simplified database service using direct psycopg2 connections."""

    def __init__(self, client: Optional[WorkspaceClient], config: dict):
        self.client = client
        self.config = config

    @contextmanager
    def get_connection(self):
        """Get a database connection using the simple psycopg2 pattern."""
        try:
            # Get database instance name from config
            instance_name = self.config.get("database.instance_name")
            user = self.config.get("database.user", "databricks")
            database = self.config.get("database.database", "databricks_postgres")

            if not instance_name:
                logger.error("No database instance_name configured")
                yield None
                return

            # Get database instance from Databricks (following the user's example)
            instance = self.client.database.get_database_instance(name=instance_name)
            cred = self.client.database.generate_database_credential(
                request_id=str(uuid.uuid4()), instance_names=[instance_name]
            )

            # Create psycopg2 connection (following the user's example)
            conn = psycopg2.connect(
                host=instance.read_write_dns,
                dbname=database,
                user=user,
                password=cred.token,
                sslmode="require",
            )

            try:
                yield conn
            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Failed to create database connection: {str(e)}")
            yield None