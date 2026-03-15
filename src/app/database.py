"""Database backend abstraction for test and prod modes.

- test: MockBackend with in-memory CSV data
- prod: ServerlessBackend querying Delta tables via Databricks Connect serverless
- lakebase: LakebaseBackend with PostgreSQL on Databricks
"""

from __future__ import annotations

import logging
import os
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

if TYPE_CHECKING:
    from src.config import Config  # noqa: F401

logger = logging.getLogger(__name__)


class DatabaseBackend(ABC):
    """Abstract base class for database backends."""

    @abstractmethod
    def execute_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute a SQL query and return results as DataFrame."""
        pass

    @abstractmethod
    def execute_write(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        """Execute a SQL write operation."""
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if backend is connected."""
        pass


class MockBackend(DatabaseBackend):
    """Mock backend for test mode using CSV files from assets/."""

    def __init__(self, config: "Config"):
        from pathlib import Path

        self.config = config
        assets_dir = Path(__file__).parent.parent.parent / "assets"

        # Load mock data from CSV files
        try:
            # Load invoices (main data source)
            invoices_file = assets_dir / "invoices.csv"
            if invoices_file.exists():
                self._invoices = pd.read_csv(invoices_file)
                logger.info(
                    f"MockBackend loaded {len(self._invoices)} invoices from {invoices_file}"
                )
            else:
                logger.warning(f"Invoices file not found: {invoices_file}")
                self._invoices = pd.DataFrame()

            # Load the appropriate classification table based on config
            classification_file = f"cat_{config.categorization_source}.csv"
            self._categorizations = pd.read_csv(assets_dir / classification_file)

            # Load reviews
            self._reviews = pd.read_csv(assets_dir / "cat_reviews.csv")

            # Load categories from main categories.csv
            categories_file = config.categories_file
            self._categories = pd.read_csv(
                Path(__file__).parent.parent.parent / categories_file
            )

            logger.info(
                f"MockBackend initialized with {len(self._categorizations)} classifications from {classification_file}"
            )
        except Exception as e:
            logger.error(f"Failed to load mock data: {e}")
            # Fallback to empty DataFrames
            self._invoices = pd.DataFrame()
            self._categorizations = pd.DataFrame()
            self._reviews = pd.DataFrame()
            self._categories = pd.DataFrame()

    def execute_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute a SQL query on mock data."""
        query_lower = query.lower()

        # Handle invoice queries
        if "invoices" in query_lower and "cat_" not in query_lower:
            return self._invoices.copy()

        # Handle categorization queries
        if "cat_" in query_lower:
            return self._categorizations.copy()

        # Handle reviews queries
        if "cat_reviews" in query_lower or "reviews" in query_lower:
            return self._reviews.copy()

        # Handle category queries
        if "categories" in query_lower and "cat_" not in query_lower:
            # For distinct category queries
            if "distinct" in query_lower:
                if "category_level_2" in query_lower:
                    return pd.DataFrame(
                        {
                            "category_level_2": self._categories[
                                "category_level_2"
                            ].unique()
                        }
                    )
                elif "category_level_3" in query_lower:
                    return pd.DataFrame(
                        {
                            "category_level_3": self._categories[
                                "category_level_3"
                            ].unique()
                        }
                    )
            return self._categories.copy()

        # Default: return empty DataFrame
        logger.warning(f"MockBackend: unhandled query pattern: {query[:100]}")
        return pd.DataFrame()

    def execute_write(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        """Mock write operation - appends to reviews DataFrame."""
        logger.info(f"MockBackend execute_write: {query[:100]}...")

        # If this is a review INSERT, append to _reviews
        if "INSERT INTO" in query and parameters:
            new_row = pd.DataFrame([parameters])
            self._reviews = pd.concat([self._reviews, new_row], ignore_index=True)
            logger.info(f"Added review to _reviews, now has {len(self._reviews)} rows")

    def is_connected(self) -> bool:
        return True


class LakebaseBackend(DatabaseBackend):
    """PostgreSQL backend using Databricks Lakebase.

    Queries are written using Delta 3-part names (catalog.schema.table).
    This backend rewrites them to PG format at execution time:
      - synced tables: "schema"."table_synced"
      - native tables (cat_reviews): "schema"."table"
    """

    NATIVE_TABLES = {"cat_reviews"}

    def __init__(
        self,
        instance: str,
        dbname: str = "databricks_postgres",
        catalog: str = "",
        schema_name: str = "",
    ):
        self.instance = instance
        self.dbname = dbname
        self.catalog = catalog
        self.schema_name = schema_name
        self._client = None

    @property
    def client(self):
        if self._client is None:
            from databricks.sdk import WorkspaceClient

            self._client = WorkspaceClient()
        return self._client

    def _rewrite_query(self, query: str) -> str:
        """Rewrite Delta 3-part table refs to Lakebase PG format."""
        if not self.catalog or not self.schema_name:
            return query
        import re

        prefix = f"{self.catalog}.{self.schema_name}."
        prefix_re = re.escape(prefix)

        def _replace(m: re.Match) -> str:
            table = m.group(1)
            if table in self.NATIVE_TABLES:
                return f'"{self.schema_name}"."{table}"'
            return f'"{self.schema_name}"."{table}_synced"'

        return re.sub(prefix_re + r"(\w+)", _replace, query)

    @contextmanager
    def get_connection(self):
        """Get a database connection using psycopg2."""
        import psycopg2

        try:
            db_instance = self.client.database.get_database_instance(name=self.instance)
            cred = self.client.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[self.instance],
            )

            user = self.client.current_user.me()
            if hasattr(user, "emails") and user.emails:
                user_email = user.emails[0].value
            elif hasattr(user, "user_name") and user.user_name:
                user_email = user.user_name
            else:
                raise ValueError("Could not determine user email")

            conn = psycopg2.connect(
                host=db_instance.read_write_dns,
                dbname=self.dbname,
                user=user_email,
                password=cred.token,
                sslmode="require",
            )

            try:
                yield conn
            finally:
                conn.close()

        except Exception as e:
            logger.error(f"Connection failed: {e}")
            raise

    def execute_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        query = self._rewrite_query(query)
        if parameters:
            for key in parameters:
                query = query.replace(f":{key}", f"%({key})s")

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, parameters)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return pd.DataFrame(rows, columns=columns)

    def execute_write(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        query = self._rewrite_query(query)
        if parameters:
            for key in parameters:
                query = query.replace(f":{key}", f"%({key})s")

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, parameters)
            conn.commit()

    def is_connected(self) -> bool:
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return False


class ServerlessBackend(DatabaseBackend):
    """Backend querying Delta tables via WorkspaceClient statement execution.

    Uses the Databricks SDK's statement_execution API with a serverless SQL
    warehouse. Works in Databricks Apps, local dev, and anywhere the SDK can
    authenticate.
    """

    def __init__(self, warehouse_id: str = ""):
        self._client = None
        self._warehouse_id = warehouse_id

    @property
    def client(self):
        if self._client is None:
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient()
        return self._client

    @property
    def warehouse_id(self) -> str:
        return self._warehouse_id or os.getenv("DATABRICKS_WAREHOUSE_ID", "")

    def _substitute_params(self, query: str, parameters: Optional[Dict[str, Any]]) -> str:
        if parameters:
            for key, val in parameters.items():
                replacement = f"'{val}'" if isinstance(val, str) else str(val)
                query = query.replace(f":{key}", replacement)
        return query

    def execute_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        query = self._substitute_params(query, parameters)
        resp = self.client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=query,
            wait_timeout="50s",
        )
        if resp.status and resp.status.state and resp.status.state.value != "SUCCEEDED":
            error_msg = resp.status.error.message if resp.status.error else str(resp.status)
            raise RuntimeError(f"Statement failed: {error_msg}")
        if not resp.result or not resp.result.data_array:
            return pd.DataFrame()
        columns = [c.name for c in resp.manifest.schema.columns]
        return pd.DataFrame(resp.result.data_array, columns=columns)

    def execute_write(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        query = self._substitute_params(query, parameters)
        resp = self.client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=query,
            wait_timeout="50s",
        )
        if resp.status and resp.status.state and resp.status.state.value != "SUCCEEDED":
            error_msg = resp.status.error.message if resp.status.error else str(resp.status)
            raise RuntimeError(f"Statement failed: {error_msg}")

    def is_connected(self) -> bool:
        try:
            self.execute_query("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"ServerlessBackend connection check failed: {e}")
            return False


def create_backend(config: Config) -> DatabaseBackend:
    """Factory function to create the appropriate backend."""
    if config.app_mode == "test":
        logger.info("Creating MockBackend for test mode")
        return MockBackend(config)

    if config.app_mode == "prod":
        logger.info("Creating ServerlessBackend for prod mode (statement execution)")
        return ServerlessBackend(
            warehouse_id=config.warehouse_id,
        )

    if config.app_mode == "lakebase":
        if not config.lakebase_instance:
            raise ValueError("lakebase_instance required for lakebase mode")
        logger.info(f"Creating LakebaseBackend (instance: {config.lakebase_instance})")
        return LakebaseBackend(
            instance=config.lakebase_instance,
            dbname=config.lakebase_dbname,
            catalog=config.catalog,
            schema_name=config.schema_name,
        )

    raise ValueError(f"Unknown app_mode: {config.app_mode}")


# Global backend instance
_backend: Optional[DatabaseBackend] = None


def get_backend() -> DatabaseBackend:
    """Get the global database backend."""
    global _backend
    if _backend is None:
        raise RuntimeError("Backend not initialized. Call init_backend() first.")
    return _backend


def init_backend(config: Config) -> DatabaseBackend:
    """Initialize the global database backend."""
    global _backend
    _backend = create_backend(config)
    return _backend


def reset_backend() -> None:
    """Reset the global backend."""
    global _backend
    _backend = None
