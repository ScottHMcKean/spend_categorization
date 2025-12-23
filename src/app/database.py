"""Database backend abstraction for test and prod modes.

- test: MockBackend with in-memory data
- prod: LakebaseBackend with PostgreSQL on Databricks
"""

from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

if TYPE_CHECKING:
    from .config import AppConfig

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
    """Mock backend for test mode using in-memory data."""

    def __init__(self):
        from src.utils import generate_sample_invoices, get_sample_categories

        self._invoices = generate_sample_invoices(100)
        self._categories = get_sample_categories()

        # Create mock categorization data (simulates categorization_sync)
        self._categorization = pd.DataFrame(
            {
                "invoice_id": self._invoices["invoice_id"],
                "category": self._invoices["category"],
                "confidence": self._invoices["confidence_score"],
                "run_id": "mock_run_001",
            }
        )

        self._reviews = pd.DataFrame(
            columns=[
                "label_id",
                "invoice_id",
                "schema_id",
                "prompt_id",
                "category",
                "label_version_date",
                "labeler_id",
                "source",
                "is_current",
                "created_at",
            ]
        )

    def execute_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        query_lower = query.lower().strip()

        # Distinct category query from categorization_sync
        if "distinct" in query_lower and "category" in query_lower:
            if "categorization" in query_lower:
                return pd.DataFrame({"category": self._categories})
            return pd.DataFrame({"category": self._categories})

        # Reviews table
        if "reviews" in query_lower and "from" in query_lower:
            return self._reviews.copy()

        # Categorization_sync queries
        if "categorization" in query_lower and "from" in query_lower:
            return self._categorization.copy()

        # Invoices queries (invoices_sync)
        if "from" in query_lower and (
            "invoices" in query_lower or "invoice" in query_lower
        ):
            df = self._invoices.copy()

            # Handle JOIN with categorization
            if "join" in query_lower and "categorization" in query_lower:
                # Merge invoices with categorization
                df = df.merge(
                    self._categorization[["invoice_id", "category", "confidence", "run_id"]],
                    on="invoice_id",
                    how="left",
                    suffixes=("", "_cat"),
                )
                # Rename for expected output
                if "category_cat" in df.columns:
                    df["llm_category"] = df["category_cat"]
                    df = df.drop(columns=["category_cat"])

                # Handle confidence filter for flagged
                if "confidence <" in query_lower:
                    threshold = 0.7  # Default
                    import re
                    match = re.search(r"confidence\s*<\s*([\d.]+)", query_lower)
                    if match:
                        threshold = float(match.group(1))
                    df = df[(df["confidence"] < threshold) | (df["category"].isna())]
                    df = df.sort_values("confidence")

            else:
                # Search
                if parameters and "search_pattern" in parameters:
                    pattern = parameters["search_pattern"].replace("%", "")
                    if pattern:
                        mask = (
                            df["invoice_number"].str.contains(pattern, case=False, na=False)
                            | df["vendor_name"].str.contains(pattern, case=False, na=False)
                            | df["description"].str.contains(pattern, case=False, na=False)
                        )
                        df = df[mask]

                # Filter by IDs
                if "invoice_id in" in query_lower:
                    import re
                    match = re.search(r"invoice_id in \(([^)]+)\)", query_lower)
                    if match:
                        ids = re.findall(r"'([^']+)'", query)
                        if ids:
                            df = df[df["invoice_id"].isin(ids)]

                # Flagged invoices (old style without join)
                if "confidence_score <" in query_lower or "category is null" in query_lower:
                    df = df[(df["confidence_score"] < 0.7) | (df["category"].isna())]
                    df = df.sort_values("confidence_score")

            # Limit
            if "limit" in query_lower:
                try:
                    import re
                    match = re.search(r"limit\s+(\d+)", query_lower)
                    if match:
                        limit = int(match.group(1))
                        df = df.head(limit)
                except (ValueError, IndexError):
                    pass

            return df

        return pd.DataFrame()

    def execute_write(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        query_lower = query.lower().strip()

        # Insert into reviews
        if "insert" in query_lower and "reviews" in query_lower:
            if parameters:
                new_row = pd.DataFrame(
                    [
                        {
                            "label_id": str(uuid.uuid4()),
                            "invoice_id": parameters.get("invoice_id"),
                            "schema_id": parameters.get("schema_id"),
                            "prompt_id": parameters.get("prompt_id"),
                            "category": parameters.get("category"),
                            "label_version_date": parameters.get("label_version_date"),
                            "labeler_id": parameters.get("labeler_id"),
                            "source": parameters.get("source", "human"),
                            "is_current": True,
                            "created_at": parameters.get("created_at"),
                        }
                    ]
                )
                self._reviews = pd.concat([self._reviews, new_row], ignore_index=True)

    def is_connected(self) -> bool:
        return True


class LakebaseBackend(DatabaseBackend):
    """PostgreSQL backend using Databricks Lakebase."""

    def __init__(self, instance: str, dbname: str = "databricks_postgres"):
        self.instance = instance
        self.dbname = dbname
        self._client = None

    @property
    def client(self):
        if self._client is None:
            from databricks.sdk import WorkspaceClient

            self._client = WorkspaceClient()
        return self._client

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


def create_backend(config: AppConfig) -> DatabaseBackend:
    """Factory function to create the appropriate backend."""
    if config.is_test_mode:
        logger.info("Creating MockBackend for test mode")
        return MockBackend()

    if config.is_prod_mode:
        if not config.lakebase_instance:
            raise ValueError("lakebase_instance required for prod mode")
        logger.info(f"Creating LakebaseBackend (instance: {config.lakebase_instance})")
        return LakebaseBackend(
            instance=config.lakebase_instance,
            dbname=config.lakebase_dbname,
        )

    raise ValueError(f"Unknown mode: {config.mode}")


# Global backend instance
_backend: Optional[DatabaseBackend] = None


def get_backend() -> DatabaseBackend:
    """Get the global database backend."""
    global _backend
    if _backend is None:
        raise RuntimeError("Backend not initialized. Call init_backend() first.")
    return _backend


def init_backend(config: AppConfig) -> DatabaseBackend:
    """Initialize the global database backend."""
    global _backend
    _backend = create_backend(config)
    return _backend


def reset_backend() -> None:
    """Reset the global backend."""
    global _backend
    _backend = None
