"""Database backend abstraction for test and prod modes.

Supports:
- test mode: MockBackend with in-memory data
- prod mode: LakebaseBackend with PostgreSQL on Databricks
"""

from __future__ import annotations

import logging
import uuid
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, List, Dict, Any
from contextlib import contextmanager
from datetime import datetime, timezone
import pandas as pd

if TYPE_CHECKING:
    from .config import Config

logger = logging.getLogger(__name__)


class DatabaseBackend(ABC):
    """Abstract base class for database backends."""

    @abstractmethod
    def execute_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame."""
        pass

    @abstractmethod
    def execute_write(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        """Execute a SQL write operation."""
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if the backend is connected."""
        pass


class MockBackend(DatabaseBackend):
    """Mock database backend for test mode using in-memory data."""

    def __init__(self):
        """Initialize mock backend with sample data."""
        # Import here to avoid circular imports
        from src.utils import generate_sample_invoices, get_sample_categories

        self._invoices = generate_sample_invoices(100)
        self._corrections = pd.DataFrame(
            columns=[
                "correction_id",
                "transaction_id",
                "invoice_id",
                "category",
                "start_date",
                "end_date",
                "is_current",
                "comment",
                "corrected_by",
                "correction_timestamp",
            ]
        )
        self._categories = get_sample_categories()

    def execute_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute a simulated query against in-memory data."""
        query_lower = query.lower().strip()

        # Handle DISTINCT category query (check first to avoid matching invoices)
        if "distinct" in query_lower and "category" in query_lower:
            return pd.DataFrame({"category": self._categories})

        # Handle corrections table queries
        if "corrections" in query_lower:
            return self._corrections.copy()

        # Handle SELECT queries on invoices
        if "from" in query_lower and (
            "invoices" in query_lower or "invoice" in query_lower
        ):
            df = self._invoices.copy()

            # Handle WHERE clauses for search
            if parameters and "search_pattern" in parameters:
                pattern = parameters["search_pattern"].replace("%", "")
                mask = (
                    df["invoice_number"].str.contains(pattern, case=False, na=False)
                    | df["vendor_name"].str.contains(pattern, case=False, na=False)
                    | df["description"].str.contains(pattern, case=False, na=False)
                )
                df = df[mask]

            # Handle WHERE clauses for specific IDs
            if "invoice_id in" in query_lower:
                # Extract IDs from query - parse ('ID1', 'ID2', ...) format
                import re

                match = re.search(r"invoice_id in \(([^)]+)\)", query_lower)
                if match:
                    ids_str = match.group(1)
                    # Extract quoted IDs
                    ids = re.findall(
                        r"'([^']+)'", query
                    )  # Use original query to preserve case
                    if ids:
                        df = df[df["invoice_id"].isin(ids)]

            # Handle flagged invoices
            if "confidence_score <" in query_lower or "category is null" in query_lower:
                df = df[(df["confidence_score"] < 0.7) | (df["category"].isna())]
                df = df.sort_values("confidence_score")

            # Handle LIMIT
            if "limit" in query_lower:
                try:
                    limit_idx = query_lower.index("limit")
                    limit_str = query_lower[limit_idx:].split()[1]
                    limit = int(limit_str)
                    df = df.head(limit)
                except (ValueError, IndexError):
                    pass

            return df

        return pd.DataFrame()

    def execute_write(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        """Execute a simulated write operation against in-memory data."""
        query_lower = query.lower().strip()

        # Handle INSERT into corrections
        if "insert" in query_lower and "correction" in query_lower:
            if parameters:
                new_row = pd.DataFrame(
                    [
                        {
                            "correction_id": str(uuid.uuid4()),
                            "transaction_id": parameters.get("transaction_id"),
                            "invoice_id": parameters.get("invoice_id"),
                            "category": parameters.get("category"),
                            "start_date": parameters.get("start_date"),
                            "end_date": None,
                            "is_current": True,
                            "comment": parameters.get("comment"),
                            "corrected_by": parameters.get("corrected_by"),
                            "correction_timestamp": parameters.get(
                                "correction_timestamp"
                            ),
                        }
                    ]
                )
                self._corrections = pd.concat(
                    [self._corrections, new_row], ignore_index=True
                )

        # Handle UPDATE on corrections (close out current record)
        if "update" in query_lower and "is_current = false" in query_lower:
            if parameters and "transaction_id" in parameters:
                mask = (
                    self._corrections["transaction_id"] == parameters["transaction_id"]
                ) & (self._corrections["is_current"] == True)
                self._corrections.loc[mask, "is_current"] = False
                self._corrections.loc[mask, "end_date"] = parameters.get("end_date")

    def is_connected(self) -> bool:
        """Mock backend is always connected."""
        return True

    def get_invoices_by_ids(self, invoice_ids: List[str]) -> pd.DataFrame:
        """Get invoices by their IDs (convenience method for mock)."""
        return self._invoices[self._invoices["invoice_id"].isin(invoice_ids)]


class LakebaseBackend(DatabaseBackend):
    """PostgreSQL backend using Databricks Lakebase."""

    def __init__(self, instance: str, dbname: str = "databricks_postgres"):
        """
        Initialize Lakebase backend.

        Args:
            instance: Lakebase instance name
            dbname: Database name (default: databricks_postgres)
        """
        self.instance = instance
        self.dbname = dbname
        self._client = None
        self._connection = None

    @property
    def client(self):
        """Lazy-load the Databricks WorkspaceClient."""
        if self._client is None:
            from databricks.sdk import WorkspaceClient

            self._client = WorkspaceClient()
        return self._client

    @contextmanager
    def get_connection(self):
        """
        Get a database connection using psycopg2.

        Following the Databricks Lakebase connection pattern.
        """
        import psycopg2

        try:
            # Get the database instance to retrieve the host DNS
            db_instance = self.client.database.get_database_instance(name=self.instance)

            # Generate short-lived credential from Databricks
            cred = self.client.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[self.instance],
            )

            # Get current user email
            user = self.client.current_user.me()
            # Try to get email from user object
            if hasattr(user, "emails") and user.emails:
                user_email = user.emails[0].value
            elif hasattr(user, "user_name") and user.user_name:
                user_email = user.user_name
            else:
                raise ValueError(
                    "Could not determine user email from Databricks user object"
                )

            # Create psycopg2 connection
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
            logger.error(f"Failed to create database connection: {str(e)}")
            raise

    def execute_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.

        Args:
            query: SQL query string (uses %(param)s for parameters in psycopg2)
            parameters: Optional dictionary of query parameters

        Returns:
            DataFrame with query results
        """
        # Convert :param style to %(param)s style for psycopg2
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
        """
        Execute a SQL write operation.

        Args:
            query: SQL query string (uses %(param)s for parameters in psycopg2)
            parameters: Optional dictionary of query parameters
        """
        # Convert :param style to %(param)s style for psycopg2
        if parameters:
            for key in parameters:
                query = query.replace(f":{key}", f"%({key})s")

        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query, parameters)
            conn.commit()

    def is_connected(self) -> bool:
        """Check if the backend can connect."""
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return True
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return False


def create_backend(config: Config) -> DatabaseBackend:
    """
    Factory function to create the appropriate database backend.

    Args:
        config: Application configuration

    Returns:
        DatabaseBackend instance
    """
    from .config import Config

    if config.is_test_mode:
        logger.info("Creating MockBackend for test mode")
        return MockBackend()

    if config.is_prod_mode:
        if not config.lakebase_instance:
            raise ValueError("lakebase_instance is required for prod mode")
        logger.info(
            f"Creating LakebaseBackend for prod mode (instance: {config.lakebase_instance})"
        )
        return LakebaseBackend(
            instance=config.lakebase_instance,
            dbname=config.lakebase_dbname,
        )

    raise ValueError(f"Unknown mode: {config.mode}")


# Global backend instance (lazy initialized)
_backend: Optional[DatabaseBackend] = None


def get_backend() -> DatabaseBackend:
    """Get the global database backend instance."""
    global _backend
    if _backend is None:
        raise RuntimeError(
            "Database backend not initialized. Call init_backend() first."
        )
    return _backend


def init_backend(config: Config) -> DatabaseBackend:
    """
    Initialize the global database backend.

    Args:
        config: Application configuration

    Returns:
        The initialized backend
    """
    global _backend
    _backend = create_backend(config)
    return _backend


def reset_backend() -> None:
    """Reset the global backend (useful for testing)."""
    global _backend
    _backend = None

