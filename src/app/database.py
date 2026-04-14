"""Database backend abstraction.

- lakebase: LakebaseBackend with PostgreSQL on Databricks (primary)
- prod: ServerlessBackend querying Delta tables via statement execution (notebooks)
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set

import pandas as pd

if TYPE_CHECKING:
    from src.config import Config

logger = logging.getLogger(__name__)


class DatabaseBackend(ABC):
    """Abstract base class for database backends."""

    @abstractmethod
    def execute_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def execute_write(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        pass


class LakebaseBackend(DatabaseBackend):
    """PostgreSQL backend using Databricks Lakebase.

    Queries are written using Delta 3-part names (catalog.schema.table).
    This backend rewrites them to PG format at execution time:
      - synced tables: "schema"."table_synced"
      - native tables: "schema"."table"

    Which tables are native is determined by the ``native_tables`` set
    passed at construction time (populated from config).
    """

    _CRED_TTL_SECONDS = 2700

    def __init__(
        self,
        instance: str,
        dbname: str = "databricks_postgres",
        catalog: str = "",
        schema_name: str = "",
        native_tables: Optional[Set[str]] = None,
    ):
        self.instance = instance
        self.dbname = dbname
        self.catalog = catalog
        self.schema_name = schema_name
        self.native_tables: Set[str] = native_tables or set()
        self._client = None
        self._conn = None
        self._host: Optional[str] = None
        self._user_email: Optional[str] = None
        self._token: Optional[str] = None
        self._cred_time: float = 0

    @property
    def client(self):
        if self._client is None:
            from databricks.sdk import WorkspaceClient
            self._client = WorkspaceClient()
        return self._client

    def _rewrite_query(self, query: str) -> str:
        if not self.catalog or not self.schema_name:
            return query

        prefix_re = re.escape(f"{self.catalog}.{self.schema_name}.")

        def _replace(m: re.Match) -> str:
            table = m.group(1)
            if table in self.native_tables:
                return f'"{self.schema_name}"."{table}"'
            return f'"{self.schema_name}"."{table}_synced"'

        return re.sub(prefix_re + r"(\w+)", _replace, query)

    def _ensure_credentials(self) -> None:
        import time

        now = time.time()
        if self._host and self._token and (now - self._cred_time) < self._CRED_TTL_SECONDS:
            return

        db_instance = self.client.database.get_database_instance(name=self.instance)
        cred = self.client.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[self.instance],
        )
        user = self.client.current_user.me()
        if hasattr(user, "emails") and user.emails:
            self._user_email = user.emails[0].value
        elif hasattr(user, "user_name") and user.user_name:
            self._user_email = user.user_name
        else:
            raise ValueError("Could not determine user email")

        self._host = db_instance.read_write_dns
        self._token = cred.token
        self._cred_time = now
        self._close_conn()
        logger.info("Lakebase credentials refreshed")

    def _close_conn(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _get_conn(self):
        import psycopg2

        self._ensure_credentials()

        if self._conn is not None:
            try:
                with self._conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return self._conn
            except Exception:
                self._close_conn()

        self._conn = psycopg2.connect(
            host=self._host,
            dbname=self.dbname,
            user=self._user_email,
            password=self._token,
            sslmode="require",
        )
        self._conn.autocommit = False
        logger.info("Lakebase connection established")
        return self._conn

    @contextmanager
    def get_connection(self):
        conn = self._get_conn()
        yield conn

    def execute_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        query = self._rewrite_query(query)
        if parameters:
            for key in parameters:
                query = query.replace(f":{key}", f"%({key})s")

        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, parameters)
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                conn.commit()
                return pd.DataFrame(rows, columns=columns)
        except Exception:
            conn.rollback()
            raise

    def execute_write(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> None:
        query = self._rewrite_query(query)
        if parameters:
            for key in parameters:
                query = query.replace(f":{key}", f"%({key})s")

        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(query, parameters)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def execute_writes_batch(
        self, queries: List[tuple[str, Optional[Dict[str, Any]]]]
    ) -> None:
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                for query, parameters in queries:
                    q = self._rewrite_query(query)
                    if parameters:
                        for key in parameters:
                            q = q.replace(f":{key}", f"%({key})s")
                    cursor.execute(q, parameters)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def is_connected(self) -> bool:
        try:
            conn = self._get_conn()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            conn.commit()
            return True
        except Exception as e:
            logger.error(f"Connection check failed: {e}")
            return False


class ServerlessBackend(DatabaseBackend):
    """Backend querying Delta tables via WorkspaceClient statement execution.

    Used primarily by notebooks; not used by the app.
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


def create_backend(config: "Config") -> DatabaseBackend:
    """Create a LakebaseBackend from configuration."""
    if not config.lakebase_instance:
        raise ValueError(
            "lakebase_instance is required in config.yaml app section"
        )

    native_tables: set[str] = set()
    for label, tdef in config.app_tables.items():
        if tdef.native:
            native_tables.add(tdef.table)

    logger.info(f"Creating LakebaseBackend (instance: {config.lakebase_instance})")
    return LakebaseBackend(
        instance=config.lakebase_instance,
        dbname=config.lakebase_dbname,
        catalog=config.catalog,
        schema_name=config.schema_name,
        native_tables=native_tables,
    )


_backend: Optional[DatabaseBackend] = None


def get_backend() -> DatabaseBackend:
    global _backend
    if _backend is None:
        raise RuntimeError("Backend not initialized. Call init_backend() first.")
    return _backend


def init_backend(config: "Config") -> DatabaseBackend:
    global _backend
    _backend = create_backend(config)
    return _backend


def reset_backend() -> None:
    global _backend
    _backend = None
