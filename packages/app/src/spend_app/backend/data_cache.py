"""Data cache lifespan: loads all tables into app.state at startup."""

from __future__ import annotations

import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncGenerator

import pandas as pd
from fastapi import FastAPI, Request

from .core._base import LifespanDependency
from .core._config import logger as _logger

logger = _logger

_project_root = Path(__file__).resolve().parents[5]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


def _col_summary(df: pd.DataFrame) -> tuple[int, str]:
    n = len(df.columns)
    display = ", ".join(df.columns[:6]) + ("..." if n > 6 else "")
    return n, display


class DataCacheDependency(LifespanDependency):
    """Loads all tables defined in config.yaml app.tables into app.state."""

    @asynccontextmanager
    async def lifespan(self, app: FastAPI) -> AsyncGenerator[None, None]:
        from src.config import load_config
        from src.app.database import create_backend

        config = load_config(str(_project_root / "config.yaml"))
        backend = create_backend(config)

        data: dict[str, pd.DataFrame] = {}
        table_status: list[dict[str, Any]] = []

        for label, tdef in config.app_tables.items():
            table_path = config.app_table_path(label)
            try:
                df = backend.execute_query(f"SELECT * FROM {table_path}")
                data[label] = df
                n_cols, col_display = _col_summary(df)
                table_status.append({
                    "table": label,
                    "description": tdef.description,
                    "rows": len(df),
                    "columns": n_cols,
                    "column_names": col_display,
                    "status": "ok" if len(df) > 0 else "empty",
                })
                logger.info(f"Loaded {label}: {len(df)} rows")
            except Exception as e:
                logger.warning(f"Failed to load {label}: {e}")
                data[label] = pd.DataFrame()
                table_status.append({
                    "table": label,
                    "description": tdef.description,
                    "rows": 0,
                    "columns": 0,
                    "column_names": "--",
                    "status": "missing",
                })

        app.state.data = data
        app.state.table_status = table_status
        app.state.project_config = config
        app.state.backend = backend

        logger.info("Data cache loaded successfully")
        yield

    @staticmethod
    def __call__(request: Request) -> dict[str, pd.DataFrame]:
        return request.app.state.data
