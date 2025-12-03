"""Invoice app package initialization."""

__version__ = "0.1.0"

from .config import DatabricksConfig, AppConfig
from .database import get_connection, execute_query, execute_write
from .queries import (
    search_invoices,
    get_flagged_invoices,
    get_invoices_by_ids,
    get_available_categories,
)
from .corrections import (
    write_correction,
    write_corrections_batch,
    get_correction_history,
    initialize_corrections_table,
)

__all__ = [
    "DatabricksConfig",
    "AppConfig",
    "get_connection",
    "execute_query",
    "execute_write",
    "search_invoices",
    "get_flagged_invoices",
    "get_invoices_by_ids",
    "get_available_categories",
    "write_correction",
    "write_corrections_batch",
    "get_correction_history",
    "initialize_corrections_table",
]


