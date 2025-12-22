"""Invoice app package initialization."""

__version__ = "0.1.0"

from .config import DatabricksConfig, AppConfig, LakebaseConfig, load_config
from .database import (
    DatabaseBackend,
    MockBackend,
    LakebaseBackend,
    create_backend,
    get_backend,
    init_backend,
    reset_backend,
    execute_query,
    execute_write,
    get_connection,
)
from .queries import (
    search_invoices,
    get_flagged_invoices,
    get_invoices_by_ids,
    get_available_categories,
    get_invoice_by_transaction_id,
)
from .corrections import (
    write_correction,
    write_corrections_batch,
    get_correction_history,
    initialize_corrections_table,
    initialize_invoices_table,
)

__all__ = [
    # Config classes
    "DatabricksConfig",
    "AppConfig",
    "LakebaseConfig",
    "load_config",
    # Database backend
    "DatabaseBackend",
    "MockBackend",
    "LakebaseBackend",
    "create_backend",
    "get_backend",
    "init_backend",
    "reset_backend",
    "execute_query",
    "execute_write",
    "get_connection",
    # Query functions
    "search_invoices",
    "get_flagged_invoices",
    "get_invoices_by_ids",
    "get_available_categories",
    "get_invoice_by_transaction_id",
    # Correction functions
    "write_correction",
    "write_corrections_batch",
    "get_correction_history",
    "initialize_corrections_table",
    "initialize_invoices_table",
]
