"""App package initialization."""

__version__ = "0.1.0"

from src.config import Config, load_config
from .database import (
    DatabaseBackend,
    MockBackend,
    LakebaseBackend,
    create_backend,
    get_backend,
    init_backend,
    reset_backend,
)
from .queries import (
    search_invoices,
    get_flagged_invoices,
    get_invoices_by_ids,
    get_available_categories,
    get_invoice_with_categorization,
)
from .reviews import (
    write_review,
    write_reviews_batch,
    get_review_history,
    initialize_reviews_table,
)

__all__ = [
    # Config
    "Config",
    "load_config",
    # Database backend
    "DatabaseBackend",
    "MockBackend",
    "LakebaseBackend",
    "create_backend",
    "get_backend",
    "init_backend",
    "reset_backend",
    # Query functions
    "search_invoices",
    "get_flagged_invoices",
    "get_invoices_by_ids",
    "get_available_categories",
    "get_invoice_with_categorization",
    # Review functions
    "write_review",
    "write_reviews_batch",
    "get_review_history",
    "initialize_reviews_table",
]
