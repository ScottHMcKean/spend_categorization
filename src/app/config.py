"""App configuration for the Streamlit application.

Reads from the 'app' section in config.yaml.
"""

from pathlib import Path
from typing import List, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field


class AppConfig(BaseModel):
    """Configuration for the Streamlit app."""

    model_config = ConfigDict(extra="ignore")

    # App settings
    mode: Literal["test", "prod"] = "test"
    page_size: int = 50
    default_user: str = "user"
    lakebase_instance: str = ""
    lakebase_dbname: str = "databricks_postgres"

    # Lakebase table names
    invoices_sync: str = "invoices_sync"
    categorization_sync: str = "categorization_sync"
    reviews_table: str = "reviews"

    # UI
    ui_title: str = "Spend Categorization"
    ui_icon: str = "databricks"

    # Search
    search_fields: List[str] = Field(
        default=["invoice_number", "vendor_name", "description"]
    )
    max_results: int = 500

    # Flagging thresholds
    low_confidence_threshold: float = 0.7
    critical_confidence_threshold: float = 0.5

    @property
    def is_test_mode(self) -> bool:
        return self.mode == "test"

    @property
    def is_prod_mode(self) -> bool:
        return self.mode == "prod"

    @classmethod
    def from_yaml(cls, config_path: Optional[str] = None) -> "AppConfig":
        """Load from config.yaml app section."""
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config.yaml"
        else:
            config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        app = data.get("app", {})

        flat = {
            "mode": app.get("mode", "test"),
            "page_size": app.get("page_size", 50),
            "default_user": app.get("default_user", "user"),
            "lakebase_instance": app.get("lakebase_instance", ""),
            "lakebase_dbname": app.get("lakebase_dbname", "databricks_postgres"),
        }

        if "tables" in app:
            tables = app["tables"]
            flat["invoices_sync"] = tables.get("invoices_sync", "invoices_sync")
            flat["categorization_sync"] = tables.get(
                "categorization_sync", "categorization_sync"
            )
            flat["reviews_table"] = tables.get("reviews", "reviews")

        if "ui" in app:
            ui = app["ui"]
            flat["ui_title"] = ui.get("title", "Spend Categorization")
            flat["ui_icon"] = ui.get("icon", "databricks")

        if "search" in app:
            search = app["search"]
            flat["search_fields"] = search.get(
                "default_fields", ["invoice_number", "vendor_name", "description"]
            )
            flat["max_results"] = search.get("max_results", 500)

        if "flagging" in app:
            flagging = app["flagging"]
            flat["low_confidence_threshold"] = flagging.get(
                "low_confidence_threshold", 0.7
            )
            flat["critical_confidence_threshold"] = flagging.get(
                "critical_confidence_threshold", 0.5
            )

        return cls.model_validate(flat)


# Backward compatibility
Config = AppConfig


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """Load app configuration from YAML."""
    return AppConfig.from_yaml(config_path)
