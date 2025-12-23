"""App configuration - Single Pydantic model for the Streamlit application.

This module handles configuration for the invoice correction app (app.py).
For notebook configurations, see src.generate and src.utils.
"""

from pathlib import Path
from typing import Optional, List, Literal
from pydantic import BaseModel, Field, ConfigDict
import yaml


class AppConfig(BaseModel):
    """Single configuration model for the Streamlit app.

    Flattens all app settings into one model for simplicity.
    """

    model_config = ConfigDict(extra="ignore")

    # App settings
    mode: Literal["test", "prod"] = "test"
    page_size: int = 50
    default_user: str = "user"
    lakebase_instance: str = ""
    lakebase_dbname: str = "databricks_postgres"

    # Table settings
    invoices_table: str = "invoices"
    corrections_table: str = "invoice_corrections"
    flagged_view: Optional[str] = None

    # UI settings
    ui_title: str = "Spend Categorization"
    ui_icon: str = "databricks"

    # Search settings
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
        """Load app configuration from YAML file."""
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / "config.yaml"
        else:
            config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        # Flatten nested YAML structure into single model
        flat_data = {}

        # App settings
        if "app" in data:
            app = data["app"]
            flat_data["mode"] = app.get("mode", "test")
            flat_data["page_size"] = app.get("page_size", 50)
            flat_data["default_user"] = app.get("default_user", "user")
            flat_data["lakebase_instance"] = app.get("lakebase_instance", "")
            flat_data["lakebase_dbname"] = app.get(
                "lakebase_dbname", "databricks_postgres"
            )

        # Table settings
        if "tables" in data:
            tables = data["tables"]
            flat_data["invoices_table"] = tables.get("invoices", "invoices")
            flat_data["corrections_table"] = tables.get(
                "corrections", "invoice_corrections"
            )
            flat_data["flagged_view"] = tables.get("flagged_view")

        # UI settings
        if "ui" in data:
            ui = data["ui"]
            flat_data["ui_title"] = ui.get("title", "Spend Categorization")
            flat_data["ui_icon"] = ui.get("icon", "databricks")

        # Search settings
        if "search" in data:
            search = data["search"]
            flat_data["search_fields"] = search.get(
                "default_fields", ["invoice_number", "vendor_name", "description"]
            )
            flat_data["max_results"] = search.get("max_results", 500)

        # Flagging settings
        if "flagging" in data:
            flagging = data["flagging"]
            flat_data["low_confidence_threshold"] = flagging.get(
                "low_confidence_threshold", 0.7
            )
            flat_data["critical_confidence_threshold"] = flagging.get(
                "critical_confidence_threshold", 0.5
            )

        return cls.model_validate(flat_data)


# Backward compatibility alias
Config = AppConfig


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """Load app configuration from YAML file into Pydantic model."""
    return AppConfig.from_yaml(config_path)

