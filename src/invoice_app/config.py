"""Simple configuration using Pydantic - loads YAML directly into typed models."""

from pathlib import Path
from typing import Optional, List, Literal
from pydantic import BaseModel, Field, ConfigDict
import yaml


class AppConfig(BaseModel):
    """App-level settings."""
    mode: Literal["test", "prod"] = "test"
    page_size: int = 50
    default_user: str = "user"
    # Lakebase settings (only needed for prod mode)
    lakebase_instance: str = ""
    lakebase_dbname: str = "databricks_postgres"

    @property
    def is_test_mode(self) -> bool:
        return self.mode == "test"

    @property
    def is_prod_mode(self) -> bool:
        return self.mode == "prod"


class TablesConfig(BaseModel):
    """Table name settings."""
    invoices: str = "invoices"
    corrections: str = "invoice_corrections"
    flagged_view: Optional[str] = None


class UIConfig(BaseModel):
    """UI settings."""
    title: str = "Spend Categorization"
    icon: str = "databricks"


class SearchConfig(BaseModel):
    """Search settings."""
    default_fields: List[str] = Field(
        default=["invoice_number", "vendor_name", "description"]
    )
    max_results: int = 500


class FlaggingConfig(BaseModel):
    """Flagging thresholds."""
    low_confidence_threshold: float = 0.7
    critical_confidence_threshold: float = 0.5


class Config(BaseModel):
    """Root configuration - maps directly to config.yaml structure."""
    model_config = ConfigDict(extra="allow")  # Allow extra fields like company, data_generation

    app: AppConfig = Field(default_factory=AppConfig)
    tables: TablesConfig = Field(default_factory=TablesConfig)
    ui: UIConfig = Field(default_factory=UIConfig)
    search: SearchConfig = Field(default_factory=SearchConfig)
    flagging: FlaggingConfig = Field(default_factory=FlaggingConfig)


def load_config(config_path: Optional[str] = None) -> Config:
    """Load configuration from YAML file into Pydantic model."""
    if config_path is None:
        config_path = Path(__file__).parent.parent.parent / "config.yaml"
    else:
        config_path = Path(config_path)

    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(config_path, "r") as f:
        data = yaml.safe_load(f)

    return Config.model_validate(data)
