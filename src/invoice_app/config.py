"""Configuration management for database connections."""

import os
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, Literal
from pathlib import Path
import yaml


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from YAML file.
    
    Args:
        config_path: Path to config.yaml file. If None, looks in workspace root.
        
    Returns:
        Dictionary containing configuration
    """
    if config_path is None:
        # Look for config.yaml in the workspace root
        config_path = Path(__file__).parent.parent.parent / "config.yaml"
    else:
        config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


@dataclass
class LakebaseConfig:
    """Configuration for Lakebase (PostgreSQL on Databricks) connection."""
    
    instance_name: str
    database: str = "databricks_postgres"
    user: str = "databricks"
    schema: str = "public"
    
    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "LakebaseConfig":
        """Load configuration from dictionary."""
        lakebase_config = config.get("lakebase", {})
        return cls(
            instance_name=lakebase_config.get("instance_name", ""),
            database=lakebase_config.get("database", "databricks_postgres"),
            user=lakebase_config.get("user", "databricks"),
            schema=lakebase_config.get("schema", "public"),
        )
    
    def validate(self) -> None:
        """Validate required configuration values."""
        if not self.instance_name:
            raise ValueError("lakebase.instance_name is required in config.yaml for prod mode")


@dataclass
class DatabricksConfig:
    """Configuration for Databricks SQL connection (legacy, for warehouse queries)."""
    
    server_hostname: str
    http_path: str
    access_token: str
    catalog: str = "main"
    schema: str = "default"
    
    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "DatabricksConfig":
        """Load configuration from dictionary."""
        db_config = config.get("databricks", {})
        return cls(
            server_hostname=db_config.get("server_hostname", ""),
            http_path=db_config.get("http_path", ""),
            access_token=db_config.get("access_token", ""),
            catalog=db_config.get("catalog", "main"),
            schema=db_config.get("schema", "default"),
        )
    
    def validate(self) -> None:
        """Validate required configuration values."""
        if not self.server_hostname:
            raise ValueError("databricks.server_hostname is required in config.yaml")
        if not self.http_path:
            raise ValueError("databricks.http_path is required in config.yaml")
        if not self.access_token:
            raise ValueError("databricks.access_token is required in config.yaml")


@dataclass
class AppConfig:
    """Application-level configuration."""
    
    invoices_table: str
    corrections_table: str
    flagged_invoices_view: Optional[str] = None
    page_size: int = 50
    mode: Literal["test", "prod"] = "test"
    default_user: str = "user"
    ui_title: str = "Invoice Classification Correction"
    ui_icon: str = "📊"
    search_fields: list = None
    max_results: int = 500
    low_confidence_threshold: float = 0.7
    critical_confidence_threshold: float = 0.5
    
    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "AppConfig":
        """Load configuration from dictionary."""
        tables = config.get("tables", {})
        app = config.get("app", {})
        ui = config.get("ui", {})
        search = config.get("search", {})
        flagging = config.get("flagging", {})
        
        # Support both old demo_mode and new mode settings
        mode = app.get("mode", "test")
        if config.get("demo_mode", False):
            mode = "test"
        
        return cls(
            invoices_table=tables.get("invoices", "invoices"),
            corrections_table=tables.get("corrections", "invoice_corrections"),
            flagged_invoices_view=tables.get("flagged_view"),
            page_size=app.get("page_size", 50),
            mode=mode,
            default_user=app.get("default_user", "user"),
            ui_title=ui.get("title", "Invoice Classification Correction"),
            ui_icon=ui.get("icon", "📊"),
            search_fields=search.get("default_fields", ["invoice_number", "vendor_name", "description"]),
            max_results=search.get("max_results", 500),
            low_confidence_threshold=flagging.get("low_confidence_threshold", 0.7),
            critical_confidence_threshold=flagging.get("critical_confidence_threshold", 0.5),
        )
    
    @property
    def is_test_mode(self) -> bool:
        """Check if running in test mode (no backend)."""
        return self.mode == "test"
    
    @property
    def is_prod_mode(self) -> bool:
        """Check if running in prod mode (with Lakebase)."""
        return self.mode == "prod"
    
    # Backward compatibility property
    @property
    def demo_mode(self) -> bool:
        """Backward compatible property - returns True if in test mode."""
        return self.is_test_mode
