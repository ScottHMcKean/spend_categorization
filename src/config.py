"""Unified configuration for Spend Categorization.

Single config class that loads everything from config.yaml.
"""

from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
import yaml
from pydantic import BaseModel, Field


def _load_yaml(config_path: Optional[str] = None) -> dict:
    """Load and return the config.yaml file."""
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config.yaml"
    else:
        config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


class Config(BaseModel):
    """Unified configuration for all notebooks and apps."""

    # Company info
    company_name: str
    industry: str
    cost_centres: List[str]
    plants: List[Dict[str, Any]]
    categories: Dict[str, Dict[str, List[str]]]  # Loaded from CSV
    categories_file: str
    category_cost_centre_mapping: Dict[str, str]

    # Database
    catalog: str
    schema_name: str

    # Tables - Generate
    invoices_raw: str
    invoices: str
    categories_table: str

    # Tables - Categorize
    prompts: str = "prompts"
    cat_bootstrap: str = "cat_bootstrap"
    cat_catboost: str = "cat_catboost"
    cat_vectorsearch: str = "cat_vectorsearch"

    # LLM
    small_llm_endpoint: str
    large_llm_endpoint: str
    prompt: str

    # Data generation
    rows: int = 10000
    start_date: str
    end_date: str
    distribution: Dict[str, float]
    python_columns: List[str]
    llm_columns: List[str]

    # App settings
    app_mode: Literal["test", "prod"] = "test"
    page_size: int = 50
    default_user: str = "user"
    lakebase_instance: str = ""
    lakebase_dbname: str = "databricks_postgres"
    
    # App tables
    invoices_sync: str = "invoices_sync"
    categorization_sync: str = "categorization_sync"
    reviews: str = "reviews"
    
    # App UI
    ui_title: str = "Spend Categorization"
    ui_icon: str = "databricks"
    
    # App search
    search_fields: List[str] = Field(default=["invoice_number", "vendor_name", "description"])
    max_results: int = 500
    
    # App flagging
    low_confidence_threshold: float = 0.7
    critical_confidence_threshold: float = 0.5

    # --- Table Path Properties ---

    @property
    def full_invoices_raw_table_path(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.invoices_raw}"

    @property
    def full_invoices_table_path(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.invoices}"

    @property
    def full_categories_table_path(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.categories_table}"

    @property
    def full_prompts_table_path(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.prompts}"

    @property
    def full_cat_bootstrap_table_path(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.cat_bootstrap}"

    @property
    def full_cat_catboost_table_path(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.cat_catboost}"

    @property
    def full_cat_vectorsearch_table_path(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.cat_vectorsearch}"

    # --- App Properties ---

    @property
    def is_test_mode(self) -> bool:
        return self.app_mode == "test"

    @property
    def is_prod_mode(self) -> bool:
        return self.app_mode == "prod"

    # --- Category Methods ---

    def get_category_descriptions(self, config_path: Optional[str] = None) -> Dict[str, str]:
        """Get Level 3 category descriptions from CSV."""
        if config_path is None:
            csv_path = Path(__file__).parent.parent / self.categories_file
        else:
            csv_path = Path(config_path).parent / self.categories_file

        df = pd.read_csv(csv_path)
        return dict(zip(df["category_level_3"], df["category_level_3_description"]))

    @classmethod
    def from_yaml(cls, config_path: Optional[str] = None) -> "Config":
        """Load unified config from config.yaml."""
        data = _load_yaml(config_path)
        
        # Load generate section
        gen = data["generate"]
        company = gen["company"]
        gen_tables = gen["tables"]
        gen_data = gen["data"]

        # Load categories from CSV
        csv_file = company["categories_file"]
        if config_path:
            csv_path = Path(config_path).parent / csv_file
        else:
            csv_path = Path(__file__).parent.parent / csv_file
        
        if not csv_path.exists():
            raise FileNotFoundError(f"Categories CSV not found: {csv_path}")

        df = pd.read_csv(csv_path)
        categories = {}
        for _, row in df.iterrows():
            l1, l2, l3 = row["category_level_1"], row["category_level_2"], row["category_level_3"]
            if l1 not in categories:
                categories[l1] = {}
            if l2 not in categories[l1]:
                categories[l1][l2] = []
            categories[l1][l2].append(l3)

        # Load categorize section (uses same catalog/schema as generate)
        cat = data.get("categorize", {})
        cat_tables = cat.get("tables", {})

        # Load app section
        app = data.get("app", {})

        return cls(
            # Company
            company_name=company["name"],
            industry=company["industry"],
            cost_centres=company["cost_centres"],
            plants=company["plants"],
            categories=categories,
            categories_file=csv_file,
            category_cost_centre_mapping=company["category_cost_centre_mapping"],
            
            # Database
            catalog=gen_data["catalog"],
            schema_name=gen_data["schema"],
            
            # Generate tables
            invoices_raw=gen_tables["invoices_raw"],
            invoices=gen_tables["invoices"],
            categories_table=gen_tables["categories"],
            
            # Categorize tables
            prompts=cat_tables.get("prompts", "prompts"),
            cat_bootstrap=cat_tables.get("cat_bootstrap", "cat_bootstrap"),
            cat_catboost=cat_tables.get("cat_catboost", "cat_catboost"),
            cat_vectorsearch=cat_tables.get("cat_vectorsearch", "cat_vectorsearch"),
            
            # LLM
            small_llm_endpoint=gen_data["small_llm_endpoint"],
            large_llm_endpoint=gen_data["large_llm_endpoint"],
            prompt=gen_data["prompt"],
            
            # Data generation
            rows=gen_data.get("rows", 10000),
            start_date=str(gen_data["start"]),
            end_date=str(gen_data["end"]),
            distribution=gen_data["distribution"],
            python_columns=gen_data["python_columns"],
            llm_columns=gen_data["llm_columns"],
            
            # App
            app_mode=app.get("mode", "test"),
            page_size=app.get("page_size", 50),
            default_user=app.get("default_user", "user"),
            lakebase_instance=app.get("lakebase_instance", ""),
            lakebase_dbname=app.get("lakebase_dbname", "databricks_postgres"),
            invoices_sync=app.get("tables", {}).get("invoices_sync", "invoices_sync"),
            categorization_sync=app.get("tables", {}).get("categorization_sync", "categorization_sync"),
            reviews=app.get("tables", {}).get("reviews", "reviews"),
            ui_title=app.get("ui", {}).get("title", "Spend Categorization"),
            ui_icon=app.get("ui", {}).get("icon", "databricks"),
            search_fields=app.get("search", {}).get("default_fields", ["invoice_number", "vendor_name", "description"]),
            max_results=app.get("search", {}).get("max_results", 500),
            low_confidence_threshold=app.get("flagging", {}).get("low_confidence_threshold", 0.7),
            critical_confidence_threshold=app.get("flagging", {}).get("critical_confidence_threshold", 0.5),
        )


def load_config(path: Optional[str] = None) -> Config:
    """Load unified configuration from config.yaml."""
    return Config.from_yaml(path)
