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


class AppTableDef(BaseModel):
    """Definition of a single table loaded by the app."""
    table: str
    description: str = ""
    native: bool = False


class ColumnMap(BaseModel):
    """Maps logical column roles to physical column names in the dataset."""
    id: str = "order_id"
    date: str = "date"
    description: str = "description"
    supplier: str = "supplier"
    amount: str = "total"
    category_l1: str = "category_level_1"
    category_l2: str = "category_level_2"
    category_l3: str = "category_level_3"
    region: str = "region"
    pred_l1: str = "pred_level_1"
    pred_l2: str = "pred_level_2"
    pred_l3: str = "pred_level_3"
    actual_l1: str = "actual_level_1"
    actual_l2: str = "actual_level_2"
    actual_l3: str = "actual_level_3"
    confidence: str = "confidence"


class Config(BaseModel):
    """Unified configuration for all notebooks and apps."""

    # Company info
    company_name: str
    industry: str
    cost_centres: List[str]
    plants: List[Dict[str, Any]]
    categories: Dict[str, Dict[str, List[str]]]
    categories_file: str
    category_cost_centre_mapping: Dict[str, str]

    # Database
    catalog: str
    schema_name: str

    # Tables - Generate
    invoices_raw: str
    invoices: str
    categories_table: str
    categories_str: str = "categories_str"

    # Tables - Categorize
    prompts: str = "prompts"
    cat_bootstrap: str = "cat_bootstrap"
    cat_catboost: str = "cat_catboost"
    cat_vectorsearch: str = "cat_vectorsearch"
    cat_reviews: str = "cat_reviews"

    # Active categorization source
    categorization_source: Literal["bootstrap", "catboost", "vectorsearch"] = "bootstrap"

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
    page_size: int = 50
    default_user: str = "user"
    lakebase_instance: str = ""
    lakebase_dbname: str = "databricks_postgres"

    # App column mapping (generic)
    col: ColumnMap = Field(default_factory=ColumnMap)

    # App table definitions (generic)
    app_tables: Dict[str, AppTableDef] = Field(default_factory=dict)

    # App search / flagging
    max_results: int = 500
    low_confidence_threshold: float = 3.5
    critical_confidence_threshold: float = 2.5

    # --- Table Path Helpers ---

    def full_table_path(self, table_name: str) -> str:
        """Return catalog.schema.table for any table name."""
        return f"{self.catalog}.{self.schema_name}.{table_name}"

    def app_table_path(self, label: str) -> str:
        """Return the full path for an app table by its label."""
        td = self.app_tables.get(label)
        if td is None:
            raise KeyError(f"No app table defined with label '{label}'")
        return self.full_table_path(td.table)

    def app_table_is_native(self, label: str) -> bool:
        td = self.app_tables.get(label)
        return td.native if td else False

    @property
    def full_invoices_raw_table_path(self) -> str:
        return self.full_table_path(self.invoices_raw)

    @property
    def full_invoices_table_path(self) -> str:
        return self.full_table_path(self.invoices)

    @property
    def full_categories_table_path(self) -> str:
        return self.full_table_path(self.categories_table)

    @property
    def full_categories_str_table_path(self) -> str:
        return self.full_table_path(self.categories_str)

    @property
    def full_prompts_table_path(self) -> str:
        return self.full_table_path(self.prompts)

    @property
    def full_cat_bootstrap_table_path(self) -> str:
        return self.full_table_path(self.cat_bootstrap)

    @property
    def full_cat_catboost_table_path(self) -> str:
        return self.full_table_path(self.cat_catboost)

    @property
    def full_cat_vectorsearch_table_path(self) -> str:
        return self.full_table_path(self.cat_vectorsearch)

    @property
    def full_cat_reviews_table_path(self) -> str:
        return self.full_table_path(self.cat_reviews)

    @property
    def active_categorization_table_path(self) -> str:
        source_map = {
            "bootstrap": self.full_cat_bootstrap_table_path,
            "catboost": self.full_cat_catboost_table_path,
            "vectorsearch": self.full_cat_vectorsearch_table_path,
        }
        return source_map[self.categorization_source]

    # --- Category Methods ---

    def get_category_descriptions(self, config_path: Optional[str] = None) -> Dict[str, str]:
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

        gen = data["generate"]
        company = gen["company"]
        gen_tables = gen["tables"]
        gen_data = gen["data"]

        csv_file = company["categories_file"]
        if config_path:
            csv_path = Path(config_path).parent / csv_file
        else:
            csv_path = Path(__file__).parent.parent / csv_file

        if not csv_path.exists():
            raise FileNotFoundError(f"Categories CSV not found: {csv_path}")

        df = pd.read_csv(csv_path)
        categories: Dict[str, Dict[str, List[str]]] = {}
        category_cost_centre_mapping: Dict[str, str] = {}
        for _, row in df.iterrows():
            l1, l2, l3 = row["category_level_1"], row["category_level_2"], row["category_level_3"]
            categories.setdefault(l1, {}).setdefault(l2, []).append(l3)
            if l2 not in category_cost_centre_mapping and "cost_centre" in row:
                category_cost_centre_mapping[l2] = row["cost_centre"]

        cat = data.get("categorize", {})
        cat_tables = cat.get("tables", {})
        app = data.get("app", {})

        # Parse generic app tables
        raw_tables = app.get("tables", {})
        app_tables = {
            label: AppTableDef(**tdef) if isinstance(tdef, dict) else AppTableDef(table=tdef)
            for label, tdef in raw_tables.items()
        }

        # Parse column mapping
        col = ColumnMap(**app.get("columns", {}))

        return cls(
            company_name=company["name"],
            industry=company["industry"],
            cost_centres=company["cost_centres"],
            plants=company["plants"],
            categories=categories,
            categories_file=csv_file,
            category_cost_centre_mapping=category_cost_centre_mapping,
            catalog=gen_data["catalog"],
            schema_name=gen_data["schema"],
            invoices_raw=gen_tables["invoices_raw"],
            invoices=gen_tables["invoices"],
            categories_table=gen_tables["categories"],
            categories_str=gen_tables.get("categories_str", "categories_str"),
            prompts=cat_tables.get("prompts", "prompts"),
            cat_bootstrap=cat_tables.get("cat_bootstrap", "cat_bootstrap"),
            cat_catboost=cat_tables.get("cat_catboost", "cat_catboost"),
            cat_vectorsearch=cat_tables.get("cat_vectorsearch", "cat_vectorsearch"),
            cat_reviews=cat_tables.get("cat_reviews", "cat_reviews"),
            categorization_source=app.get("categorization_source", "bootstrap"),
            small_llm_endpoint=gen_data["small_llm_endpoint"],
            large_llm_endpoint=gen_data["large_llm_endpoint"],
            prompt=gen_data["prompt"],
            rows=gen_data.get("rows", 10000),
            start_date=str(gen_data["start"]),
            end_date=str(gen_data["end"]),
            distribution=gen_data["distribution"],
            python_columns=gen_data["python_columns"],
            llm_columns=gen_data["llm_columns"],
            page_size=app.get("page_size", 50),
            default_user=app.get("default_user", "user"),
            lakebase_instance=app.get("lakebase_instance", ""),
            lakebase_dbname=app.get("lakebase_dbname", "databricks_postgres"),
            col=col,
            app_tables=app_tables,
            max_results=app.get("search", {}).get("max_results", 500),
            low_confidence_threshold=app.get("flagging", {}).get("low_confidence_threshold", 3.5),
            critical_confidence_threshold=app.get("flagging", {}).get("critical_confidence_threshold", 2.5),
        )


def load_config(path: Optional[str] = None) -> Config:
    """Load unified configuration from config.yaml."""
    return Config.from_yaml(path)
