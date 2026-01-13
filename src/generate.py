"""Generate module - Configuration for data generation (0_generate notebook).

Creates:
- invoices_raw: Python-generated base invoice data
- invoices: LLM-enhanced with descriptions and supplier info
- categories: Category hierarchy from config
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, ConfigDict


class GenerateConfig(BaseModel):
    """Configuration for 0_generate notebook."""

    model_config = ConfigDict(extra="ignore")

    # Company
    company_name: str
    industry: str
    cost_centres: List[str]
    plants: List[Dict[str, Any]]
    categories: Dict[str, Dict[str, List[str]]]
    category_cost_centre_mapping: Dict[str, str]

    # Delta location
    catalog: str
    schema_name: str

    # Table names
    invoices_raw_table: str
    invoices_table: str
    categories_table: str

    # LLM settings
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

    @property
    def full_invoices_raw(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.invoices_raw_table}"

    @property
    def full_invoices(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.invoices_table}"

    @property
    def full_categories(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.categories_table}"

    @classmethod
    def from_yaml(cls, config_path: Optional[str] = None) -> "GenerateConfig":
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config.yaml"
        else:
            config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Config not found: {config_path}")

        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        gen = data["generate"]
        company = gen["company"]
        tables = gen["tables"]
        data_gen = gen["data"]

        return cls.model_validate(
            {
                "company_name": company["name"],
                "industry": company["industry"],
                "cost_centres": company["cost_centres"],
                "plants": company["plants"],
                "categories": company["categories"],
                "category_cost_centre_mapping": company["category_cost_centre_mapping"],
                "catalog": data_gen["catalog"],
                "schema_name": data_gen["schema"],
                "invoices_raw_table": tables["invoices_raw"],
                "invoices_table": tables["invoices"],
                "categories_table": tables["categories"],
                "small_llm_endpoint": data_gen["small_llm_endpoint"],
                "large_llm_endpoint": data_gen["large_llm_endpoint"],
                "prompt": data_gen["prompt"],
                "rows": data_gen.get("rows", 10000),
                "start_date": str(data_gen["start"]),
                "end_date": str(data_gen["end"]),
                "distribution": data_gen["distribution"],
                "python_columns": data_gen["python_columns"],
                "llm_columns": data_gen["llm_columns"],
            }
        )


def load_generate_config(config_path: Optional[str] = None) -> GenerateConfig:
    """Load configuration for 0_generate notebook."""
    return GenerateConfig.from_yaml(config_path)
