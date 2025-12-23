"""Generate module - Configuration and utilities for data generation (0_generate notebook)."""

from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, ConfigDict


class GenerateConfig(BaseModel):
    """Single configuration model for 0_generate notebook.

    Flattens company and data_generation settings into one model.
    """

    model_config = ConfigDict(extra="ignore")

    # Company settings
    company_name: str
    industry: str
    cost_centres: List[str]
    plants: List[Dict[str, Any]]  # List of {name, id, region, country}
    categories: Dict[str, Dict[str, List[str]]]  # Nested category hierarchy
    category_cost_centre_mapping: Dict[str, str]

    # Data generation settings
    catalog: str
    schema_name: str
    llm_endpoint: str
    table: str
    rows: int = 10000
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD
    distribution: Dict[str, float]  # e.g. {"Direct": 0.6, "Indirect": 0.35}
    python_columns: List[str]
    llm_columns: List[str]
    prompt: str

    @classmethod
    def from_yaml(cls, config_path: Optional[str] = None) -> "GenerateConfig":
        """Load generate configuration from YAML file."""
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config.yaml"
        else:
            config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r") as f:
            data = yaml.safe_load(f)

        company = data["company"]
        data_gen = data["data_generation"]

        flat_data = {
            # Company
            "company_name": company["name"],
            "industry": company["industry"],
            "cost_centres": company["cost_centres"],
            "plants": company["plants"],
            "categories": company["categories"],
            "category_cost_centre_mapping": company["category_cost_centre_mapping"],
            # Data generation
            "catalog": data_gen["catalog"],
            "schema_name": data_gen["schema"],
            "llm_endpoint": data_gen["llm_endpoint"],
            "table": data_gen["table"],
            "rows": data_gen.get("rows", 10000),
            "start_date": str(data_gen["start"]),
            "end_date": str(data_gen["end"]),
            "distribution": data_gen["distribution"],
            "python_columns": data_gen["python_columns"],
            "llm_columns": data_gen["llm_columns"],
            "prompt": data_gen["prompt"],
        }

        return cls.model_validate(flat_data)


def load_generate_config(config_path: Optional[str] = None) -> GenerateConfig:
    """Load configuration for 0_generate notebook."""
    return GenerateConfig.from_yaml(config_path)

