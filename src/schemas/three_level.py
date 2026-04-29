"""Existing 3-level Direct/Indirect spend hierarchy."""

from __future__ import annotations

import pandas as pd

from .base import ClassificationSchema, SchemaSpec, _resolve_asset


def default_spec() -> SchemaSpec:
    return SchemaSpec(
        name="three_level",
        display_name="3-Level Spend (Direct/Indirect)",
        description="Internal 3-level spend taxonomy used by the original bootstrap pipeline.",
        source_path="assets/categories.csv",
        code_column="category_level_3",
        label_column="category_level_3",
        level_columns=["category_level_1", "category_level_2", "category_level_3"],
        description_columns=["category_level_3_description"],
    )


class ThreeLevelSchema(ClassificationSchema):
    """Wraps assets/categories.csv."""

    def __init__(self, spec: SchemaSpec | None = None):
        super().__init__(spec or default_spec())

    def _load_taxonomy_df(self) -> pd.DataFrame:
        path = _resolve_asset(self.spec.source_path)
        return pd.read_csv(path)
