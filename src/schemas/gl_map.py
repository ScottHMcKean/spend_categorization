"""GL account mapping (small, hierarchical, ~292 leaves)."""

from __future__ import annotations

import pandas as pd

from .base import ClassificationSchema, SchemaSpec, _resolve_asset


def default_spec() -> SchemaSpec:
    return SchemaSpec(
        name="gl_map",
        display_name="GL Account Map",
        description="Finance-side GL account hierarchy from new_gl_map.xlsx.",
        source_path="assets/new_gl_map.xlsx",
        code_column="GL Account",
        label_column="objval_desc",
        level_columns=["nodedesc02", "Level 2", "Level 3", "Level 4", "Level 5"],
        description_columns=[],
    )


class GLMapSchema(ClassificationSchema):
    """Wraps assets/new_gl_map.xlsx (sheet 'GLMap')."""

    def __init__(self, spec: SchemaSpec | None = None):
        super().__init__(spec or default_spec())

    def _load_taxonomy_df(self) -> pd.DataFrame:
        path = _resolve_asset(self.spec.source_path)
        df = pd.read_excel(path, sheet_name="GLMap")
        # Cast the GL account to a clean string key.
        df["GL Account"] = df["GL Account"].astype(str).str.strip()
        df["objval_desc"] = df["objval_desc"].fillna("").astype(str).str.strip()
        return df
