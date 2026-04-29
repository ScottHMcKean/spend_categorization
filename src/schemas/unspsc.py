"""UNSPSC commodity codes (large, ~150k leaves -- vector search only)."""

from __future__ import annotations

import pandas as pd

from .base import ClassificationSchema, SchemaSpec, _resolve_asset


def default_spec() -> SchemaSpec:
    return SchemaSpec(
        name="unspsc",
        display_name="UNSPSC Commodity Codes",
        description="UN Standard Products and Services Code, ~150k commodity-level codes. Hybrid pgvector + tsvector search.",
        source_path="assets/unspsc-english-v260801.1.xlsx",
        code_column="Commodity",
        label_column="Commodity Title",
        level_columns=["Segment Title", "Family Title", "Class Title", "Commodity Title"],
        description_columns=["Commodity Definition", "Class Definition", "Synonym"],
    )


class UNSPSCSchema(ClassificationSchema):
    """Wraps the UN UNSPSC export.

    The spreadsheet has 12 preamble rows; the real header is on row 12.
    Drops rows without a commodity-level code.
    """

    def __init__(self, spec: SchemaSpec | None = None):
        super().__init__(spec or default_spec())

    def _load_taxonomy_df(self) -> pd.DataFrame:
        path = _resolve_asset(self.spec.source_path)
        df = pd.read_excel(path, sheet_name="Sheet1", header=12)
        df = df[df["Version"].astype(str).str.startswith("UN")]
        df = df[df["Commodity"].notna()].copy()
        df["Commodity"] = df["Commodity"].astype("Int64").astype(str)
        for col in [
            "Commodity Title",
            "Commodity Definition",
            "Segment Title",
            "Family Title",
            "Class Title",
            "Class Definition",
            "Synonym",
        ]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.strip()
        return df.reset_index(drop=True)
