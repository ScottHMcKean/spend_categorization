"""Base abstraction for classification schemas (a.k.a. taxonomies).

A *schema* is a flat-or-hierarchical taxonomy invoices can be classified
against (3-level internal categories, GL accounts, UNSPSC commodities, ...).

Every schema produces rows in a single, schema-agnostic ``predictions``
table (see ``docs/DESIGN.md`` and notebook 0 for the full column list),
keyed on ``(order_id, schema_name, source)`` with ``source`` one of
``'llm'`` or ``'catboost'``.

A schema's only job is **load its taxonomy**. There is one classifier for
every schema (LLM grounded by pgvector retrieval, plus optional CatBoost),
so a schema no longer declares a strategy.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import pandas as pd


@dataclass(frozen=True)
class SchemaSpec:
    """Declarative metadata for a schema.

    ``top_k`` is how many similar examples the LLM classifier retrieves from
    pgvector for this taxonomy (bigger taxonomies may want a few more).
    """

    name: str
    display_name: str
    description: str = ""
    source_path: Optional[str] = None
    code_column: str = "code"
    label_column: str = "label"
    level_columns: List[str] = field(default_factory=list)
    description_columns: List[str] = field(default_factory=list)
    top_k: int = 20


class ClassificationSchema(ABC):
    """Abstract base every concrete schema inherits from."""

    spec: SchemaSpec

    def __init__(self, spec: SchemaSpec):
        self.spec = spec

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def display_name(self) -> str:
        return self.spec.display_name

    @abstractmethod
    def _load_taxonomy_df(self) -> pd.DataFrame:
        """Return the raw taxonomy as a DataFrame. Implementers must populate
        ``self.spec.code_column``, ``self.spec.label_column``, and
        ``self.spec.level_columns``.
        """

    def load_taxonomy(self) -> pd.DataFrame:
        """Return a normalized taxonomy DataFrame.

        Adds ``code``, ``label``, ``level_path`` columns (string-typed).
        Drops rows missing the code; deduplicates on code.
        """
        df = self._load_taxonomy_df().copy()
        df = df.dropna(subset=[self.spec.code_column])
        df["code"] = df[self.spec.code_column].astype(str).str.strip()
        df["label"] = df[self.spec.label_column].astype(str).str.strip()

        levels = self.spec.level_columns or [self.spec.code_column]

        def _row_path(row: pd.Series) -> List[str]:
            return [str(row[c]).strip() for c in levels if pd.notna(row.get(c))]

        df["level_path"] = df.apply(_row_path, axis=1)
        return df.drop_duplicates(subset=["code"]).reset_index(drop=True)


def _resolve_asset(path: str) -> Path:
    """Resolve an asset path: absolute, repo-root, or wheel-bundled."""
    p = Path(path)
    if p.is_absolute() and p.exists():
        return p
    here = Path(__file__).resolve().parent
    src_dir = here.parent
    candidates = [Path.cwd() / path, src_dir.parent / path, src_dir / path]
    for c in candidates:
        if c.exists():
            return c
    return p
