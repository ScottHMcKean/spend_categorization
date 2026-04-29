"""Tools the review agent uses to interrogate any classification taxonomy.

Generic over schema depth: the toolset works on the normalized taxonomy
shape produced by ``ClassificationSchema.load_taxonomy()``:

    code         STRING   -- canonical leaf code
    label        STRING   -- short human label
    level_path   ARRAY<STRING>  -- ordered path from root to leaf
    description  STRING   (optional)

This means the same agent works for three_level (3 levels), gl_map
(1-5 levels), and UNSPSC (4 levels) without any schema-specific code.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

import pandas as pd


def _norm_path(value: Any) -> List[str]:
    """Coerce a level_path cell to a clean ``list[str]``."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if v is not None and str(v).strip()]
    # numpy arrays, tuples, etc.
    try:
        return [str(v).strip() for v in list(value) if v is not None and str(v).strip()]
    except TypeError:
        return []


@dataclass
class TaxonomyToolset:
    """Bundle of tools that operate on a normalized taxonomy DataFrame.

    Constructor performs a one-time normalization so per-call tool
    invocations stay cheap.
    """

    taxonomy: pd.DataFrame

    def __post_init__(self) -> None:
        df = self.taxonomy.copy()
        if "level_path" not in df.columns:
            raise ValueError("taxonomy DataFrame must contain a 'level_path' column")
        df["level_path"] = df["level_path"].apply(_norm_path)
        if "code" not in df.columns:
            df["code"] = df["level_path"].apply(lambda p: p[-1] if p else "")
        if "label" not in df.columns:
            df["label"] = df["code"]
        if "description" not in df.columns:
            df["description"] = ""
        df["code"] = df["code"].astype(str)
        df["label"] = df["label"].astype(str)
        df["description"] = df["description"].astype(str)
        # path_str — an ASCII-friendly path used for matching/display.
        df["_path_str"] = df["level_path"].apply(lambda p: " > ".join(p))
        self.taxonomy = df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Public tools
    # ------------------------------------------------------------------

    def list_root_categories(self) -> List[str]:
        """Distinct top-level (level_path[0]) values, sorted."""
        return sorted({p[0] for p in self.taxonomy["level_path"] if p})

    def list_children(self, parent_path: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Direct children of a given path.

        Each entry is ``{name, depth, leaf_count, is_leaf}``. When
        ``parent_path`` is empty, returns root categories.
        """
        parent = _norm_path(parent_path or [])
        depth = len(parent)
        rows = self.taxonomy
        if parent:
            mask = rows["level_path"].apply(
                lambda p: len(p) > depth and p[:depth] == parent
            )
            rows = rows[mask]
        seen: Dict[str, Dict[str, Any]] = {}
        for path in rows["level_path"]:
            if len(path) <= depth:
                continue
            child = path[depth]
            entry = seen.setdefault(
                child, {"name": child, "depth": depth + 1, "leaf_count": 0, "is_leaf": False}
            )
            entry["leaf_count"] += 1
            if len(path) == depth + 1:
                entry["is_leaf"] = True
        return sorted(seen.values(), key=lambda e: e["name"])

    def search_taxonomy(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Substring search across code, label, level_path, and description."""
        if not query:
            return []
        q = query.lower()
        df = self.taxonomy
        hay = (
            df["code"].str.lower()
            + " " + df["label"].str.lower()
            + " " + df["_path_str"].str.lower()
            + " " + df["description"].str.lower()
        )
        hits = df[hay.str.contains(q, regex=False, na=False)]
        out: List[Dict[str, Any]] = []
        for _, r in hits.head(limit).iterrows():
            out.append(
                {
                    "code": r["code"],
                    "label": r["label"],
                    "level_path": list(r["level_path"]),
                    "description": r["description"][:240] if r["description"] else "",
                }
            )
        return out

    def get_leaf(self, code: str) -> Dict[str, Any]:
        """Full record for a leaf code."""
        df = self.taxonomy
        hits = df[df["code"] == str(code)]
        if hits.empty:
            return {"found": False, "message": f"No leaf with code '{code}'"}
        r = hits.iloc[0]
        return {
            "found": True,
            "code": r["code"],
            "label": r["label"],
            "level_path": list(r["level_path"]),
            "description": r["description"],
        }

    def validate_path(self, level_path: List[str]) -> Dict[str, Any]:
        """Confirm an ordered path exists as a prefix or full leaf path."""
        path = _norm_path(level_path)
        if not path:
            return {"valid": False, "reason": "empty path"}
        df = self.taxonomy
        mask = df["level_path"].apply(
            lambda p: len(p) >= len(path) and p[: len(path)] == path
        )
        matches = df[mask]
        if matches.empty:
            return {"valid": False, "reason": f"path {path!r} not found"}
        leaves = matches[matches["level_path"].apply(lambda p: list(p) == path)]
        return {
            "valid": True,
            "is_leaf": not leaves.empty,
            "child_count": int(matches["level_path"].apply(lambda p: len(p) > len(path)).sum()),
        }


# ---------------------------------------------------------------------------
# Backwards-compatible alias for the original three-level toolset.
#
# The legacy notebook 4 + tests use ``HierarchyToolset`` with explicit
# ``category_level_*`` columns. We synthesize a level_path column from those
# columns and delegate to ``TaxonomyToolset`` so the same call surface
# (list_level1 / list_level2 / list_level3 / validate_path / search /
# get_category_description) keeps working.
# ---------------------------------------------------------------------------


@dataclass
class HierarchyToolset:
    """Three-level toolset retained for backwards compatibility."""

    hierarchy: pd.DataFrame

    def __post_init__(self) -> None:
        df = self.hierarchy.copy()
        if "level_path" not in df.columns:
            cols = [c for c in ("category_level_1", "category_level_2", "category_level_3") if c in df.columns]
            df["level_path"] = df.apply(
                lambda r: [str(r[c]).strip() for c in cols if pd.notna(r.get(c))], axis=1
            )
        if "code" not in df.columns and "category_level_3" in df.columns:
            df["code"] = df["category_level_3"].astype(str)
        if "label" not in df.columns and "category_level_3" in df.columns:
            df["label"] = df["category_level_3"].astype(str)
        if "description" not in df.columns and "category_level_3_description" in df.columns:
            df["description"] = df["category_level_3_description"].astype(str)
        self.hierarchy = df
        self._inner = TaxonomyToolset(taxonomy=df[["code", "label", "level_path", "description"]] if "description" in df.columns else df[["code", "label", "level_path"]])

    def list_level1(self) -> List[str]:
        return self._inner.list_root_categories()

    def list_level2(self, level_1: str) -> List[str]:
        children = self._inner.list_children([level_1])
        return [c["name"] for c in children]

    def list_level3(
        self, level_1: Optional[str] = None, level_2: Optional[str] = None
    ) -> List[Dict[str, str]]:
        df = self.hierarchy
        if level_1 and "category_level_1" in df.columns:
            df = df[df["category_level_1"] == level_1]
        if level_2 and "category_level_2" in df.columns:
            df = df[df["category_level_2"] == level_2]
        cols = [c for c in ("category_level_3", "category_level_3_description") if c in df.columns]
        if not cols:
            return []
        return df[cols].dropna().drop_duplicates().to_dict(orient="records")

    def get_category_description(self, level_3: str) -> Dict[str, Any]:
        df = self.hierarchy
        if "category_level_3" not in df.columns:
            return {"found": False, "message": "no category_level_3 column"}
        df = df[df["category_level_3"] == level_3]
        if df.empty:
            return {"found": False, "message": f"No category named '{level_3}'"}
        row = df.iloc[0]
        return {
            "found": True,
            "category_level_1": row.get("category_level_1", ""),
            "category_level_2": row.get("category_level_2", ""),
            "category_level_3": row.get("category_level_3", ""),
            "description": row.get("category_level_3_description", ""),
        }

    def search_categories(self, query: str, limit: int = 10) -> List[Dict[str, str]]:
        if not query:
            return []
        q = query.lower()
        df = self.hierarchy.copy()
        haystack_cols = [
            c
            for c in (
                "category_level_1",
                "category_level_2",
                "category_level_3",
                "category_level_3_description",
            )
            if c in df.columns
        ]
        if not haystack_cols:
            return []
        mask = pd.Series(False, index=df.index)
        for col in haystack_cols:
            mask = mask | df[col].fillna("").astype(str).str.lower().str.contains(q, regex=False)
        cols_out = [c for c in haystack_cols if c != "category_level_3_description"]
        cols_out = haystack_cols
        hits = df[mask][cols_out].dropna(subset=["category_level_3"]).drop_duplicates()
        return hits.head(limit).to_dict(orient="records")

    def validate_path(
        self, level_1: str, level_2: Optional[str] = None, level_3: Optional[str] = None
    ) -> Dict[str, Any]:
        df = self.hierarchy
        if "category_level_1" not in df.columns:
            return {"valid": False, "reason": "no category_level_1 column"}
        df = df[df["category_level_1"] == level_1]
        if df.empty:
            return {"valid": False, "reason": f"Level 1 '{level_1}' not found"}
        if level_2:
            df = df[df["category_level_2"] == level_2]
            if df.empty:
                return {
                    "valid": False,
                    "reason": f"Level 2 '{level_2}' not under Level 1 '{level_1}'",
                }
        if level_3:
            df = df[df["category_level_3"] == level_3]
            if df.empty:
                return {
                    "valid": False,
                    "reason": f"Level 3 '{level_3}' not under given parents",
                }
        return {"valid": True}


# ---------------------------------------------------------------------------
# Tool specs (OpenAI/Foundation Models function-calling shape)
# ---------------------------------------------------------------------------


def build_tool_specs() -> List[Dict[str, Any]]:
    """Three-level legacy specs (used by ``ClassificationReviewAgent``)."""
    return [
        {
            "type": "function",
            "function": {
                "name": "list_level1",
                "description": "List all top-level (L1) spend categories.",
                "parameters": {"type": "object", "properties": {}},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_level2",
                "description": "List Level 2 categories under a given Level 1.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "level_1": {"type": "string", "description": "Exact L1 category name"},
                    },
                    "required": ["level_1"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_level3",
                "description": (
                    "List Level 3 leaf categories (with descriptions). "
                    "Optionally filter by parent L1 and/or L2."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "level_1": {"type": "string"},
                        "level_2": {"type": "string"},
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_category_description",
                "description": "Look up the full description for a Level 3 category.",
                "parameters": {
                    "type": "object",
                    "properties": {"level_3": {"type": "string"}},
                    "required": ["level_3"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_categories",
                "description": (
                    "Free-text search across category names and descriptions. "
                    "Returns up to `limit` matching L3 rows with their parents."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "limit": {"type": "integer", "default": 10},
                    },
                    "required": ["query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "validate_path",
                "description": (
                    "Confirm that a (Level 1, Level 2, Level 3) tuple exists "
                    "in the hierarchy. Use this to verify your final answer."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "level_1": {"type": "string"},
                        "level_2": {"type": "string"},
                        "level_3": {"type": "string"},
                    },
                    "required": ["level_1"],
                },
            },
        },
    ]


def build_taxonomy_tool_specs() -> List[Dict[str, Any]]:
    """Generic specs for the level_path-native ``TaxonomyToolset``."""
    return [
        {
            "type": "function",
            "function": {
                "name": "list_root_categories",
                "description": "List all top-level (root) categories in the taxonomy.",
                "parameters": {"type": "object", "properties": {}},
            },
        },
        {
            "type": "function",
            "function": {
                "name": "list_children",
                "description": (
                    "List the direct children under a given path. "
                    "Each result has {name, depth, leaf_count, is_leaf}. "
                    "Pass an empty array to get root categories."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "parent_path": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Ordered path from root, e.g. ['Mining', 'Drills'].",
                        }
                    },
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_taxonomy",
                "description": (
                    "Free-text search across code, label, path, and description. "
                    "Use this when you don't know which subtree to drill into."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string"},
                        "limit": {"type": "integer", "default": 10},
                    },
                    "required": ["query"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_leaf",
                "description": "Look up full information for a specific leaf code.",
                "parameters": {
                    "type": "object",
                    "properties": {"code": {"type": "string"}},
                    "required": ["code"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "validate_path",
                "description": (
                    "Confirm an ordered path exists in the taxonomy. "
                    "Always validate before submitting your final answer."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "level_path": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Path from root to (or toward) leaf.",
                        }
                    },
                    "required": ["level_path"],
                },
            },
        },
    ]


def make_dispatcher(toolset: HierarchyToolset) -> Callable[[str, Dict[str, Any]], Any]:
    """Return a function that routes (tool_name, args) -> tool result.

    Routes both legacy ``HierarchyToolset`` calls and generic
    ``TaxonomyToolset`` calls so a single dispatcher works for either.
    """
    if isinstance(toolset, TaxonomyToolset):
        handlers: Dict[str, Callable[..., Any]] = {
            "list_root_categories": lambda **kw: toolset.list_root_categories(),
            "list_children": toolset.list_children,
            "search_taxonomy": toolset.search_taxonomy,
            "get_leaf": toolset.get_leaf,
            "validate_path": toolset.validate_path,
        }
    else:
        handlers = {
            "list_level1": lambda **kw: toolset.list_level1(),
            "list_level2": toolset.list_level2,
            "list_level3": toolset.list_level3,
            "get_category_description": toolset.get_category_description,
            "search_categories": toolset.search_categories,
            "validate_path": toolset.validate_path,
        }
        inner: Optional[TaxonomyToolset] = getattr(toolset, "_inner", None)
        if inner is not None:
            handlers.update(
                {
                    "list_root_categories": lambda **kw: inner.list_root_categories(),
                    "list_children": inner.list_children,
                    "search_taxonomy": inner.search_taxonomy,
                    "get_leaf": inner.get_leaf,
                }
            )

    def dispatch(name: str, args: Dict[str, Any]) -> Any:
        if name not in handlers:
            return {"error": f"unknown tool '{name}'"}
        try:
            return handlers[name](**(args or {}))
        except TypeError as e:
            return {"error": f"bad arguments for '{name}': {e}"}

    return dispatch
