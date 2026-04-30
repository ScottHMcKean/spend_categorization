"""Pluggable classification schemas (taxonomies).

A schema knows how to load its taxonomy AND declares its preferred
classification strategy via ``SchemaSpec.classify_strategy``. The notebooks
read the strategy off the spec rather than maintaining a separate dict.

The ``STRATEGY`` mapping is kept as a derived view for backwards
compatibility with code (and the app) that still imports it.
"""

from __future__ import annotations

from typing import Dict, List

from .base import ClassificationSchema, SchemaSpec
from .gl_map import GLMapSchema
from .three_level import ThreeLevelSchema
from .unspsc import UNSPSCSchema


_DEFAULT_REGISTRY: Dict[str, ClassificationSchema] = {
    "three_level": ThreeLevelSchema(),
    "gl_map": GLMapSchema(),
    "unspsc": UNSPSCSchema(),
}


def _strategy_view() -> Dict[str, str]:
    return {name: schema.spec.classify_strategy for name, schema in _DEFAULT_REGISTRY.items()}


# Module-level alias kept for back-compat. The schema-spec field is the
# source of truth — mutating this dict has no effect on runtime behavior.
STRATEGY: Dict[str, str] = _strategy_view()


def list_schemas() -> List[ClassificationSchema]:
    return list(_DEFAULT_REGISTRY.values())


def get_schema(name: str) -> ClassificationSchema:
    if name not in _DEFAULT_REGISTRY:
        raise KeyError(f"Unknown schema '{name}'. Known: {sorted(_DEFAULT_REGISTRY)}")
    return _DEFAULT_REGISTRY[name]


def register_schema(schema: ClassificationSchema, strategy: str | None = None) -> None:
    """Register a schema. If ``strategy`` is provided it overrides the
    schema's spec; otherwise the spec's ``classify_strategy`` is used."""
    if strategy is not None:
        # Replace the spec with one carrying the override.
        from dataclasses import replace
        schema.spec = replace(schema.spec, classify_strategy=strategy)
    _DEFAULT_REGISTRY[schema.name] = schema
    STRATEGY[schema.name] = schema.spec.classify_strategy


__all__ = [
    "ClassificationSchema",
    "SchemaSpec",
    "ThreeLevelSchema",
    "GLMapSchema",
    "UNSPSCSchema",
    "STRATEGY",
    "list_schemas",
    "get_schema",
    "register_schema",
]
