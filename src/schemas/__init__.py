"""Pluggable classification schemas (taxonomies).

A schema knows how to load its taxonomy. The classification *strategy* is
chosen by the runner based on the leaf count or via the STRATEGY map below.
"""

from __future__ import annotations

from typing import Dict, List

from .base import ClassificationSchema, SchemaSpec
from .gl_map import GLMapSchema
from .three_level import ThreeLevelSchema
from .unspsc import UNSPSCSchema


# Strategy is a property of the *runner*, not the schema. Runners look up
# their schema's name here. Override at runtime as needed.
STRATEGY: Dict[str, str] = {
    "three_level": "ai_query",
    "gl_map": "ai_query",
    "unspsc": "pgvector",
}


_DEFAULT_REGISTRY: Dict[str, ClassificationSchema] = {
    "three_level": ThreeLevelSchema(),
    "gl_map": GLMapSchema(),
    "unspsc": UNSPSCSchema(),
}


def list_schemas() -> List[ClassificationSchema]:
    return list(_DEFAULT_REGISTRY.values())


def get_schema(name: str) -> ClassificationSchema:
    if name not in _DEFAULT_REGISTRY:
        raise KeyError(f"Unknown schema '{name}'. Known: {sorted(_DEFAULT_REGISTRY)}")
    return _DEFAULT_REGISTRY[name]


def register_schema(schema: ClassificationSchema, strategy: str = "ai_classify") -> None:
    _DEFAULT_REGISTRY[schema.name] = schema
    STRATEGY[schema.name] = strategy


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
