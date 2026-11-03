"""Pluggable classification schemas (taxonomies).

A schema knows how to load its taxonomy. That's it — there is one classifier
for every schema, so schemas no longer declare a strategy.

Add a taxonomy: subclass ``ClassificationSchema``, then register it below.
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


def list_schemas() -> List[ClassificationSchema]:
    return list(_DEFAULT_REGISTRY.values())


def get_schema(name: str) -> ClassificationSchema:
    if name not in _DEFAULT_REGISTRY:
        raise KeyError(f"Unknown schema '{name}'. Known: {sorted(_DEFAULT_REGISTRY)}")
    return _DEFAULT_REGISTRY[name]


def register_schema(schema: ClassificationSchema) -> None:
    """Register a schema so the pipeline and app pick it up."""
    _DEFAULT_REGISTRY[schema.name] = schema


__all__ = [
    "ClassificationSchema",
    "SchemaSpec",
    "ThreeLevelSchema",
    "GLMapSchema",
    "UNSPSCSchema",
    "list_schemas",
    "get_schema",
    "register_schema",
]
