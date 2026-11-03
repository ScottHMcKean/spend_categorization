"""Confidence lives on one 0-1 scale so the two classifiers compare.

Only two sources write predictions now:
  * the LLM self-reports 1-5  → `from_llm_1to5`
  * CatBoost gives predict_proba (already 0-1) → `from_proba`
"""

from __future__ import annotations

from typing import Optional


def from_llm_1to5(value: Optional[float]) -> Optional[float]:
    """Map the LLM's 1-5 self-rating to 0-1. Returns None if unparseable."""
    if value is None:
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    x = max(1.0, min(5.0, x))
    return round((x - 1.0) / 4.0, 4)


def from_proba(value: Optional[float]) -> Optional[float]:
    """Clamp a probability into 0-1. Returns None if unparseable."""
    if value is None:
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return round(max(0.0, min(1.0, x)), 4)
