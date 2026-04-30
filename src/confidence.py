"""Unified confidence normalization.

Every prediction writer in the pipeline emits ``confidence`` into the
single ``cat_predictions.confidence`` column. Different sources produce
different raw signals — model self-reports, predict_proba, ts_rank —
so without a shared normalizer the column means different things in
different rows. This module is the *one* place to map raw signals to
[0, 1].

Sources:

  ai_classify     – SQL ``ai_classify`` doesn't return a confidence;
                    we synthesize one from "agreement with the next
                    candidate" if available, otherwise return ``None``.
                    Hierarchical decomposition reduces the value of a
                    self-reported confidence anyway.

  ai_query        – ai_query with structured ``confidence`` field.
                    Model is asked to report 1–5; we map (x-1)/4.

  llm_chat        – chat completions where the model returns a 1–5
                    confidence in JSON. Same (x-1)/4 mapping.

  retrieval_llm   – LLM picks from K candidates returned by tsvector;
                    self-reported 1–5 → (x-1)/4.

  catboost        – ``predict_proba.max()`` is already 0–1.

  tsvector        – ``ts_rank`` is unbounded and tiny. We blend three
                    signals (margin, strength, coverage); see
                    ``normalize_tsvector``.

  agent_review    – agent's 1–5 audit score, mapped (x-1)/4 (matches
                    other LLM sources). The original notebook used
                    /5.0 — kept as a fallback for legacy rows.
"""

from __future__ import annotations

from typing import Iterable, Optional


def _from_one_to_five(value: float) -> float:
    """Map a 1-5 self-reported scale to 0-1."""
    if value < 1:
        return 0.0
    if value > 5:
        return 1.0
    return round((value - 1.0) / 4.0, 4)


def normalize_llm_self_report(value: Optional[float]) -> Optional[float]:
    """For ai_query / llm_chat / retrieval_llm / agent_review."""
    if value is None:
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    if 0.0 <= x <= 1.0 and x not in (1.0,):
        # Already a probability — leave it.
        return round(x, 4)
    return _from_one_to_five(x)


def normalize_predict_proba(value: Optional[float]) -> Optional[float]:
    """For catboost / sklearn classifiers — already 0-1."""
    if value is None:
        return None
    try:
        x = float(value)
    except (TypeError, ValueError):
        return None
    return round(max(0.0, min(1.0, x)), 4)


def normalize_tsvector(scores: Iterable[float], n_query_tokens: int) -> float:
    """Map raw ts_rank scores into a calibrated 0-1 confidence.

    Three signals combine equally:
      margin   – (top1 - top2) / top1.  Captures discrimination.
      strength – soft-clipped absolute top1 via 1 - 1/(1+20·top1).
      coverage – min(1, n_query_tokens/4). Caps over-confidence on thin
                 invoice text.
    """
    score_list = list(scores)
    if not score_list:
        return 0.0
    top1 = float(score_list[0])
    top2 = float(score_list[1]) if len(score_list) > 1 else 0.0
    margin = (top1 - top2) / max(top1, 1e-9)
    strength = 1.0 - 1.0 / (1.0 + 20.0 * top1)
    coverage = min(1.0, max(0, n_query_tokens) / 4.0)
    return round((margin + strength + coverage) / 3.0, 4)


def normalize(source: str, raw: Optional[float], **ctx) -> Optional[float]:
    """Single-entry-point normalizer.

    ``ctx`` carries source-specific extras:
      * ``scores`` (list[float]) for ``tsvector``
      * ``n_query_tokens`` (int) for ``tsvector``

    Unknown sources fall back to the LLM self-report mapping (the most
    common shape) so callers never silently drop a value.
    """
    if source == "tsvector":
        return normalize_tsvector(ctx.get("scores", [raw] if raw is not None else []),
                                  ctx.get("n_query_tokens", 0))
    if source in ("catboost",):
        return normalize_predict_proba(raw)
    # ai_query / ai_classify (when synthesized) / llm_chat / retrieval_llm /
    # agent_review all use the LLM self-report scale.
    return normalize_llm_self_report(raw)
