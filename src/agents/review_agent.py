"""Agent that reviews bootstrap classifications and suggests a final label.

The agent receives:
  - the invoice text
  - the existing bootstrap prediction (code/label/level_path)
  - tools to walk the category hierarchy

It must return a single JSON object via the ``submit_classification`` tool
with the suggested code/label/level_path, an integer confidence 1-5,
whether it agrees with the bootstrap, and a short rationale. Forcing a
final tool call gives us a structured, parse-safe answer.

Two flavors:
  * ``ClassificationReviewAgent`` -- legacy three-level (uses
    ``HierarchyToolset`` and ``build_tool_specs``); kept for
    backwards-compat with notebook 4 and existing tests.
  * ``TaxonomyReviewAgent`` -- generic over taxonomy depth; uses
    ``TaxonomyToolset`` and ``build_taxonomy_tool_specs``. Used by the
    in-app review endpoint so a single implementation handles
    three_level, gl_map, and UNSPSC.
"""

from __future__ import annotations

import concurrent.futures
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from .tools import (
    HierarchyToolset,
    TaxonomyToolset,
    build_tool_specs,
    build_taxonomy_tool_specs,
    make_dispatcher,
)

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = """You are a procurement spend-classification reviewer.

For each invoice you receive an existing bootstrap classification and you must
audit it against the official 3-level category hierarchy.

Workflow:
1. Read the invoice (supplier, description, cost centre, amount).
2. Use the hierarchy tools to inspect candidate categories. Always validate
   that any category you propose actually exists via `validate_path`.
3. Decide whether the bootstrap label is correct. If not, propose a better one.
4. Pick the most specific Level 3 category you can justify; if none clearly
   fits, you may leave Level 3 null.
5. Call `submit_classification` exactly once with your final answer.

Confidence scale (must be an integer 1-5):
  1 — cannot classify, defaulted to "Unknown"
  2 — could be several categories, low specificity
  3 — likely correct but rationale is weak
  4 — confident in the choice with a clear rationale
  5 — exact, validated match supported by hierarchy lookups

Be terse. Do not produce free-form prose; only call tools and finally
`submit_classification`."""


SUBMIT_TOOL = {
    "type": "function",
    "function": {
        "name": "submit_classification",
        "description": "Submit your final reviewed classification. Call exactly once.",
        "parameters": {
            "type": "object",
            "properties": {
                "suggested_level_1": {"type": "string"},
                "suggested_level_2": {"type": "string"},
                "suggested_level_3": {
                    "type": "string",
                    "description": "Most specific category, or empty string if not determinable.",
                },
                "confidence": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 5,
                },
                "agrees_with_bootstrap": {"type": "boolean"},
                "rationale": {
                    "type": "string",
                    "description": "1-2 sentences. No preamble.",
                },
            },
            "required": [
                "suggested_level_1",
                "suggested_level_2",
                "confidence",
                "agrees_with_bootstrap",
                "rationale",
            ],
        },
    },
}


GENERIC_SYSTEM_PROMPT = """You are a procurement spend-classification reviewer.

For each invoice you receive an existing bootstrap classification and you must
audit it against an official taxonomy. The taxonomy may have any number of
levels — call the tools to discover the structure.

Workflow:
1. Read the invoice (supplier, description, cost centre, amount) AND the
   bootstrap classification's level_path (which tells you the taxonomy's
   shape and language — e.g., GL accounts use accounting language like
   "G&A / Contractors / Contract Labour", UNSPSC uses commodity titles).
2. Use ``get_leaf`` on the bootstrap code first to understand the
   taxonomy's terminology. If the bootstrap is plausible, confirm and
   submit early — don't drill the whole tree.
3. If bootstrap looks wrong, search using terms from the BOOTSTRAP's
   level_path (e.g. for GL: "Contract Labour", "Contractors") rather than
   raw invoice text — taxonomy labels rarely match invoice descriptions
   verbatim.
4. Use `validate_path` to confirm any new candidate before submitting.
5. Call `submit_classification` exactly once. Budget your tool calls —
   you have at most 12 steps total. If you can't find a clearly better
   answer in a few searches, AGREE with the bootstrap and submit.

Confidence scale (integer 1-5):
  1 — cannot classify
  2 — could be several leaves, low specificity
  3 — likely correct but rationale is weak
  4 — confident in the choice with a clear rationale
  5 — exact, validated match supported by lookups

Be terse. Do not produce free-form prose; only call tools and finally
`submit_classification`."""


GENERIC_SUBMIT_TOOL = {
    "type": "function",
    "function": {
        "name": "submit_classification",
        "description": "Submit your final reviewed classification. Call exactly once.",
        "parameters": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "Canonical leaf code from the taxonomy.",
                },
                "label": {
                    "type": "string",
                    "description": "Short human label for the leaf.",
                },
                "level_path": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Full ordered path from root to leaf.",
                },
                "confidence": {"type": "integer", "minimum": 1, "maximum": 5},
                "agrees_with_bootstrap": {"type": "boolean"},
                "rationale": {
                    "type": "string",
                    "description": "1-2 sentences. No preamble.",
                },
            },
            "required": [
                "code",
                "label",
                "level_path",
                "confidence",
                "agrees_with_bootstrap",
                "rationale",
            ],
        },
    },
}


@dataclass
class ReviewResult:
    order_id: str
    bootstrap_level_1: Optional[str]
    bootstrap_level_2: Optional[str]
    bootstrap_confidence: Optional[float]
    suggested_level_1: Optional[str]
    suggested_level_2: Optional[str]
    suggested_level_3: Optional[str]
    agent_confidence: Optional[int]
    agrees_with_bootstrap: Optional[bool]
    rationale: str = ""
    tool_calls: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None
    latency_seconds: float = 0.0
    # Generic-taxonomy fields (populated only by TaxonomyReviewAgent).
    suggested_code: Optional[str] = None
    suggested_label: Optional[str] = None
    suggested_level_path: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["tool_calls"] = json.dumps(self.tool_calls)
        return d


class ClassificationReviewAgent:
    """Single-row agent loop. Stateless across rows."""

    def __init__(
        self,
        openai_client: Any,
        model: str,
        toolset: HierarchyToolset,
        max_steps: int = 8,
        temperature: float = 0.0,
    ):
        self.client = openai_client
        self.model = model
        self.toolset = toolset
        self.dispatch = make_dispatcher(toolset)
        self.tool_specs = build_tool_specs() + [SUBMIT_TOOL]
        self.max_steps = max_steps
        self.temperature = temperature

    def review(
        self,
        order_id: str,
        invoice_text: str,
        bootstrap_l1: Optional[str],
        bootstrap_l2: Optional[str],
        bootstrap_confidence: Optional[float],
        bootstrap_rationale: Optional[str] = None,
    ) -> ReviewResult:
        started = time.time()
        user_msg = self._format_user_message(
            invoice_text,
            bootstrap_l1,
            bootstrap_l2,
            bootstrap_confidence,
            bootstrap_rationale,
        )
        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ]

        result = ReviewResult(
            order_id=order_id,
            bootstrap_level_1=bootstrap_l1,
            bootstrap_level_2=bootstrap_l2,
            bootstrap_confidence=bootstrap_confidence,
            suggested_level_1=None,
            suggested_level_2=None,
            suggested_level_3=None,
            agent_confidence=None,
            agrees_with_bootstrap=None,
        )

        try:
            for _ in range(self.max_steps):
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    tools=self.tool_specs,
                    temperature=self.temperature,
                )
                msg = resp.choices[0].message
                tool_calls = getattr(msg, "tool_calls", None) or []

                if not tool_calls:
                    result.error = "agent_returned_no_tool_call"
                    break

                messages.append(
                    {
                        "role": "assistant",
                        "content": msg.content or "",
                        "tool_calls": [
                            {
                                "id": tc.id,
                                "type": "function",
                                "function": {
                                    "name": tc.function.name,
                                    "arguments": tc.function.arguments,
                                },
                            }
                            for tc in tool_calls
                        ],
                    }
                )

                submitted = False
                for tc in tool_calls:
                    name = tc.function.name
                    try:
                        args = json.loads(tc.function.arguments or "{}")
                    except json.JSONDecodeError:
                        args = {}

                    if name == "submit_classification":
                        result.suggested_level_1 = args.get("suggested_level_1")
                        result.suggested_level_2 = args.get("suggested_level_2")
                        l3 = args.get("suggested_level_3") or None
                        result.suggested_level_3 = l3 if l3 else None
                        result.agent_confidence = args.get("confidence")
                        result.agrees_with_bootstrap = args.get("agrees_with_bootstrap")
                        result.rationale = args.get("rationale", "")
                        result.tool_calls.append({"name": name, "args": args})
                        submitted = True
                        break

                    tool_result = self.dispatch(name, args)
                    result.tool_calls.append({"name": name, "args": args, "result": tool_result})
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "name": name,
                            "content": json.dumps(tool_result, default=str),
                        }
                    )

                if submitted:
                    break
            else:
                result.error = "max_steps_exceeded"
        except Exception as e:  # noqa: BLE001 — surface every failure as a row
            logger.exception("agent review failed for %s", order_id)
            result.error = f"{type(e).__name__}: {e}"

        result.latency_seconds = round(time.time() - started, 3)
        return result

    @staticmethod
    def _format_user_message(
        invoice_text: str,
        bootstrap_l1: Optional[str],
        bootstrap_l2: Optional[str],
        bootstrap_confidence: Optional[float],
        bootstrap_rationale: Optional[str],
    ) -> str:
        parts = [
            "## Invoice",
            invoice_text.strip(),
            "",
            "## Existing Bootstrap Classification",
            f"- Level 1: {bootstrap_l1 or 'Unknown'}",
            f"- Level 2: {bootstrap_l2 or 'Unknown'}",
            f"- Confidence (1-5): {bootstrap_confidence}",
        ]
        if bootstrap_rationale:
            parts.append(f"- Rationale: {bootstrap_rationale}")
        parts.append("")
        parts.append("Audit this classification and submit your final answer.")
        return "\n".join(parts)


class TaxonomyReviewAgent:
    """Generic-taxonomy review agent. Works for any schema depth."""

    def __init__(
        self,
        openai_client: Any,
        model: str,
        toolset: TaxonomyToolset,
        schema_display_name: str = "the taxonomy",
        max_steps: int = 8,
        temperature: float = 0.0,
    ):
        self.client = openai_client
        self.model = model
        self.toolset = toolset
        self.dispatch = make_dispatcher(toolset)
        self.tool_specs = build_taxonomy_tool_specs() + [GENERIC_SUBMIT_TOOL]
        self.schema_display_name = schema_display_name
        self.max_steps = max_steps
        self.temperature = temperature

    def review(
        self,
        order_id: str,
        invoice_text: str,
        bootstrap_code: Optional[str],
        bootstrap_label: Optional[str],
        bootstrap_level_path: Optional[List[str]],
        bootstrap_confidence: Optional[float],
        bootstrap_rationale: Optional[str] = None,
    ) -> ReviewResult:
        started = time.time()
        path = list(bootstrap_level_path or [])
        system = GENERIC_SYSTEM_PROMPT.replace(
            "an official taxonomy", f"the official {self.schema_display_name} taxonomy"
        )
        user_msg = self._format_user_message(
            invoice_text,
            bootstrap_code,
            bootstrap_label,
            path,
            bootstrap_confidence,
            bootstrap_rationale,
        )
        messages: List[Dict[str, Any]] = [
            {"role": "system", "content": system},
            {"role": "user", "content": user_msg},
        ]

        result = ReviewResult(
            order_id=order_id,
            bootstrap_level_1=path[0] if len(path) > 0 else None,
            bootstrap_level_2=path[1] if len(path) > 1 else None,
            bootstrap_confidence=bootstrap_confidence,
            suggested_level_1=None,
            suggested_level_2=None,
            suggested_level_3=None,
            agent_confidence=None,
            agrees_with_bootstrap=None,
        )

        try:
            for _ in range(self.max_steps):
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    tools=self.tool_specs,
                    temperature=self.temperature,
                )
                msg = resp.choices[0].message
                tool_calls = getattr(msg, "tool_calls", None) or []

                if not tool_calls:
                    result.error = "agent_returned_no_tool_call"
                    break

                messages.append(
                    {
                        "role": "assistant",
                        "content": msg.content or "",
                        "tool_calls": [
                            {
                                "id": tc.id,
                                "type": "function",
                                "function": {
                                    "name": tc.function.name,
                                    "arguments": tc.function.arguments,
                                },
                            }
                            for tc in tool_calls
                        ],
                    }
                )

                submitted = False
                for tc in tool_calls:
                    name = tc.function.name
                    try:
                        args = json.loads(tc.function.arguments or "{}")
                    except json.JSONDecodeError:
                        args = {}

                    if name == "submit_classification":
                        suggested_path = list(args.get("level_path") or [])
                        result.suggested_code = args.get("code") or None
                        result.suggested_label = args.get("label") or None
                        result.suggested_level_path = suggested_path
                        result.suggested_level_1 = suggested_path[0] if suggested_path else None
                        result.suggested_level_2 = suggested_path[1] if len(suggested_path) > 1 else None
                        # The deepest non-root level is the most-specific reviewed pick.
                        result.suggested_level_3 = (
                            suggested_path[-1]
                            if len(suggested_path) > 2
                            else (suggested_path[1] if len(suggested_path) > 1 else None)
                        )
                        result.agent_confidence = args.get("confidence")
                        result.agrees_with_bootstrap = args.get("agrees_with_bootstrap")
                        result.rationale = args.get("rationale", "")
                        result.tool_calls.append({"name": name, "args": args})
                        submitted = True
                        break

                    tool_result = self.dispatch(name, args)
                    result.tool_calls.append({"name": name, "args": args, "result": tool_result})
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tc.id,
                            "name": name,
                            "content": json.dumps(tool_result, default=str),
                        }
                    )

                if submitted:
                    break
            else:
                # Step budget exhausted without an explicit submit. If we
                # have a bootstrap to fall back on, treat that as "agree"
                # at low confidence so the UI gets a usable answer.
                if bootstrap_code:
                    result.suggested_code = bootstrap_code
                    result.suggested_label = bootstrap_label
                    result.suggested_level_path = list(path)
                    result.suggested_level_1 = path[0] if path else None
                    result.suggested_level_2 = path[1] if len(path) > 1 else None
                    result.suggested_level_3 = (
                        path[-1] if len(path) > 2 else (path[1] if len(path) > 1 else None)
                    )
                    result.agent_confidence = 2
                    result.agrees_with_bootstrap = True
                    result.rationale = (
                        "Step budget exhausted; defaulting to the bootstrap classification."
                    )
                    result.error = "max_steps_exceeded_fallback_to_bootstrap"
                else:
                    result.error = "max_steps_exceeded"
        except Exception as e:  # noqa: BLE001
            logger.exception("agent review failed for %s", order_id)
            result.error = f"{type(e).__name__}: {e}"

        result.latency_seconds = round(time.time() - started, 3)
        return result

    @staticmethod
    def _format_user_message(
        invoice_text: str,
        bootstrap_code: Optional[str],
        bootstrap_label: Optional[str],
        bootstrap_path: List[str],
        bootstrap_confidence: Optional[float],
        bootstrap_rationale: Optional[str],
    ) -> str:
        path_str = " > ".join(bootstrap_path) if bootstrap_path else "(none)"
        parts = [
            "## Invoice",
            invoice_text.strip(),
            "",
            "## Existing Bootstrap Classification",
            f"- Code: {bootstrap_code or '(none)'}",
            f"- Label: {bootstrap_label or '(none)'}",
            f"- Path: {path_str}",
            f"- Confidence: {bootstrap_confidence}",
        ]
        if bootstrap_rationale:
            parts.append(f"- Rationale: {bootstrap_rationale}")
        parts.append("")
        parts.append("Audit this classification and submit your final answer.")
        return "\n".join(parts)


def run_agent_review(
    rows: pd.DataFrame,
    agent: ClassificationReviewAgent,
    invoice_text_col: str = "invoice_info",
    order_id_col: str = "order_id",
    bootstrap_l1_col: str = "pred_level_1",
    bootstrap_l2_col: str = "pred_level_2",
    bootstrap_conf_col: str = "confidence",
    bootstrap_rationale_col: str = "rationale",
    max_workers: int = 8,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> pd.DataFrame:
    """Review every row in `rows` in parallel; return a DataFrame of results."""
    work = rows.to_dict(orient="records")

    def _one(row: Dict[str, Any]) -> ReviewResult:
        return agent.review(
            order_id=str(row.get(order_id_col, "")),
            invoice_text=str(row.get(invoice_text_col, "")),
            bootstrap_l1=row.get(bootstrap_l1_col),
            bootstrap_l2=row.get(bootstrap_l2_col),
            bootstrap_confidence=row.get(bootstrap_conf_col),
            bootstrap_rationale=row.get(bootstrap_rationale_col),
        )

    results: List[ReviewResult] = []
    total = len(work)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_one, r) for r in work]
        for i, fut in enumerate(concurrent.futures.as_completed(futures), 1):
            results.append(fut.result())
            if progress_cb is not None:
                progress_cb(i, total)

    return pd.DataFrame([r.to_dict() for r in results])
