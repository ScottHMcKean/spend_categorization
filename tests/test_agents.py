"""Tests for the agent toolset and dispatcher."""

from __future__ import annotations

import json
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pandas as pd
import pytest

from src.agents.review_agent import ClassificationReviewAgent, TaxonomyReviewAgent
from src.agents.tools import (
    HierarchyToolset,
    TaxonomyToolset,
    build_tool_specs,
    build_taxonomy_tool_specs,
    make_dispatcher,
)


@pytest.fixture
def hierarchy() -> pd.DataFrame:
    return pd.DataFrame(
        [
            ("Direct", "Raw Materials", "Steel plate", "Heavy gauge steel"),
            ("Direct", "Raw Materials", "Aluminum ingot", "Raw aluminum for casting"),
            ("Direct", "Components", "Bearings", "Tapered roller bearings"),
            ("Indirect", "IT", "Laptops", "Office laptops"),
            ("Indirect", "IT", "SaaS subscriptions", "Software-as-a-service"),
        ],
        columns=[
            "category_level_1",
            "category_level_2",
            "category_level_3",
            "category_level_3_description",
        ],
    )


@pytest.fixture
def toolset(hierarchy: pd.DataFrame) -> HierarchyToolset:
    return HierarchyToolset(hierarchy=hierarchy)


def test_list_level1(toolset: HierarchyToolset) -> None:
    assert toolset.list_level1() == ["Direct", "Indirect"]


def test_list_level2(toolset: HierarchyToolset) -> None:
    assert toolset.list_level2("Direct") == ["Components", "Raw Materials"]
    assert toolset.list_level2("Missing") == []


def test_list_level3(toolset: HierarchyToolset) -> None:
    out = toolset.list_level3(level_1="Indirect")
    names = sorted(r["category_level_3"] for r in out)
    assert names == ["Laptops", "SaaS subscriptions"]


def test_validate_path(toolset: HierarchyToolset) -> None:
    assert toolset.validate_path("Direct", "Raw Materials", "Steel plate")["valid"]
    bad = toolset.validate_path("Direct", "Raw Materials", "Servers")
    assert not bad["valid"] and "Servers" in bad["reason"]
    assert not toolset.validate_path("Direct", "IT")["valid"]


def test_search_categories(toolset: HierarchyToolset) -> None:
    out = toolset.search_categories("aluminum")
    assert any(r["category_level_3"] == "Aluminum ingot" for r in out)
    assert toolset.search_categories("") == []


def test_get_category_description(toolset: HierarchyToolset) -> None:
    out = toolset.get_category_description("Bearings")
    assert out["found"] is True
    assert out["category_level_2"] == "Components"
    assert toolset.get_category_description("Nope")["found"] is False


def test_dispatcher_routes_known(toolset: HierarchyToolset) -> None:
    dispatch = make_dispatcher(toolset)
    out = dispatch("list_level2", {"level_1": "Direct"})
    assert "Raw Materials" in out


def test_dispatcher_unknown_tool(toolset: HierarchyToolset) -> None:
    dispatch = make_dispatcher(toolset)
    assert dispatch("does_not_exist", {})["error"].startswith("unknown tool")


def test_dispatcher_bad_args(toolset: HierarchyToolset) -> None:
    dispatch = make_dispatcher(toolset)
    assert "error" in dispatch("get_category_description", {"wrong_arg": "x"})


def test_tool_specs_include_submit_in_agent(toolset: HierarchyToolset) -> None:
    specs = build_tool_specs()
    names = {t["function"]["name"] for t in specs}
    assert {
        "list_level1",
        "list_level2",
        "list_level3",
        "get_category_description",
        "search_categories",
        "validate_path",
    } <= names


def _fake_completion(tool_name: str, arguments: Dict[str, Any]) -> Any:
    """Build a fake OpenAI ChatCompletion-like response with one tool call."""
    msg = MagicMock()
    msg.content = ""
    tc = MagicMock()
    tc.id = "call_1"
    tc.function = MagicMock()
    tc.function.name = tool_name
    tc.function.arguments = json.dumps(arguments)
    msg.tool_calls = [tc]
    choice = MagicMock()
    choice.message = msg
    resp = MagicMock()
    resp.choices = [choice]
    return resp


def test_agent_review_loops_until_submit(toolset: HierarchyToolset) -> None:
    """Agent should call hierarchy tools then submit a final classification."""
    scripted: List[Any] = [
        _fake_completion("list_level1", {}),
        _fake_completion("list_level2", {"level_1": "Direct"}),
        _fake_completion(
            "submit_classification",
            {
                "suggested_level_1": "Direct",
                "suggested_level_2": "Raw Materials",
                "suggested_level_3": "Steel plate",
                "confidence": 4,
                "agrees_with_bootstrap": True,
                "rationale": "Description matches Steel plate exactly.",
            },
        ),
    ]

    client = MagicMock()
    client.chat.completions.create.side_effect = scripted

    agent = ClassificationReviewAgent(
        openai_client=client, model="fake", toolset=toolset, max_steps=5
    )

    result = agent.review(
        order_id="ORD-1",
        invoice_text="Heavy gauge steel sheets for tower",
        bootstrap_l1="Direct",
        bootstrap_l2="Raw Materials",
        bootstrap_confidence=3.0,
    )

    assert result.error is None
    assert result.suggested_level_1 == "Direct"
    assert result.suggested_level_3 == "Steel plate"
    assert result.agent_confidence == 4
    assert result.agrees_with_bootstrap is True
    # Three calls scripted; agent should have made exactly that many.
    assert client.chat.completions.create.call_count == 3
    # Two non-submit tools were dispatched.
    dispatched_names = [tc["name"] for tc in result.tool_calls]
    assert dispatched_names[:2] == ["list_level1", "list_level2"]
    assert dispatched_names[-1] == "submit_classification"


@pytest.fixture
def unspsc_toolset() -> TaxonomyToolset:
    """Mini UNSPSC-shaped fixture (4 levels, leaf is 8-digit code)."""
    rows = [
        ("23000000", "Mining and Well Drilling Machinery and Accessories",
         ["Industrial Manufacturing", "Mining", "Drilling", "Mining and Well Drilling"]),
        ("23131502", "Auger drills",
         ["Industrial Manufacturing", "Mining", "Drilling rigs and equipment", "Auger drills"]),
        ("23131503", "Rotary drills",
         ["Industrial Manufacturing", "Mining", "Drilling rigs and equipment", "Rotary drills"]),
        ("44103105", "Laser printers",
         ["Office Equipment", "Printers", "Computer printers", "Laser printers"]),
    ]
    return TaxonomyToolset(
        taxonomy=pd.DataFrame(rows, columns=["code", "label", "level_path"])
    )


def test_taxonomy_list_root(unspsc_toolset: TaxonomyToolset) -> None:
    roots = unspsc_toolset.list_root_categories()
    assert roots == ["Industrial Manufacturing", "Office Equipment"]


def test_taxonomy_list_children(unspsc_toolset: TaxonomyToolset) -> None:
    out = unspsc_toolset.list_children(["Industrial Manufacturing", "Mining"])
    names = sorted(c["name"] for c in out)
    assert names == ["Drilling", "Drilling rigs and equipment"]


def test_taxonomy_search(unspsc_toolset: TaxonomyToolset) -> None:
    hits = unspsc_toolset.search_taxonomy("auger")
    assert any(h["code"] == "23131502" for h in hits)


def test_taxonomy_get_leaf(unspsc_toolset: TaxonomyToolset) -> None:
    out = unspsc_toolset.get_leaf("23131502")
    assert out["found"] is True
    assert out["label"] == "Auger drills"


def test_taxonomy_validate_path(unspsc_toolset: TaxonomyToolset) -> None:
    valid_full = unspsc_toolset.validate_path([
        "Industrial Manufacturing", "Mining", "Drilling rigs and equipment", "Auger drills",
    ])
    assert valid_full["valid"] and valid_full["is_leaf"]
    valid_prefix = unspsc_toolset.validate_path(["Industrial Manufacturing"])
    assert valid_prefix["valid"] and not valid_prefix["is_leaf"]
    invalid = unspsc_toolset.validate_path(["Industrial Manufacturing", "Bogus"])
    assert not invalid["valid"]


def test_taxonomy_tool_specs_shape() -> None:
    specs = build_taxonomy_tool_specs()
    names = {s["function"]["name"] for s in specs}
    assert names == {
        "list_root_categories",
        "list_children",
        "search_taxonomy",
        "get_leaf",
        "validate_path",
    }


def test_taxonomy_dispatcher(unspsc_toolset: TaxonomyToolset) -> None:
    dispatch = make_dispatcher(unspsc_toolset)
    out = dispatch("list_root_categories", {})
    assert "Industrial Manufacturing" in out


def test_taxonomy_review_agent_loops_until_submit(
    unspsc_toolset: TaxonomyToolset,
) -> None:
    scripted: List[Any] = [
        _fake_completion("list_root_categories", {}),
        _fake_completion(
            "search_taxonomy", {"query": "auger"},
        ),
        _fake_completion(
            "submit_classification",
            {
                "code": "23131502",
                "label": "Auger drills",
                "level_path": [
                    "Industrial Manufacturing",
                    "Mining",
                    "Drilling rigs and equipment",
                    "Auger drills",
                ],
                "confidence": 5,
                "agrees_with_bootstrap": False,
                "rationale": "Auger drill rental fits exactly.",
            },
        ),
    ]

    client = MagicMock()
    client.chat.completions.create.side_effect = scripted

    agent = TaxonomyReviewAgent(
        openai_client=client,
        model="fake",
        toolset=unspsc_toolset,
        schema_display_name="UNSPSC",
        max_steps=5,
    )

    result = agent.review(
        order_id="ORD-9",
        invoice_text="Auger drill rental for site survey",
        bootstrap_code="23131503",
        bootstrap_label="Rotary drills",
        bootstrap_level_path=[
            "Industrial Manufacturing",
            "Mining",
            "Drilling rigs and equipment",
            "Rotary drills",
        ],
        bootstrap_confidence=2.0,
    )

    assert result.error is None
    assert result.suggested_code == "23131502"
    assert result.suggested_label == "Auger drills"
    assert result.suggested_level_path == [
        "Industrial Manufacturing",
        "Mining",
        "Drilling rigs and equipment",
        "Auger drills",
    ]
    assert result.suggested_level_3 == "Auger drills"
    assert result.agent_confidence == 5
    assert result.agrees_with_bootstrap is False


def test_agent_review_handles_no_tool_call(toolset: HierarchyToolset) -> None:
    msg = MagicMock()
    msg.content = "I'm done"
    msg.tool_calls = []
    choice = MagicMock()
    choice.message = msg
    resp = MagicMock()
    resp.choices = [choice]

    client = MagicMock()
    client.chat.completions.create.return_value = resp

    agent = ClassificationReviewAgent(
        openai_client=client, model="fake", toolset=toolset
    )
    result = agent.review("ORD-2", "Some invoice", "Direct", "Raw Materials", 2.0)
    assert result.error == "agent_returned_no_tool_call"
    assert result.suggested_level_1 is None
