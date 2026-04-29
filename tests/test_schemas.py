"""Schema interface smoke tests using bundled assets."""

from __future__ import annotations

import pandas as pd
import pytest

from src.schemas import (
    GLMapSchema,
    STRATEGY,
    ThreeLevelSchema,
    UNSPSCSchema,
    get_schema,
    list_schemas,
)


@pytest.fixture(scope="module")
def three_level() -> ThreeLevelSchema:
    return ThreeLevelSchema()


@pytest.fixture(scope="module")
def gl_map() -> GLMapSchema:
    return GLMapSchema()


@pytest.fixture(scope="module")
def unspsc() -> UNSPSCSchema:
    return UNSPSCSchema()


def _assert_normalized(df: pd.DataFrame) -> None:
    for col in ["code", "label", "level_path"]:
        assert col in df.columns
    assert df["code"].is_unique
    assert df["code"].dtype == object


def test_three_level_loads(three_level: ThreeLevelSchema) -> None:
    df = three_level.load_taxonomy()
    _assert_normalized(df)
    assert len(df) > 50
    assert len(df.iloc[0]["level_path"]) == 3


def test_gl_map_loads(gl_map: GLMapSchema) -> None:
    df = gl_map.load_taxonomy()
    _assert_normalized(df)
    assert len(df) > 100
    assert df.iloc[0]["code"].isdigit()


def test_unspsc_loads(unspsc: UNSPSCSchema) -> None:
    df = unspsc.load_taxonomy()
    _assert_normalized(df)
    assert len(df) > 100_000
    assert len(df.iloc[0]["code"]) == 8


def test_registry_has_all_three() -> None:
    assert [s.name for s in list_schemas()] == ["three_level", "gl_map", "unspsc"]


def test_get_schema_unknown_raises() -> None:
    with pytest.raises(KeyError):
        get_schema("does_not_exist")


def test_strategy_map() -> None:
    assert STRATEGY["three_level"] == "ai_query"
    assert STRATEGY["gl_map"] == "ai_query"
    assert STRATEGY["unspsc"] == "pgvector"
