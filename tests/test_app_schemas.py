"""End-to-end test of schema-aware app routes using TestClient."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app(monkeypatch):
    """Build the FastAPI app and short-circuit the lifespan with synthetic state."""
    from spend_app.backend.app import app as fastapi_app

    schema_registry = pd.DataFrame([
        {
            "name": "three_level",
            "display_name": "3-Level Spend",
            "description": "Internal taxonomy",
            "classify_method": "ai_classify",
            "taxonomy_table": "three_level_taxonomy",
            "level_columns": ["category_level_1", "category_level_2", "category_level_3"],
            "code_column": "category_level_3",
            "label_column": "category_level_3",
            "description_columns": ["category_level_3_description"],
            "leaf_count": 102,
        },
        {
            "name": "gl_map",
            "display_name": "GL Account Map",
            "description": "GL accounts",
            "classify_method": "ai_classify",
            "taxonomy_table": "gl_map_taxonomy",
            "level_columns": ["nodedesc02", "Level 2", "Level 3"],
            "code_column": "GL Account",
            "label_column": "objval_desc",
            "description_columns": [],
            "leaf_count": 292,
        },
    ])

    cat_predictions = pd.DataFrame([
        {
            "order_id": "ORD-1", "schema_name": "three_level",
            "code": "Steel plate", "label": "Steel plate",
            "level_path": ["Direct", "Raw Materials", "Steel plate"],
            "confidence": 4.0, "rationale": "matches", "source": "ai_classify",
            "candidates": [], "classified_at": datetime.utcnow(),
        },
        {
            "order_id": "ORD-2", "schema_name": "gl_map",
            "code": "6010004", "label": "FUEL - MATERIALS",
            "level_path": ["Other", "Other", "Controllable Costs"],
            "confidence": None, "rationale": None, "source": "ai_classify",
            "candidates": [], "classified_at": datetime.utcnow(),
        },
    ])

    three_level_taxonomy = pd.DataFrame([
        {"code": "Steel plate", "label": "Steel plate",
         "level_path": ["Direct", "Raw Materials", "Steel plate"],
         "schema_name": "three_level"},
    ])

    fake_state = MagicMock()
    fake_state.data = {
        "schema_registry": schema_registry,
        "cat_predictions": cat_predictions,
        "three_level_taxonomy": three_level_taxonomy,
        "gl_map_taxonomy": pd.DataFrame(),
        "invoices": pd.DataFrame(),
        "categories": pd.DataFrame(),
    }

    # Mount state without running the real lifespan (which would hit Databricks).
    fastapi_app.state.data = fake_state.data
    fastapi_app.state.project_config = MagicMock(catalog="cat", schema_name="sch")
    fastapi_app.state.table_status = []

    return fastapi_app


def _client(app):
    """TestClient without the `with` context so the real lifespan does NOT run.

    Our fixture pre-sets app.state with synthetic data; running the lifespan
    would overwrite it with a real Databricks call.
    """
    return TestClient(app, raise_server_exceptions=False)


def test_list_schemas(app):
    client = _client(app)
    r = client.get("/api/schemas")
    assert r.status_code == 200
    body = r.json()
    assert {row["name"] for row in body} == {"three_level", "gl_map"}


def test_get_taxonomy(app):
    client = _client(app)
    r = client.get("/api/schemas/three_level/taxonomy")
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 1
    assert body[0]["code"] == "Steel plate"


def test_get_predictions_by_schema(app):
    client = _client(app)
    r = client.get("/api/predictions/three_level")
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 1
    assert body[0]["order_id"] == "ORD-1"


def test_get_predictions_filtered_by_source(app):
    client = _client(app)
    r = client.get("/api/predictions/three_level?source=does_not_exist")
    assert r.status_code == 200
    assert r.json() == []


def test_get_predictions_for_invoice(app):
    client = _client(app)
    r = client.get("/api/predictions/gl_map/ORD-2")
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 1
    assert body[0]["code"] == "6010004"


def test_unknown_schema_returns_empty(app):
    client = _client(app)
    r = client.get("/api/predictions/does_not_exist")
    assert r.status_code == 200
    assert r.json() == []
