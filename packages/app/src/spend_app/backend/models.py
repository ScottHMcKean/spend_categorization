from __future__ import annotations
from typing import Any
from pydantic import BaseModel
from .. import __version__


class VersionOut(BaseModel):
    version: str

    @classmethod
    def from_metadata(cls):
        return cls(version=__version__)


class TableStatus(BaseModel):
    table: str
    description: str
    rows: int
    columns: int
    column_names: str
    status: str


class StatusResponse(BaseModel):
    tables: list[TableStatus]
    backend: str
    catalog: str
    schema_name: str


class InvoiceOut(BaseModel):
    model_config = {"from_attributes": True}
    order_id: str | None = None
    date: str | None = None
    description: str | None = None
    supplier: str | None = None
    supplier_country: str | None = None
    amount: float | None = None
    unit_price: float | None = None
    total: float | None = None
    category_level_1: str | None = None
    category_level_2: str | None = None
    category_level_3: str | None = None
    cost_centre: str | None = None
    plant: str | None = None
    plant_id: str | None = None
    region: str | None = None


class ClassificationOut(BaseModel):
    model_config = {"from_attributes": True}
    order_id: str | None = None
    pred_level_1: str | None = None
    pred_level_2: str | None = None
    pred_level_3: str | None = None
    confidence: float | None = None
    source: str | None = None
    description: str | None = None
    supplier: str | None = None
    total: float | None = None
    category_level_1: str | None = None
    category_level_2: str | None = None


class ReviewIn(BaseModel):
    order_id: str
    source: str = ""
    reviewer: str = "user"
    original_level_1: str = ""
    original_level_2: str = ""
    original_level_3: str = ""
    reviewed_level_1: str = ""
    reviewed_level_2: str = ""
    reviewed_level_3: str = ""
    review_status: str = "corrected"
    comments: str = ""
    # Schema-aware leaf selection (used when reviewing taxonomies that
    # aren't 3-level — e.g. UNSPSC, gl_map). When provided, the backend
    # derives ``reviewed_level_1/2/3`` from ``reviewed_level_path``.
    reviewed_code: str | None = None
    reviewed_label: str | None = None
    reviewed_level_path: list[str] | None = None


class ReviewOut(BaseModel):
    model_config = {"from_attributes": True}
    order_id: str | None = None
    source: str | None = None
    reviewer: str | None = None
    review_date: str | None = None
    original_level_1: str | None = None
    original_level_2: str | None = None
    reviewed_level_1: str | None = None
    reviewed_level_2: str | None = None
    review_status: str | None = None
    comments: str | None = None
    created_at: str | None = None


class AnalyticsSummary(BaseModel):
    total_spend: float
    invoice_count: int
    supplier_count: int
    prediction_count: int
    review_count: int
    predictions_by_source: list[dict[str, Any]]
    spend_by_l1: list[dict[str, Any]]
    spend_by_l2: list[dict[str, Any]]
    monthly_trend: list[dict[str, Any]]
    top_suppliers: list[dict[str, Any]]
    region_category: list[dict[str, Any]]


class CategoryAccuracy(BaseModel):
    category: str
    precision: float
    recall: float
    f1: float
    spend: float
    count: int


class AccuracyResponse(BaseModel):
    overall_accuracy: float
    overall_precision: float
    overall_recall: float
    overall_f1: float
    total_classified: int
    total_correct: int
    source: str
    categories: list[CategoryAccuracy]


class AgentToolCall(BaseModel):
    name: str
    args: dict[str, Any] = {}
    result: Any | None = None


class AgentReviewOut(BaseModel):
    order_id: str
    schema_name: str
    bootstrap: dict[str, Any]
    suggested_code: str | None = None
    suggested_label: str | None = None
    suggested_level_path: list[str] | None = None
    suggested_level_1: str | None = None
    suggested_level_2: str | None = None
    suggested_level_3: str | None = None
    confidence: int | None = None
    agrees_with_bootstrap: bool | None = None
    rationale: str = ""
    tool_calls: list[AgentToolCall] = []
    error: str | None = None
    latency_seconds: float = 0.0
