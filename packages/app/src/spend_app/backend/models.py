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
    schema_name: str | None = None
    source: str | None = None
    code: str | None = None
    label: str | None = None
    level_path: list[str] | None = None
    confidence: float | None = None
    status: str | None = None
    reason: str | None = None
    rationale: str | None = None


class ReviewIn(BaseModel):
    """One human review of a prediction. `action` is approved | corrected |
    rejected; code/label/level_path carry the accepted leaf (for
    approve/correct)."""
    order_id: str
    schema_name: str
    reviewed_source: str = ""
    action: str = "corrected"
    code: str | None = None
    label: str | None = None
    level_path: list[str] | None = None
    reviewer: str = "user"
    comment: str = ""


class ReviewOut(BaseModel):
    model_config = {"from_attributes": True}
    review_id: str | None = None
    order_id: str | None = None
    schema_name: str | None = None
    reviewed_source: str | None = None
    action: str | None = None
    code: str | None = None
    label: str | None = None
    level_path: list[str] | None = None
    reviewer: str | None = None
    comment: str | None = None
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


