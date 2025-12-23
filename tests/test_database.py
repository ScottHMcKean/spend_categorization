"""Test database module."""

import pytest
import pandas as pd

from src.app.config import AppConfig
from src.app.database import (
    MockBackend,
    create_backend,
    init_backend,
    get_backend,
    reset_backend,
)
from src.app.queries import (
    search_invoices,
    get_invoices_by_ids,
    get_available_categories,
)
from src.app.reviews import (
    write_review,
    write_reviews_batch,
)


@pytest.fixture
def test_config():
    """Create a test AppConfig."""
    return AppConfig(
        mode="test",
        invoices_sync="invoices_sync",
        categorization_sync="categorization_sync",
        reviews_table="reviews",
    )


@pytest.fixture
def mock_backend(test_config):
    """Initialize and return a MockBackend."""
    reset_backend()
    backend = init_backend(test_config)
    yield backend
    reset_backend()


class TestMockBackend:
    """Tests for MockBackend."""

    def test_mock_backend_is_connected(self, mock_backend):
        assert mock_backend.is_connected() is True

    def test_mock_backend_has_invoices(self, mock_backend):
        assert len(mock_backend._invoices) > 0

    def test_mock_backend_has_categories(self, mock_backend):
        assert len(mock_backend._categories) > 0


class TestCreateBackend:
    """Tests for create_backend factory."""

    def test_create_mock_backend_for_test_mode(self):
        reset_backend()
        config = AppConfig(mode="test")
        backend = create_backend(config)
        assert isinstance(backend, MockBackend)

    def test_prod_mode_requires_lakebase_instance(self):
        config = AppConfig(mode="prod", lakebase_instance="")
        with pytest.raises(ValueError, match="lakebase_instance required"):
            create_backend(config)


class TestQueries:
    """Tests for query functions with MockBackend."""

    def test_get_available_categories(self, mock_backend, test_config):
        categories = get_available_categories(test_config, mock_backend)
        assert isinstance(categories, list)
        assert len(categories) > 0

    def test_search_invoices(self, mock_backend, test_config):
        results = search_invoices(
            test_config,
            search_term="Dell",
            limit=10,
            backend=mock_backend,
        )
        assert isinstance(results, pd.DataFrame)

    def test_get_invoices_by_ids(self, mock_backend, test_config):
        all_invoices = mock_backend._invoices
        test_ids = all_invoices["invoice_id"].head(3).tolist()

        results = get_invoices_by_ids(test_config, test_ids, backend=mock_backend)
        assert len(results) == 3
        assert set(results["invoice_id"].tolist()) == set(test_ids)


class TestReviews:
    """Tests for review functions with MockBackend."""

    def test_write_review(self, mock_backend, test_config):
        initial_count = len(mock_backend._reviews)

        write_review(
            test_config,
            invoice_id="INV000001",
            category="Hardware",
            schema_id="v1",
            prompt_id="p1",
            labeler_id="test_user",
            backend=mock_backend,
        )

        assert len(mock_backend._reviews) == initial_count + 1
        new_review = mock_backend._reviews.iloc[-1]
        assert new_review["invoice_id"] == "INV000001"
        assert new_review["category"] == "Hardware"
        assert new_review["is_current"] == True

    def test_write_reviews_batch(self, mock_backend, test_config):
        initial_count = len(mock_backend._reviews)

        reviews = [
            {
                "invoice_id": "INV000002",
                "category": "Software",
                "schema_id": "v1",
                "prompt_id": "p1",
                "labeler_id": "test_user",
            },
            {
                "invoice_id": "INV000003",
                "category": "Cloud Services",
                "schema_id": "v1",
                "prompt_id": "p1",
                "labeler_id": "test_user",
            },
        ]

        write_reviews_batch(test_config, reviews, backend=mock_backend)

        assert len(mock_backend._reviews) == initial_count + 2


class TestBackendGlobalState:
    """Tests for backend global state management."""

    def test_get_backend_without_init_raises(self):
        reset_backend()
        with pytest.raises(RuntimeError, match="not initialized"):
            get_backend()

    def test_init_backend_sets_global(self, test_config):
        reset_backend()
        backend = init_backend(test_config)
        assert get_backend() is backend
        reset_backend()
