"""Test database module."""

import pytest
import pandas as pd

from src.config import Config
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
    """Create a test Config."""
    return Config.from_yaml()


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

    def test_mock_backend_has_categorizations(self, mock_backend):
        """Test that MockBackend loads categorizations from CSV."""
        assert len(mock_backend._categorizations) > 0

    def test_mock_backend_has_categories(self, mock_backend):
        """Test that MockBackend loads categories from CSV."""
        assert len(mock_backend._categories) > 0


class TestCreateBackend:
    """Tests for create_backend factory."""

    def test_create_mock_backend_for_test_mode(self):
        reset_backend()
        config = Config.from_yaml()
        backend = create_backend(config)
        assert isinstance(backend, MockBackend)


class TestQueries:
    """Tests for query functions with MockBackend."""

    def test_get_available_categories(self, mock_backend, test_config):
        categories = get_available_categories(test_config, mock_backend)
        assert isinstance(categories, list)
        assert len(categories) > 0

    def test_search_invoices(self, mock_backend, test_config):
        results = search_invoices(
            test_config,
            search_term="bearing",
            limit=10,
            backend=mock_backend,
        )
        assert isinstance(results, pd.DataFrame)

    def test_get_invoices_by_ids(self, mock_backend, test_config):
        # Use categorizations to get order_ids
        all_categorizations = mock_backend._categorizations
        test_ids = all_categorizations["order_id"].head(3).tolist()

        results = get_invoices_by_ids(test_config, test_ids, backend=mock_backend)
        assert isinstance(results, pd.DataFrame)
        # MockBackend may not have full invoice data, just verify it returns a DataFrame


class TestReviews:
    """Tests for review functions with MockBackend."""

    def test_write_review(self, mock_backend, test_config):
        initial_count = len(mock_backend._reviews)

        write_review(
            test_config,
            order_id="ORD-001",
            source="bootstrap",
            original_level_1="Operations",
            original_level_2="Maintenance",
            original_level_3="Preventive Maintenance",
            reviewed_level_1="Operations",
            reviewed_level_2="Maintenance",
            reviewed_level_3="Corrective Maintenance",
            reviewer="test_user",
            review_status="corrected",
            comments="Test review",
            backend=mock_backend,
        )

        assert len(mock_backend._reviews) == initial_count + 1
        new_review = mock_backend._reviews.iloc[-1]
        assert new_review["order_id"] == "ORD-001"
        assert new_review["reviewed_level_3"] == "Corrective Maintenance"

    def test_write_reviews_batch(self, mock_backend, test_config):
        initial_count = len(mock_backend._reviews)

        reviews = [
            {
                "order_id": "ORD-002",
                "source": "bootstrap",
                "original_level_1": "Operations",
                "original_level_2": "Maintenance",
                "original_level_3": "Preventive Maintenance",
                "reviewed_level_1": "Operations",
                "reviewed_level_2": "Maintenance",
                "reviewed_level_3": "Corrective Maintenance",
                "reviewer": "test_user",
                "review_status": "corrected",
                "comments": "Test review 1",
            },
            {
                "order_id": "ORD-003",
                "source": "bootstrap",
                "original_level_1": "Procurement",
                "original_level_2": "Materials",
                "original_level_3": "Raw Materials",
                "reviewed_level_1": "Procurement",
                "reviewed_level_2": "Materials",
                "reviewed_level_3": "Consumables",
                "reviewer": "test_user",
                "review_status": "corrected",
                "comments": "Test review 2",
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
