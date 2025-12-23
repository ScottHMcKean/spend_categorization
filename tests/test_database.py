"""Test database module."""

import pytest
import pandas as pd

from invoice_app.config import Config, AppConfig, TablesConfig
from invoice_app.database import (
    MockBackend,
    create_backend,
    init_backend,
    get_backend,
    reset_backend,
)
from invoice_app.queries import (
    search_invoices,
    get_flagged_invoices,
    get_invoices_by_ids,
    get_available_categories,
)
from invoice_app.corrections import (
    write_correction,
    write_corrections_batch,
)


@pytest.fixture
def test_config():
    """Create a test Config in test mode."""
    return Config(
        app=AppConfig(mode="test"),
        tables=TablesConfig(invoices="invoices", corrections="invoice_corrections"),
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
        """Test that MockBackend is always connected."""
        assert mock_backend.is_connected() is True

    def test_mock_backend_has_invoices(self, mock_backend):
        """Test that MockBackend has invoice data."""
        assert len(mock_backend._invoices) > 0

    def test_mock_backend_has_categories(self, mock_backend):
        """Test that MockBackend has categories."""
        assert len(mock_backend._categories) > 0


class TestCreateBackend:
    """Tests for create_backend factory function."""

    def test_create_mock_backend_for_test_mode(self):
        """Test that test mode creates MockBackend."""
        reset_backend()
        config = Config(app=AppConfig(mode="test"))
        backend = create_backend(config)
        assert isinstance(backend, MockBackend)

    def test_prod_mode_requires_lakebase_instance(self):
        """Test that prod mode requires lakebase_instance."""
        config = Config(app=AppConfig(mode="prod", lakebase_instance=""))
        with pytest.raises(ValueError, match="lakebase_instance is required"):
            create_backend(config)


class TestQueries:
    """Tests for query functions with MockBackend."""

    def test_get_available_categories(self, mock_backend, test_config):
        """Test getting available categories."""
        categories = get_available_categories(test_config, mock_backend)
        assert isinstance(categories, list)
        assert len(categories) > 0
        assert all(isinstance(c, str) for c in categories)

    def test_get_flagged_invoices(self, mock_backend, test_config):
        """Test getting flagged invoices."""
        flagged = get_flagged_invoices(test_config, limit=10, backend=mock_backend)
        assert isinstance(flagged, pd.DataFrame)
        # All returned invoices should have low confidence or null category
        if not flagged.empty:
            assert all(
                (flagged["confidence_score"] < 0.7) | (flagged["category"].isna())
            )

    def test_search_invoices(self, mock_backend, test_config):
        """Test searching invoices."""
        results = search_invoices(
            test_config,
            search_term="Dell",
            limit=10,
            backend=mock_backend,
        )
        assert isinstance(results, pd.DataFrame)

    def test_get_invoices_by_ids(self, mock_backend, test_config):
        """Test getting invoices by IDs."""
        # First get some invoice IDs
        all_invoices = mock_backend._invoices
        test_ids = all_invoices["invoice_id"].head(3).tolist()

        results = get_invoices_by_ids(test_config, test_ids, backend=mock_backend)
        assert len(results) == 3
        assert set(results["invoice_id"].tolist()) == set(test_ids)


class TestCorrections:
    """Tests for correction functions with MockBackend."""

    def test_write_correction(self, mock_backend, test_config):
        """Test writing a single correction."""
        initial_count = len(mock_backend._corrections)

        write_correction(
            test_config,
            invoice_id="INV000001",
            transaction_ids=["TXN000001"],
            corrected_category="Hardware",
            comment="Test correction",
            corrected_by="test_user",
            backend=mock_backend,
        )

        assert len(mock_backend._corrections) == initial_count + 1
        new_correction = mock_backend._corrections.iloc[-1]
        assert new_correction["invoice_id"] == "INV000001"
        assert new_correction["category"] == "Hardware"
        assert new_correction["is_current"] == True

    def test_write_corrections_batch(self, mock_backend, test_config):
        """Test writing multiple corrections in a batch."""
        initial_count = len(mock_backend._corrections)

        corrections = [
            {
                "invoice_id": "INV000002",
                "transaction_ids": ["TXN000002"],
                "corrected_category": "Software",
                "comment": "Batch correction 1",
                "corrected_by": "test_user",
            },
            {
                "invoice_id": "INV000003",
                "transaction_ids": ["TXN000003"],
                "corrected_category": "Cloud Services",
                "comment": "Batch correction 2",
                "corrected_by": "test_user",
            },
        ]

        write_corrections_batch(test_config, corrections, backend=mock_backend)

        assert len(mock_backend._corrections) == initial_count + 2


class TestBackendGlobalState:
    """Tests for backend global state management."""

    def test_get_backend_without_init_raises(self):
        """Test that get_backend raises if not initialized."""
        reset_backend()
        with pytest.raises(RuntimeError, match="not initialized"):
            get_backend()

    def test_init_backend_sets_global(self, test_config):
        """Test that init_backend sets the global backend."""
        reset_backend()
        backend = init_backend(test_config)
        assert get_backend() is backend
        reset_backend()
