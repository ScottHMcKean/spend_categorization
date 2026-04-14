"""Test database module."""

import pytest

from src.config import Config
from src.app.database import (
    LakebaseBackend,
    create_backend,
    get_backend,
    reset_backend,
)


class TestCreateBackend:
    """Tests for create_backend factory."""

    def test_create_backend_returns_lakebase(self):
        config = Config.from_yaml()
        backend = create_backend(config)
        assert isinstance(backend, LakebaseBackend)

    def test_create_backend_missing_instance_raises(self):
        config = Config.from_yaml()
        config.lakebase_instance = ""
        with pytest.raises(ValueError, match="lakebase_instance"):
            create_backend(config)

    def test_create_backend_native_tables_from_config(self):
        config = Config.from_yaml()
        backend = create_backend(config)
        assert isinstance(backend, LakebaseBackend)
        for label, tdef in config.app_tables.items():
            if tdef.native:
                assert tdef.table in backend.native_tables


class TestLakebaseRewrite:
    """Test SQL rewrite logic without a live connection."""

    def test_rewrite_synced_table(self):
        backend = LakebaseBackend(
            instance="test",
            catalog="shm",
            schema_name="spend",
            native_tables={"cat_reviews"},
        )
        result = backend._rewrite_query("SELECT * FROM shm.spend.invoices")
        assert result == 'SELECT * FROM "spend"."invoices_synced"'

    def test_rewrite_native_table(self):
        backend = LakebaseBackend(
            instance="test",
            catalog="shm",
            schema_name="spend",
            native_tables={"cat_reviews"},
        )
        result = backend._rewrite_query("SELECT * FROM shm.spend.cat_reviews")
        assert result == 'SELECT * FROM "spend"."cat_reviews"'

    def test_rewrite_no_catalog_returns_unchanged(self):
        backend = LakebaseBackend(instance="test", catalog="", schema_name="")
        query = "SELECT * FROM some_table"
        assert backend._rewrite_query(query) == query


class TestBackendGlobalState:
    """Tests for backend global state management."""

    def test_get_backend_without_init_raises(self):
        reset_backend()
        with pytest.raises(RuntimeError, match="not initialized"):
            get_backend()
