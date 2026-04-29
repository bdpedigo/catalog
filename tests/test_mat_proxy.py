"""Tests for mat_proxy module — caching and CAVEclient integration."""

from unittest.mock import MagicMock, patch

import pytest

from cave_catalog.mat_proxy import (
    MatProxyError,
    _columns_cache,
    _tables_cache,
    _views_cache,
    get_linkable_targets,
    get_mat_tables,
    get_mat_views,
    get_target_columns,
)


@pytest.fixture(autouse=True)
def clear_caches():
    """Clear all caches before each test."""
    _tables_cache.clear()
    _views_cache.clear()
    _columns_cache.clear()
    yield
    _tables_cache.clear()
    _views_cache.clear()
    _columns_cache.clear()


@pytest.fixture
def mock_settings():
    """Provide settings with CAVE_TOKEN configured."""
    with patch("cave_catalog.mat_proxy.get_settings") as mock:
        settings = MagicMock()
        settings.cave_token = "test-token"
        settings.caveclient_server_address = "https://test-server.com"
        mock.return_value = settings
        yield settings


@pytest.fixture
def mock_caveclient():
    """Mock CAVEclient constructor."""
    with patch("cave_catalog.mat_proxy.CAVEclient") as mock_cls:
        client = MagicMock()
        mock_cls.return_value = client
        yield client


class TestGetMatTables:
    async def test_fetches_tables(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.get_tables.return_value = [
            "synapses",
            "nucleus_detection",
        ]
        result = await get_mat_tables("minnie65_phase3")
        assert result == ["synapses", "nucleus_detection"]

    async def test_cache_hit(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.get_tables.return_value = ["synapses"]
        await get_mat_tables("minnie65_phase3", version=1)
        await get_mat_tables("minnie65_phase3", version=1)
        # Should only call CAVEclient once due to cache
        assert mock_caveclient.materialize.get_tables.call_count == 1

    async def test_different_version_is_cache_miss(
        self, mock_settings, mock_caveclient
    ):
        mock_caveclient.materialize.get_tables.return_value = ["synapses"]
        await get_mat_tables("minnie65_phase3", version=1)
        await get_mat_tables("minnie65_phase3", version=2)
        assert mock_caveclient.materialize.get_tables.call_count == 2

    async def test_error_without_cave_token(self):
        with patch("cave_catalog.mat_proxy.get_settings") as mock:
            settings = MagicMock()
            settings.cave_token = None
            mock.return_value = settings
            with pytest.raises(MatProxyError, match="CAVE_TOKEN is not configured"):
                await get_mat_tables("minnie65_phase3")

    async def test_wraps_unexpected_error(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.get_tables.side_effect = RuntimeError("timeout")
        with pytest.raises(MatProxyError, match="Failed to fetch tables"):
            await get_mat_tables("minnie65_phase3")


class TestGetMatViews:
    async def test_fetches_views(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.get_views.return_value = [
            "synapse_with_nucleus",
            "cell_type_view",
        ]
        result = await get_mat_views("minnie65_phase3")
        assert result == ["synapse_with_nucleus", "cell_type_view"]

    async def test_cache_hit(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.get_views.return_value = ["view1"]
        await get_mat_views("minnie65_phase3")
        await get_mat_views("minnie65_phase3")
        assert mock_caveclient.materialize.get_views.call_count == 1


class TestGetLinkableTargets:
    async def test_combines_tables_and_views(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.get_tables.return_value = ["b_table", "a_table"]
        mock_caveclient.materialize.get_views.return_value = ["c_view"]
        targets = await get_linkable_targets("minnie65_phase3")
        # Should be sorted by name
        names = [t.name for t in targets]
        assert names == ["a_table", "b_table", "c_view"]
        types = [t.target_type for t in targets]
        assert types == ["table", "table", "view"]


class TestGetTargetColumns:
    async def test_table_columns_via_schema(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.get_table_metadata.return_value = {
            "schema_type": "synapse"
        }
        mock_caveclient.schema.schema_definition.return_value = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "definitions": {
                "BoundSpatialPoint": {"type": "object"},
                "SynapseSchema": {
                    "type": "object",
                    "properties": {
                        "pre_pt": {"$ref": "#/definitions/BoundSpatialPoint"},
                        "post_pt": {"$ref": "#/definitions/BoundSpatialPoint"},
                    },
                },
            },
            "$ref": "#/definitions/SynapseSchema",
        }
        result = await get_target_columns("minnie65_phase3", "synapses", "table")
        assert len(result) == 2
        assert result[0]["name"] == "pre_pt"

    async def test_view_columns_direct(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.get_view_schema.return_value = {
            "id": "integer",
            "cell_type": "string",
        }
        result = await get_target_columns("minnie65_phase3", "cell_type_view", "view")
        assert len(result) == 2
        col_names = [c["name"] for c in result]
        assert "id" in col_names
        assert "cell_type" in col_names

    async def test_cache_hit(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.get_view_schema.return_value = {"id": "integer"}
        await get_target_columns("minnie65_phase3", "v1", "view")
        await get_target_columns("minnie65_phase3", "v1", "view")
        assert mock_caveclient.materialize.get_view_schema.call_count == 1

    async def test_error_on_missing_schema_type(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.get_table_metadata.return_value = {}
        with pytest.raises(MatProxyError, match="Could not determine schema type"):
            await get_target_columns("minnie65_phase3", "bad_table", "table")
