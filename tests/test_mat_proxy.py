"""Tests for mat_proxy module — caching and CAVEclient integration."""

from unittest.mock import MagicMock, patch

import pytest
from cave_catalog.mat_proxy import (
    MatProxyError,
    _client_cache,
    get_linkable_targets,
    get_mat_tables,
    get_mat_views,
    get_target_columns,
)


@pytest.fixture(autouse=True)
def clear_caches():
    """Clear client cache before each test."""
    _client_cache.clear()
    yield
    _client_cache.clear()


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
        mock_caveclient.materialize.tables.table_names = [
            "synapses",
            "nucleus_detection",
        ]
        result = await get_mat_tables("minnie65_phase3")
        assert result == ["synapses", "nucleus_detection"]

    async def test_error_without_cave_token(self):
        with patch("cave_catalog.mat_proxy.get_settings") as mock:
            settings = MagicMock()
            settings.cave_token = None
            mock.return_value = settings
            with pytest.raises(MatProxyError, match="CAVE_TOKEN is not configured"):
                await get_mat_tables("minnie65_phase3")

    async def test_wraps_unexpected_error(self, mock_settings, mock_caveclient):
        type(mock_caveclient.materialize).tables = property(
            lambda self: (_ for _ in ()).throw(RuntimeError("timeout"))
        )
        with pytest.raises(MatProxyError, match="Failed to fetch tables"):
            await get_mat_tables("minnie65_phase3")


class TestGetMatViews:
    async def test_fetches_views(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.views.table_names = [
            "synapse_with_nucleus",
            "cell_type_view",
        ]
        result = await get_mat_views("minnie65_phase3")
        assert result == ["synapse_with_nucleus", "cell_type_view"]


class TestGetLinkableTargets:
    async def test_combines_tables_and_views(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.tables.table_names = ["b_table", "a_table"]
        mock_caveclient.materialize.views.table_names = ["c_view"]
        targets = await get_linkable_targets("minnie65_phase3")
        # Should be sorted by name
        names = [t.name for t in targets]
        assert names == ["a_table", "b_table", "c_view"]
        types = [t.target_type for t in targets]
        assert types == ["table", "table", "view"]


class TestGetTargetColumns:
    async def test_table_columns_via_fields(self, mock_settings, mock_caveclient):
        table_mock = MagicMock()
        table_mock.fields = ["pre_pt_position_bbox", "post_pt_position_bbox", "size"]
        mock_caveclient.materialize.tables.__getitem__.return_value = table_mock
        result = await get_target_columns("minnie65_phase3", "synapses", "table")
        assert result == ["pre_pt_position", "post_pt_position", "size"]

    async def test_view_columns_via_fields(self, mock_settings, mock_caveclient):
        view_mock = MagicMock()
        view_mock.fields = ["id", "cell_type", "pt_position_bbox"]
        mock_caveclient.materialize.views.__getitem__.return_value = view_mock
        result = await get_target_columns("minnie65_phase3", "cell_type_view", "view")
        assert result == ["id", "cell_type", "pt_position"]

    async def test_bbox_suffix_stripped(self, mock_settings, mock_caveclient):
        table_mock = MagicMock()
        table_mock.fields = ["col_bbox", "normal_col", "another_bbox"]
        mock_caveclient.materialize.tables.__getitem__.return_value = table_mock
        result = await get_target_columns("minnie65_phase3", "test_table", "table")
        assert result == ["col", "normal_col", "another"]

    async def test_wraps_unexpected_error(self, mock_settings, mock_caveclient):
        mock_caveclient.materialize.tables.__getitem__.side_effect = RuntimeError(
            "connection failed"
        )
        with pytest.raises(MatProxyError, match="Failed to fetch columns"):
            await get_target_columns("minnie65_phase3", "bad_table", "table")
