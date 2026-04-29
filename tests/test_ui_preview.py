"""Tests for the table preview UI route handler."""

from __future__ import annotations

from unittest.mock import AsyncMock


from cave_catalog.table_schemas import ColumnInfo, TableMetadata


class TestPreviewRoute:
    """Tests for POST /ui/preview."""

    async def test_empty_uri_returns_error(self, client):
        resp = await client.post("/ui/preview", data={"uri": "", "format": "delta"})
        assert resp.status_code == 200
        assert "Please enter a URI" in resp.text

    async def test_unsupported_format(self, client):
        resp = await client.post(
            "/ui/preview", data={"uri": "gs://bucket/path", "format": "csv"}
        )
        assert resp.status_code == 200
        assert "Unsupported format" in resp.text
        assert "csv" in resp.text

    async def test_uri_not_found(self, client, monkeypatch):
        """URI that doesn't exist shows diagnostic error."""
        mock_extractor = AsyncMock()
        mock_extractor.extract.side_effect = FileNotFoundError(
            "No such file or directory: gs://bucket/missing"
        )
        monkeypatch.setattr(
            "cave_catalog.routers.ui.get_extractor", lambda fmt: mock_extractor
        )
        resp = await client.post(
            "/ui/preview", data={"uri": "gs://bucket/missing", "format": "delta"}
        )
        assert resp.status_code == 200
        assert "URI unreachable" in resp.text
        assert "does not exist" in resp.text

    async def test_permission_denied(self, client, monkeypatch):
        """Permission error shows diagnostic."""
        mock_extractor = AsyncMock()
        mock_extractor.extract.side_effect = PermissionError(
            "403 Forbidden: Access denied"
        )
        monkeypatch.setattr(
            "cave_catalog.routers.ui.get_extractor", lambda fmt: mock_extractor
        )
        resp = await client.post(
            "/ui/preview", data={"uri": "gs://secret/data", "format": "delta"}
        )
        assert resp.status_code == 200
        assert "URI unreachable" in resp.text or "permission" in resp.text.lower()

    async def test_extraction_failure(self, client, monkeypatch):
        """Generic extraction error shows format-specific diagnostic."""
        mock_extractor = AsyncMock()
        mock_extractor.extract.side_effect = RuntimeError("Corrupt delta log")
        monkeypatch.setattr(
            "cave_catalog.routers.ui.get_extractor", lambda fmt: mock_extractor
        )
        resp = await client.post(
            "/ui/preview", data={"uri": "gs://bucket/bad", "format": "delta"}
        )
        assert resp.status_code == 200
        assert "Failed to read delta data" in resp.text

    async def test_successful_preview(self, client, monkeypatch):
        """Successful preview returns metadata fragment."""
        metadata = TableMetadata(
            n_rows=1000,
            n_columns=3,
            n_bytes=1048576,
            columns=[
                ColumnInfo(name="id", dtype="int64"),
                ColumnInfo(name="pt_position", dtype="list<float>"),
                ColumnInfo(name="label", dtype="string"),
            ],
            partition_columns=[],
        )
        mock_extractor = AsyncMock()
        mock_extractor.extract.return_value = metadata
        monkeypatch.setattr(
            "cave_catalog.routers.ui.get_extractor", lambda fmt: mock_extractor
        )
        resp = await client.post(
            "/ui/preview", data={"uri": "gs://bucket/table", "format": "delta"}
        )
        assert resp.status_code == 200
        assert "DELTA" in resp.text
        assert "1,000" in resp.text  # formatted row count
        assert "id" in resp.text
        assert "pt_position" in resp.text
        assert "registration-fields" in resp.text  # JS to show next step

    async def test_successful_preview_parquet(self, client, monkeypatch):
        """Parquet format shows correctly."""
        metadata = TableMetadata(
            n_rows=500,
            n_columns=2,
            columns=[
                ColumnInfo(name="col_a", dtype="float64"),
                ColumnInfo(name="col_b", dtype="string"),
            ],
        )
        mock_extractor = AsyncMock()
        mock_extractor.extract.return_value = metadata
        monkeypatch.setattr(
            "cave_catalog.routers.ui.get_extractor", lambda fmt: mock_extractor
        )
        resp = await client.post(
            "/ui/preview", data={"uri": "gs://bucket/file.parquet", "format": "parquet"}
        )
        assert resp.status_code == 200
        assert "PARQUET" in resp.text
        assert "col_a" in resp.text
