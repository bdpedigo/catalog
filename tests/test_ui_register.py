"""Tests for the registration page and submit flow."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from cave_catalog.schemas import ValidationCheck, ValidationReport
from cave_catalog.table_schemas import ColumnInfo, TableMetadata


def _passing_report():
    return ValidationReport(
        auth_check=ValidationCheck(passed=True),
        duplicate_check=ValidationCheck(passed=True),
        name_reservation_check=ValidationCheck(passed=True),
        uri_reachable=ValidationCheck(passed=True),
        format_sniff=ValidationCheck(passed=True),
    )


def _mock_metadata():
    return TableMetadata(
        n_rows=100,
        n_columns=2,
        columns=[
            ColumnInfo(name="id", dtype="int64"),
            ColumnInfo(name="value", dtype="float64"),
        ],
    )


class TestRegisterPageRenders:
    async def test_register_page_renders(self, client):
        """Register page loads with form elements."""
        resp = await client.get("/ui/register")
        assert resp.status_code == 200
        assert "Register a Table" in resp.text
        assert "Preview" in resp.text
        assert "uri" in resp.text

    async def test_preview_returns_column_table(self, client, monkeypatch):
        """Successful preview includes annotation table with description fields."""
        mock_extractor = AsyncMock()
        mock_extractor.extract.return_value = _mock_metadata()
        monkeypatch.setattr(
            "cave_catalog.routers.ui.get_extractor", lambda fmt: mock_extractor
        )
        resp = await client.post(
            "/ui/preview", data={"uri": "gs://bucket/table", "format": "delta"}
        )
        assert resp.status_code == 200
        # Should contain annotation inputs
        assert "col_name_0" in resp.text
        assert "col_desc_0" in resp.text
        assert "Add Link" in resp.text


class TestRegisterSubmit:
    async def test_successful_registration(self, client, monkeypatch):
        """Full registration flow: preview + submit → success."""
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        from cave_catalog.config import get_settings

        get_settings.cache_clear()

        # Mock validation and extraction
        monkeypatch.setattr(
            "cave_catalog.routers.tables.run_validation_pipeline",
            AsyncMock(return_value=_passing_report()),
        )
        mock_extractor = AsyncMock()
        mock_extractor.extract.return_value = _mock_metadata()
        monkeypatch.setattr(
            "cave_catalog.routers.tables.get_extractor", lambda fmt: mock_extractor
        )

        resp = await client.post(
            "/ui/register/submit",
            data={
                "uri": "gs://bucket/table",
                "format": "delta",
                "name": "test_table",
                "mat_version": "",
                "n_columns": "2",
                "col_name_0": "id",
                "col_dtype_0": "int64",
                "col_desc_0": "Primary key",
                "col_name_1": "value",
                "col_dtype_1": "float64",
                "col_desc_1": "",
            },
            cookies={"cave_catalog_datastack": "minnie65_public"},
        )
        assert resp.status_code == 200
        assert "successfully" in resp.text.lower() or "register_success" in resp.url.path if hasattr(resp, 'url') else True
        assert "test_table" in resp.text

    async def test_missing_fields(self, client, monkeypatch):
        """Submit without required fields returns error."""
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        from cave_catalog.config import get_settings

        get_settings.cache_clear()

        resp = await client.post(
            "/ui/register/submit",
            data={"uri": "", "format": "delta", "name": ""},
            cookies={"cave_catalog_datastack": "minnie65_public"},
        )
        assert resp.status_code == 200
        assert "required" in resp.text.lower()

    async def test_validation_failure_shows_error(self, client, monkeypatch):
        """Validation failure renders error fragment with details."""
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        from cave_catalog.config import get_settings

        get_settings.cache_clear()

        # Make validation fail
        failing_report = _passing_report()
        failing_report.uri_reachable = ValidationCheck(
            passed=False, message="URI not reachable"
        )
        monkeypatch.setattr(
            "cave_catalog.routers.tables.run_validation_pipeline",
            AsyncMock(return_value=failing_report),
        )

        resp = await client.post(
            "/ui/register/submit",
            data={
                "uri": "gs://bad/path",
                "format": "delta",
                "name": "fail_table",
                "n_columns": "0",
            },
            cookies={"cave_catalog_datastack": "minnie65_public"},
        )
        assert resp.status_code == 200
        # Should show an error message
        assert "failed" in resp.text.lower() or "error" in resp.text.lower()
