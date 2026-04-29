"""Tests for the name availability check endpoint and UI fragment."""

from __future__ import annotations

from unittest.mock import AsyncMock


from cave_catalog.schemas import ValidationCheck


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _asset_payload(**overrides):
    base = {
        "datastack": "minnie65_public",
        "name": "my_table",
        "mat_version": 943,
        "revision": 0,
        "uri": "gs://bucket/data/",
        "format": "delta",
        "asset_type": "table",
        "is_managed": True,
        "mutability": "static",
        "maturity": "stable",
        "properties": {},
    }
    base.update(overrides)
    return base


def _passing_report():
    from cave_catalog.schemas import ValidationReport

    return ValidationReport(
        auth_check=ValidationCheck(passed=True),
        duplicate_check=ValidationCheck(passed=True),
        name_reservation_check=ValidationCheck(passed=True),
        uri_reachable=ValidationCheck(passed=True),
        format_sniff=ValidationCheck(passed=True),
    )


# ---------------------------------------------------------------------------
# API endpoint tests: GET /api/v1/assets/check-name
# ---------------------------------------------------------------------------


class TestCheckNameAPI:
    async def test_name_available(self, client, monkeypatch):
        """Name that is not reserved and has no duplicate returns available."""
        monkeypatch.setattr(
            "cave_catalog.routers.assets._check_name_reservation",
            AsyncMock(return_value=ValidationCheck(passed=True)),
        )
        resp = await client.get(
            "/api/v1/assets/check-name",
            params={"datastack": "minnie65_public", "name": "new_table"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] is True

    async def test_name_reserved(self, client, monkeypatch):
        """Name that matches a mat table returns reserved."""
        monkeypatch.setattr(
            "cave_catalog.routers.assets._check_name_reservation",
            AsyncMock(return_value=ValidationCheck(passed=False, message="reserved")),
        )
        resp = await client.get(
            "/api/v1/assets/check-name",
            params={"datastack": "minnie65_public", "name": "synapses"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] is False
        assert data["reason"] == "reserved"

    async def test_name_duplicate(self, client, monkeypatch):
        """Name that already exists as an asset returns duplicate."""
        # Bypass validation for register
        monkeypatch.setattr(
            "cave_catalog.routers.assets.run_validation_pipeline",
            AsyncMock(return_value=_passing_report()),
        )
        # Register an asset first
        await client.post(
            "/api/v1/assets/register",
            json=_asset_payload(name="taken_name"),
        )

        # Now check name — reservation passes but duplicate exists
        monkeypatch.setattr(
            "cave_catalog.routers.assets._check_name_reservation",
            AsyncMock(return_value=ValidationCheck(passed=True)),
        )
        resp = await client.get(
            "/api/v1/assets/check-name",
            params={
                "datastack": "minnie65_public",
                "name": "taken_name",
                "mat_version": 943,
                "revision": 0,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] is False
        assert data["reason"] == "duplicate"
        assert "existing_id" in data


# ---------------------------------------------------------------------------
# UI fragment tests: GET /ui/fragments/check-name
# ---------------------------------------------------------------------------


class TestCheckNameFragment:
    async def test_empty_name_returns_empty(self, client):
        """Empty name returns empty response."""
        resp = await client.get(
            "/ui/fragments/check-name",
            params={"name": ""},
        )
        assert resp.status_code == 200
        assert resp.text == ""

    async def test_available_name_shows_check(self, client, monkeypatch):
        """Available name returns ✓ fragment."""
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        from cave_catalog.config import get_settings

        get_settings.cache_clear()

        monkeypatch.setattr(
            "cave_catalog.routers.ui.check_name_reservation",
            AsyncMock(return_value=ValidationCheck(passed=True)),
        )
        resp = await client.get(
            "/ui/fragments/check-name",
            params={"name": "new_table"},
            cookies={"cave_catalog_datastack": "minnie65_public"},
        )
        assert resp.status_code == 200
        assert "&#10003;" in resp.text or "✓" in resp.text
        assert "Available" in resp.text

    async def test_reserved_name_shows_x(self, client, monkeypatch):
        """Reserved name returns ✗ fragment."""
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        from cave_catalog.config import get_settings

        get_settings.cache_clear()

        monkeypatch.setattr(
            "cave_catalog.routers.ui.check_name_reservation",
            AsyncMock(return_value=ValidationCheck(passed=False, message="reserved")),
        )
        resp = await client.get(
            "/ui/fragments/check-name",
            params={"name": "synapses"},
            cookies={"cave_catalog_datastack": "minnie65_public"},
        )
        assert resp.status_code == 200
        assert "&#10007;" in resp.text or "✗" in resp.text
        assert "reserved" in resp.text.lower()


# ---------------------------------------------------------------------------
# Name format validation tests
# ---------------------------------------------------------------------------

import pytest

from cave_catalog.validation import validate_asset_name


class TestValidateAssetName:
    @pytest.mark.parametrize(
        "name",
        [
            "my_table",
            "synapses",
            "abc_123",
            "a",
            "synapses_v4",
            "my_table.by_pre_root",
            "x.y",
        ],
    )
    def test_valid_names(self, name):
        assert validate_asset_name(name) == name

    @pytest.mark.parametrize(
        "name",
        [
            "MyTable",
            "123_table",
            "_hidden",
            "my-table",
            "my table",
            "",
            "my_table.",
            ".suffix",
            "a.b.c",
            "UPPER",
            "has Space",
        ],
    )
    def test_invalid_names(self, name):
        with pytest.raises(ValueError):
            validate_asset_name(name)


class TestCheckNameFormatAPI:
    """Format validation in GET /api/v1/assets/check-name."""

    async def test_invalid_format_returns_unavailable(self, client):
        resp = await client.get(
            "/api/v1/assets/check-name",
            params={"datastack": "minnie65_public", "name": "Bad-Name"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] is False
        assert data["reason"] == "invalid_format"

    async def test_valid_format_proceeds_to_reservation(self, client, monkeypatch):
        monkeypatch.setattr(
            "cave_catalog.routers.assets._check_name_reservation",
            AsyncMock(return_value=ValidationCheck(passed=True)),
        )
        resp = await client.get(
            "/api/v1/assets/check-name",
            params={"datastack": "minnie65_public", "name": "good_name"},
        )
        assert resp.status_code == 200
        assert resp.json()["available"] is True


class TestCheckNameFormatFragment:
    """Format validation in GET /ui/fragments/check-name."""

    async def test_invalid_format_shows_error(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        from cave_catalog.config import get_settings

        get_settings.cache_clear()

        resp = await client.get(
            "/ui/fragments/check-name",
            params={"name": "Bad-Name"},
            cookies={"cave_catalog_datastack": "minnie65_public"},
        )
        assert resp.status_code == 200
        assert "&#10007;" in resp.text or "✗" in resp.text
        assert "invalid" in resp.text.lower() or "must be" in resp.text.lower()
