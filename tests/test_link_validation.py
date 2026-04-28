"""Tests for column link validation against the materialization service.

Phase 3 tests — covers task 3.1 (column link validator).
Uses httpx mocking to simulate ME API responses.
"""

from __future__ import annotations

import httpx
import pytest
from cave_catalog.validation import (
    LinkValidationError,
    LinkValidationResult,
    validate_column_links,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _annotations_with_links(*links: tuple[str, str, str, str]) -> list[dict]:
    """Build annotations list from (col_name, link_type, target_table, target_col) tuples."""
    by_col: dict[str, list[dict]] = {}
    for col_name, link_type, target_table, target_col in links:
        by_col.setdefault(col_name, []).append(
            {
                "link_type": link_type,
                "target_table": target_table,
                "target_column": target_col,
            }
        )
    return [
        {"column_name": col, "links": col_links}
        for col, col_links in by_col.items()
    ]


def _mock_transport(status_code: int = 200, json_body: list | None = None):
    """Return an httpx transport that returns a canned response."""

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json_body if json_body is not None else []
        return httpx.Response(status_code, json=body)

    return httpx.MockTransport(handler)


# ---------------------------------------------------------------------------
# No links → passes trivially
# ---------------------------------------------------------------------------


async def test_no_links_passes():
    annotations = [{"column_name": "a", "description": "col a"}]
    async with httpx.AsyncClient() as client:
        result = await validate_column_links(annotations, "ds1", client)
    assert result.passed is True
    assert result.errors == []


async def test_empty_annotations_passes():
    async with httpx.AsyncClient() as client:
        result = await validate_column_links([], "ds1", client)
    assert result.passed is True


# ---------------------------------------------------------------------------
# ME not configured → skipped
# ---------------------------------------------------------------------------


async def test_skipped_when_me_not_configured(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_links(
        ("col_a", "foreign_key", "synapses", "id"),
    )
    async with httpx.AsyncClient() as client:
        result = await validate_column_links(annotations, "ds1", client)

    assert result.passed is True
    assert result.skipped is True
    assert "not configured" in result.message

    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Valid links → passes
# ---------------------------------------------------------------------------


async def test_valid_table_link_passes(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_links(
        ("pre_pt_root_id", "foreign_key", "synapses", "pre_pt_root_id"),
        ("post_pt_root_id", "foreign_key", "synapses", "post_pt_root_id"),
    )
    transport = _mock_transport(200, ["synapses", "nucleus_detection_v0"])
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_links(annotations, "minnie65", client)

    assert result.passed is True
    assert result.errors == []

    get_settings.cache_clear()


async def test_multiple_tables_all_valid(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_links(
        ("syn_id", "foreign_key", "synapses", "id"),
        ("cell_id", "foreign_key", "nucleus_detection_v0", "id"),
    )
    transport = _mock_transport(200, ["synapses", "nucleus_detection_v0"])
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_links(annotations, "minnie65", client)

    assert result.passed is True

    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Invalid links → fails
# ---------------------------------------------------------------------------


async def test_invalid_table_fails(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_links(
        ("col_a", "foreign_key", "nonexistent_table", "id"),
    )
    transport = _mock_transport(200, ["synapses"])
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_links(annotations, "minnie65", client)

    assert result.passed is False
    assert len(result.errors) == 1
    err = result.errors[0]
    assert err.target_table == "nonexistent_table"
    assert err.column_name == "col_a"
    assert "not found" in err.reason

    get_settings.cache_clear()


async def test_mixed_valid_and_invalid(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_links(
        ("good_col", "foreign_key", "synapses", "id"),
        ("bad_col", "foreign_key", "fake_table", "id"),
    )
    transport = _mock_transport(200, ["synapses"])
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_links(annotations, "minnie65", client)

    assert result.passed is False
    assert len(result.errors) == 1
    assert result.errors[0].target_table == "fake_table"

    get_settings.cache_clear()


async def test_multiple_links_to_same_bad_table(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_links(
        ("col_a", "foreign_key", "bad_table", "id"),
        ("col_b", "derived_from", "bad_table", "value"),
    )
    transport = _mock_transport(200, ["synapses"])
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_links(annotations, "minnie65", client)

    assert result.passed is False
    assert len(result.errors) == 2

    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# ME errors → graceful skip
# ---------------------------------------------------------------------------


async def test_me_auth_failure_skips(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_links(
        ("col_a", "foreign_key", "synapses", "id"),
    )
    transport = _mock_transport(403)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_links(annotations, "minnie65", client)

    assert result.passed is True
    assert result.skipped is True
    assert "auth failed" in result.message

    get_settings.cache_clear()


async def test_me_server_error_skips(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_links(
        ("col_a", "foreign_key", "synapses", "id"),
    )
    transport = _mock_transport(500)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_links(annotations, "minnie65", client)

    assert result.passed is True
    assert result.skipped is True

    get_settings.cache_clear()


async def test_me_connection_error_skips(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_links(
        ("col_a", "foreign_key", "synapses", "id"),
    )

    async def _raise(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    transport = httpx.MockTransport(_raise)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_links(annotations, "minnie65", client)

    assert result.passed is True
    assert result.skipped is True

    get_settings.cache_clear()
