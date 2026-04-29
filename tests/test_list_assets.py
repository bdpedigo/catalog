"""Tests for asset list pagination, filtering, sorting, and PATCH endpoint."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

from cave_catalog.schemas import ValidationCheck, ValidationReport

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _asset_payload(**overrides: Any) -> dict:
    base = {
        "datastack": "minnie65_public",
        "name": "synapses",
        "mat_version": 943,
        "revision": 0,
        "uri": "gs://bucket/minnie65/synapses/",
        "format": "delta",
        "asset_type": "table",
        "is_managed": True,
        "mutability": "static",
        "maturity": "stable",
        "properties": {},
    }
    base.update(overrides)
    return base


def _passing_report() -> ValidationReport:
    ok = ValidationCheck(passed=True)
    return ValidationReport(
        name_reservation_check=ok,
        uri_reachable=ok,
        format_sniff=ok,
    )


def _patch_validation(monkeypatch) -> None:
    monkeypatch.setattr(
        "cave_catalog.routers.assets.run_validation_pipeline",
        AsyncMock(return_value=_passing_report()),
    )


async def _register(client, monkeypatch, **overrides) -> dict:
    _patch_validation(monkeypatch)
    resp = await client.post(
        "/api/v1/assets/register", json=_asset_payload(**overrides)
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


async def _seed_assets(client, monkeypatch):
    """Register a handful of assets for list/filter/sort testing."""
    assets = []
    assets.append(
        await _register(
            client, monkeypatch, name="synapse_table", mat_version=1078, format="delta"
        )
    )
    assets.append(
        await _register(
            client,
            monkeypatch,
            name="nucleus_detection",
            mat_version=1078,
            format="delta",
        )
    )
    assets.append(
        await _register(
            client,
            monkeypatch,
            name="my_upload",
            mat_version=None,
            format="parquet",
            maturity="draft",
        )
    )
    assets.append(
        await _register(
            client,
            monkeypatch,
            name="synapse_partners",
            mat_version=1077,
            format="delta",
        )
    )
    assets.append(
        await _register(
            client,
            monkeypatch,
            name="precomputed_mesh",
            mat_version=None,
            format="precomputed",
            asset_type="asset",
            revision=1,
        )
    )
    return assets


# ---------------------------------------------------------------------------
# Pagination tests
# ---------------------------------------------------------------------------


async def test_list_no_limit_returns_all(client, monkeypatch):
    await _seed_assets(client, monkeypatch)
    resp = await client.get("/api/v1/assets/", params={"datastack": "minnie65_public"})
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 5
    # No X-Total-Count header when limit is not provided
    assert "X-Total-Count" not in resp.headers


async def test_list_with_limit(client, monkeypatch):
    await _seed_assets(client, monkeypatch)
    resp = await client.get(
        "/api/v1/assets/", params={"datastack": "minnie65_public", "limit": 2}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert resp.headers["X-Total-Count"] == "5"


async def test_list_with_limit_and_offset(client, monkeypatch):
    await _seed_assets(client, monkeypatch)
    resp = await client.get(
        "/api/v1/assets/",
        params={"datastack": "minnie65_public", "limit": 2, "offset": 3},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert resp.headers["X-Total-Count"] == "5"


async def test_list_offset_beyond_total(client, monkeypatch):
    await _seed_assets(client, monkeypatch)
    resp = await client.get(
        "/api/v1/assets/",
        params={"datastack": "minnie65_public", "limit": 10, "offset": 100},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 0
    assert resp.headers["X-Total-Count"] == "5"


# ---------------------------------------------------------------------------
# Substring filter tests
# ---------------------------------------------------------------------------


async def test_name_contains_filter(client, monkeypatch):
    await _seed_assets(client, monkeypatch)
    resp = await client.get(
        "/api/v1/assets/",
        params={"datastack": "minnie65_public", "name_contains": "synapse"},
    )
    assert resp.status_code == 200
    data = resp.json()
    # Should match "synapse_table" and "synapse_partners"
    assert len(data) == 2
    assert all("synapse" in d["name"] for d in data)


async def test_name_contains_case_insensitive(client, monkeypatch):
    await _seed_assets(client, monkeypatch)
    resp = await client.get(
        "/api/v1/assets/",
        params={"datastack": "minnie65_public", "name_contains": "SYNAPSE"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2


async def test_name_contains_combined_with_format(client, monkeypatch):
    await _seed_assets(client, monkeypatch)
    resp = await client.get(
        "/api/v1/assets/",
        params={
            "datastack": "minnie65_public",
            "name_contains": "synapse",
            "format": "delta",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert all(d["format"] == "delta" for d in data)


# ---------------------------------------------------------------------------
# Sorting tests
# ---------------------------------------------------------------------------


async def test_sort_by_name_asc(client, monkeypatch):
    await _seed_assets(client, monkeypatch)
    resp = await client.get(
        "/api/v1/assets/",
        params={"datastack": "minnie65_public", "sort_by": "name", "sort_order": "asc"},
    )
    assert resp.status_code == 200
    data = resp.json()
    names = [d["name"] for d in data]
    assert names == sorted(names)


async def test_sort_by_name_desc(client, monkeypatch):
    await _seed_assets(client, monkeypatch)
    resp = await client.get(
        "/api/v1/assets/",
        params={
            "datastack": "minnie65_public",
            "sort_by": "name",
            "sort_order": "desc",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    names = [d["name"] for d in data]
    assert names == sorted(names, reverse=True)


async def test_sort_by_mat_version_nulls_first(client, monkeypatch):
    await _seed_assets(client, monkeypatch)
    resp = await client.get(
        "/api/v1/assets/",
        params={
            "datastack": "minnie65_public",
            "sort_by": "mat_version",
            "sort_order": "asc",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    mat_versions = [d["mat_version"] for d in data]
    # NULLs first
    null_count = sum(1 for v in mat_versions if v is None)
    assert null_count == 2
    # First entries are None
    for v in mat_versions[:null_count]:
        assert v is None
    # Remaining are sorted ascending
    non_null = [v for v in mat_versions if v is not None]
    assert non_null == sorted(non_null)


async def test_sort_invalid_column_falls_back_to_name(client, monkeypatch):
    await _seed_assets(client, monkeypatch)
    resp = await client.get(
        "/api/v1/assets/",
        params={
            "datastack": "minnie65_public",
            "sort_by": "nonexistent_column",
            "sort_order": "asc",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    names = [d["name"] for d in data]
    assert names == sorted(names)


# ---------------------------------------------------------------------------
# PATCH endpoint tests
# ---------------------------------------------------------------------------


async def test_patch_update_maturity(client, monkeypatch):
    asset = await _register(client, monkeypatch, name="test_asset")
    asset_id = asset["id"]
    assert asset["maturity"] == "stable"

    resp = await client.patch(
        f"/api/v1/assets/{asset_id}", json={"maturity": "deprecated"}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["maturity"] == "deprecated"
    # Other fields unchanged
    assert data["name"] == "test_asset"


async def test_patch_update_access_group(client, monkeypatch):
    asset = await _register(client, monkeypatch, name="test_asset")
    asset_id = asset["id"]

    resp = await client.patch(
        f"/api/v1/assets/{asset_id}", json={"access_group": "team-alpha"}
    )
    assert resp.status_code == 200
    assert resp.json()["access_group"] == "team-alpha"


async def test_patch_set_access_group_to_null(client, monkeypatch):
    asset = await _register(client, monkeypatch, name="test_asset")
    asset_id = asset["id"]

    # Set it first
    await client.patch(
        f"/api/v1/assets/{asset_id}", json={"access_group": "team-alpha"}
    )
    # Clear it
    resp = await client.patch(f"/api/v1/assets/{asset_id}", json={"access_group": None})
    assert resp.status_code == 200
    assert resp.json()["access_group"] is None


async def test_patch_update_expires_at(client, monkeypatch):
    asset = await _register(client, monkeypatch, name="test_asset")
    asset_id = asset["id"]

    resp = await client.patch(
        f"/api/v1/assets/{asset_id}",
        json={"expires_at": "2026-12-31T00:00:00Z"},
    )
    assert resp.status_code == 200
    assert "2026-12-31" in resp.json()["expires_at"]


async def test_patch_multiple_fields(client, monkeypatch):
    asset = await _register(client, monkeypatch, name="test_asset")
    asset_id = asset["id"]

    resp = await client.patch(
        f"/api/v1/assets/{asset_id}",
        json={"maturity": "draft", "access_group": "beta"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["maturity"] == "draft"
    assert data["access_group"] == "beta"


async def test_patch_empty_body_no_change(client, monkeypatch):
    asset = await _register(client, monkeypatch, name="test_asset")
    asset_id = asset["id"]

    resp = await client.patch(f"/api/v1/assets/{asset_id}", json={})
    assert resp.status_code == 200
    assert resp.json()["maturity"] == "stable"


async def test_patch_nonexistent_asset_404(client, monkeypatch):
    fake_id = "00000000-0000-0000-0000-000000000000"
    resp = await client.patch(f"/api/v1/assets/{fake_id}", json={"maturity": "draft"})
    assert resp.status_code == 404
