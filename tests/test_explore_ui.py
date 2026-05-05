"""Tests for the explore assets UI routes."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

from cave_catalog.schemas import ValidationCheck, ValidationReport


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
    base = {
        "datastack": "minnie65_public",
        "name": "test_table",
        "mat_version": 1078,
        "revision": 0,
        "uri": f"gs://bucket/{uuid.uuid4()}",
        "format": "delta",
        "asset_type": "table",
        "is_managed": True,
        "mutability": "static",
        "maturity": "stable",
        "properties": {},
    }
    base.update(overrides)
    resp = await client.post("/api/v1/assets/register", json=base)
    assert resp.status_code == 201, resp.text
    return resp.json()


class TestExplorePage:
    """Tests for GET /ui/explore."""

    async def test_explore_page_renders(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        await _register(client, monkeypatch, name="synapse_table")

        resp = await client.get("/ui/explore")
        assert resp.status_code == 200
        assert "Explore Assets" in resp.text
        assert "synapse_table" in resp.text

    async def test_explore_page_empty_datastack(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")

        resp = await client.get("/ui/explore")
        assert resp.status_code == 200
        assert "No assets found" in resp.text

    async def test_explore_page_shows_column_toggles(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")

        resp = await client.get("/ui/explore")
        assert resp.status_code == 200
        assert "data-col-toggle" in resp.text

    async def test_explore_page_shows_filter_bar(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")

        resp = await client.get("/ui/explore")
        assert resp.status_code == 200
        assert "filter-form" in resp.text


class TestExploreFragment:
    """Tests for GET /ui/fragments/assets."""

    async def test_fragment_returns_table_rows(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        await _register(client, monkeypatch, name="synapse_table")
        await _register(client, monkeypatch, name="nucleus_det", mat_version=1077)

        resp = await client.get(
            "/ui/fragments/assets", params={"limit": 25, "offset": 0}
        )
        assert resp.status_code == 200
        assert "synapse_table" in resp.text
        assert "nucleus_det" in resp.text

    async def test_fragment_respects_name_filter(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        await _register(client, monkeypatch, name="synapse_table")
        await _register(client, monkeypatch, name="nucleus_det", mat_version=1077)

        resp = await client.get(
            "/ui/fragments/assets", params={"name": "synapse", "limit": 25}
        )
        assert resp.status_code == 200
        assert "synapse_table" in resp.text
        assert "nucleus_det" not in resp.text

    async def test_fragment_respects_format_filter(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        await _register(client, monkeypatch, name="delta_table", format="delta")
        await _register(
            client, monkeypatch, name="parquet_table", format="parquet", mat_version=999
        )

        resp = await client.get(
            "/ui/fragments/assets", params={"format": "parquet", "limit": 25}
        )
        assert resp.status_code == 200
        assert "parquet_table" in resp.text
        assert "delta_table" not in resp.text

    async def test_fragment_pagination(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        await _register(client, monkeypatch, name="table_a", mat_version=1)
        await _register(client, monkeypatch, name="table_b", mat_version=2)
        await _register(client, monkeypatch, name="table_c", mat_version=3)

        resp = await client.get(
            "/ui/fragments/assets", params={"limit": 2, "offset": 0}
        )
        assert resp.status_code == 200
        assert "Page 1 of 2" in resp.text

    async def test_fragment_sort_order(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        await _register(client, monkeypatch, name="aaa_first", mat_version=1)
        await _register(client, monkeypatch, name="zzz_last", mat_version=2)

        resp = await client.get(
            "/ui/fragments/assets",
            params={"sort_by": "name", "sort_order": "desc", "limit": 25},
        )
        assert resp.status_code == 200
        # zzz should appear before aaa in the response
        text = resp.text
        assert text.index("zzz_last") < text.index("aaa_first")


class TestDetailPage:
    """Tests for GET /ui/explore/{id}."""

    async def test_detail_page_table_asset(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        asset = await _register(client, monkeypatch, name="synapse_table")

        resp = await client.get(f"/ui/explore/{asset['id']}")
        assert resp.status_code == 200
        assert "synapse_table" in resp.text
        assert "Summary" in resp.text
        assert "Cached Metadata" in resp.text
        assert "Columns" in resp.text

    async def test_detail_page_non_table_asset(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        asset = await _register(
            client, monkeypatch, name="generic_file", asset_type="asset"
        )

        resp = await client.get(f"/ui/explore/{asset['id']}")
        assert resp.status_code == 200
        assert "generic_file" in resp.text
        assert "Summary" in resp.text
        # Table-specific sections should not appear
        assert "Cached Metadata" not in resp.text

    async def test_detail_page_404_invalid_id(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        fake_id = "00000000-0000-0000-0000-000000000000"
        resp = await client.get(f"/ui/explore/{fake_id}")
        assert resp.status_code == 404

    async def test_detail_page_shows_edit_button(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        asset = await _register(client, monkeypatch, name="editable_table")

        resp = await client.get(f"/ui/explore/{asset['id']}")
        assert resp.status_code == 200
        assert "/edit" in resp.text

    async def test_detail_page_shows_column_kind(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")

        from cave_catalog.table_schemas import ColumnInfo, TableMetadata

        # Mock extractor so cached metadata includes the annotated columns
        meta = TableMetadata(
            n_rows=50,
            n_columns=2,
            n_bytes=1000,
            columns=[
                ColumnInfo(name="pt_root_id", dtype="int64"),
                ColumnInfo(name="pre_pt_root_id", dtype="int64"),
            ],
            partition_columns=[],
        )
        mock_extractor = AsyncMock()
        mock_extractor.extract = AsyncMock(return_value=meta)
        monkeypatch.setattr(
            "cave_catalog.routers.tables.get_extractor",
            lambda fmt: mock_extractor,
        )
        _patch_validation(monkeypatch)
        monkeypatch.setattr(
            "cave_catalog.routers.tables.run_validation_pipeline",
            AsyncMock(return_value=_passing_report()),
        )

        payload = {
            "datastack": "minnie65_public",
            "name": "kind_display_table",
            "revision": 0,
            "uri": "gs://bucket/kind_display_table/",
            "format": "delta",
            "is_managed": True,
            "mutability": "static",
            "maturity": "stable",
            "properties": {},
            "column_annotations": [
                {
                    "column_name": "pt_root_id",
                    "description": "Root ID",
                    "kind": {"kind": "segmentation", "node_level": "root_id"},
                },
                {
                    "column_name": "pre_pt_root_id",
                    "description": "Pre-synaptic",
                    "kind": {
                        "kind": "materialization",
                        "target_table": "nucleus_detection_v0",
                        "target_column": "id",
                    },
                },
            ],
        }
        resp = await client.post("/api/v1/tables/register", json=payload)
        assert resp.status_code == 201, resp.text
        asset = resp.json()

        resp = await client.get(f"/ui/explore/{asset['id']}")
        assert resp.status_code == 200
        assert "badge-seg" in resp.text
        assert "root_id" in resp.text
        assert "badge-mat" in resp.text
        assert "nucleus_detection_v0" in resp.text


class TestEditPage:
    """Tests for GET/POST /ui/explore/{id}/edit."""

    async def test_edit_page_renders_form(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        asset = await _register(client, monkeypatch, name="edit_form_table")

        resp = await client.get(f"/ui/explore/{asset['id']}/edit")
        assert resp.status_code == 200
        assert "edit_form_table" in resp.text
        assert 'name="maturity"' in resp.text
        assert 'name="access_group"' in resp.text
        assert 'name="expires_at"' in resp.text

    async def test_edit_saves_maturity_change(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        asset = await _register(client, monkeypatch, name="mat_change_table")

        resp = await client.post(
            f"/ui/explore/{asset['id']}/edit",
            data={"maturity": "deprecated", "access_group": "", "expires_at": ""},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert f"/ui/explore/{asset['id']}" in resp.headers["location"]

        # Verify the change persisted
        get_resp = await client.get(f"/api/v1/assets/{asset['id']}")
        assert get_resp.json()["maturity"] == "deprecated"

    async def test_edit_saves_access_group(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        asset = await _register(client, monkeypatch, name="access_grp_table")

        resp = await client.post(
            f"/ui/explore/{asset['id']}/edit",
            data={"maturity": "stable", "access_group": "team-alpha", "expires_at": ""},
            follow_redirects=False,
        )
        assert resp.status_code == 303

        get_resp = await client.get(f"/api/v1/assets/{asset['id']}")
        assert get_resp.json()["access_group"] == "team-alpha"

    async def test_edit_clears_access_group(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        asset = await _register(client, monkeypatch, name="clear_grp_table")

        # First set an access group
        await client.post(
            f"/ui/explore/{asset['id']}/edit",
            data={"maturity": "stable", "access_group": "team-beta", "expires_at": ""},
            follow_redirects=False,
        )
        # Now clear it
        resp = await client.post(
            f"/ui/explore/{asset['id']}/edit",
            data={"maturity": "stable", "access_group": "", "expires_at": ""},
            follow_redirects=False,
        )
        assert resp.status_code == 303

        get_resp = await client.get(f"/api/v1/assets/{asset['id']}")
        assert get_resp.json()["access_group"] is None

    async def test_edit_page_404_invalid_id(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        fake_id = "00000000-0000-0000-0000-000000000000"
        resp = await client.get(f"/ui/explore/{fake_id}/edit")
        assert resp.status_code == 404

    async def test_edit_post_invalid_maturity_shows_error(self, client, monkeypatch):
        monkeypatch.setenv("DATASTACKS", "minnie65_public")
        asset = await _register(client, monkeypatch, name="bad_mat_table")

        resp = await client.post(
            f"/ui/explore/{asset['id']}/edit",
            data={"maturity": "invalid_value", "access_group": "", "expires_at": ""},
        )
        # Should re-render the form with error
        assert resp.status_code == 422
        assert "bad_mat_table" in resp.text
