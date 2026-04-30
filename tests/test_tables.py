"""Tests for table endpoints and column merging.

Phase 4 tests — covers tasks 4.1–4.8.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock

from cave_catalog.table_schemas import (
    ColumnAnnotation,
    ColumnInfo,
    MatKind,
    MergedColumn,
    TableMetadata,
    merge_columns,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DELTA_METADATA = TableMetadata(
    n_rows=100,
    n_columns=3,
    n_bytes=5000,
    columns=[
        ColumnInfo(name="a", dtype="int64"),
        ColumnInfo(name="b", dtype="string"),
        ColumnInfo(name="c", dtype="float64"),
    ],
    partition_columns=[],
)


def _table_payload(**overrides: Any) -> dict:
    base = {
        "datastack": "minnie65_public",
        "name": "my_table",
        "revision": 0,
        "uri": f"gs://bucket/tables/{uuid.uuid4()}/",
        "format": "delta",
        "is_managed": True,
        "mutability": "static",
        "maturity": "stable",
        "properties": {},
    }
    base.update(overrides)
    return base


def _preview_payload(**overrides: Any) -> dict:
    base = {
        "uri": "gs://bucket/tables/my_table/",
        "format": "delta",
        "datastack": "minnie65_public",
    }
    base.update(overrides)
    return base


def _patch_extraction(monkeypatch, metadata: TableMetadata | None = None):
    """Patch the extractor registry so extract() returns canned metadata."""
    meta = metadata or _DELTA_METADATA
    mock_extractor = AsyncMock()
    mock_extractor.extract = AsyncMock(return_value=meta)
    monkeypatch.setattr(
        "cave_catalog.routers.tables.get_extractor",
        lambda fmt: mock_extractor,
    )


def _patch_validation(monkeypatch):
    """Patch the validation pipeline to always pass."""
    from cave_catalog.schemas import ValidationCheck, ValidationReport

    ok = ValidationCheck(passed=True)
    report = ValidationReport(
        name_reservation_check=ok, uri_reachable=ok, format_sniff=ok
    )
    monkeypatch.setattr(
        "cave_catalog.routers.tables.run_validation_pipeline",
        AsyncMock(return_value=report),
    )


def _patch_kind_validation(monkeypatch, passed: bool = True, errors=None):
    """Patch column kind validation."""
    from cave_catalog.validation import KindValidationResult

    result = KindValidationResult(passed=passed, errors=errors or [])
    monkeypatch.setattr(
        "cave_catalog.routers.tables.validate_column_kinds",
        AsyncMock(return_value=result),
    )


async def _register_table(client, monkeypatch, **overrides) -> dict:
    """Register a table and return the response JSON."""
    _patch_validation(monkeypatch)
    _patch_extraction(monkeypatch)
    _patch_kind_validation(monkeypatch)
    resp = await client.post(
        "/api/v1/tables/register", json=_table_payload(**overrides)
    )
    assert resp.status_code == 201, resp.text
    return resp.json()


# ---------------------------------------------------------------------------
# 4.7 Column merging
# ---------------------------------------------------------------------------


def test_merge_no_metadata():
    result = merge_columns(None, [])
    assert result == []


def test_merge_no_annotations():
    result = merge_columns(_DELTA_METADATA, [])
    assert len(result) == 3
    assert all(isinstance(c, MergedColumn) for c in result)
    assert all(c.description is None for c in result)
    assert all(c.kind is None for c in result)


def test_merge_with_annotations():
    anns = [
        ColumnAnnotation(column_name="a", description="Column A"),
        ColumnAnnotation(
            column_name="b",
            description="Column B",
            kind=MatKind(target_table="t", target_column="c"),
        ),
    ]
    result = merge_columns(_DELTA_METADATA, anns)
    by_name = {c.name: c for c in result}

    assert by_name["a"].description == "Column A"
    assert by_name["b"].kind is not None
    assert by_name["b"].kind.kind == "materialization"
    assert by_name["c"].description is None  # unannotated


def test_merge_orphaned_annotation_dropped():
    """Annotations for columns not in metadata are silently dropped."""
    anns = [ColumnAnnotation(column_name="nonexistent", description="Orphan")]
    result = merge_columns(_DELTA_METADATA, anns)
    names = [c.name for c in result]
    assert "nonexistent" not in names
    assert len(result) == 3


# ---------------------------------------------------------------------------
# 4.2 Preview endpoint
# ---------------------------------------------------------------------------


async def test_preview_success(client, monkeypatch):
    _patch_extraction(monkeypatch)

    resp = await client.post("/api/v1/tables/preview", json=_preview_payload())
    assert resp.status_code == 200

    data = resp.json()
    assert data["metadata"]["n_rows"] == 100
    assert len(data["metadata"]["columns"]) == 3


async def test_preview_unsupported_format(client, monkeypatch):
    monkeypatch.setattr(
        "cave_catalog.routers.tables.get_extractor",
        lambda fmt: (_ for _ in ()).throw(ValueError(f"No extractor for '{fmt}'")),
    )
    resp = await client.post(
        "/api/v1/tables/preview", json=_preview_payload(format="lance")
    )
    assert resp.status_code == 422


async def test_preview_extraction_failure(client, monkeypatch):
    mock_ext = AsyncMock()
    mock_ext.extract = AsyncMock(side_effect=Exception("read failed"))
    monkeypatch.setattr(
        "cave_catalog.routers.tables.get_extractor", lambda fmt: mock_ext
    )

    resp = await client.post("/api/v1/tables/preview", json=_preview_payload())
    assert resp.status_code == 422
    assert "extraction failed" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# 4.3 Registration endpoint
# ---------------------------------------------------------------------------


async def test_register_table_success(client, monkeypatch):
    data = await _register_table(client, monkeypatch)

    assert data["name"] == "my_table"
    assert data["asset_type"] == "table"
    assert data["source"] == "user"
    assert data["cached_metadata"]["n_rows"] == 100
    assert len(data["columns"]) == 3  # merged columns


async def test_register_table_with_annotations(client, monkeypatch):
    annotations = [
        {"column_name": "a", "description": "Col A"},
    ]
    data = await _register_table(client, monkeypatch, column_annotations=annotations)

    assert data["column_annotations"][0]["column_name"] == "a"
    # Merged column should have the annotation
    by_name = {c["name"]: c for c in data["columns"]}
    assert by_name["a"]["description"] == "Col A"


async def test_register_table_with_segmentation_kind(client, monkeypatch):
    """Segmentation kind round-trip — no ME validation."""
    annotations = [
        {
            "column_name": "a",
            "kind": {"kind": "segmentation", "node_level": "root_id"},
        }
    ]
    data = await _register_table(client, monkeypatch, column_annotations=annotations)

    ann = data["column_annotations"][0]
    assert ann["kind"]["kind"] == "segmentation"
    assert ann["kind"]["node_level"] == "root_id"
    # Merged column should carry the kind
    by_name = {c["name"]: c for c in data["columns"]}
    assert by_name["a"]["kind"]["kind"] == "segmentation"


async def test_register_table_with_split_point_kind(client, monkeypatch):
    """Split point kind round-trip with point_group."""
    annotations = [
        {
            "column_name": "c",
            "kind": {"kind": "split_point", "axis": "x", "point_group": "pt_position"},
        }
    ]
    data = await _register_table(client, monkeypatch, column_annotations=annotations)

    ann = data["column_annotations"][0]
    assert ann["kind"]["kind"] == "split_point"
    assert ann["kind"]["axis"] == "x"
    assert ann["kind"]["point_group"] == "pt_position"
    by_name = {c["name"]: c for c in data["columns"]}
    assert by_name["c"]["kind"]["kind"] == "split_point"


async def test_register_table_duplicate(client, monkeypatch):
    await _register_table(client, monkeypatch)

    # Second registration with same key
    _patch_validation(monkeypatch)
    _patch_extraction(monkeypatch)
    _patch_kind_validation(monkeypatch)
    resp = await client.post("/api/v1/tables/register", json=_table_payload())
    assert resp.status_code == 409


async def test_register_table_validation_failure(client, monkeypatch):
    from cave_catalog.schemas import ValidationCheck, ValidationReport

    report = ValidationReport(
        name_reservation_check=ValidationCheck(passed=True),
        uri_reachable=ValidationCheck(passed=False, message="not found"),
        format_sniff=ValidationCheck(passed=True),
    )
    monkeypatch.setattr(
        "cave_catalog.routers.tables.run_validation_pipeline",
        AsyncMock(return_value=report),
    )
    _patch_extraction(monkeypatch)

    resp = await client.post("/api/v1/tables/register", json=_table_payload())
    assert resp.status_code == 422


async def test_register_table_link_validation_failure(client, monkeypatch):
    from cave_catalog.validation import KindValidationError

    _patch_validation(monkeypatch)
    _patch_extraction(monkeypatch)
    _patch_kind_validation(
        monkeypatch,
        passed=False,
        errors=[
            KindValidationError(
                column_name="a",
                kind="materialization",
                reason="not found",
            )
        ],
    )

    payload = _table_payload(
        column_annotations=[
            {
                "column_name": "a",
                "kind": {
                    "kind": "materialization",
                    "target_table": "bad_table",
                    "target_column": "id",
                },
            }
        ]
    )
    resp = await client.post("/api/v1/tables/register", json=payload)
    assert resp.status_code == 422
    assert "kind validation" in resp.json()["detail"]["message"].lower()


# ---------------------------------------------------------------------------
# 4.4 Annotation update endpoint
# ---------------------------------------------------------------------------


async def test_update_annotations_success(client, monkeypatch):
    table = await _register_table(client, monkeypatch)
    table_id = table["id"]

    _patch_kind_validation(monkeypatch)
    resp = await client.patch(
        f"/api/v1/tables/{table_id}/annotations",
        json={
            "column_annotations": [
                {"column_name": "a", "description": "Updated A"},
            ]
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["column_annotations"][0]["description"] == "Updated A"


async def test_update_annotations_clear(client, monkeypatch):
    table = await _register_table(
        client,
        monkeypatch,
        column_annotations=[{"column_name": "a", "description": "Old"}],
    )
    table_id = table["id"]

    _patch_kind_validation(monkeypatch)
    resp = await client.patch(
        f"/api/v1/tables/{table_id}/annotations",
        json={"column_annotations": []},
    )
    assert resp.status_code == 200
    assert resp.json()["column_annotations"] == []


async def test_update_annotations_on_non_table_returns_400(client, monkeypatch):
    """Patching annotations on a base asset should return 400."""
    # Register via the base asset endpoint
    from cave_catalog.schemas import ValidationCheck, ValidationReport

    ok = ValidationCheck(passed=True)
    monkeypatch.setattr(
        "cave_catalog.routers.assets.run_validation_pipeline",
        AsyncMock(
            return_value=ValidationReport(
                name_reservation_check=ok, uri_reachable=ok, format_sniff=ok
            )
        ),
    )
    resp = await client.post(
        "/api/v1/assets/register",
        json={
            "datastack": "minnie65_public",
            "name": "image_vol",
            "revision": 0,
            "uri": "gs://bucket/images/",
            "format": "precomputed",
            "asset_type": "asset",
            "is_managed": False,
            "mutability": "static",
            "maturity": "stable",
            "properties": {},
        },
    )
    assert resp.status_code == 201
    asset_id = resp.json()["id"]

    resp2 = await client.patch(
        f"/api/v1/tables/{asset_id}/annotations",
        json={"column_annotations": []},
    )
    assert resp2.status_code == 400


# ---------------------------------------------------------------------------
# 4.5 Metadata refresh endpoint
# ---------------------------------------------------------------------------


async def test_refresh_metadata_success(client, monkeypatch):
    table = await _register_table(client, monkeypatch)
    table_id = table["id"]

    # Refresh with updated metadata
    new_meta = TableMetadata(
        n_rows=200,
        n_columns=3,
        n_bytes=10000,
        columns=_DELTA_METADATA.columns,
        partition_columns=[],
    )
    _patch_extraction(monkeypatch, metadata=new_meta)

    resp = await client.post(f"/api/v1/tables/{table_id}/refresh")
    assert resp.status_code == 200
    data = resp.json()
    assert data["cached_metadata"]["n_rows"] == 200


async def test_refresh_preserves_annotations(client, monkeypatch):
    table = await _register_table(
        client,
        monkeypatch,
        column_annotations=[
            {"column_name": "a", "description": "Keep me", "links": []}
        ],
    )
    table_id = table["id"]

    _patch_extraction(monkeypatch)
    resp = await client.post(f"/api/v1/tables/{table_id}/refresh")
    assert resp.status_code == 200
    data = resp.json()
    assert data["column_annotations"][0]["description"] == "Keep me"


async def test_refresh_non_table_returns_400(client, monkeypatch):
    from cave_catalog.schemas import ValidationCheck, ValidationReport

    ok = ValidationCheck(passed=True)
    monkeypatch.setattr(
        "cave_catalog.routers.assets.run_validation_pipeline",
        AsyncMock(
            return_value=ValidationReport(
                name_reservation_check=ok, uri_reachable=ok, format_sniff=ok
            )
        ),
    )
    resp = await client.post(
        "/api/v1/assets/register",
        json={
            "datastack": "minnie65_public",
            "name": "image_vol2",
            "revision": 0,
            "uri": "gs://bucket/images/",
            "format": "precomputed",
            "asset_type": "asset",
            "is_managed": False,
            "mutability": "static",
            "maturity": "stable",
            "properties": {},
        },
    )
    asset_id = resp.json()["id"]

    resp2 = await client.post(f"/api/v1/tables/{asset_id}/refresh")
    assert resp2.status_code == 400


# ---------------------------------------------------------------------------
# 4.6 List tables endpoint
# ---------------------------------------------------------------------------


async def test_list_tables_empty(client):
    resp = await client.get("/api/v1/tables/?datastack=minnie65_public")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_list_tables_returns_tables(client, monkeypatch):
    await _register_table(client, monkeypatch)

    resp = await client.get("/api/v1/tables/?datastack=minnie65_public")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["asset_type"] == "table"
    assert "columns" in data[0]  # merged columns present


async def test_list_tables_filters_by_format(client, monkeypatch):
    await _register_table(client, monkeypatch, name="delta_table", format="delta")

    resp = await client.get("/api/v1/tables/?datastack=minnie65_public&format=parquet")
    assert resp.json() == []

    resp2 = await client.get("/api/v1/tables/?datastack=minnie65_public&format=delta")
    assert len(resp2.json()) == 1


async def test_list_tables_filters_by_source(client, monkeypatch):
    await _register_table(client, monkeypatch, name="t1", source="user")
    await _register_table(client, monkeypatch, name="t2", source="materialization")

    resp = await client.get(
        "/api/v1/tables/?datastack=minnie65_public&source=materialization"
    )
    data = resp.json()
    assert len(data) == 1
    assert data[0]["source"] == "materialization"


async def test_list_tables_excludes_non_tables(client, monkeypatch):
    """Base assets should not appear in the tables list."""
    # Register a table
    await _register_table(client, monkeypatch, name="real_table")

    # Register a base asset via the assets endpoint
    from cave_catalog.schemas import ValidationCheck, ValidationReport

    ok = ValidationCheck(passed=True)
    monkeypatch.setattr(
        "cave_catalog.routers.assets.run_validation_pipeline",
        AsyncMock(
            return_value=ValidationReport(
                name_reservation_check=ok, uri_reachable=ok, format_sniff=ok
            )
        ),
    )
    await client.post(
        "/api/v1/assets/register",
        json={
            "datastack": "minnie65_public",
            "name": "image_vol",
            "revision": 0,
            "uri": "gs://bucket/images/",
            "format": "precomputed",
            "asset_type": "asset",
            "is_managed": False,
            "mutability": "static",
            "maturity": "stable",
            "properties": {},
        },
    )

    resp = await client.get("/api/v1/tables/?datastack=minnie65_public")
    data = resp.json()
    assert len(data) == 1
    assert data[0]["name"] == "real_table"


async def test_list_tables_requires_datastack(client):
    resp = await client.get("/api/v1/tables/")
    assert resp.status_code == 422
