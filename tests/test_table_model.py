"""Tests for table data model: single table inheritance and Pydantic schemas.

Phase 1 tests — covers tasks 1.1–1.5.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest_asyncio
from cave_catalog.db.models import Asset, Base, Table
from cave_catalog.table_schemas import (
    AnnotationUpdateRequest,
    ColumnAnnotation,
    ColumnInfo,
    ColumnLink,
    MergedColumn,
    TableMetadata,
    TablePreviewRequest,
    TablePreviewResponse,
    TableRequest,
    TableResponse,
)
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


# ---------------------------------------------------------------------------
# DB fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def db_session(tmp_path):
    """Async SQLAlchemy session backed by a per-test file SQLite DB."""
    db_path = tmp_path / "test.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        yield session

    await engine.dispose()


# ---------------------------------------------------------------------------
# 1.1 / 1.2  Single table inheritance
# ---------------------------------------------------------------------------


async def test_create_table_asset(db_session):
    """Table assets are persisted via the Table model with asset_type='table'."""
    table = Table(
        datastack="minnie65",
        name="synapses_v943",
        uri="gs://bucket/synapses/",
        format="delta",
        asset_type="table",
        owner=1,
        is_managed=True,
        source="user",
    )
    db_session.add(table)
    await db_session.commit()

    result = await db_session.get(Table, table.id)
    assert result is not None
    assert result.asset_type == "table"
    assert result.source == "user"
    assert result.name == "synapses_v943"


async def test_table_loads_as_table_via_base_query(db_session):
    """Querying Asset should return Table instances for asset_type='table'."""
    from sqlalchemy import select

    table = Table(
        datastack="minnie65",
        name="synapses",
        uri="gs://bucket/synapses/",
        format="delta",
        asset_type="table",
        owner=1,
        is_managed=True,
        source="managed",
    )
    db_session.add(table)
    await db_session.commit()

    stmt = select(Asset).where(Asset.name == "synapses")
    result = (await db_session.execute(stmt)).scalar_one()
    assert isinstance(result, Table)
    assert result.source == "managed"


async def test_base_asset_still_works(db_session):
    """Non-table assets use the base Asset class and have null table-specific fields."""
    asset = Asset(
        datastack="minnie65",
        name="image_volume",
        uri="gs://bucket/images/",
        format="precomputed",
        asset_type="asset",
        owner=1,
        is_managed=False,
    )
    db_session.add(asset)
    await db_session.commit()

    result = await db_session.get(Asset, asset.id)
    assert result is not None
    assert result.asset_type == "asset"
    assert result.source is None
    assert result.cached_metadata is None
    assert result.column_annotations is None


async def test_unknown_asset_type_loads_as_base(db_session):
    """Unknown asset_type values load as Asset (fallback polymorphic map)."""
    asset = Asset(
        datastack="minnie65",
        name="custom_thing",
        uri="gs://bucket/custom/",
        asset_type="unknown_type",
        owner=1,
        is_managed=False,
    )
    db_session.add(asset)
    await db_session.commit()

    result = await db_session.get(Asset, asset.id)
    assert result is not None
    assert type(result) is Asset
    assert result.asset_type == "unknown_type"


async def test_table_with_cached_metadata(db_session):
    """Table-specific JSONB fields round-trip through the DB."""
    now = datetime.now(timezone.utc)
    meta = {
        "n_rows": 1000,
        "n_columns": 5,
        "n_bytes": 50000,
        "columns": [],
        "partition_columns": [],
    }
    annotations = [{"column_name": "pt_root_id", "description": "Root ID", "links": []}]

    table = Table(
        datastack="minnie65",
        name="cells",
        uri="gs://bucket/cells/",
        format="delta",
        asset_type="table",
        owner=1,
        is_managed=True,
        source="user",
        cached_metadata=meta,
        metadata_cached_at=now,
        column_annotations=annotations,
    )
    db_session.add(table)
    await db_session.commit()

    result = await db_session.get(Table, table.id)
    assert result.cached_metadata["n_rows"] == 1000
    assert result.metadata_cached_at is not None
    assert result.column_annotations[0]["column_name"] == "pt_root_id"


async def test_mixed_asset_types_in_same_query(db_session):
    """Base and Table assets coexist and can be queried together."""
    from sqlalchemy import select

    base = Asset(
        datastack="minnie65",
        name="image_vol",
        uri="gs://bucket/images/",
        asset_type="asset",
        owner=1,
        is_managed=False,
    )
    table = Table(
        datastack="minnie65",
        name="synapses",
        uri="gs://bucket/synapses/",
        format="delta",
        asset_type="table",
        owner=1,
        is_managed=True,
        source="user",
    )
    db_session.add_all([base, table])
    await db_session.commit()

    stmt = select(Asset).where(Asset.datastack == "minnie65")
    results = (await db_session.execute(stmt)).scalars().all()
    assert len(results) == 2
    types = {type(r) for r in results}
    assert types == {Asset, Table}


# ---------------------------------------------------------------------------
# 1.3  TableMetadata Pydantic model
# ---------------------------------------------------------------------------


def test_table_metadata_defaults():
    meta = TableMetadata()
    assert meta.n_rows is None
    assert meta.n_columns is None
    assert meta.n_bytes is None
    assert meta.columns == []
    assert meta.partition_columns == []


def test_table_metadata_full():
    meta = TableMetadata(
        n_rows=100,
        n_columns=3,
        n_bytes=5000,
        columns=[
            ColumnInfo(name="a", dtype="int64"),
            ColumnInfo(name="b", dtype="string"),
        ],
        partition_columns=["a"],
    )
    assert meta.n_rows == 100
    assert len(meta.columns) == 2
    assert meta.partition_columns == ["a"]


def test_table_metadata_roundtrip_json():
    meta = TableMetadata(
        n_rows=10,
        n_columns=2,
        n_bytes=1000,
        columns=[ColumnInfo(name="x", dtype="float64")],
        partition_columns=["x"],
    )
    data = meta.model_dump()
    restored = TableMetadata.model_validate(data)
    assert restored == meta


# ---------------------------------------------------------------------------
# 1.4  ColumnAnnotation / ColumnLink
# ---------------------------------------------------------------------------


def test_column_annotation_minimal():
    ann = ColumnAnnotation(column_name="pt_root_id")
    assert ann.description is None
    assert ann.links == []


def test_column_annotation_with_links():
    ann = ColumnAnnotation(
        column_name="pre_pt_root_id",
        description="Pre-synaptic root ID",
        links=[
            ColumnLink(
                link_type="foreign_key",
                target_table="synapses",
                target_column="pre_pt_root_id",
            )
        ],
    )
    assert len(ann.links) == 1
    assert ann.links[0].link_type == "foreign_key"


# ---------------------------------------------------------------------------
# 1.5  Request / Response models
# ---------------------------------------------------------------------------


def test_table_request_defaults():
    req = TableRequest(
        datastack="minnie65",
        name="synapses",
        uri="gs://bucket/synapses/",
        format="delta",
        owner=1,
        is_managed=True,
    )
    assert req.asset_type == "table"
    assert req.source == "user"
    assert req.column_annotations == []


def test_table_request_inherits_base_fields():
    req = TableRequest(
        datastack="minnie65",
        name="synapses",
        uri="gs://bucket/synapses/",
        format="delta",
        mat_version=943,
        owner=1,
        is_managed=True,
        mutability="static",
        maturity="stable",
    )
    assert req.mat_version == 943
    assert req.mutability == "static"


def test_table_response_from_orm_dict():
    resp = TableResponse.model_validate(
        {
            "id": "00000000-0000-0000-0000-000000000001",
            "datastack": "minnie65",
            "name": "synapses",
            "mat_version": 943,
            "revision": 0,
            "uri": "gs://bucket/synapses/",
            "format": "delta",
            "asset_type": "table",
            "owner": 1,
            "is_managed": True,
            "mutability": "static",
            "maturity": "stable",
            "properties": {},
            "access_group": None,
            "created_at": "2026-01-01T00:00:00Z",
            "expires_at": None,
            "source": "user",
            "cached_metadata": {
                "n_rows": 100,
                "n_columns": 2,
                "n_bytes": 5000,
                "columns": [{"name": "a", "dtype": "int64"}],
                "partition_columns": [],
            },
        }
    )
    assert resp.source == "user"
    assert resp.cached_metadata.n_rows == 100


def test_table_preview_request():
    req = TablePreviewRequest(
        uri="gs://bucket/synapses/",
        format="delta",
        datastack="minnie65",
    )
    assert req.format == "delta"


def test_table_preview_response():
    resp = TablePreviewResponse(
        metadata=TableMetadata(n_rows=10, n_columns=2, n_bytes=500, columns=[]),
    )
    assert resp.metadata.n_rows == 10


def test_annotation_update_request():
    req = AnnotationUpdateRequest(
        column_annotations=[
            ColumnAnnotation(column_name="a", description="col a"),
        ]
    )
    assert len(req.column_annotations) == 1


def test_merged_column():
    col = MergedColumn(
        name="pre_pt_root_id",
        dtype="int64",
        description="Pre-synaptic root",
        links=[
            ColumnLink(
                link_type="foreign_key",
                target_table="synapses",
                target_column="pre_pt_root_id",
            )
        ],
    )
    assert col.description == "Pre-synaptic root"
    assert len(col.links) == 1
