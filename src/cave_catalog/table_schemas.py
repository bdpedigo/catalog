"""Pydantic models for table assets.

Covers cached metadata (format-discriminated), column annotations with links,
and table-specific request/response schemas.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from cave_catalog.schemas import AssetRequest, AssetResponse

# ---------------------------------------------------------------------------
# Cached metadata models (task 1.3)
# ---------------------------------------------------------------------------


class ColumnInfo(BaseModel):
    """A single column discovered from file metadata."""

    name: str
    dtype: str


class TableMetadata(BaseModel):
    """Cached metadata common to all table formats."""

    n_rows: int | None = None
    n_columns: int | None = None
    n_bytes: int | None = None
    columns: list[ColumnInfo] = Field(default_factory=list)
    partition_columns: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Column annotation models (task 1.4)
# ---------------------------------------------------------------------------


class ColumnLink(BaseModel):
    """Semantic link from a column to a materialization service table/column."""

    link_type: str
    target_table: str
    target_column: str


class ColumnAnnotation(BaseModel):
    """User-provided annotation for a single column."""

    column_name: str
    description: str | None = None
    links: list[ColumnLink] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Merged column view (read-time merge of cached metadata + annotations)
# ---------------------------------------------------------------------------


class MergedColumn(BaseModel):
    """Unified column view returned by the API."""

    name: str
    dtype: str
    description: str | None = None
    links: list[ColumnLink] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Request / response models (task 1.5)
# ---------------------------------------------------------------------------


class TablePreviewRequest(BaseModel):
    uri: str
    format: str
    datastack: str


class TablePreviewResponse(BaseModel):
    metadata: TableMetadata


class TableRequest(AssetRequest):
    format: str  # required for tables (not optional like base)
    asset_type: str = "table"
    source: str = "user"
    column_annotations: list[ColumnAnnotation] = Field(default_factory=list)


class TableResponse(AssetResponse):
    source: str | None = None
    cached_metadata: TableMetadata | None = None
    metadata_cached_at: datetime | None = None
    column_annotations: list[ColumnAnnotation] = Field(default_factory=list)
    columns: list[MergedColumn] = Field(default_factory=list)


class AnnotationUpdateRequest(BaseModel):
    column_annotations: list[ColumnAnnotation]


# ---------------------------------------------------------------------------
# Column merging helper (task 4.7)
# ---------------------------------------------------------------------------


def merge_columns(
    metadata: TableMetadata | None,
    annotations: list[ColumnAnnotation] | None,
) -> list[MergedColumn]:
    """Merge cached column schema with user-provided annotations by column name.

    For each column in ``metadata.columns``, look up matching annotation by
    ``column_name``.  Annotated columns get description + links; unannotated
    columns get None/empty.  Orphaned annotations (no matching column in
    metadata) are silently dropped — they're inert until the column reappears.
    """
    if not metadata or not metadata.columns:
        return []

    ann_by_name: dict[str, ColumnAnnotation] = {}
    for ann in annotations or []:
        ann_by_name[ann.column_name] = ann

    merged: list[MergedColumn] = []
    for col in metadata.columns:
        ann = ann_by_name.get(col.name)
        merged.append(
            MergedColumn(
                name=col.name,
                dtype=col.dtype,
                description=ann.description if ann else None,
                links=ann.links if ann else [],
            )
        )
    return merged
