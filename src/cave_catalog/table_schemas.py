"""Pydantic models for table assets.

Covers cached metadata (format-discriminated), column annotations with kind
(discriminated union), and table-specific request/response schemas.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

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
# Column kind models (discriminated union)
# ---------------------------------------------------------------------------

_NODE_LEVEL_RE = re.compile(r"^(root_id|supervoxel_id|level\d+_id)$")


class MatKind(BaseModel):
    """Materialization kind — column joins to a specific mat table/column."""

    kind: Literal["materialization"] = "materialization"
    target_table: str
    target_column: str


class SegmentationKind(BaseModel):
    """Segmentation kind — column contains chunkedgraph node IDs."""

    kind: Literal["segmentation"] = "segmentation"
    node_level: str  # "root_id", "supervoxel_id", or "level{N}_id"

    @field_validator("node_level")
    @classmethod
    def _validate_node_level(cls, v: str) -> str:
        if not _NODE_LEVEL_RE.match(v):
            raise ValueError(
                f"Invalid node_level '{v}'. "
                "Must be 'root_id', 'supervoxel_id', or 'level{{N}}_id' "
                "(e.g., 'level2_id')."
            )
        return v


class PackedPointKind(BaseModel):
    """Packed point kind — column contains all xyz coordinates in one field."""

    kind: Literal["packed_point"] = "packed_point"
    resolution: list[float] | None = None

    @field_validator("resolution")
    @classmethod
    def _validate_resolution(cls, v: list[float] | None) -> list[float] | None:
        if v is not None and len(v) != 3:
            raise ValueError(
                "Packed point resolution must be a list of exactly 3 values [rx, ry, rz]."
            )
        return v


class SplitPointKind(BaseModel):
    """Split point kind — column represents a single spatial axis."""

    kind: Literal["split_point"] = "split_point"
    axis: Literal["x", "y", "z"]
    point_group: str | None = None
    resolution: float | None = None


ColumnKind = Annotated[
    MatKind | SegmentationKind | PackedPointKind | SplitPointKind,
    Field(discriminator="kind"),
]


# ---------------------------------------------------------------------------
# Column annotation models
# ---------------------------------------------------------------------------


class ColumnAnnotation(BaseModel):
    """User-provided annotation for a single column."""

    column_name: str
    description: str | None = None
    kind: ColumnKind | None = None


# ---------------------------------------------------------------------------
# Merged column view (read-time merge of cached metadata + annotations)
# ---------------------------------------------------------------------------


class MergedColumn(BaseModel):
    """Unified column view returned by the API."""

    name: str
    dtype: str
    description: str | None = None
    kind: ColumnKind | None = None


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
                kind=ann.kind if ann else None,
            )
        )
    return merged
