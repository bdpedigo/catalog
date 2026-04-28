"""Shared helpers for router endpoints.

Reusable building blocks for auth checks and asset lookups that are used
across multiple endpoints.  These raise ``HTTPException`` directly so they
belong in the router layer.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import structlog
from fastapi import HTTPException, status
from httpx import AsyncClient
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from cave_catalog.auth.middleware import AuthUser
from cave_catalog.config import Settings
from cave_catalog.db.models import Asset, Table
from cave_catalog.schemas import AssetResponse, ValidationReport
from cave_catalog.table_schemas import (
    ColumnAnnotation,
    TableMetadata,
    TableResponse,
    merge_columns,
)

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Shared httpx client
# ---------------------------------------------------------------------------

_http_client: AsyncClient | None = None


def get_http_client() -> AsyncClient:
    """Module-level singleton httpx client for outbound service calls."""
    global _http_client
    if _http_client is None:
        _http_client = AsyncClient()
    return _http_client


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def asset_is_expired(asset: Asset) -> bool:
    if asset.expires_at is None:
        return False
    return asset.expires_at.replace(tzinfo=timezone.utc) < now_utc()


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def require_datastack_permission(
    user: AuthUser,
    settings: Settings,
    datastack: str,
    permission: str,
) -> None:
    """Raise 403 if auth is enabled and *user* lacks *permission* on *datastack*."""
    if not settings.auth.enabled:
        return
    if user.has_permission(datastack, permission):
        return
    label = "Write" if permission == "edit" else "Read"
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=f"{label} permission required on datastack '{datastack}'",
    )


def require_asset_view_access(
    user: AuthUser,
    settings: Settings,
    asset: Asset,
) -> None:
    """Raise 403 if auth is enabled and *user* can't view *asset*.

    Checks both permission on the asset's access group (or datastack) and
    group membership — matching the existing access-control semantics.
    """
    if not settings.auth.enabled:
        return
    required_resource = asset.access_group or asset.datastack
    if user.has_permission(required_resource, "view") or user.in_group(
        required_resource
    ):
        return
    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")


# ---------------------------------------------------------------------------
# Asset lookup
# ---------------------------------------------------------------------------


async def get_asset(
    session: AsyncSession,
    asset_id: uuid.UUID,
    *,
    check_expired: bool = True,
) -> Asset:
    """Fetch an asset by ID, raising 404 if missing or (optionally) expired."""
    asset = await session.get(Asset, asset_id)
    if asset is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found"
        )
    if check_expired and asset_is_expired(asset):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found"
        )
    return asset


# ---------------------------------------------------------------------------
# Duplicate check
# ---------------------------------------------------------------------------


async def find_duplicate(
    session: AsyncSession,
    datastack: str,
    name: str,
    mat_version: int | None,
    revision: int,
) -> Asset | None:
    """Find an existing asset with the same (datastack, name, mat_version, revision)."""
    if mat_version is not None:
        stmt = select(Asset).where(
            and_(
                Asset.datastack == datastack,
                Asset.name == name,
                Asset.mat_version == mat_version,
                Asset.revision == revision,
            )
        )
    else:
        stmt = select(Asset).where(
            and_(
                Asset.datastack == datastack,
                Asset.name == name,
                Asset.mat_version.is_(None),
                Asset.revision == revision,
            )
        )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def raise_if_validation_failed(report: ValidationReport) -> None:
    """Raise 422 if any check in *report* failed."""
    failures = {
        k: v
        for k, v in report.model_dump().items()
        if v is not None and not v.get("passed", True)
    }
    if failures:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"message": "Validation failed", "checks": failures},
        )


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------


def table_to_response(table: Table) -> TableResponse:
    """Build a TableResponse from an ORM Table, including merged columns."""
    metadata = None
    if table.cached_metadata is not None:
        metadata = TableMetadata.model_validate(table.cached_metadata)

    annotations: list[ColumnAnnotation] = []
    if table.column_annotations:
        annotations = [
            ColumnAnnotation.model_validate(a) for a in table.column_annotations
        ]

    columns = merge_columns(metadata, annotations)

    return TableResponse(
        id=table.id,
        datastack=table.datastack,
        name=table.name,
        mat_version=table.mat_version,
        revision=table.revision,
        uri=table.uri,
        format=table.format,
        asset_type=table.asset_type,
        owner=table.owner,
        is_managed=table.is_managed,
        mutability=table.mutability,
        maturity=table.maturity,
        properties=table.properties,
        access_group=table.access_group,
        created_at=table.created_at,
        expires_at=table.expires_at,
        source=table.source,
        cached_metadata=metadata,
        metadata_cached_at=table.metadata_cached_at,
        column_annotations=annotations,
        columns=columns,
    )


def asset_to_response(asset: Asset) -> AssetResponse | TableResponse:
    """Build the correct response model based on the asset's type."""
    if isinstance(asset, Table):
        return table_to_response(asset)
    return AssetResponse.model_validate(asset)
