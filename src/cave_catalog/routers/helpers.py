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
from sqlalchemy.ext.asyncio import AsyncSession

from cave_catalog.auth.middleware import AuthUser
from cave_catalog.config import Settings
from cave_catalog.db.models import Asset

logger = structlog.get_logger()


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
