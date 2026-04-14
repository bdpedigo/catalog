"""Asset registry endpoints.

Tasks 2.1, 2.1c, 2.5, 2.6, 2.7 — register, validate, list, get, delete assets.
Credential vending (3.3) and view resolution (4.2) are added in later tasks.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from httpx import AsyncClient
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from cave_catalog.auth.middleware import AuthUser, require_auth
from cave_catalog.config import Settings, get_settings
from cave_catalog.db.models import Asset
from cave_catalog.db.session import get_session
from cave_catalog.schemas import AssetRequest, AssetResponse, ValidationCheck, ValidationReport
from cave_catalog.validation import run_validation_pipeline

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1/assets", tags=["assets"])

# Shared httpx client (module-level singleton; fine for service lifetime)
_http_client: AsyncClient | None = None


def _get_http_client() -> AsyncClient:
    global _http_client
    if _http_client is None:
        _http_client = AsyncClient()
    return _http_client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _asset_is_expired(asset: Asset) -> bool:
    if asset.expires_at is None:
        return False
    return asset.expires_at.replace(tzinfo=timezone.utc) < _now_utc()


def _asset_to_response(asset: Asset) -> AssetResponse:
    return AssetResponse.model_validate(asset)


async def _find_duplicate(
    session: AsyncSession, datastack: str, name: str, mat_version: int | None, revision: int
) -> Asset | None:
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
# POST /api/v1/assets/register
# ---------------------------------------------------------------------------


@router.post("/register", response_model=AssetResponse, status_code=status.HTTP_201_CREATED)
async def register_asset(
    body: AssetRequest,
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> AssetResponse:
    # Auth check: user must have write permission on the datastack
    if settings.auth.enabled and not user.has_permission(body.datastack, "edit"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Write permission required on datastack '{body.datastack}'",
        )

    # Duplicate check
    existing = await _find_duplicate(
        session, body.datastack, body.name, body.mat_version, body.revision
    )
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": "Asset already exists", "existing_id": str(existing.id)},
        )

    # Content validation pipeline
    report = await run_validation_pipeline(
        datastack=body.datastack,
        name=body.name,
        uri=body.uri,
        fmt=body.format,
        properties=body.properties,
        client=_get_http_client(),
    )

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

    asset = Asset(
        id=uuid.uuid4(),
        datastack=body.datastack,
        name=body.name,
        mat_version=body.mat_version,
        revision=body.revision,
        uri=body.uri,
        format=body.format,
        asset_type=body.asset_type,
        owner=user.email or user.user_id,
        is_managed=body.is_managed,
        mutability=body.mutability.value,
        maturity=body.maturity.value,
        properties=body.properties,
        access_group=body.access_group,
        created_at=_now_utc(),
        expires_at=body.expires_at,
    )
    session.add(asset)
    try:
        await session.commit()
        await session.refresh(asset)
    except IntegrityError:
        await session.rollback()
        # Race condition — duplicate was inserted between our check and insert
        dup = await _find_duplicate(
            session, body.datastack, body.name, body.mat_version, body.revision
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": "Asset already exists", "existing_id": str(dup.id) if dup else None},
        )

    logger.info("asset_registered", id=str(asset.id), datastack=body.datastack, name=body.name)
    return _asset_to_response(asset)


# ---------------------------------------------------------------------------
# POST /api/v1/assets/validate
# ---------------------------------------------------------------------------


@router.post("/validate", response_model=ValidationReport)
async def validate_asset(
    body: AssetRequest,
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> ValidationReport:
    report = ValidationReport()

    # Auth check
    if settings.auth.enabled and not user.has_permission(body.datastack, "edit"):
        report.auth_check = ValidationCheck(
            passed=False, message=f"Write permission required on datastack '{body.datastack}'"
        )
        return report
    report.auth_check = ValidationCheck(passed=True)

    # Duplicate check
    existing = await _find_duplicate(
        session, body.datastack, body.name, body.mat_version, body.revision
    )
    if existing is not None:
        report.duplicate_check = ValidationCheck(passed=False, existing_id=existing.id)
    else:
        report.duplicate_check = ValidationCheck(passed=True)

    # Content validation
    content_report = await run_validation_pipeline(
        datastack=body.datastack,
        name=body.name,
        uri=body.uri,
        fmt=body.format,
        properties=body.properties,
        client=_get_http_client(),
    )
    report.name_reservation_check = content_report.name_reservation_check
    report.uri_reachable = content_report.uri_reachable
    report.format_sniff = content_report.format_sniff
    report.mat_table_verify = content_report.mat_table_verify

    return report


# ---------------------------------------------------------------------------
# GET /api/v1/assets/
# ---------------------------------------------------------------------------


@router.get("/", response_model=list[AssetResponse])
async def list_assets(
    datastack: str = Query(...),
    name: str | None = Query(default=None),
    mat_version: int | None = Query(default=None),
    revision: int | None = Query(default=None),
    format: str | None = Query(default=None),
    asset_type: str | None = Query(default=None),
    mutability: str | None = Query(default=None),
    maturity: str | None = Query(default=None),
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> list[AssetResponse]:
    if settings.auth.enabled and not user.has_permission(datastack, "view"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Read permission required on datastack '{datastack}'",
        )

    now = _now_utc()
    stmt = select(Asset).where(
        and_(
            Asset.datastack == datastack,
            or_(Asset.expires_at.is_(None), Asset.expires_at > now),
        )
    )
    if name is not None:
        stmt = stmt.where(Asset.name == name)
    if mat_version is not None:
        stmt = stmt.where(Asset.mat_version == mat_version)
    if revision is not None:
        stmt = stmt.where(Asset.revision == revision)
    if format is not None:
        stmt = stmt.where(Asset.format == format)
    if asset_type is not None:
        stmt = stmt.where(Asset.asset_type == asset_type)
    if mutability is not None:
        stmt = stmt.where(Asset.mutability == mutability)
    if maturity is not None:
        stmt = stmt.where(Asset.maturity == maturity)

    result = await session.execute(stmt)
    assets = result.scalars().all()
    return [_asset_to_response(a) for a in assets]


# ---------------------------------------------------------------------------
# GET /api/v1/assets/{id}
# ---------------------------------------------------------------------------


@router.get("/{asset_id}", response_model=AssetResponse)
async def get_asset(
    asset_id: uuid.UUID,
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> AssetResponse:
    asset = await session.get(Asset, asset_id)
    if asset is None or _asset_is_expired(asset):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")

    if settings.auth.enabled:
        required_resource = asset.access_group or asset.datastack
        if not user.has_permission(required_resource, "view") and not user.in_group(
            required_resource
        ):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    return _asset_to_response(asset)


# ---------------------------------------------------------------------------
# DELETE /api/v1/assets/{id}
# ---------------------------------------------------------------------------


@router.delete("/{asset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_asset(
    asset_id: uuid.UUID,
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> None:
    asset = await session.get(Asset, asset_id)
    if asset is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Asset not found")

    if settings.auth.enabled and not user.has_permission(asset.datastack, "edit"):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Write permission required on datastack '{asset.datastack}'",
        )

    await session.delete(asset)
    await session.commit()
    logger.info("asset_deleted", id=str(asset_id), user=user.user_id)
