"""Asset registry endpoints.

Tasks 2.1, 2.1c, 2.5, 2.6, 2.7 — register, validate, list, get, delete assets.
Credential vending (3.3) and view resolution (4.2) are added in later tasks.
"""

from __future__ import annotations

import uuid

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import and_, func, nulls_first, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from cave_catalog.auth.middleware import AuthUser, require_auth
from cave_catalog.config import Settings, get_settings
from cave_catalog.db.models import Asset
from cave_catalog.db.session import get_session
from cave_catalog.routers.helpers import (
    asset_to_response,
    find_by_uri,
    find_duplicate,
    get_asset,
    get_http_client,
    now_utc,
    raise_if_validation_failed,
    require_asset_view_access,
    require_datastack_permission,
)
from cave_catalog.schemas import (
    AccessResponse,
    AssetRequest,
    AssetResponse,
    AssetUpdateRequest,
    ValidationCheck,
    ValidationReport,
)
from cave_catalog.table_schemas import TableResponse
from cave_catalog.validation import (
    check_name_reservation as _check_name_reservation,
    validate_asset_name,
)
from cave_catalog.validation import run_validation_pipeline

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1/assets", tags=["assets"])


# ---------------------------------------------------------------------------
# POST /api/v1/assets/register
# ---------------------------------------------------------------------------


@router.post(
    "/register", response_model=AssetResponse, status_code=status.HTTP_201_CREATED
)
async def register_asset(
    body: AssetRequest,
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> AssetResponse:
    logger.debug(
        "register_asset",
        datastack=body.datastack,
        name=body.name,
        uri=body.uri,
        fmt=body.format,
    )
    require_datastack_permission(user, settings, body.datastack, "edit")

    # Duplicate check
    existing = await find_duplicate(
        session, body.datastack, body.name, body.mat_version, body.revision
    )
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": "Asset already exists", "existing_id": str(existing.id)},
        )

    # URI uniqueness check
    uri_conflict = await find_by_uri(session, body.uri)
    if uri_conflict is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "URI is already registered to another asset",
                "existing_id": str(uri_conflict.id),
            },
        )

    # Content validation pipeline
    report = await run_validation_pipeline(
        datastack=body.datastack,
        name=body.name,
        uri=body.uri,
        fmt=body.format,
        properties=body.properties,
        client=get_http_client(),
        token=user.token,
    )
    raise_if_validation_failed(report)

    asset = Asset(
        id=uuid.uuid4(),
        datastack=body.datastack,
        name=body.name,
        mat_version=body.mat_version,
        revision=body.revision,
        uri=body.uri,
        format=body.format,
        asset_type=body.asset_type,
        owner=user.user_id,
        is_managed=body.is_managed,
        mutability=body.mutability.value,
        maturity=body.maturity.value,
        properties=body.properties,
        access_group=body.access_group,
        created_at=now_utc(),
        expires_at=body.expires_at,
    )
    session.add(asset)
    try:
        await session.commit()
        await session.refresh(asset)
    except IntegrityError:
        await session.rollback()
        # Race condition — duplicate was inserted between our check and insert
        dup = await find_duplicate(
            session, body.datastack, body.name, body.mat_version, body.revision
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "Asset already exists",
                "existing_id": str(dup.id) if dup else None,
            },
        )

    logger.info(
        "asset_registered", id=str(asset.id), datastack=body.datastack, name=body.name
    )
    return asset_to_response(asset)


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
    logger.debug(
        "validate_asset",
        datastack=body.datastack,
        name=body.name,
        uri=body.uri,
        fmt=body.format,
    )
    report = ValidationReport()

    # Auth check
    if settings.auth.enabled and not user.has_permission(body.datastack, "edit"):
        report.auth_check = ValidationCheck(
            passed=False,
            message=f"Write permission required on datastack '{body.datastack}'",
        )
        return report
    report.auth_check = ValidationCheck(passed=True)

    # Duplicate check
    existing = await find_duplicate(
        session, body.datastack, body.name, body.mat_version, body.revision
    )
    if existing is not None:
        report.duplicate_check = ValidationCheck(passed=False, existing_id=existing.id)
    else:
        report.duplicate_check = ValidationCheck(passed=True)

    # URI uniqueness check
    uri_conflict = await find_by_uri(session, body.uri)
    if uri_conflict is not None:
        report.uri_unique_check = ValidationCheck(
            passed=False,
            message="URI is already registered to another asset",
            existing_id=uri_conflict.id,
        )
    else:
        report.uri_unique_check = ValidationCheck(passed=True)

    # Content validation
    content_report = await run_validation_pipeline(
        datastack=body.datastack,
        name=body.name,
        uri=body.uri,
        fmt=body.format,
        properties=body.properties,
        client=get_http_client(),
        token=user.token,
    )
    report.name_reservation_check = content_report.name_reservation_check
    report.uri_reachable = content_report.uri_reachable
    report.format_sniff = content_report.format_sniff
    report.mat_table_verify = content_report.mat_table_verify

    return report


# ---------------------------------------------------------------------------
# GET /api/v1/assets/check-name
# ---------------------------------------------------------------------------


@router.get("/check-name")
async def check_name(
    datastack: str = Query(...),
    name: str = Query(...),
    mat_version: int | None = Query(default=None),
    revision: int = Query(default=0),
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
) -> dict:
    # 0. Check name format
    try:
        validate_asset_name(name)
    except ValueError as exc:
        return {"available": False, "reason": "invalid_format", "message": str(exc)}

    # 1. Check name reservation against mat tables
    reservation = await _check_name_reservation(
        datastack=datastack,
        name=name,
        is_mat_source=False,
        client=get_http_client(),
        token=user.token,
    )
    if not reservation.passed:
        return {"available": False, "reason": "reserved"}

    # 2. Check for duplicate asset in DB
    existing = await find_duplicate(session, datastack, name, mat_version, revision)
    if existing is not None:
        return {
            "available": False,
            "reason": "duplicate",
            "existing_id": str(existing.id),
        }

    return {"available": True}


# ---------------------------------------------------------------------------
# GET /api/v1/assets/check-uri
# ---------------------------------------------------------------------------


@router.get("/check-uri")
async def check_uri(
    uri: str = Query(...),
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
) -> dict:
    existing = await find_by_uri(session, uri)
    if existing is not None:
        return {
            "available": False,
            "reason": "duplicate_uri",
            "existing_id": str(existing.id),
        }
    return {"available": True}


# ---------------------------------------------------------------------------
# GET /api/v1/assets/
# ---------------------------------------------------------------------------


@router.get("/", response_model=list[TableResponse | AssetResponse])
async def list_assets(
    response: Response,
    datastack: str = Query(...),
    name: str | None = Query(default=None),
    name_contains: str | None = Query(default=None),
    mat_version: int | None = Query(default=None),
    revision: int | None = Query(default=None),
    format: str | None = Query(default=None),
    asset_type: str | None = Query(default=None),
    mutability: str | None = Query(default=None),
    maturity: str | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    sort_by: str = Query(default="name"),
    sort_order: str = Query(default="asc", pattern="^(asc|desc)$"),
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> list[AssetResponse | TableResponse]:
    logger.debug("list_assets", datastack=datastack, name=name, mat_version=mat_version)
    require_datastack_permission(user, settings, datastack, "view")

    now = now_utc()
    stmt = select(Asset).where(
        and_(
            Asset.datastack == datastack,
            or_(Asset.expires_at.is_(None), Asset.expires_at > now),
        )
    )
    if name is not None:
        stmt = stmt.where(Asset.name == name)
    if name_contains is not None:
        stmt = stmt.where(Asset.name.ilike(f"%{name_contains}%"))
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

    # Sorting with NULLs first
    sort_column = getattr(Asset, sort_by, None)
    if sort_column is None:
        sort_column = Asset.name
    if sort_order == "desc":
        order_clause = nulls_first(sort_column.desc())
    else:
        order_clause = nulls_first(sort_column.asc())
    stmt = stmt.order_by(order_clause)

    # Pagination: if limit is provided, include total count header
    if limit is not None:
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total = (await session.execute(count_stmt)).scalar_one()
        response.headers["X-Total-Count"] = str(total)
        stmt = stmt.offset(offset).limit(limit)

    result = await session.execute(stmt)
    assets = result.scalars().all()
    return [asset_to_response(a) for a in assets]


# ---------------------------------------------------------------------------
# GET /api/v1/assets/{id}
# ---------------------------------------------------------------------------


@router.get("/{asset_id}", response_model=TableResponse | AssetResponse)
async def get_asset_by_id(
    asset_id: uuid.UUID,
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> AssetResponse | TableResponse:
    logger.debug("get_asset", asset_id=str(asset_id))
    asset = await get_asset(session, asset_id)
    require_asset_view_access(user, settings, asset)
    return asset_to_response(asset)


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
    logger.debug("delete_asset", asset_id=str(asset_id))
    asset = await get_asset(session, asset_id, check_expired=False)
    require_datastack_permission(user, settings, asset.datastack, "edit")

    await session.delete(asset)
    await session.commit()
    logger.info("asset_deleted", id=str(asset_id), user=user.user_id)


# ---------------------------------------------------------------------------
# PATCH /api/v1/assets/{id}
# ---------------------------------------------------------------------------


@router.patch("/{asset_id}", response_model=TableResponse | AssetResponse)
async def update_asset(
    asset_id: uuid.UUID,
    body: AssetUpdateRequest,
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> AssetResponse | TableResponse:
    logger.debug("update_asset", asset_id=str(asset_id))
    asset = await get_asset(session, asset_id)
    require_datastack_permission(user, settings, asset.datastack, "edit")

    # Apply only the fields that were explicitly sent
    update_data = body.model_dump(exclude_unset=True)
    for field_name, value in update_data.items():
        setattr(asset, field_name, value)

    await session.commit()
    await session.refresh(asset)
    logger.info("asset_updated", id=str(asset_id), fields=list(update_data.keys()))
    return asset_to_response(asset)


# ---------------------------------------------------------------------------
# POST /api/v1/assets/{id}/access
# ---------------------------------------------------------------------------


@router.post("/{asset_id}/access", response_model=AccessResponse)
async def get_asset_access(
    asset_id: uuid.UUID,
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> AccessResponse:
    logger.debug("get_asset_access", asset_id=str(asset_id))
    asset = await get_asset(session, asset_id)
    require_asset_view_access(user, settings, asset)

    # Unmanaged assets: passthrough (no credentials)
    if not asset.is_managed:
        return AccessResponse(
            uri=asset.uri,
            format=asset.format,
            token=None,
            token_type=None,
            expires_in=None,
            storage_provider=None,
            is_managed=False,
        )

    # Managed assets: dispatch to provider
    from cave_catalog.credentials.dispatch import get_provider

    provider = get_provider(asset.uri)
    response = await provider.vend(asset.uri)
    # Fill in the format from the asset record (provider doesn't know it)
    return response.model_copy(update={"format": asset.format})
