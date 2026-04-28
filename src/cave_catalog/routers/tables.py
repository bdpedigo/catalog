"""Table-specific endpoints.

Handles table registration, preview, annotation updates, metadata refresh,
and table-specific listing.
"""

from __future__ import annotations

import uuid

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from cave_catalog.auth.middleware import AuthUser, require_auth
from cave_catalog.config import Settings, get_settings
from cave_catalog.db.models import Table
from cave_catalog.db.session import get_session
from cave_catalog.extractors import get_extractor
from cave_catalog.routers.helpers import (
    find_duplicate,
    get_asset,
    get_http_client,
    now_utc,
    raise_if_validation_failed,
    require_datastack_permission,
    table_to_response,
)
from cave_catalog.table_schemas import (
    AnnotationUpdateRequest,
    TablePreviewRequest,
    TablePreviewResponse,
    TableRequest,
    TableResponse,
)
from cave_catalog.validation import run_validation_pipeline, validate_column_links

logger = structlog.get_logger()

router = APIRouter(prefix="/api/v1/tables", tags=["tables"])


# ---------------------------------------------------------------------------
# POST /api/v1/tables/preview  (task 4.2)
# ---------------------------------------------------------------------------


@router.post("/preview", response_model=TablePreviewResponse)
async def preview_table(
    body: TablePreviewRequest,
    user: AuthUser = Depends(require_auth),
    settings: Settings = Depends(get_settings),
) -> TablePreviewResponse:
    logger.debug(
        "preview_table", uri=body.uri, format=body.format, datastack=body.datastack
    )
    require_datastack_permission(user, settings, body.datastack, "view")

    try:
        extractor = get_extractor(body.format)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )

    try:
        metadata = await extractor.extract(body.uri)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Metadata extraction failed: {exc}",
        )

    return TablePreviewResponse(metadata=metadata)


# ---------------------------------------------------------------------------
# POST /api/v1/tables/register  (task 4.3)
# ---------------------------------------------------------------------------


@router.post(
    "/register", response_model=TableResponse, status_code=status.HTTP_201_CREATED
)
async def register_table(
    body: TableRequest,
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> TableResponse:
    logger.debug(
        "register_table",
        datastack=body.datastack,
        name=body.name,
        uri=body.uri,
        format=body.format,
    )
    require_datastack_permission(user, settings, body.datastack, "edit")

    # Duplicate check
    existing = await find_duplicate(
        session, body.datastack, body.name, body.mat_version, body.revision
    )
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"message": "Table already exists", "existing_id": str(existing.id)},
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

    # Column link validation (if annotations provided)
    annotations_dicts = [a.model_dump() for a in body.column_annotations]
    if annotations_dicts:
        link_result = await validate_column_links(
            annotations_dicts,
            body.datastack,
            get_http_client(),
            token=user.token,
        )
        if not link_result.passed:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "message": "Column link validation failed",
                    "errors": [
                        {
                            "column_name": e.column_name,
                            "target_table": e.target_table,
                            "target_column": e.target_column,
                            "reason": e.reason,
                        }
                        for e in link_result.errors
                    ],
                },
            )

    # Extract metadata
    try:
        extractor = get_extractor(body.format)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )

    try:
        metadata = await extractor.extract(body.uri)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Metadata extraction failed: {exc}",
        )

    now = now_utc()
    table = Table(
        id=uuid.uuid4(),
        datastack=body.datastack,
        name=body.name,
        mat_version=body.mat_version,
        revision=body.revision,
        uri=body.uri,
        format=body.format,
        asset_type="table",
        owner=user.user_id,
        is_managed=body.is_managed,
        mutability=body.mutability.value,
        maturity=body.maturity.value,
        properties=body.properties,
        access_group=body.access_group,
        created_at=now,
        expires_at=body.expires_at,
        source=body.source,
        cached_metadata=metadata.model_dump(),
        metadata_cached_at=now,
        column_annotations=annotations_dicts,
    )

    session.add(table)
    try:
        await session.commit()
        await session.refresh(table)
    except IntegrityError:
        await session.rollback()
        dup = await find_duplicate(
            session, body.datastack, body.name, body.mat_version, body.revision
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "Table already exists",
                "existing_id": str(dup.id) if dup else None,
            },
        )

    logger.info(
        "table_registered",
        id=str(table.id),
        datastack=body.datastack,
        name=body.name,
    )
    return table_to_response(table)


# ---------------------------------------------------------------------------
# PATCH /api/v1/tables/{id}/annotations  (task 4.4)
# ---------------------------------------------------------------------------


@router.patch("/{table_id}/annotations", response_model=TableResponse)
async def update_annotations(
    table_id: uuid.UUID,
    body: AnnotationUpdateRequest,
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> TableResponse:
    logger.debug("update_annotations", table_id=str(table_id))
    table = await get_asset(session, table_id)

    if table.asset_type != "table":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Asset is not a table",
        )

    require_datastack_permission(user, settings, table.datastack, "edit")

    # Column link validation
    annotations_dicts = [a.model_dump() for a in body.column_annotations]
    if annotations_dicts:
        link_result = await validate_column_links(
            annotations_dicts,
            table.datastack,
            get_http_client(),
            token=user.token,
        )
        if not link_result.passed:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail={
                    "message": "Column link validation failed",
                    "errors": [
                        {
                            "column_name": e.column_name,
                            "target_table": e.target_table,
                            "target_column": e.target_column,
                            "reason": e.reason,
                        }
                        for e in link_result.errors
                    ],
                },
            )

    table.column_annotations = annotations_dicts
    await session.commit()
    await session.refresh(table)

    logger.info("annotations_updated", table_id=str(table_id))
    return table_to_response(table)


# ---------------------------------------------------------------------------
# POST /api/v1/tables/{id}/refresh  (task 4.5)
# ---------------------------------------------------------------------------


@router.post("/{table_id}/refresh", response_model=TableResponse)
async def refresh_metadata(
    table_id: uuid.UUID,
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> TableResponse:
    logger.debug("refresh_metadata", table_id=str(table_id))
    table = await get_asset(session, table_id)

    if table.asset_type != "table":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Asset is not a table",
        )

    require_datastack_permission(user, settings, table.datastack, "edit")

    try:
        extractor = get_extractor(table.format)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )

    try:
        metadata = await extractor.extract(table.uri)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Metadata extraction failed: {exc}",
        )

    table.cached_metadata = metadata.model_dump()
    table.metadata_cached_at = now_utc()
    # column_annotations intentionally NOT modified
    await session.commit()
    await session.refresh(table)

    logger.info("metadata_refreshed", table_id=str(table_id))
    return table_to_response(table)


# ---------------------------------------------------------------------------
# GET /api/v1/tables/  (task 4.6)
# ---------------------------------------------------------------------------


@router.get("/", response_model=list[TableResponse])
async def list_tables(
    datastack: str = Query(...),
    name: str | None = Query(default=None),
    mat_version: int | None = Query(default=None),
    revision: int | None = Query(default=None),
    format: str | None = Query(default=None),
    source: str | None = Query(default=None),
    mutability: str | None = Query(default=None),
    maturity: str | None = Query(default=None),
    user: AuthUser = Depends(require_auth),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> list[TableResponse]:
    logger.debug("list_tables", datastack=datastack, name=name, format=format)
    require_datastack_permission(user, settings, datastack, "view")

    now = now_utc()
    stmt = select(Table).where(
        and_(
            Table.datastack == datastack,
            Table.asset_type == "table",
            or_(Table.expires_at.is_(None), Table.expires_at > now),
        )
    )
    if name is not None:
        stmt = stmt.where(Table.name == name)
    if mat_version is not None:
        stmt = stmt.where(Table.mat_version == mat_version)
    if revision is not None:
        stmt = stmt.where(Table.revision == revision)
    if format is not None:
        stmt = stmt.where(Table.format == format)
    if source is not None:
        stmt = stmt.where(Table.source == source)
    if mutability is not None:
        stmt = stmt.where(Table.mutability == mutability)
    if maturity is not None:
        stmt = stmt.where(Table.maturity == maturity)

    result = await session.execute(stmt)
    tables = result.scalars().all()
    return [table_to_response(t) for t in tables]
