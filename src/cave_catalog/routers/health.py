import structlog
from fastapi import APIRouter, Depends

from cave_catalog.config import Settings, get_settings

logger = structlog.get_logger()

router = APIRouter(tags=["health"])


@router.get("/", include_in_schema=False)
async def root() -> dict:
    return {"service": "cave-catalog", "docs": "/docs"}


@router.get("/health")
async def health(settings: Settings = Depends(get_settings)) -> dict:
    return {"status": "ok", "service": settings.service_name}
