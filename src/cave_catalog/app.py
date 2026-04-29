import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

import structlog
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from cave_catalog.config import get_settings
from cave_catalog.db.session import get_engine
from cave_catalog.field_registry import resolve_registry
from cave_catalog.routers import assets, health, tables, ui
from cave_catalog.routers.ui import _RedirectException
from cave_catalog.schemas import AssetResponse
from cave_catalog.table_schemas import TableResponse

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level.upper())
    resolve_registry(AssetResponse, TableResponse)
    logger.info(
        "startup", service=settings.service_name, auth_enabled=settings.auth.enabled
    )
    logger.debug(
        "config",
        database_url=settings.database_url.rsplit("@", 1)[-1],  # hide credentials
        service_name=settings.service_name,
        log_level=settings.log_level,
        mat_engine_url=settings.mat_engine_url,
        auth_enabled=settings.auth.enabled,
        auth_service_url=settings.auth.service_url,
    )
    yield
    await get_engine().dispose()
    logger.info("shutdown", service=settings.service_name)


def create_app() -> FastAPI:
    app = FastAPI(
        title="CAVE Catalog",
        description="Asset registry, discovery, and credential vending for the CAVE stack.",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(health.router)
    app.include_router(assets.router)
    app.include_router(tables.router)
    app.include_router(ui.router)

    @app.exception_handler(_RedirectException)
    async def _handle_redirect(request: Request, exc: _RedirectException):
        return RedirectResponse(url=exc.url, status_code=302)

    _pkg_dir = Path(__file__).resolve().parent
    app.mount("/static", StaticFiles(directory=_pkg_dir / "static"), name="static")

    return app
