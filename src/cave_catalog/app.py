import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from cave_catalog.config import get_settings
from cave_catalog.db.session import get_engine
from cave_catalog.routers import assets, health, tables

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level.upper())
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

    return app
