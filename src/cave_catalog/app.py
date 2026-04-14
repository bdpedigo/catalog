from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from cave_catalog.config import get_settings
from cave_catalog.db.session import get_engine
from cave_catalog.routers import assets, health

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings = get_settings()
    logger.info("startup", service=settings.service_name, auth_enabled=settings.auth.enabled)
    yield
    await get_engine().dispose()
    logger.info("shutdown", service=settings.service_name)


def create_app() -> FastAPI:
    settings = get_settings()

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

    return app
