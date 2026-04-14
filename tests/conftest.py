import httpx
import pytest
import pytest_asyncio
from cave_catalog.config import get_settings
from cave_catalog.db.models import Base
from cave_catalog.db.session import get_session, reset_engine
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    """Set env vars and clear caches before/after each test."""
    monkeypatch.setenv("AUTH_ENABLED", "false")
    monkeypatch.setenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    get_settings.cache_clear()
    reset_engine()
    yield
    get_settings.cache_clear()
    reset_engine()


@pytest_asyncio.fixture
async def client(tmp_path):
    """Async httpx client backed by a per-test file SQLite DB.

    Creates the app, engine, and tables inside a single async context so
    everything runs in the same event loop.
    """
    # Import here so env vars are set by the _env fixture first
    from cave_catalog.app import create_app

    db_path = tmp_path / "test.db"
    db_url = f"sqlite+aiosqlite:///{db_path}"

    engine = create_async_engine(db_url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def _override():
        async with factory() as session:
            yield session

    app = create_app()
    app.dependency_overrides[get_session] = _override

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as c:
        yield c

    app.dependency_overrides.clear()
    await engine.dispose()
