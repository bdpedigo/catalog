"""Materialization service proxy — cached CAVEclient queries for reference data."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from cachetools import TTLCache
from caveclient import CAVEclient

from cave_catalog.config import get_settings

logger = logging.getLogger(__name__)

# Cache configuration
_CACHE_TTL = 300  # 5 minutes
_CACHE_MAXSIZE = 256

# Cache CAVEclient instances so that built-in .tables/.views memoization persists
_client_cache: TTLCache[tuple[str, int | None], CAVEclient] = TTLCache(
    maxsize=_CACHE_MAXSIZE, ttl=_CACHE_TTL
)


class MatProxyError(Exception):
    """Raised when a materialization proxy operation fails."""


@dataclass
class LinkableTarget:
    name: str
    target_type: str  # "table" or "view"


def _get_cave_client(datastack: str, version: int | None = None) -> CAVEclient:
    """Get or create a cached CAVEclient instance with the service token."""
    cache_key = (datastack, version)
    if cache_key in _client_cache:
        return _client_cache[cache_key]
    settings = get_settings()
    if not settings.cave_token:
        raise MatProxyError(
            "CAVE_TOKEN is not configured. Cannot query materialization service."
        )
    kwargs: dict = {
        "datastack_name": datastack,
        "auth_token": settings.cave_token,
    }
    if settings.caveclient_server_address:
        kwargs["server_address"] = settings.caveclient_server_address
    if version is not None:
        kwargs["version"] = version
    client = CAVEclient(**kwargs)
    _client_cache[cache_key] = client
    return client


def _sync_get_tables(datastack: str, version: int | None = None) -> list[str]:
    """Synchronous: fetch table list via cached CAVEclient."""
    client = _get_cave_client(datastack, version)
    return list(client.materialize.tables.table_names)


def _sync_get_views(datastack: str, version: int | None = None) -> list[str]:
    """Synchronous: fetch view list via cached CAVEclient."""
    client = _get_cave_client(datastack, version)
    return list(client.materialize.views.table_names)


def _strip_bbox_suffix(fields: list[str]) -> list[str]:
    """Strip '_bbox' suffix from field names (materialization spatial filter fields)."""
    return [name[:-5] if name.endswith("_bbox") else name for name in fields]


def _sync_get_table_columns(
    datastack: str, table_name: str, version: int | None = None
) -> list[str]:
    """Synchronous: resolve columns for a materialization table via .fields API."""
    client = _get_cave_client(datastack, version)
    fields = client.materialize.tables[table_name].fields
    return _strip_bbox_suffix(fields)


def _sync_get_view_columns(
    datastack: str, view_name: str, version: int | None = None
) -> list[str]:
    """Synchronous: resolve columns for a materialization view via .fields API."""
    client = _get_cave_client(datastack, version)
    fields = client.materialize.views[view_name].fields
    return _strip_bbox_suffix(fields)


async def get_mat_tables(datastack: str, version: int | None = None) -> list[str]:
    """Get materialization tables for a datastack.

    Leverages cached client instance — .tables property is memoized internally.
    """
    try:
        tables = await asyncio.to_thread(_sync_get_tables, datastack, version)
    except MatProxyError:
        raise
    except Exception as e:
        logger.exception("Failed to fetch mat tables for %s", datastack)
        raise MatProxyError(f"Failed to fetch tables: {e}") from e
    return tables


async def get_mat_views(datastack: str, version: int | None = None) -> list[str]:
    """Get materialization views for a datastack.

    Leverages cached client instance — .views property is memoized internally.
    """
    try:
        views = await asyncio.to_thread(_sync_get_views, datastack, version)
    except MatProxyError:
        raise
    except Exception as e:
        logger.exception("Failed to fetch mat views for %s", datastack)
        raise MatProxyError(f"Failed to fetch views: {e}") from e
    return views


async def get_linkable_targets(
    datastack: str, version: int | None = None
) -> list[LinkableTarget]:
    """Get combined list of tables and views as linkable targets."""
    tables = await get_mat_tables(datastack, version)
    views = await get_mat_views(datastack, version)
    targets = [LinkableTarget(name=t, target_type="table") for t in tables]
    targets += [LinkableTarget(name=v, target_type="view") for v in views]
    targets.sort(key=lambda t: t.name)
    return targets


async def get_target_columns(
    datastack: str,
    target_name: str,
    target_type: str,
    version: int | None = None,
) -> list[str]:
    """Get columns for a linkable target (table or view).

    Leverages CAVEclient's built-in caching via cached client instances
    and the .tables/.views property memoization.
    """
    try:
        if target_type == "view":
            columns = await asyncio.to_thread(
                _sync_get_view_columns, datastack, target_name, version
            )
        else:
            columns = await asyncio.to_thread(
                _sync_get_table_columns, datastack, target_name, version
            )
    except MatProxyError:
        raise
    except Exception as e:
        logger.exception("Failed to fetch columns for %s/%s", datastack, target_name)
        raise MatProxyError(f"Failed to fetch columns for '{target_name}': {e}") from e
    return columns


async def warm_cache(datastack: str, version: int | None = None) -> None:
    """Pre-warm the CAVEclient cache by fetching table and view lists.

    Best-effort: failures are logged but not raised.
    """
    try:
        await asyncio.gather(
            get_mat_tables(datastack, version),
            get_mat_views(datastack, version),
        )
        logger.info("Cache warmed for datastack=%s", datastack)
    except Exception:
        logger.warning(
            "Failed to warm cache for datastack=%s", datastack, exc_info=True
        )
