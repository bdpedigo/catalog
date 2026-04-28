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

# Caches keyed by (datastack, version) or (datastack, version, target_name)
_tables_cache: TTLCache[tuple[str, int | None], list[str]] = TTLCache(
    maxsize=_CACHE_MAXSIZE, ttl=_CACHE_TTL
)
_views_cache: TTLCache[tuple[str, int | None], list[str]] = TTLCache(
    maxsize=_CACHE_MAXSIZE, ttl=_CACHE_TTL
)
_columns_cache: TTLCache[tuple[str, int | None, str, str], list[dict]] = TTLCache(
    maxsize=_CACHE_MAXSIZE, ttl=_CACHE_TTL
)


class MatProxyError(Exception):
    """Raised when a materialization proxy operation fails."""


@dataclass
class LinkableTarget:
    name: str
    target_type: str  # "table" or "view"


def _get_cave_client(datastack: str, version: int | None = None) -> CAVEclient:
    """Create a CAVEclient instance with the service token."""
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
    return CAVEclient(**kwargs)


def _sync_get_tables(datastack: str, version: int | None = None) -> list[str]:
    """Synchronous: fetch table list via CAVEclient."""
    client = _get_cave_client(datastack, version)
    return client.materialize.get_tables()


def _sync_get_views(datastack: str, version: int | None = None) -> list[str]:
    """Synchronous: fetch view list via CAVEclient."""
    client = _get_cave_client(datastack, version)
    return client.materialize.get_views()


def _sync_get_table_columns(
    datastack: str, table_name: str, version: int | None = None
) -> list[dict]:
    """Synchronous: resolve columns for a materialization table.

    Path: get_table_metadata() → schema_type → schema_definition() → columns.
    """
    client = _get_cave_client(datastack, version)
    metadata = client.materialize.get_table_metadata(table_name)
    schema_type = metadata.get("schema_type") or metadata.get("schema")
    if not schema_type:
        raise MatProxyError(
            f"Could not determine schema type for table '{table_name}'"
        )
    schema_def = client.schema.schema_definition(schema_type)
    # schema_def is a JSON Schema; resolve top-level $ref then read "properties"
    resolved = schema_def
    ref = schema_def.get("$ref", "")
    if ref.startswith("#/definitions/"):
        def_name = ref.split("/")[-1]
        resolved = schema_def.get("definitions", {}).get(def_name, schema_def)
    properties = resolved.get("properties", resolved)
    columns = []
    for col_name, col_info in properties.items():
        columns.append({"name": col_name, "type": str(col_info)})
    return columns


def _sync_get_view_columns(
    datastack: str, view_name: str, version: int | None = None
) -> list[dict]:
    """Synchronous: resolve columns for a materialization view."""
    client = _get_cave_client(datastack, version)
    schema = client.materialize.get_view_schema(view_name)
    # schema is a dict with column names → type info
    columns = []
    for col_name, col_info in schema.items():
        columns.append({"name": col_name, "type": str(col_info)})
    return columns


async def get_mat_tables(datastack: str, version: int | None = None) -> list[str]:
    """Get materialization tables for a datastack (cached)."""
    cache_key = (datastack, version)
    if cache_key in _tables_cache:
        return _tables_cache[cache_key]
    try:
        tables = await asyncio.to_thread(_sync_get_tables, datastack, version)
    except MatProxyError:
        raise
    except Exception as e:
        logger.exception("Failed to fetch mat tables for %s", datastack)
        raise MatProxyError(f"Failed to fetch tables: {e}") from e
    _tables_cache[cache_key] = tables
    return tables


async def get_mat_views(datastack: str, version: int | None = None) -> list[str]:
    """Get materialization views for a datastack (cached)."""
    cache_key = (datastack, version)
    if cache_key in _views_cache:
        return _views_cache[cache_key]
    try:
        views = await asyncio.to_thread(_sync_get_views, datastack, version)
    except MatProxyError:
        raise
    except Exception as e:
        logger.exception("Failed to fetch mat views for %s", datastack)
        raise MatProxyError(f"Failed to fetch views: {e}") from e
    _views_cache[cache_key] = views
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
) -> list[dict]:
    """Get columns for a linkable target (table or view), cached."""
    cache_key = (datastack, version, target_name, target_type)
    if cache_key in _columns_cache:
        return _columns_cache[cache_key]
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
        logger.exception(
            "Failed to fetch columns for %s/%s", datastack, target_name
        )
        raise MatProxyError(f"Failed to fetch columns for '{target_name}': {e}") from e
    _columns_cache[cache_key] = columns
    return columns
