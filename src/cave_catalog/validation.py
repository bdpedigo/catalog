"""Shared validation pipeline for asset registration.

Tasks 2.1b – 2.4: URI reachability, format sniff, and source-conditional
materialization checks.  The pipeline is used by both POST /register and
POST /validate so all checks stay in one place.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Coroutine
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import structlog
from cloudpathlib import AnyPath, CloudPath
from httpx import AsyncClient

from cave_catalog.config import get_settings
from cave_catalog.schemas import ValidationCheck, ValidationReport

logger = structlog.get_logger()

# --- Name format validation -------------------------------------------------

_NAME_SEGMENT_RE = re.compile(r"^[a-z][a-z0-9_]*$")

NAME_FORMAT_MESSAGE = (
    "Name must be lowercase alphanumeric with underscores only "
    "(e.g. 'my_table', 'synapses_v4'). "
    "Each segment must start with a letter. "
    "Layout suffixes (dot-separated) follow the same rules."
)


def validate_asset_name(name: str) -> str:
    """Validate that *name* follows snake_case format.

    Allows an optional dot-separated layout suffix (e.g. ``synapses.by_pre_root``).
    Each segment must match ``^[a-z][a-z0-9_]*$``.

    Returns the name unchanged if valid; raises ``ValueError`` otherwise.
    """
    if not name:
        raise ValueError("Name must not be empty.")

    segments = name.split(".")
    if len(segments) > 2:
        raise ValueError(
            f"Name '{name}' has too many dot-separated segments (max 2). "
            + NAME_FORMAT_MESSAGE
        )

    for segment in segments:
        if not segment:
            raise ValueError(
                f"Name '{name}' has an empty segment. " + NAME_FORMAT_MESSAGE
            )
        if not _NAME_SEGMENT_RE.match(segment):
            raise ValueError(
                f"Name segment '{segment}' is invalid. " + NAME_FORMAT_MESSAGE
            )

    return name


# --- Format sniffers --------------------------------------------------------


async def _sniff_parquet(uri: str) -> ValidationCheck:
    """Validate parquet format by reading schema metadata via polars."""
    import polars as pl

    try:
        await asyncio.to_thread(lambda: pl.scan_parquet(uri).collect_schema())
        return ValidationCheck(passed=True)
    except Exception as exc:
        return ValidationCheck(
            passed=False,
            message=f"Parquet sniff failed: {exc}",
        )


async def _sniff_delta(uri: str) -> ValidationCheck:
    """Validate Delta Lake format by reading the transaction log via deltalake."""
    from deltalake import DeltaTable

    try:
        await asyncio.to_thread(lambda: DeltaTable(uri).schema())
        return ValidationCheck(passed=True)
    except Exception as exc:
        return ValidationCheck(
            passed=False,
            message=f"Delta sniff failed: {exc}",
        )


# Maps format name to an async callable (uri -> ValidationCheck) that validates
# via a third-party library.
_Sniffer = Callable[[str], Coroutine[Any, Any, ValidationCheck]]
FORMAT_SNIFFERS: dict[str, _Sniffer] = {
    "parquet": _sniff_parquet,
    "delta": _sniff_delta,
}


# --- Individual checks ------------------------------------------------------


async def check_uri_reachable(uri: str) -> ValidationCheck:
    """Verify the URI exists using cloudpathlib (supports gs://, s3://, and local paths)."""
    logger.debug("check_uri_reachable", uri=uri)
    try:
        path: CloudPath | Path = AnyPath(uri)  # type: ignore[assignment]
        exists = await asyncio.to_thread(path.exists)
        logger.debug("check_uri_reachable_result", uri=uri, exists=exists)
        if exists:
            return ValidationCheck(passed=True)
        return ValidationCheck(passed=False, message=f"URI does not exist: {uri}")
    except Exception as exc:
        logger.debug("check_uri_reachable_error", uri=uri, error=str(exc))
        return ValidationCheck(passed=False, message=f"URI unreachable: {exc}")


async def check_format_sniff(uri: str, fmt: str) -> ValidationCheck:
    """Validate the format at *uri* using a library-based sniffer.

    Returns a passing check with a message if no sniffer is registered for *fmt*.
    """
    logger.debug("check_format_sniff", uri=uri, fmt=fmt)

    sniffer = FORMAT_SNIFFERS.get(fmt.lower())
    if sniffer is None:
        return ValidationCheck(
            passed=True, message=f"No sniffer registered for format '{fmt}'"
        )

    return await sniffer(uri)


async def check_mat_table(
    datastack: str,
    source_table: str,
    mat_version: int,
    client: AsyncClient,
    token: str = "",
) -> ValidationCheck:
    """Verify a materialization table + version exist via the MaterializationEngine API."""
    logger.debug(
        "check_mat_table",
        datastack=datastack,
        source_table=source_table,
        mat_version=mat_version,
    )
    settings = get_settings()
    if not settings.mat_engine_url:
        logger.warning(
            "mat_table_verify_skipped", reason="MAT_ENGINE_URL not configured"
        )
        return ValidationCheck(
            passed=True,
            message="mat_table_verify skipped: MAT_ENGINE_URL not configured",
        )

    base = settings.mat_engine_url.rstrip("/")
    url = f"{base}/api/v2/datastack/{datastack}/version/{mat_version}/tables"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        response = await client.get(url, headers=headers, timeout=15.0)
        if response.status_code == 404:
            return ValidationCheck(
                passed=False,
                message=f"Version {mat_version} not found for datastack '{datastack}' in MaterializationEngine",
            )
        if response.status_code in (301, 302, 303, 307, 308, 401, 403):
            return ValidationCheck(
                passed=False,
                message=f"MaterializationEngine authentication failed (HTTP {response.status_code}): service token may be missing or invalid",
            )
        if response.status_code != 200:
            return ValidationCheck(
                passed=False,
                message=f"MaterializationEngine returned HTTP {response.status_code}",
            )
        mat_tables: list[str] = response.json()
        if source_table in mat_tables:
            return ValidationCheck(passed=True)
        return ValidationCheck(
            passed=False,
            message=f"Mat table '{source_table}' at version {mat_version} not found in MaterializationEngine",
        )
    except Exception as exc:
        return ValidationCheck(
            passed=False,
            message=f"Failed to reach MaterializationEngine: {exc}",
        )


async def check_name_reservation(
    datastack: str,
    name: str,
    is_mat_source: bool,
    client: AsyncClient,
    token: str = "",
) -> ValidationCheck:
    """Check whether `name` is reserved because it matches a mat table for `datastack`.

    Layout variants (`name.suffix` where `name` matches a mat table) are also reserved.
    Passes through if MAT_ENGINE_URL is not configured.
    """
    logger.debug(
        "check_name_reservation",
        datastack=datastack,
        name=name,
        is_mat_source=is_mat_source,
    )
    settings = get_settings()
    if not settings.mat_engine_url:
        return ValidationCheck(
            passed=True,
            message="name_reservation skipped: MAT_ENGINE_URL not configured",
        )

    base = settings.mat_engine_url.rstrip("/")
    url = f"{base}/api/v2/datastack/{datastack}/tables"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    try:
        response = await client.get(url, headers=headers, timeout=15.0)
        if response.status_code in (301, 302, 303, 307, 308, 401, 403):
            # Auth redirect/rejection — skip rather than block registration
            return ValidationCheck(
                passed=True,
                message=f"name_reservation skipped: ME authentication failed (HTTP {response.status_code})",
            )
        if response.status_code != 200:
            # Can't reach ME — skip the check rather than block registration
            return ValidationCheck(
                passed=True,
                message=f"name_reservation skipped: ME returned {response.status_code}",
            )
        mat_tables: list[str] = response.json()
    except Exception as exc:
        return ValidationCheck(passed=True, message=f"name_reservation skipped: {exc}")

    base_name = name.split(".")[0]
    if base_name in mat_tables:
        if is_mat_source:
            return ValidationCheck(passed=True)
        return ValidationCheck(
            passed=False,
            message=(
                f"Name '{name}' is reserved for materialization. "
                "Use properties.source='materialization' with admin/service permission to register."
            ),
        )
    return ValidationCheck(passed=True)


# --- Column kind validation -------------------------------------------------


@dataclass
class KindValidationError:
    """A single column kind that failed validation."""

    column_name: str
    kind: str
    reason: str


@dataclass
class KindValidationResult:
    """Result of validating column kinds."""

    passed: bool
    errors: list[KindValidationError] = field(default_factory=list)
    skipped: bool = False
    message: str | None = None


# --- Column dtype validation for kinds --------------------------------------

_INTEGER_DTYPES = frozenset(
    {
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
    }
)

_NUMERIC_DTYPES = _INTEGER_DTYPES | frozenset({"float32", "float64"})


def validate_kind_dtypes(
    annotations: list[dict[str, Any]],
    columns: list[dict[str, str]],
) -> list[KindValidationError]:
    """Check that column dtypes are compatible with their assigned kind.

    - ``segmentation`` requires integer dtype.
    - ``packed_point`` / ``split_point`` require numeric (integer or float) dtype.
    - ``materialization`` has no dtype constraint.

    If cached metadata (``columns``) is empty/unavailable, returns no errors
    (optimistic acceptance).

    Parameters
    ----------
    annotations
        Column annotation dicts with optional ``kind``.
    columns
        List of ``{"name": ..., "dtype": ...}`` from cached metadata.

    Returns
    -------
    list[KindValidationError]
        Errors for columns with incompatible dtypes.
    """
    if not columns:
        return []

    dtype_by_name = {col["name"]: col["dtype"] for col in columns}

    errors: list[KindValidationError] = []
    for ann in annotations:
        kind = ann.get("kind")
        if not kind:
            continue

        col_name = ann.get("column_name", "")
        dtype = dtype_by_name.get(col_name)
        if dtype is None:
            # Column not in metadata — skip (orphaned annotation)
            continue

        dtype_lower = dtype.lower()
        kind_type = kind.get("kind")
        if kind_type == "segmentation" and dtype_lower not in _INTEGER_DTYPES:
            errors.append(
                KindValidationError(
                    column_name=col_name,
                    kind="segmentation",
                    reason=f"Segmentation kind requires an integer column, but '{col_name}' has dtype '{dtype}'",
                )
            )
        elif (
            kind_type in ("packed_point", "split_point")
            and dtype_lower not in _NUMERIC_DTYPES
        ):
            errors.append(
                KindValidationError(
                    column_name=col_name,
                    kind=kind_type,
                    reason=f"Point kind requires a numeric column (int or float), but '{col_name}' has dtype '{dtype}'",
                )
            )

    return errors


def validate_point_group_uniqueness(
    annotations: list[dict[str, Any]],
) -> list[KindValidationError]:
    """Check that no two split_point annotations share the same (point_group, axis).

    Only applies to split_point kinds with a non-null point_group.
    Returns errors for duplicates.
    """
    seen: dict[tuple[str, str], str] = {}  # (point_group, axis) -> first column_name
    errors: list[KindValidationError] = []

    for ann in annotations:
        kind = ann.get("kind")
        if not kind or kind.get("kind") != "split_point":
            continue
        point_group = kind.get("point_group")
        axis = kind.get("axis")
        if not point_group or not axis:
            continue

        key = (point_group, axis)
        if key in seen:
            errors.append(
                KindValidationError(
                    column_name=ann.get("column_name", ""),
                    kind="split_point",
                    reason=f"Duplicate (point_group='{point_group}', axis='{axis}'): "
                    f"already assigned to column '{seen[key]}'",
                )
            )
        else:
            seen[key] = ann.get("column_name", "")

    return errors


async def validate_column_kinds(
    annotations: list[dict[str, Any]],
    datastack: str,
    client: AsyncClient,
    token: str = "",
) -> KindValidationResult:
    """Validate column kinds based on their ``kind`` discriminator.

    - ``materialization``: validate ``target_table`` exists in ME.
    - ``segmentation``: node_level validated by Pydantic (regex); no ME call.
    - ``packed_point`` / ``split_point``: validated by Pydantic; no ME call.

    Parameters
    ----------
    annotations
        List of column annotation dicts, each with ``column_name`` and optional
        ``kind`` dict containing ``{kind, ...variant fields}``.
    datastack
        Datastack name to validate materialization kinds against.
    client
        httpx async client for ME calls.
    token
        Optional bearer token.

    Returns
    -------
    KindValidationResult
    """
    # Collect materialization kinds that need ME validation
    mat_targets: list[tuple[str, str, str]] = []  # (col_name, target_table, target_col)
    for ann in annotations:
        col_name = ann.get("column_name", "")
        kind = ann.get("kind")
        if not kind or kind.get("kind") != "materialization":
            continue
        target_table = kind.get("target_table", "")
        target_column = kind.get("target_column", "")
        if target_table:
            mat_targets.append((col_name, target_table, target_column))

    if not mat_targets:
        return KindValidationResult(passed=True)

    settings = get_settings()
    if not settings.mat_engine_url:
        logger.warning(
            "kind_validation_skipped", reason="MAT_ENGINE_URL not configured"
        )
        return KindValidationResult(
            passed=True,
            skipped=True,
            message="Column kind validation skipped: MAT_ENGINE_URL not configured",
        )

    # Fetch table list from ME
    base = settings.mat_engine_url.rstrip("/")
    url = f"{base}/api/v2/datastack/{datastack}/tables"
    headers = {"Authorization": f"Bearer {token}"} if token else {}

    try:
        response = await client.get(url, headers=headers, timeout=15.0)
    except Exception as exc:
        logger.warning("kind_validation_skipped", reason=str(exc))
        return KindValidationResult(
            passed=True,
            skipped=True,
            message=f"Column kind validation skipped: {exc}",
        )

    if response.status_code in (301, 302, 303, 307, 308, 401, 403):
        return KindValidationResult(
            passed=True,
            skipped=True,
            message=f"Column kind validation skipped: ME auth failed (HTTP {response.status_code})",
        )
    if response.status_code != 200:
        return KindValidationResult(
            passed=True,
            skipped=True,
            message=f"Column kind validation skipped: ME returned HTTP {response.status_code}",
        )

    mat_tables: set[str] = set(response.json())

    # Validate each materialization target table exists
    errors: list[KindValidationError] = []
    for col_name, target_table, target_column in mat_targets:
        if target_table not in mat_tables:
            errors.append(
                KindValidationError(
                    column_name=col_name,
                    kind="materialization",
                    reason=f"Table '{target_table}' not found in materialization service for datastack '{datastack}'",
                )
            )

    if errors:
        return KindValidationResult(passed=False, errors=errors)
    return KindValidationResult(passed=True)


# --- Main pipeline ----------------------------------------------------------


async def run_validation_pipeline(
    *,
    datastack: str,
    name: str,
    uri: str,
    fmt: str,
    properties: dict,
    existing_id: Any = None,
    client: AsyncClient,
    token: str = "",
) -> ValidationReport:
    """Run all content-level validation checks and return a report.

    Auth and duplicate checks are handled at the route level; this function
    covers URI reachability, format sniff, name reservation, and
    source-conditional mat verification.
    """
    logger.debug(
        "run_validation_pipeline", datastack=datastack, name=name, uri=uri, fmt=fmt
    )
    report = ValidationReport()

    is_mat_source = properties.get("source") == "materialization"

    # Name reservation check
    report.name_reservation_check = await check_name_reservation(
        datastack, name, is_mat_source, client, token
    )

    # URI reachability
    report.uri_reachable = await check_uri_reachable(uri)

    # Format sniff
    report.format_sniff = await check_format_sniff(uri, fmt)

    # Source-conditional mat table verification
    if is_mat_source:
        source_table = properties.get("source_table")
        mat_ver = properties.get("mat_version")
        if source_table and mat_ver is not None:
            report.mat_table_verify = await check_mat_table(
                datastack, str(source_table), int(mat_ver), client, token
            )
        else:
            report.mat_table_verify = ValidationCheck(
                passed=False,
                message="properties.source_table and properties.mat_version are required when source='materialization'",
            )

    return report
