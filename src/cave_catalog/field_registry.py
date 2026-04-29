"""Data-driven field registry for explore UI rendering.

Schema-first: formatter, filter_type, and enum_values are derived from Pydantic
model annotations at startup. The registry config only stores UI-specific
overrides (label, default visibility, filterable). Adding a new field to the
explore page = adding one FieldConfig entry.
"""

from __future__ import annotations

import types
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, Union, get_args, get_origin
from uuid import UUID

# ---------------------------------------------------------------------------
# Global type → formatter mapping
# ---------------------------------------------------------------------------

TYPE_FORMATTER_MAP: dict[type, str] = {
    int: "number",
    float: "number",
    datetime: "datetime",
    str: "text",
    bool: "text",
    UUID: "text",
}

# Leaf field names that override the type-based formatter
NAME_FORMATTER_OVERRIDES: dict[str, str] = {
    "n_bytes": "bytes",
}

# StrEnum subclasses always render as badge
ENUM_FORMATTER = "badge"

# Formatter → default filter_type
FORMATTER_FILTER_MAP: dict[str, str] = {
    "text": "substring",
    "number": "exact",
    "badge": "enum",
    "datetime": "range",
    "bytes": "range",
}


# ---------------------------------------------------------------------------
# FieldDef — fully resolved, immutable, used by all rendering code
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldDef:
    """Fully resolved field definition for UI rendering."""

    key: str
    label: str
    default: bool
    formatter: str
    filterable: bool
    filter_type: str
    enum_values: tuple[str, ...]
    asset_types: tuple[str, ...] | None
    number_format: str = ","


# ---------------------------------------------------------------------------
# FieldConfig — minimal UI-only configuration (schema provides the rest)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FieldConfig:
    """Minimal UI configuration for a field. Schema provides the rest."""

    key: str
    """Dot-path into the response dict."""

    label: str = ""
    """Override label. Empty = auto-derived from key."""

    default: bool = False
    """Show in list view by default."""

    filterable: bool = True
    """Whether this field appears in filter controls."""

    formatter: str = ""
    """Override formatter. Empty = auto-derived from schema type."""

    number_format: str = ","
    """Format spec for numbers: ',' = comma-separated (default), '' = plain."""


# ---------------------------------------------------------------------------
# The registry config — minimal, only UI concerns
# ---------------------------------------------------------------------------

_FIELD_CONFIGS: list[FieldConfig] = [
    FieldConfig(key="name", default=True),
    FieldConfig(key="mat_version", label="Mat Version", default=True, number_format=""),
    FieldConfig(key="format", default=True, formatter="badge"),
    FieldConfig(key="maturity", default=True),
    FieldConfig(
        key="cached_metadata.n_rows", label="Rows", default=True, filterable=False
    ),
    FieldConfig(key="source"),
    FieldConfig(key="asset_type", label="Type"),
    FieldConfig(key="mutability"),
    FieldConfig(key="created_at", label="Created", filterable=False),
    FieldConfig(key="cached_metadata.n_columns", label="Columns", filterable=False),
    FieldConfig(key="cached_metadata.n_bytes", label="Size", filterable=False),
    FieldConfig(key="revision"),
    FieldConfig(
        key="owner", label="Owner ID", filterable=False, number_format="", default=False
    ),
]


# ---------------------------------------------------------------------------
# Schema introspection helpers
# ---------------------------------------------------------------------------


def _unwrap_optional(annotation: Any) -> Any:
    """Unwrap Optional[X] / X | None to get the inner type."""
    origin = get_origin(annotation)
    if origin is Union or origin is types.UnionType:
        args = get_args(annotation)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return non_none[0]
    return annotation


def _get_annotation(model: type, key: str) -> Any | None:
    """Resolve a dot-path key to its type annotation in a Pydantic model."""
    segments = key.split(".")
    current_model = model
    for i, segment in enumerate(segments):
        if not hasattr(current_model, "model_fields"):
            return None
        fields = current_model.model_fields
        if segment not in fields:
            return None
        raw_annotation = fields[segment].annotation
        unwrapped = _unwrap_optional(raw_annotation)

        if i < len(segments) - 1:
            if hasattr(unwrapped, "model_fields"):
                current_model = unwrapped
            else:
                return None
        else:
            return unwrapped
    return None


def _infer_formatter(annotation: Any, key: str) -> str:
    """Infer formatter from type annotation and field name."""
    leaf_name = key.split(".")[-1]
    if leaf_name in NAME_FORMATTER_OVERRIDES:
        return NAME_FORMATTER_OVERRIDES[leaf_name]

    if isinstance(annotation, type) and issubclass(annotation, StrEnum):
        return ENUM_FORMATTER

    if annotation in TYPE_FORMATTER_MAP:
        return TYPE_FORMATTER_MAP[annotation]

    if isinstance(annotation, type):
        for base_type, fmt in TYPE_FORMATTER_MAP.items():
            if issubclass(annotation, base_type):
                return fmt

    return "text"


def _extract_enum_values(annotation: Any) -> tuple[str, ...]:
    """Extract enum member values if annotation is a StrEnum subclass."""
    if isinstance(annotation, type) and issubclass(annotation, StrEnum):
        return tuple(member.value for member in annotation)
    return ()


def _derive_label(key: str) -> str:
    """Derive a human-readable label from a dot-path key."""
    leaf = key.split(".")[-1]
    for prefix in ("n_", "is_"):
        if leaf.startswith(prefix):
            leaf = leaf[len(prefix) :]
            break
    return leaf.replace("_", " ").title()


# ---------------------------------------------------------------------------
# Resolution — called once at startup, populates ASSET_FIELDS
# ---------------------------------------------------------------------------

ASSET_FIELDS: list[FieldDef] = []


def resolve_registry(*models: type) -> None:
    """Resolve field configs into full FieldDefs by introspecting schema models.

    Called at startup with AssetResponse, TableResponse, etc.
    Populates ASSET_FIELDS. Raises ValueError if any key is invalid.
    """
    ASSET_FIELDS.clear()
    invalid: list[str] = []

    for config in _FIELD_CONFIGS:
        # Find annotation from any provided model
        annotation = None
        present_in_base = False
        for model in models:
            ann = _get_annotation(model, config.key)
            if ann is not None:
                annotation = ann
                model_name = getattr(model, "__name__", "")
                if "Table" not in model_name:
                    present_in_base = True

        if annotation is None:
            invalid.append(config.key)
            continue

        # asset_types: if only in Table* model, restrict to table
        asset_types: tuple[str, ...] | None = None
        if not present_in_base:
            asset_types = ("table",)

        formatter = config.formatter or _infer_formatter(annotation, config.key)
        filter_type = FORMATTER_FILTER_MAP.get(formatter, "substring")
        enum_values = _extract_enum_values(annotation)
        label = config.label or _derive_label(config.key)

        ASSET_FIELDS.append(
            FieldDef(
                key=config.key,
                label=label,
                default=config.default,
                formatter=formatter,
                filterable=config.filterable,
                filter_type=filter_type,
                enum_values=enum_values,
                asset_types=asset_types,
                number_format=config.number_format,
            )
        )

    if invalid:
        raise ValueError(f"Field registry keys not found in response models: {invalid}")


def get_default_fields() -> list[FieldDef]:
    """Return fields marked as shown by default."""
    return [f for f in ASSET_FIELDS if f.default]


def get_fields_for_asset_type(asset_type: str | None) -> list[FieldDef]:
    """Return fields applicable to the given asset type."""
    return [
        f for f in ASSET_FIELDS if f.asset_types is None or asset_type in f.asset_types
    ]


def get_filterable_fields() -> list[FieldDef]:
    """Return fields that can be used as filters."""
    return [f for f in ASSET_FIELDS if f.filterable]


# ---------------------------------------------------------------------------
# Dot-path resolution
# ---------------------------------------------------------------------------


def resolve_field(data: dict, key: str):
    """Resolve a dot-path key against a nested dictionary.

    Returns None if any segment is missing or an intermediate is None.
    """
    segments = key.split(".")
    current = data
    for segment in segments:
        if current is None:
            return None
        if isinstance(current, dict):
            current = current.get(segment)
        else:
            return None
    return current


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _format_number(value, fmt: str = ",") -> str:
    """Format a number. fmt controls grouping (',' = commas, '' = plain)."""
    if isinstance(value, int):
        return f"{value:{fmt}}"
    if isinstance(value, float):
        return f"{value:{fmt}.1f}"
    return str(value)


def _format_bytes(value) -> str:
    """Format byte count as human-readable size."""
    if value is None:
        return "—"
    try:
        n = float(value)
    except (TypeError, ValueError):
        return str(value)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < 1024:
            if unit == "B":
                return f"{int(n)} {unit}"
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} EB"


def _format_datetime(value) -> str:
    """Format a datetime string or object to short ISO."""
    if value is None:
        return "—"
    s = str(value)
    # Truncate to date + time (no microseconds/tz noise)
    if "T" in s:
        return s[:19].replace("T", " ")
    return s[:19]


def _format_badge(value) -> str:
    """Format a value as badge-style text."""
    if value is None:
        return "—"
    return str(value)


def format_field_value(data: dict, field_def: FieldDef) -> str:
    """Resolve and format a field value for display."""
    value = resolve_field(data, field_def.key)
    if value is None:
        return "—"
    match field_def.formatter:
        case "number":
            return _format_number(value, field_def.number_format)
        case "bytes":
            return _format_bytes(value)
        case "datetime":
            return _format_datetime(value)
        case "badge":
            return _format_badge(value)
        case _:
            return str(value)
