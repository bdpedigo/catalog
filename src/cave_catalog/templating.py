"""Jinja2 template configuration for the UI."""

from pathlib import Path

from fastapi.templating import Jinja2Templates

from cave_catalog.field_registry import (
    ASSET_FIELDS,
    format_field_value,
    get_default_fields,
    get_filterable_fields,
)

templates = Jinja2Templates(directory=Path(__file__).resolve().parent / "templates")

# Register custom filters and globals
templates.env.filters["format_field"] = format_field_value
templates.env.globals["ASSET_FIELDS"] = ASSET_FIELDS
templates.env.globals["get_default_fields"] = get_default_fields
templates.env.globals["get_filterable_fields"] = get_filterable_fields
