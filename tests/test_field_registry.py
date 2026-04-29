"""Tests for the field registry module."""

import pytest
from cave_catalog.field_registry import (
    ASSET_FIELDS,
    FieldDef,
    format_field_value,
    get_default_fields,
    get_fields_for_asset_type,
    get_filterable_fields,
    resolve_field,
    resolve_registry,
)


@pytest.fixture(autouse=True)
def _resolve():
    """Ensure registry is resolved before every test."""
    from cave_catalog.schemas import AssetResponse
    from cave_catalog.table_schemas import TableResponse

    resolve_registry(AssetResponse, TableResponse)


# ---------------------------------------------------------------------------
# resolve_field tests
# ---------------------------------------------------------------------------


class TestResolveField:
    def test_top_level_key(self):
        data = {"name": "synapse_table", "format": "delta"}
        assert resolve_field(data, "name") == "synapse_table"

    def test_nested_key(self):
        data = {"cached_metadata": {"n_rows": 1000, "n_columns": 5}}
        assert resolve_field(data, "cached_metadata.n_rows") == 1000

    def test_deeply_nested(self):
        data = {"a": {"b": {"c": 42}}}
        assert resolve_field(data, "a.b.c") == 42

    def test_missing_top_level_key(self):
        data = {"name": "x"}
        assert resolve_field(data, "nonexistent") is None

    def test_missing_nested_key(self):
        data = {"cached_metadata": {"n_rows": 100}}
        assert resolve_field(data, "cached_metadata.nonexistent") is None

    def test_none_intermediate(self):
        data = {"cached_metadata": None}
        assert resolve_field(data, "cached_metadata.n_rows") is None

    def test_empty_dict(self):
        assert resolve_field({}, "name") is None

    def test_non_dict_intermediate(self):
        data = {"cached_metadata": "not_a_dict"}
        assert resolve_field(data, "cached_metadata.n_rows") is None

    def test_zero_value(self):
        data = {"count": 0}
        assert resolve_field(data, "count") == 0

    def test_false_value(self):
        data = {"is_managed": False}
        assert resolve_field(data, "is_managed") is False


# ---------------------------------------------------------------------------
# format_field_value tests
# ---------------------------------------------------------------------------


class TestFormatFieldValue:
    def _field(self, key="x", label="X", formatter="text"):
        return FieldDef(
            key=key,
            label=label,
            default=False,
            formatter=formatter,
            filterable=False,
            filter_type="substring",
            enum_values=(),
            asset_types=None,
        )

    def test_text_formatter(self):
        assert (
            format_field_value({"name": "hello"}, self._field("name", "Name"))
            == "hello"
        )

    def test_number_formatter_int(self):
        f = self._field("count", "Count", "number")
        assert format_field_value({"count": 337412891}, f) == "337,412,891"

    def test_number_formatter_float(self):
        f = self._field("val", "Val", "number")
        assert format_field_value({"val": 1234.56}, f) == "1,234.6"

    def test_bytes_formatter_gb(self):
        f = self._field("size", "Size", "bytes")
        result = format_field_value({"size": 45_400_000_000}, f)
        assert "GB" in result

    def test_bytes_formatter_mb(self):
        f = self._field("size", "Size", "bytes")
        result = format_field_value({"size": 5_000_000}, f)
        assert "MB" in result

    def test_bytes_formatter_small(self):
        f = self._field("size", "Size", "bytes")
        result = format_field_value({"size": 500}, f)
        assert "B" in result

    def test_datetime_formatter(self):
        f = self._field("created_at", "Created", "datetime")
        result = format_field_value(
            {"created_at": "2026-04-15T10:30:00.123456+00:00"}, f
        )
        assert result == "2026-04-15 10:30:00"

    def test_badge_formatter(self):
        f = self._field("maturity", "Maturity", "badge")
        assert format_field_value({"maturity": "stable"}, f) == "stable"

    def test_none_value_returns_dash(self):
        f = self._field("mat_version", "Mat Version", "number")
        assert format_field_value({"mat_version": None}, f) == "—"

    def test_missing_key_returns_dash(self):
        f = self._field("nonexistent", "X")
        assert format_field_value({"name": "x"}, f) == "—"

    def test_nested_field_formatting(self):
        f = self._field("cached_metadata.n_rows", "Rows", "number")
        data = {"cached_metadata": {"n_rows": 1000}}
        assert format_field_value(data, f) == "1,000"

    def test_nested_none_returns_dash(self):
        f = self._field("cached_metadata.n_rows", "Rows", "number")
        data = {"cached_metadata": None}
        assert format_field_value(data, f) == "—"


# ---------------------------------------------------------------------------
# resolve_registry tests
# ---------------------------------------------------------------------------


class TestResolveRegistry:
    def test_resolves_all_fields(self):
        assert len(ASSET_FIELDS) > 0
        keys = [f.key for f in ASSET_FIELDS]
        assert "name" in keys
        assert "maturity" in keys
        assert "cached_metadata.n_rows" in keys

    def test_invalid_key_raises(self):
        from cave_catalog.field_registry import _FIELD_CONFIGS, FieldConfig
        from cave_catalog.schemas import AssetResponse
        from cave_catalog.table_schemas import TableResponse

        # Temporarily add an invalid field config
        _FIELD_CONFIGS.append(FieldConfig(key="nonexistent_field"))
        try:
            with pytest.raises(ValueError, match="nonexistent_field"):
                resolve_registry(AssetResponse, TableResponse)
        finally:
            _FIELD_CONFIGS.pop()
            # Re-resolve to restore valid state
            resolve_registry(AssetResponse, TableResponse)

    def test_enum_values_derived_from_schema(self):
        maturity_field = next(f for f in ASSET_FIELDS if f.key == "maturity")
        assert "stable" in maturity_field.enum_values
        assert "draft" in maturity_field.enum_values
        assert "deprecated" in maturity_field.enum_values

    def test_mutability_enum_values(self):
        mut_field = next(f for f in ASSET_FIELDS if f.key == "mutability")
        assert "static" in mut_field.enum_values
        assert "mutable" in mut_field.enum_values

    def test_formatter_inferred_for_enum(self):
        maturity_field = next(f for f in ASSET_FIELDS if f.key == "maturity")
        assert maturity_field.formatter == "badge"

    def test_formatter_inferred_for_int(self):
        rev_field = next(f for f in ASSET_FIELDS if f.key == "revision")
        assert rev_field.formatter == "number"

    def test_formatter_inferred_for_datetime(self):
        created_field = next(f for f in ASSET_FIELDS if f.key == "created_at")
        assert created_field.formatter == "datetime"

    def test_formatter_override_bytes(self):
        size_field = next(f for f in ASSET_FIELDS if f.key == "cached_metadata.n_bytes")
        assert size_field.formatter == "bytes"

    def test_filter_type_derived(self):
        name_field = next(f for f in ASSET_FIELDS if f.key == "name")
        assert name_field.filter_type == "substring"

        maturity_field = next(f for f in ASSET_FIELDS if f.key == "maturity")
        assert maturity_field.filter_type == "enum"

    def test_asset_types_inferred(self):
        # source only in TableResponse
        source_field = next(f for f in ASSET_FIELDS if f.key == "source")
        assert source_field.asset_types == ("table",)

        # name in AssetResponse (base)
        name_field = next(f for f in ASSET_FIELDS if f.key == "name")
        assert name_field.asset_types is None

    def test_label_derived(self):
        rev_field = next(f for f in ASSET_FIELDS if f.key == "revision")
        assert rev_field.label == "Revision"

    def test_label_override(self):
        mat_field = next(f for f in ASSET_FIELDS if f.key == "mat_version")
        assert mat_field.label == "Mat Version"


# ---------------------------------------------------------------------------
# Registry helper tests
# ---------------------------------------------------------------------------


class TestRegistryHelpers:
    def test_default_fields_not_empty(self):
        defaults = get_default_fields()
        assert len(defaults) >= 4
        keys = [f.key for f in defaults]
        assert "name" in keys
        assert "mat_version" in keys
        assert "format" in keys
        assert "maturity" in keys

    def test_fields_for_table_includes_table_only(self):
        table_fields = get_fields_for_asset_type("table")
        keys = [f.key for f in table_fields]
        assert "cached_metadata.n_rows" in keys
        assert "source" in keys

    def test_fields_for_generic_excludes_table_only(self):
        generic_fields = get_fields_for_asset_type("generic")
        keys = [f.key for f in generic_fields]
        assert "cached_metadata.n_rows" not in keys
        assert "source" not in keys
        # But includes universal fields
        assert "name" in keys

    def test_filterable_fields(self):
        filterable = get_filterable_fields()
        assert all(f.filterable for f in filterable)
        keys = [f.key for f in filterable]
        assert "name" in keys
        # non-filterable fields excluded
        assert "created_at" not in keys
