"""Tests for column kind validation against the materialization service.

Covers validate_column_kinds — materialization kinds validated against ME,
segmentation/point kinds pass through without external calls.
"""

from __future__ import annotations

import httpx
from cave_catalog.validation import (
    validate_column_kinds,
    validate_kind_dtypes,
    validate_point_group_uniqueness,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _annotations_with_mat_kinds(
    *kinds: tuple[str, str, str],
) -> list[dict]:
    """Build annotations from (col_name, target_table, target_col) tuples."""
    return [
        {
            "column_name": col_name,
            "kind": {
                "kind": "materialization",
                "target_table": target_table,
                "target_column": target_col,
            },
        }
        for col_name, target_table, target_col in kinds
    ]


def _mock_transport(status_code: int = 200, json_body: list | None = None):
    """Return an httpx transport that returns a canned response."""

    async def handler(request: httpx.Request) -> httpx.Response:
        body = json_body if json_body is not None else []
        return httpx.Response(status_code, json=body)

    return httpx.MockTransport(handler)


# ---------------------------------------------------------------------------
# No kinds → passes trivially
# ---------------------------------------------------------------------------


async def test_no_kind_passes():
    annotations = [{"column_name": "a", "description": "col a"}]
    async with httpx.AsyncClient() as client:
        result = await validate_column_kinds(annotations, "ds1", client)
    assert result.passed is True
    assert result.errors == []


async def test_empty_annotations_passes():
    async with httpx.AsyncClient() as client:
        result = await validate_column_kinds([], "ds1", client)
    assert result.passed is True


# ---------------------------------------------------------------------------
# Non-materialization kinds → no ME call, passes
# ---------------------------------------------------------------------------


async def test_segmentation_kind_passes_without_me():
    annotations = [
        {
            "column_name": "pt_root_id",
            "kind": {"kind": "segmentation", "node_level": "root_id"},
        }
    ]
    async with httpx.AsyncClient() as client:
        result = await validate_column_kinds(annotations, "ds1", client)
    assert result.passed is True


async def test_split_point_kind_passes_without_me():
    annotations = [
        {
            "column_name": "pt_position_x",
            "kind": {"kind": "split_point", "axis": "x", "point_group": "pt_position"},
        }
    ]
    async with httpx.AsyncClient() as client:
        result = await validate_column_kinds(annotations, "ds1", client)
    assert result.passed is True


async def test_packed_point_kind_passes_without_me():
    annotations = [
        {
            "column_name": "pt_position",
            "kind": {"kind": "packed_point", "resolution": [8.0, 8.0, 40.0]},
        }
    ]
    async with httpx.AsyncClient() as client:
        result = await validate_column_kinds(annotations, "ds1", client)
    assert result.passed is True


# ---------------------------------------------------------------------------
# ME not configured → skipped
# ---------------------------------------------------------------------------


async def test_skipped_when_me_not_configured(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_mat_kinds(
        ("col_a", "synapses", "id"),
    )
    async with httpx.AsyncClient() as client:
        result = await validate_column_kinds(annotations, "ds1", client)

    assert result.passed is True
    assert result.skipped is True
    assert "not configured" in result.message

    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Valid materialization kinds → passes
# ---------------------------------------------------------------------------


async def test_valid_mat_kind_passes(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_mat_kinds(
        ("pre_pt_root_id", "synapses", "pre_pt_root_id"),
        ("post_pt_root_id", "synapses", "post_pt_root_id"),
    )
    transport = _mock_transport(200, ["synapses", "nucleus_detection_v0"])
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_kinds(annotations, "minnie65", client)

    assert result.passed is True
    assert result.errors == []

    get_settings.cache_clear()


async def test_multiple_tables_all_valid(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_mat_kinds(
        ("syn_id", "synapses", "id"),
        ("cell_id", "nucleus_detection_v0", "id"),
    )
    transport = _mock_transport(200, ["synapses", "nucleus_detection_v0"])
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_kinds(annotations, "minnie65", client)

    assert result.passed is True

    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Invalid materialization kinds → fails
# ---------------------------------------------------------------------------


async def test_invalid_table_fails(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_mat_kinds(
        ("col_a", "nonexistent_table", "id"),
    )
    transport = _mock_transport(200, ["synapses"])
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_kinds(annotations, "minnie65", client)

    assert result.passed is False
    assert len(result.errors) == 1
    err = result.errors[0]
    assert err.kind == "materialization"
    assert err.column_name == "col_a"
    assert "not found" in err.reason

    get_settings.cache_clear()


async def test_mixed_valid_and_invalid(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_mat_kinds(
        ("good_col", "synapses", "id"),
        ("bad_col", "fake_table", "id"),
    )
    transport = _mock_transport(200, ["synapses"])
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_kinds(annotations, "minnie65", client)

    assert result.passed is False
    assert len(result.errors) == 1
    assert result.errors[0].column_name == "bad_col"

    get_settings.cache_clear()


async def test_multiple_kinds_to_same_bad_table(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_mat_kinds(
        ("col_a", "bad_table", "id"),
        ("col_b", "bad_table", "value"),
    )
    transport = _mock_transport(200, ["synapses"])
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_kinds(annotations, "minnie65", client)

    assert result.passed is False
    assert len(result.errors) == 2

    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# ME errors → graceful skip
# ---------------------------------------------------------------------------


async def test_me_auth_failure_skips(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_mat_kinds(
        ("col_a", "synapses", "id"),
    )
    transport = _mock_transport(403)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_kinds(annotations, "minnie65", client)

    assert result.passed is True
    assert result.skipped is True
    assert "auth failed" in result.message

    get_settings.cache_clear()


async def test_me_server_error_skips(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_mat_kinds(
        ("col_a", "synapses", "id"),
    )
    transport = _mock_transport(500)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_kinds(annotations, "minnie65", client)

    assert result.passed is True
    assert result.skipped is True

    get_settings.cache_clear()


async def test_me_connection_error_skips(monkeypatch):
    monkeypatch.setenv("MAT_ENGINE_URL", "http://me:5000")
    from cave_catalog.config import get_settings

    get_settings.cache_clear()

    annotations = _annotations_with_mat_kinds(
        ("col_a", "synapses", "id"),
    )

    async def _raise(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    transport = httpx.MockTransport(_raise)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await validate_column_kinds(annotations, "minnie65", client)

    assert result.passed is True
    assert result.skipped is True

    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Dtype validation for kinds
# ---------------------------------------------------------------------------


def test_dtype_segmentation_int64_passes():
    annotations = [
        {
            "column_name": "root_id",
            "kind": {"kind": "segmentation", "node_level": "root_id"},
        }
    ]
    columns = [{"name": "root_id", "dtype": "int64"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert errors == []


def test_dtype_segmentation_uint64_passes():
    annotations = [
        {
            "column_name": "root_id",
            "kind": {"kind": "segmentation", "node_level": "root_id"},
        }
    ]
    columns = [{"name": "root_id", "dtype": "uint64"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert errors == []


def test_dtype_segmentation_string_fails():
    annotations = [
        {
            "column_name": "root_id",
            "kind": {"kind": "segmentation", "node_level": "root_id"},
        }
    ]
    columns = [{"name": "root_id", "dtype": "string"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert len(errors) == 1
    assert errors[0].kind == "segmentation"
    assert "integer" in errors[0].reason


def test_dtype_segmentation_float_fails():
    annotations = [
        {
            "column_name": "root_id",
            "kind": {"kind": "segmentation", "node_level": "root_id"},
        }
    ]
    columns = [{"name": "root_id", "dtype": "float64"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert len(errors) == 1
    assert errors[0].kind == "segmentation"


def test_dtype_split_point_float64_passes():
    annotations = [
        {"column_name": "pt_x", "kind": {"kind": "split_point", "axis": "x"}}
    ]
    columns = [{"name": "pt_x", "dtype": "float64"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert errors == []


def test_dtype_split_point_int32_passes():
    annotations = [
        {"column_name": "pt_x", "kind": {"kind": "split_point", "axis": "x"}}
    ]
    columns = [{"name": "pt_x", "dtype": "int32"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert errors == []


def test_dtype_split_point_string_fails():
    annotations = [
        {"column_name": "pt_x", "kind": {"kind": "split_point", "axis": "x"}}
    ]
    columns = [{"name": "pt_x", "dtype": "string"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert len(errors) == 1
    assert errors[0].kind == "split_point"
    assert "numeric" in errors[0].reason


def test_dtype_packed_point_float64_passes():
    annotations = [{"column_name": "pt", "kind": {"kind": "packed_point"}}]
    columns = [{"name": "pt", "dtype": "float64"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert errors == []


def test_dtype_packed_point_string_fails():
    annotations = [{"column_name": "pt", "kind": {"kind": "packed_point"}}]
    columns = [{"name": "pt", "dtype": "string"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert len(errors) == 1
    assert errors[0].kind == "packed_point"
    assert "numeric" in errors[0].reason


def test_dtype_materialization_no_constraint():
    """Mat kinds have no dtype constraint."""
    annotations = [
        {
            "column_name": "id",
            "kind": {
                "kind": "materialization",
                "target_table": "t",
                "target_column": "c",
            },
        }
    ]
    columns = [{"name": "id", "dtype": "string"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert errors == []


def test_dtype_no_columns_skips():
    """When no metadata columns available, validation is skipped."""
    annotations = [
        {
            "column_name": "root_id",
            "kind": {"kind": "segmentation", "node_level": "root_id"},
        }
    ]
    errors = validate_kind_dtypes(annotations, [])
    assert errors == []


def test_dtype_no_kind_skips():
    """Annotations without kind are skipped."""
    annotations = [{"column_name": "col", "description": "just a desc"}]
    columns = [{"name": "col", "dtype": "string"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert errors == []


def test_dtype_orphaned_annotation_skips():
    """Annotation for a column not in metadata is silently skipped."""
    annotations = [
        {
            "column_name": "missing",
            "kind": {"kind": "segmentation", "node_level": "root_id"},
        }
    ]
    columns = [{"name": "other_col", "dtype": "int64"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert errors == []


# ---------------------------------------------------------------------------
# Point group uniqueness validation
# ---------------------------------------------------------------------------


def test_point_group_uniqueness_no_duplicates():
    annotations = [
        {
            "column_name": "pt_x",
            "kind": {"kind": "split_point", "axis": "x", "point_group": "pos"},
        },
        {
            "column_name": "pt_y",
            "kind": {"kind": "split_point", "axis": "y", "point_group": "pos"},
        },
        {
            "column_name": "pt_z",
            "kind": {"kind": "split_point", "axis": "z", "point_group": "pos"},
        },
    ]
    errors = validate_point_group_uniqueness(annotations)
    assert errors == []


def test_point_group_uniqueness_duplicate_axis():
    annotations = [
        {
            "column_name": "pt_x",
            "kind": {"kind": "split_point", "axis": "x", "point_group": "pos"},
        },
        {
            "column_name": "pt_x2",
            "kind": {"kind": "split_point", "axis": "x", "point_group": "pos"},
        },
    ]
    errors = validate_point_group_uniqueness(annotations)
    assert len(errors) == 1
    assert errors[0].column_name == "pt_x2"
    assert "Duplicate" in errors[0].reason


def test_point_group_uniqueness_different_groups_ok():
    annotations = [
        {
            "column_name": "pt_x",
            "kind": {"kind": "split_point", "axis": "x", "point_group": "pre"},
        },
        {
            "column_name": "pt_x2",
            "kind": {"kind": "split_point", "axis": "x", "point_group": "post"},
        },
    ]
    errors = validate_point_group_uniqueness(annotations)
    assert errors == []


def test_point_group_uniqueness_null_group_skipped():
    """Annotations without point_group are not checked for uniqueness."""
    annotations = [
        {"column_name": "pt_x", "kind": {"kind": "split_point", "axis": "x"}},
        {"column_name": "pt_x2", "kind": {"kind": "split_point", "axis": "x"}},
    ]
    errors = validate_point_group_uniqueness(annotations)
    assert errors == []


# ---------------------------------------------------------------------------
# Case-insensitive dtype matching (e.g., pandas nullable "Int64" vs "int64")
# ---------------------------------------------------------------------------


def test_dtype_segmentation_Int64_passes():
    """Pandas nullable Int64 dtype should match case-insensitively."""
    annotations = [
        {
            "column_name": "root_id",
            "kind": {"kind": "segmentation", "node_level": "root_id"},
        }
    ]
    columns = [{"name": "root_id", "dtype": "Int64"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert errors == []


def test_dtype_split_point_Int64_passes():
    annotations = [
        {"column_name": "pt_x", "kind": {"kind": "split_point", "axis": "x"}}
    ]
    columns = [{"name": "pt_x", "dtype": "Int64"}]
    errors = validate_kind_dtypes(annotations, columns)
    assert errors == []
