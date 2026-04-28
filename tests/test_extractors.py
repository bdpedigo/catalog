"""Tests for metadata extractors: Delta and Parquet.

Phase 2 tests — covers tasks 2.1–2.4.
Uses real fixture data on disk (same pattern as test_validation.py).
"""

from __future__ import annotations

import polars as pl
import pytest
from cave_catalog.extractors import (
    DeltaMetadataExtractor,
    ParquetMetadataExtractor,
    get_extractor,
)
from cave_catalog.table_schemas import TableMetadata

# ---------------------------------------------------------------------------
# Fixtures (reuse the same pattern as test_validation.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def parquet_file(tmp_path) -> str:
    path = tmp_path / "data.parquet"
    pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).write_parquet(str(path))
    return str(path)


@pytest.fixture
def delta_table(tmp_path) -> str:
    path = tmp_path / "delta_table"
    pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).write_delta(str(path))
    return str(path)


@pytest.fixture
def partitioned_delta(tmp_path) -> str:
    path = tmp_path / "partitioned_delta"
    pl.DataFrame(
        {"part": ["x", "x", "y"], "val": [1, 2, 3]}
    ).write_delta(str(path), delta_write_options={"partition_by": ["part"]})
    return str(path)


# ---------------------------------------------------------------------------
# Delta extractor
# ---------------------------------------------------------------------------


async def test_delta_extract_columns(delta_table):
    ext = DeltaMetadataExtractor()
    result = await ext.extract(delta_table)

    assert isinstance(result, TableMetadata)
    col_names = [c.name for c in result.columns]
    assert "a" in col_names
    assert "b" in col_names


async def test_delta_extract_counts(delta_table):
    ext = DeltaMetadataExtractor()
    result = await ext.extract(delta_table)

    assert result.n_columns == 2
    assert result.n_rows == 3
    assert result.n_bytes is not None and result.n_bytes > 0


async def test_delta_extract_partition_columns(partitioned_delta):
    ext = DeltaMetadataExtractor()
    result = await ext.extract(partitioned_delta)

    assert result.partition_columns == ["part"]


async def test_delta_extract_no_partitions(delta_table):
    ext = DeltaMetadataExtractor()
    result = await ext.extract(delta_table)

    assert result.partition_columns == []


async def test_delta_extract_invalid_uri(tmp_path):
    ext = DeltaMetadataExtractor()
    with pytest.raises(Exception):
        await ext.extract(str(tmp_path / "nonexistent"))


# ---------------------------------------------------------------------------
# Parquet extractor
# ---------------------------------------------------------------------------


async def test_parquet_extract_columns(parquet_file):
    ext = ParquetMetadataExtractor()
    result = await ext.extract(parquet_file)

    assert isinstance(result, TableMetadata)
    col_names = [c.name for c in result.columns]
    assert "a" in col_names
    assert "b" in col_names


async def test_parquet_extract_counts(parquet_file):
    ext = ParquetMetadataExtractor()
    result = await ext.extract(parquet_file)

    assert result.n_columns == 2
    assert result.n_rows == 3
    assert result.n_bytes is not None and result.n_bytes > 0


async def test_parquet_extract_no_partition_columns(parquet_file):
    ext = ParquetMetadataExtractor()
    result = await ext.extract(parquet_file)

    assert result.partition_columns == []


async def test_parquet_extract_invalid_uri(tmp_path):
    ext = ParquetMetadataExtractor()
    with pytest.raises(Exception):
        await ext.extract(str(tmp_path / "nonexistent.parquet"))


# ---------------------------------------------------------------------------
# Extractor registry
# ---------------------------------------------------------------------------


def test_get_extractor_delta():
    ext = get_extractor("delta")
    assert isinstance(ext, DeltaMetadataExtractor)


def test_get_extractor_parquet():
    ext = get_extractor("parquet")
    assert isinstance(ext, ParquetMetadataExtractor)


def test_get_extractor_case_insensitive():
    ext = get_extractor("Delta")
    assert isinstance(ext, DeltaMetadataExtractor)


def test_get_extractor_unknown_raises():
    with pytest.raises(ValueError, match="No metadata extractor"):
        get_extractor("lance")
