"""Unit tests for format-sniff validation (check_format_sniff and sniffers).

Real files are created on disk with tmp_path so the actual third-party
libraries (polars, deltalake) perform the sniff rather than mocks.
"""

from __future__ import annotations

import polars as pl
import pytest
from cave_catalog.validation import _sniff_delta, _sniff_parquet, check_format_sniff

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def parquet_file(tmp_path) -> str:
    """A valid Parquet file on disk."""
    path = tmp_path / "data.parquet"
    pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).write_parquet(str(path))
    return str(path)


@pytest.fixture
def delta_table(tmp_path) -> str:
    """A valid Delta table on disk."""
    path = tmp_path / "delta_table"
    pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).write_delta(str(path))
    return str(path)


@pytest.fixture
def text_file(tmp_path) -> str:
    """A plain text file — not a valid parquet or delta dataset."""
    path = tmp_path / "data.txt"
    path.write_text("not a parquet or delta file")
    return str(path)


# ---------------------------------------------------------------------------
# check_format_sniff — dispatcher behaviour
# ---------------------------------------------------------------------------


async def test_unknown_format_passes_with_message():
    result = await check_format_sniff("gs://bucket/path/", "lance")

    assert result.passed is True
    assert result.message is not None
    assert "lance" in result.message.lower()


async def test_unknown_format_case_insensitive():
    result = await check_format_sniff("gs://bucket/path/", "LANCE")

    assert result.passed is True


async def test_delta_format_passes_for_valid_table(delta_table):
    result = await check_format_sniff(delta_table, "delta")

    assert result.passed is True


async def test_delta_format_fails_for_parquet_file(parquet_file):
    """A plain Parquet file has no _delta_log/ — delta sniff should fail."""
    result = await check_format_sniff(parquet_file, "delta")

    assert result.passed is False
    assert result.message is not None


async def test_delta_format_case_insensitive(delta_table):
    result = await check_format_sniff(delta_table, "Delta")

    assert result.passed is True


async def test_parquet_format_passes_for_valid_file(parquet_file):
    result = await check_format_sniff(parquet_file, "parquet")

    assert result.passed is True


async def test_parquet_format_fails_for_text_file(text_file):
    """A plain text file is not valid Parquet — sniff should fail."""
    result = await check_format_sniff(text_file, "parquet")

    assert result.passed is False
    assert result.message is not None


# ---------------------------------------------------------------------------
# _sniff_delta — sniffer internals
# ---------------------------------------------------------------------------


async def test_sniff_delta_passes_for_valid_table(delta_table):
    result = await _sniff_delta(delta_table)

    assert result.passed is True
    assert result.message is None


async def test_sniff_delta_fails_for_parquet_file(parquet_file):
    """A parquet file has no _delta_log/ directory — DeltaTable should raise."""
    result = await _sniff_delta(parquet_file)

    assert result.passed is False
    assert "Delta sniff failed" in result.message


async def test_sniff_delta_fails_for_empty_dir(tmp_path):
    result = await _sniff_delta(str(tmp_path))

    assert result.passed is False
    assert "Delta sniff failed" in result.message


# ---------------------------------------------------------------------------
# _sniff_parquet — sniffer internals
# ---------------------------------------------------------------------------


async def test_sniff_parquet_passes_for_valid_file(parquet_file):
    result = await _sniff_parquet(parquet_file)

    assert result.passed is True
    assert result.message is None


async def test_sniff_parquet_fails_for_text_file(text_file):
    """A plain text file is not Parquet — polars should raise."""
    result = await _sniff_parquet(text_file)

    assert result.passed is False
    assert "Parquet sniff failed" in result.message


async def test_sniff_parquet_fails_for_empty_dir(tmp_path):
    result = await _sniff_parquet(str(tmp_path))

    assert result.passed is False
    assert "Parquet sniff failed" in result.message
