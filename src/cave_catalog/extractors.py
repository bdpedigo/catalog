"""Metadata extractors for table assets.

Each extractor reads lightweight metadata (schema, row count, size, partition
info) from a cloud storage URI and returns a ``TableMetadata`` instance.
"""

from __future__ import annotations

import abc
import asyncio
from typing import Any

import structlog

from cave_catalog.table_schemas import ColumnInfo, TableMetadata

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Base interface (task 2.1)
# ---------------------------------------------------------------------------


class MetadataExtractor(abc.ABC):
    """Base interface for format-specific metadata extractors."""

    @abc.abstractmethod
    async def extract(
        self,
        uri: str,
        storage_options: dict[str, Any] | None = None,
    ) -> TableMetadata:
        """Extract metadata from the table at *uri*.

        Parameters
        ----------
        uri
            Cloud or local path to the table/file.
        storage_options
            Optional storage credentials (e.g. GCS token dict).

        Returns
        -------
        TableMetadata
            Discovered metadata.
        """


# ---------------------------------------------------------------------------
# Delta Lake extractor (task 2.2)
# ---------------------------------------------------------------------------


class DeltaMetadataExtractor(MetadataExtractor):
    """Extract metadata from a Delta Lake table via the transaction log."""

    async def extract(
        self,
        uri: str,
        storage_options: dict[str, Any] | None = None,
    ) -> TableMetadata:
        from deltalake import DeltaTable

        logger.debug("delta_extract_start", uri=uri)

        kwargs: dict[str, Any] = {}
        if storage_options:
            kwargs["storage_options"] = storage_options

        try:
            dt = await asyncio.to_thread(lambda: DeltaTable(uri, **kwargs))
        except Exception as exc:
            msg = str(exc)
            if "no files in log segment" in msg.lower() or "log segment" in msg.lower():
                raise ValueError(
                    f"No Delta transaction log found at '{uri}'. "
                    "This path may not contain a Delta Lake table."
                ) from exc
            raise

        schema = await asyncio.to_thread(lambda: dt.schema())
        columns = [
            ColumnInfo(name=field.name, dtype=str(field.type))
            for field in schema.fields
        ]

        metadata = await asyncio.to_thread(lambda: dt.metadata())
        partition_columns = list(metadata.partition_columns)

        n_rows: int | None = None
        n_bytes: int | None = None

        # Try to get row count and size from file stats
        try:
            actions_table = await asyncio.to_thread(
                lambda: dt.get_add_actions(flatten=True)
            )
            # Convert arro3 Table to dict-of-lists via pyarrow
            import pyarrow as pa

            file_actions = pa.table(actions_table).to_pydict()
            if "num_records" in file_actions:
                row_counts = file_actions["num_records"]
                if all(r is not None for r in row_counts):
                    n_rows = sum(row_counts)
            if "size_bytes" in file_actions:
                sizes = file_actions["size_bytes"]
                if all(s is not None for s in sizes):
                    n_bytes = sum(sizes)
        except Exception:
            logger.debug("delta_stats_unavailable", uri=uri)

        return TableMetadata(
            n_rows=n_rows,
            n_columns=len(columns),
            n_bytes=n_bytes,
            columns=columns,
            partition_columns=partition_columns,
        )


# ---------------------------------------------------------------------------
# Parquet extractor (task 2.3)
# ---------------------------------------------------------------------------


class ParquetMetadataExtractor(MetadataExtractor):
    """Extract metadata from a Parquet file/dataset via polars."""

    async def extract(
        self,
        uri: str,
        storage_options: dict[str, Any] | None = None,
    ) -> TableMetadata:
        import polars as pl

        logger.debug("parquet_extract_start", uri=uri)

        kwargs: dict[str, Any] = {}
        if storage_options:
            kwargs["storage_options"] = storage_options

        lf = await asyncio.to_thread(lambda: pl.scan_parquet(uri, **kwargs))
        schema = await asyncio.to_thread(lambda: lf.collect_schema())
        columns = [
            ColumnInfo(name=name, dtype=str(dtype)) for name, dtype in schema.items()
        ]

        # Get on-disk size via fsspec and row count from parquet metadata
        n_rows: int | None = None
        n_bytes: int | None = None
        try:
            import fsspec
            import pyarrow.parquet as pq

            fs, path = await asyncio.to_thread(
                lambda: fsspec.core.url_to_fs(uri, **(storage_options or {}))
            )
            info = await asyncio.to_thread(lambda: fs.info(path))
            n_bytes = info.get("size")

            pq_meta = await asyncio.to_thread(
                lambda: pq.read_metadata(path, filesystem=fs)
            )
            n_rows = pq_meta.num_rows
        except Exception as exc:
            logger.warning("parquet_stats_unavailable", uri=uri, error=str(exc))

        return TableMetadata(
            n_rows=n_rows,
            n_columns=len(columns),
            n_bytes=n_bytes,
            columns=columns,
            partition_columns=[],
        )


# ---------------------------------------------------------------------------
# Extractor registry (task 2.4)
# ---------------------------------------------------------------------------

EXTRACTORS: dict[str, MetadataExtractor] = {
    "delta": DeltaMetadataExtractor(),
    "parquet": ParquetMetadataExtractor(),
}


def get_extractor(fmt: str) -> MetadataExtractor:
    """Look up extractor by format string.

    Raises ``ValueError`` if no extractor is registered for *fmt*.
    """
    try:
        return EXTRACTORS[fmt.lower()]
    except KeyError:
        raise ValueError(
            f"No metadata extractor for format '{fmt}'. "
            f"Supported: {', '.join(sorted(EXTRACTORS))}"
        )
