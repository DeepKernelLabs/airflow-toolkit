from __future__ import annotations

import ctypes
import gc
import logging
import os
import resource
import time
from collections.abc import Iterator, Mapping
from typing import Any

import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)

# Maps source_format names to the file extensions they match.
# Used by callers to skip blobs that don't belong to the selected format.
_FORMAT_EXTENSIONS: dict[str, tuple[str, ...]] = {
    "csv": (".csv", ".csv.gz"),
    "json": (".json", ".json.gz"),
    "parquet": (".parquet", ".parquet.gz"),
    "excel": (".xlsx", ".xls"),
    "avro": (".avro",),
    "fixed_width": (".fwf", ".txt", ".dat"),
}

# Canonical extension for the temp file callers create before reading.
# Must match what the underlying reader expects (e.g. pandas read_excel
# infers the engine from the file extension).
_FORMAT_TEMP_SUFFIX: dict[str, str] = {
    "csv": ".csv",
    "json": ".json",
    "parquet": ".parquet",
    "excel": ".xlsx",
    "avro": ".avro",
    "fixed_width": ".fwf",
}


class FileBatchReaderMixin:
    """Reads a local file (csv/json/parquet/excel/avro/fixed_width) in batches.

    Shared by every operator that streams a downloaded file into batches
    without loading it entirely into memory. Expects the consuming class to
    set `self.source_format`, `self.source_format_options`, and
    `self.batch_size` — same contract as before this was extracted.
    """

    _FORMAT_EXTENSIONS = _FORMAT_EXTENSIONS
    _FORMAT_TEMP_SUFFIX = _FORMAT_TEMP_SUFFIX

    source_format: str
    source_format_options: Mapping[str, Any] | None
    batch_size: int

    def _get_file_columns(self, path: str) -> set[str]:
        """Returns the column names in the source file without loading row data."""
        options: dict[str, Any] = dict(self.source_format_options or {})
        match self.source_format:
            case "parquet":
                import pyarrow.parquet as pq

                return set(pq.ParquetFile(path).schema_arrow.names)
            case "csv":
                peek_opts = {
                    k: v
                    for k, v in options.items()
                    if k not in ("chunksize", "iterator")
                }
                return set(pd.read_csv(path, nrows=0, **peek_opts).columns)
            case "json":
                peek_opts = {
                    k: v for k, v in options.items() if k not in ("chunksize",)
                }
                if peek_opts.get("lines"):
                    return set(pd.read_json(path, nrows=1, **peek_opts).columns)
                return set(pd.read_json(path, **peek_opts).columns)
            case "excel":
                peek_opts = {k: v for k, v in options.items() if k != "sheet_name"}
                return set(pd.read_excel(path, nrows=0, **peek_opts).columns)
            case "avro":
                import fastavro

                with open(path, "rb") as f:
                    reader = fastavro.reader(f)
                    schema = reader.writer_schema
                    return {field["name"] for field in schema["fields"]}
            case "fixed_width":
                peek_opts = {k: v for k, v in options.items() if k != "chunksize"}
                return set(pd.read_fwf(path, nrows=0, **peek_opts).columns)
            case _:
                return set()

    def _get_file_schema(self, path: str) -> pa.Schema:
        """Returns the pyarrow.Schema of the file, derived from its first batch.

        Table-format destinations (Delta/Iceberg) require a schema fixed
        upfront for the whole file, unlike SQL, which tolerates per-batch
        type differences via SQLAlchemy. Pandas infers dtypes independently
        per chunk for csv/json/fixed_width, so later chunks can disagree
        with the first — see dataframe_batches_to_arrow(), which enforces
        this schema on every subsequent batch.

        Note this reads the first batch twice (once here, once when the
        caller re-invokes _iter_batches() to actually process the file) —
        a small, bounded cost accepted for keeping schema derivation
        format-agnostic instead of hand-rolling per-format Arrow mapping.
        """
        first_batch = next(self._iter_batches(path), None)
        if first_batch is None:
            raise ValueError(f"No data found in {path}")
        return pa.Table.from_pandas(first_batch, preserve_index=False).schema

    def _iter_batches(self, path: str) -> Iterator[pd.DataFrame]:
        """Yields DataFrames of up to batch_size rows from the source file."""
        options: dict[str, Any] = dict(self.source_format_options or {})
        match self.source_format:
            case "parquet":
                import pyarrow.parquet as pq

                pq_file = pq.ParquetFile(path)
                pq_meta = pq_file.metadata
                num_rg = pq_meta.num_row_groups
                logger.info(
                    f"Parquet: {num_rg} row group(s), "
                    f"{pq_meta.num_rows:,} rows, "
                    f"{pq_meta.serialized_size / 1024 / 1024:.1f} MB on disk"
                )
                integer_fields = {
                    pq_file.schema_arrow.field(i).name
                    for i in range(len(pq_file.schema_arrow))
                    if pa.types.is_integer(pq_file.schema_arrow.field(i).type)
                }
                with open(path, "rb") as _fh:
                    _fd = _fh.fileno()
                    _fsize = os.path.getsize(path)
                    _FADV_DONTNEED = 4  # POSIX_FADV_DONTNEED on Linux

                    for rg_idx in range(num_rg):
                        rg_rows = pq_file.metadata.row_group(rg_idx).num_rows
                        logger.info(
                            f"[RG {rg_idx + 1}/{num_rg}] Reading: {rg_rows:,} rows"
                        )
                        rg_start = time.monotonic()
                        # Slice at the Arrow level before converting to pandas.
                        # Arrow is 3-5x more compact than pandas for string-heavy
                        # schemas, so keeping the RG as Arrow and converting one
                        # batch at a time keeps peak RSS low. Do NOT use
                        # self_destruct=True: slices share the parent table's
                        # buffers, so self_destruct causes a double-free on del table.
                        table = pq_file.read_row_group(rg_idx)
                        num_rows = len(table)
                        start = 0
                        while start < num_rows:
                            end = min(start + self.batch_size, num_rows)
                            chunk = table.slice(start, end - start)
                            df = chunk.to_pandas()
                            del chunk
                            for field_name in integer_fields:
                                if (
                                    field_name in df.columns
                                    and df[field_name].dtype.kind == "f"
                                ):
                                    df[field_name] = df[field_name].astype("Int64")
                            yield df
                            start = end
                        del table
                        gc.collect()
                        cache_dropped = False
                        try:
                            ctypes.CDLL("libc.so.6").posix_fadvise(
                                _fd, 0, _fsize, _FADV_DONTNEED
                            )
                            cache_dropped = True
                        except Exception:
                            pass
                        try:
                            arrow_mb = (
                                pa.default_memory_pool().bytes_allocated() / 1024 / 1024
                            )
                        except Exception:
                            arrow_mb = -1
                        rss_mb = (
                            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
                        )
                        logger.info(
                            f"[RG {rg_idx + 1}/{num_rg}] Done in {time.monotonic() - rg_start:.1f}s | "
                            f"RSS={rss_mb:.0f} MB  Arrow={arrow_mb:.0f} MB  "
                            f"cache_drop={'ok' if cache_dropped else 'skipped'}"
                        )
            case "csv":
                yield from pd.read_csv(path, chunksize=self.batch_size, **options)
            case "json":
                if options.get("lines"):
                    yield from pd.read_json(path, chunksize=self.batch_size, **options)
                else:
                    yield pd.read_json(path, **options)
            case "excel":
                df = pd.read_excel(path, **options)
                for start in range(0, max(len(df), 1), self.batch_size):
                    yield df.iloc[start : start + self.batch_size].copy()
            case "avro":
                import fastavro

                with open(path, "rb") as f:
                    reader = fastavro.reader(f)
                    batch: list[dict[str, Any]] = []
                    for record in reader:
                        batch.append(record)
                        if len(batch) >= self.batch_size:
                            yield pd.DataFrame(batch)
                            batch = []
                    if batch:
                        yield pd.DataFrame(batch)
            case "fixed_width":
                yield from pd.read_fwf(path, chunksize=self.batch_size, **options)
            case _:
                raise ValueError(f"Unknown source format {self.source_format}")


def dataframe_batches_to_arrow(
    batches: Iterator[pd.DataFrame], schema: pa.Schema
) -> Iterator[pa.RecordBatch]:
    """Converts a stream of DataFrames to RecordBatches conforming to *schema*.

    Formats read via pandas chunking (csv/json/fixed_width) infer dtypes
    independently per chunk, so a later chunk's dtype can disagree with the
    schema derived from the first chunk (e.g. a column inferred as int64 in
    chunk 1 that contains text starting in chunk 5). Raises a clear error
    instead of letting a cryptic ArrowTypeError surface from inside the
    Delta/Iceberg writer.
    """
    for i, df in enumerate(batches):
        try:
            table = pa.Table.from_pandas(
                df, schema=schema, preserve_index=False, safe=False
            )
        except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError) as exc:
            raise ValueError(
                f"Batch {i} does not match the schema inferred from the file's "
                f"first batch ({schema}). This usually means the source file has "
                f"inconsistent types across chunks (e.g. a numeric column that "
                f"becomes text later in the file). Pass explicit dtypes via "
                f"source_format_options to fix this."
            ) from exc
        yield from table.to_batches()
