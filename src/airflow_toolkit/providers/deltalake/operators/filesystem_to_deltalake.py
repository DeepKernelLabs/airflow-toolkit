from __future__ import annotations

import logging
import os
import tempfile
import time
from collections.abc import Iterator, Mapping
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError

from airflow_toolkit._compact.airflow_shim import BaseOperator, Context, BaseHook
from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory
from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol
from airflow_toolkit.providers.deltalake.storage_options import (
    deltalake_storage_options,
)
from airflow_toolkit.providers.filesystem.batch_reader import (
    FileBatchReaderMixin,
    dataframe_batches_to_arrow,
)
from airflow_toolkit.types import MetadataSpec

logger = logging.getLogger(__name__)

_BATCH_SIZE = 50_000


class FilesystemToDeltalakeOperator(BaseOperator, FileBatchReaderMixin):
    """
    Copies files from a filesystem to a Delta Lake table.

    Unlike FilesystemToDatabaseOperator, metadata columns (including
    "_"-prefixed ones) are always written as strings, never coerced to a
    datetime type — this avoids Delta Lake's microsecond-precision timestamp
    constraint without a dedicated verification pass. A string "_DS" column
    still works correctly with idempotent deletes (DataFusion casts the
    predicate's string literal automatically).

    When idempotent=True (the default), write_mode is ignored: existing rows
    matching the current run's metadata are deleted once, upfront, and every
    file is then appended — equivalent to FilesystemToDatabaseOperator's
    delete-then-insert idempotency, but as a single atomic Delta commit
    instead of two.
    """

    template_fields = (
        "table_path",
        "filesystem_conn_id",
        "delta_storage_conn_id",
        "filesystem_path",
        "metadata",
    )

    def __init__(
        self,
        filesystem_conn_id: str,
        filesystem_path: str,
        table_path: str,
        delta_storage_conn_id: str | None = None,
        source_format: Literal[
            "csv", "json", "parquet", "excel", "avro", "fixed_width"
        ] = "csv",
        source_format_options: Mapping[str, Any] | None = None,
        batch_size: int = _BATCH_SIZE,
        write_mode: Literal["error", "append", "overwrite", "ignore"] = "append",
        metadata: MetadataSpec | None = None,
        metadata_columns_in_uppercase: bool = True,
        include_source_path: bool = True,
        idempotent: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.filesystem_conn_id = filesystem_conn_id
        self.filesystem_path = filesystem_path
        self.table_path = table_path
        self.delta_storage_conn_id = delta_storage_conn_id
        self.source_format = source_format
        self.source_format_options = source_format_options
        self.batch_size = batch_size
        self.write_mode = write_mode
        self.metadata = {"_DS": "{{ ds }}"} if metadata is None else metadata
        self.metadata_columns_in_uppercase = metadata_columns_in_uppercase
        self.include_source_path = include_source_path
        self.idempotent = idempotent

    def execute(self, context: Context) -> None:
        logger.info(f"Create connection for filesystem ({self.filesystem_conn_id})")
        filesystem: FilesystemProtocol = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.filesystem_conn_id),
        )

        storage_options: dict[str, str] | None = None
        if self.delta_storage_conn_id:
            storage_options = deltalake_storage_options(
                BaseHook.get_connection(self.delta_storage_conn_id)
            )

        if self.idempotent:
            self._delete_existing_run_data(storage_options)

        valid_extensions = self._FORMAT_EXTENSIONS.get(
            self.source_format, (f".{self.source_format}",)
        )
        first_file = True
        for blob_path in filesystem.list_files(prefix=self.filesystem_path):
            if not blob_path.endswith(valid_extensions):
                logger.warning(
                    f"Blob {blob_path} is not in the right format. Skipping..."
                )
                continue

            logger.info(f"Downloading {blob_path} to temporary file")
            dl_start = time.monotonic()
            raw_bytes = filesystem.read(blob_path)
            file_mb = len(raw_bytes) / 1024 / 1024
            logger.info(
                f"Downloaded {file_mb:.1f} MB in {time.monotonic() - dl_start:.1f}s"
            )

            tmp_suffix = self._FORMAT_TEMP_SUFFIX.get(
                self.source_format, f".{self.source_format}"
            )
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=tmp_suffix)
            try:
                with os.fdopen(tmp_fd, "wb") as tmp_file:
                    tmp_file.write(raw_bytes)
                del raw_bytes

                schema = self._schema_with_metadata_columns(
                    self._get_file_schema(tmp_path)
                )
                enriched_batches = self._enrich_batches(
                    self._iter_batches(tmp_path), blob_path
                )
                arrow_batches = dataframe_batches_to_arrow(enriched_batches, schema)
                # write_deltalake (>=1.6) requires an Arrow-C-Stream-exportable
                # object, not a raw generator + separate schema= kwarg (that was
                # the 0.17.x signature) — wrap the generator explicitly.
                batch_reader = pa.RecordBatchReader.from_batches(schema, arrow_batches)

                mode = (
                    "append" if (self.idempotent or not first_file) else self.write_mode
                )
                logger.info(f"Writing {blob_path} to {self.table_path} (mode={mode})")
                write_start = time.monotonic()
                try:
                    write_deltalake(
                        self.table_path,
                        batch_reader,
                        mode=mode,
                        schema_mode="merge",
                        storage_options=storage_options,
                    )
                except DeltaError as exc:
                    # Errors raised inside dataframe_batches_to_arrow() while
                    # the writer lazily pulls from the generator (via the
                    # Arrow C Stream interface) surface here wrapped in a Rust
                    # DeltaError instead of propagating as the original
                    # ValueError. Unwrap it so callers see the clear message.
                    if "does not match the schema inferred from" in str(exc):
                        raise ValueError(str(exc)) from exc
                    raise
                logger.info(
                    f"Wrote {blob_path} in {time.monotonic() - write_start:.1f}s"
                )
                first_file = False
            finally:
                os.unlink(tmp_path)

    def _enrich_batches(
        self, batches: Iterator[pd.DataFrame], blob_path: str
    ) -> Iterator[pd.DataFrame]:
        """Adds metadata + source-path columns to each DataFrame batch."""
        source_path_column = (
            "_LOADED_FROM" if self.metadata_columns_in_uppercase else "_loaded_from"
        )
        for batch_df in batches:
            for key, value in self.metadata.items():
                metadata_key = (
                    key.upper() if self.metadata_columns_in_uppercase else key.lower()
                )
                batch_df[metadata_key] = value

            if self.include_source_path:
                batch_df[source_path_column] = blob_path

            yield batch_df

    def _schema_with_metadata_columns(self, schema: pa.Schema) -> pa.Schema:
        """Extends the file's schema with the metadata/source-path columns
        _enrich_batches() adds — always typed as strings, see class docstring.
        """
        extra_names = [
            key.upper() if self.metadata_columns_in_uppercase else key.lower()
            for key in self.metadata
        ]
        if self.include_source_path:
            extra_names.append(
                "_LOADED_FROM" if self.metadata_columns_in_uppercase else "_loaded_from"
            )
        for name in extra_names:
            if name not in schema.names:
                schema = schema.append(pa.field(name, pa.string()))
        return schema

    def _delete_existing_run_data(self, storage_options: dict[str, str] | None) -> None:
        """Delete rows matching the current run's metadata values, once,
        before writing any file.

        Mirrors _delete_existing_run_data() in FilesystemToDatabaseOperator.
        Must run exactly once upfront (not per-file/per-batch) — otherwise a
        second file in the same run sharing the same metadata value would
        wipe out rows the first file just wrote in this same execute() call.

        DeltaTable.delete() takes a raw SQL predicate string evaluated by
        DataFusion, with no bind params — unlike SQLAlchemy's text()+params,
        values are escaped manually here (single quotes doubled). This is a
        known, accepted risk: there is no parameterized alternative in
        delta-rs.
        """
        if not self.metadata:
            return
        if not DeltaTable.is_deltatable(
            self.table_path, storage_options=storage_options
        ):
            return

        conditions = []
        for key, value in self.metadata.items():
            metadata_key = (
                key.upper() if self.metadata_columns_in_uppercase else key.lower()
            )
            escaped_value = str(value).replace("'", "''")
            conditions.append(f"\"{metadata_key}\" = '{escaped_value}'")
        predicate = " AND ".join(conditions)

        table = DeltaTable(self.table_path, storage_options=storage_options)
        result = table.delete(predicate)
        logger.info(f"Idempotent delete: {result} (predicate: {predicate})")
