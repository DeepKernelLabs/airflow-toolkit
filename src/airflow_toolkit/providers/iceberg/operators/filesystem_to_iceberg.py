from __future__ import annotations

import logging
import os
import tempfile
import time
from collections.abc import Iterator, Mapping
from typing import Any, Literal

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.expressions import And, BooleanExpression, EqualTo

from airflow_toolkit._compact.airflow_shim import BaseOperator, Context, BaseHook
from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory
from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol
from airflow_toolkit.providers.filesystem.batch_reader import (
    FileBatchReaderMixin,
    dataframe_batches_to_arrow,
)
from airflow_toolkit.types import MetadataSpec

logger = logging.getLogger(__name__)

_BATCH_SIZE = 50_000


class FilesystemToIcebergOperator(BaseOperator, FileBatchReaderMixin):
    """
    Copies files from a filesystem to an Iceberg table.

    Unlike FilesystemToDeltalakeOperator, pyiceberg's Table.append()/
    overwrite() require a fully materialized pyarrow.Table — there is no
    public streaming-writer API — so each batch of up to batch_size rows is
    committed as its own Iceberg snapshot: RSS stays bounded per batch, but
    a single source file can produce multiple snapshots instead of one
    atomic commit like Delta. This mirrors how streaming writers commonly
    write to Iceberg; tables accumulating many small files can be compacted
    later via table.maintenance.

    Metadata columns are always written as strings (see
    FilesystemToDeltalakeOperator for the same choice and rationale).

    When idempotent=True (the default), write_mode is ignored: existing rows
    matching the current run's metadata are deleted once, upfront (only if
    the table already existed before this run), and every batch is then
    appended.

    catalog_properties is passed through as-is to pyiceberg.catalog.
    load_catalog() — any catalog type pyiceberg supports (REST, SQL, Glue,
    Hive, ...) works, this operator has no opinion on which.
    """

    template_fields = (
        "table_identifier",
        "filesystem_conn_id",
        "filesystem_path",
        "metadata",
    )

    def __init__(
        self,
        filesystem_conn_id: str,
        filesystem_path: str,
        catalog_name: str,
        catalog_properties: Mapping[str, str],
        table_identifier: str,
        source_format: Literal[
            "csv", "json", "parquet", "excel", "avro", "fixed_width"
        ] = "csv",
        source_format_options: Mapping[str, Any] | None = None,
        batch_size: int = _BATCH_SIZE,
        write_mode: Literal["append", "overwrite"] = "append",
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
        self.catalog_name = catalog_name
        self.catalog_properties = dict(catalog_properties)
        self.table_identifier = table_identifier
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

        catalog = load_catalog(self.catalog_name, **self.catalog_properties)
        namespace = Catalog.namespace_from(self.table_identifier)
        catalog.create_namespace_if_not_exists(namespace)
        table_already_existed = catalog.table_exists(self.table_identifier)

        valid_extensions = self._FORMAT_EXTENSIONS.get(
            self.source_format, (f".{self.source_format}",)
        )
        table = None
        first_batch_overall = True
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

                if table is None:
                    table = catalog.create_table_if_not_exists(
                        self.table_identifier, schema=schema
                    )
                    if self.idempotent and table_already_existed:
                        self._delete_existing_run_data(table)

                # create_table_if_not_exists() ignores `schema` when the table
                # already existed, and never widens an existing table's schema
                # even when it just created one — union_by_name reconciles new
                # columns every time; it is a safe no-op when there are none.
                with table.update_schema() as update:
                    update.union_by_name(schema)

                enriched_batches = self._enrich_batches(
                    self._iter_batches(tmp_path), blob_path
                )
                write_start = time.monotonic()
                rows_written = 0
                for record_batch in dataframe_batches_to_arrow(
                    enriched_batches, schema
                ):
                    arrow_table = pa.Table.from_batches([record_batch])
                    rows_written += arrow_table.num_rows
                    if self.idempotent or not first_batch_overall:
                        table.append(arrow_table)
                    elif self.write_mode == "overwrite":
                        table.overwrite(arrow_table)
                    else:
                        table.append(arrow_table)
                    first_batch_overall = False
                logger.info(
                    f"Wrote {rows_written:,} rows from {blob_path} in "
                    f"{time.monotonic() - write_start:.1f}s"
                )
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

    def _delete_existing_run_data(self, table: Any) -> None:
        """Delete rows matching the current run's metadata values, once,
        before writing any batch — mirrors the same method in
        FilesystemToDeltalakeOperator/FilesystemToDatabaseOperator.

        Unlike Delta's raw SQL predicate, this uses pyiceberg's typed
        expression API (EqualTo/And) — no string escaping, no injection risk.
        """
        if not self.metadata:
            return

        conditions: list[BooleanExpression] = []
        for key, value in self.metadata.items():
            metadata_key = (
                key.upper() if self.metadata_columns_in_uppercase else key.lower()
            )
            conditions.append(EqualTo(metadata_key, str(value)))

        delete_filter = conditions[0] if len(conditions) == 1 else And(*conditions)
        table.delete(delete_filter=delete_filter)
        logger.info(f"Idempotent delete executed (filter: {delete_filter})")
