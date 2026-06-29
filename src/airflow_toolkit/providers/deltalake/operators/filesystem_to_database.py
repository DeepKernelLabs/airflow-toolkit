from __future__ import annotations

import csv
import ctypes
import gc
import io
import logging
import os
import resource
import signal
import tempfile
import time
import unicodedata
import urllib.parse
from collections.abc import Iterator, Mapping
from typing import Any, Literal

from airflow_toolkit.types import MetadataSpec

import pandas as pd
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    Integer,
    String,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.engine import Engine

from airflow_toolkit._compact.airflow_shim import (
    BaseOperator,
    Context,
    BaseHook,
)
from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory
from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol

logger = logging.getLogger(__name__)

_BATCH_SIZE = 50_000

# Maps source_format names to the file extensions they match.
# Used in execute() to skip blobs that don't belong to the selected format.
_FORMAT_EXTENSIONS: dict[str, tuple[str, ...]] = {
    "csv": (".csv", ".csv.gz"),
    "json": (".json", ".json.gz"),
    "parquet": (".parquet", ".parquet.gz"),
    "excel": (".xlsx", ".xls"),
    "avro": (".avro",),
    "fixed_width": (".fwf", ".txt", ".dat"),
}

# Canonical extension for the temp file created in execute().
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

type_mapping: dict[str, type[Any]] = {
    "int64": Integer,
    "int": Integer,
    "integer": Integer,
    "float64": Float,
    "float": Float,
    "object": String,
    "string": String,
    "str": String,
    "datetime64[ns]": DateTime,
    "bool": Boolean,
    "boolean": Boolean,
}


class FilesystemToDatabaseOperator(BaseOperator):
    """
    This operator will copy a file from a filesystem to a database table.
    """

    template_fields = (
        "db_table",
        "db_schema",
        "filesystem_conn_id",
        "database_conn_id",
        "filesystem_path",
        "metadata",
    )

    def __init__(
        self,
        filesystem_conn_id: str,
        database_conn_id: str,
        filesystem_path: str,
        db_table: str,
        db_schema: str | None = None,
        source_format: Literal[
            "csv", "json", "parquet", "excel", "avro", "fixed_width"
        ] = "csv",
        source_format_options: Mapping[str, Any] | None = None,
        batch_size: int = _BATCH_SIZE,
        table_aggregation_type: Literal["append", "fail", "replace"] = "append",
        metadata: MetadataSpec | None = None,
        metadata_columns_in_uppercase: bool = True,
        include_source_path: bool = True,
        normalize_unicode: bool = False,
        idempotent: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.filesystem_conn_id = filesystem_conn_id
        self.database_conn_id = database_conn_id
        self.filesystem_path = filesystem_path
        self.db_table = db_table
        self.db_schema = db_schema
        self.source_format = source_format
        self.source_format_options = source_format_options
        self.batch_size = batch_size
        self.table_aggregation_type = table_aggregation_type
        self.metadata = metadata or {"_DS": "{{ ds }}"}
        self.metadata_columns_in_uppercase = metadata_columns_in_uppercase
        self.include_source_path = include_source_path
        self.normalize_unicode = normalize_unicode
        self.idempotent = idempotent

    def execute(self, context: Context) -> None:
        # Install a SIGTERM handler so we can tell whether Airflow/the OS is
        # intentionally terminating this task (SIGTERM is catchable; SIGKILL is not).
        def _sigterm_handler(signum, frame):
            logger.error(
                "SIGTERM received — task is being externally terminated. "
                "Check Airflow scheduler logs or Celery worker for the reason."
            )
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            os.kill(os.getpid(), signal.SIGTERM)

        signal.signal(signal.SIGTERM, _sigterm_handler)

        logger.info(f"Create connection for filesystem ({self.filesystem_conn_id})")
        filesystem: FilesystemProtocol = FilesystemFactory.get_data_lake_filesystem(
            connection=BaseHook.get_connection(self.filesystem_conn_id),
        )

        logger.info(
            f"Create SQLAlchemy engine with connection_id {self.database_conn_id}"
        )
        ##################################################################################
        # We detected that with certain unusual characters in the password, the URI for  #
        # Postgres did not work perfectly, so we decided to avoid it and manually create #
        # the connection URL here, correctly escaping the password.                      #
        ##################################################################################
        conn = BaseHook.get_connection(self.database_conn_id)
        engine: Engine
        if conn.conn_type == "postgres":
            password_encoded = urllib.parse.quote_plus(conn.password)
            connection_url = f"postgresql://{conn.login}:{password_encoded}@{conn.host}:{conn.port}/{conn.schema}"
            engine = create_engine(
                connection_url,
                connect_args={
                    "keepalives": 1,
                    "keepalives_idle": 60,
                    "keepalives_interval": 10,
                    "keepalives_count": 5,
                },
            )
        elif conn.conn_type == "sqlite":
            # get_uri() URL-encodes the host, producing an invalid sqlite:// URL
            # for absolute paths. Build the URL directly from the raw host.
            connection_url = f"sqlite:///{conn.host}"
            engine = create_engine(connection_url)
        else:
            engine = create_engine(conn.get_uri())
        ##################################################################################

        if self.idempotent:
            self._delete_existing_run_data(engine)

        valid_extensions = _FORMAT_EXTENSIONS.get(
            self.source_format, (f".{self.source_format}",)
        )
        first_batch = True
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

            tmp_suffix = _FORMAT_TEMP_SUFFIX.get(
                self.source_format, f".{self.source_format}"
            )
            tmp_fd, tmp_path = tempfile.mkstemp(suffix=tmp_suffix)
            try:
                with os.fdopen(tmp_fd, "wb") as tmp_file:
                    tmp_file.write(raw_bytes)
                del raw_bytes
                logger.info(
                    f"Saved to {tmp_path}; starting batch load (batch_size={self.batch_size:,})"
                )
                self._log_memory("after download")

                file_columns = self._get_file_columns(tmp_path)
                missing_from_file = self._check_and_fix_column_differences(
                    file_columns, self.db_table, engine
                )

                batch_num = 0
                total_rows_loaded = 0
                load_start = time.monotonic()

                with engine.begin() as connection:
                    for batch_df in self._iter_batches(tmp_path):
                        batch_num += 1
                        batch_rows = len(batch_df)
                        batch_start = time.monotonic()

                        self._check_and_fix_null_characters(batch_df)

                        if self.normalize_unicode:
                            self._normalize_unicode_text(batch_df)

                        for col in missing_from_file:
                            batch_df[col] = None

                        for key, value in self.metadata.items():
                            metadata_key = (
                                key.upper()
                                if self.metadata_columns_in_uppercase
                                else key.lower()
                            )
                            batch_df[metadata_key] = value
                            if metadata_key.startswith("_"):
                                batch_df[metadata_key] = self._convert_to_datetime(
                                    batch_df[metadata_key]
                                )

                        if self.include_source_path:
                            source_path_column = (
                                "_LOADED_FROM"
                                if self.metadata_columns_in_uppercase
                                else "_loaded_from"
                            )
                            batch_df[source_path_column] = blob_path
                            batch_df[source_path_column] = batch_df[
                                source_path_column
                            ].astype("string")

                        if_exists = (
                            self.table_aggregation_type if first_batch else "append"
                        )
                        try:
                            batch_df.to_sql(
                                name=self.db_table,
                                schema=self.db_schema,
                                con=connection,
                                if_exists=if_exists,
                                index=False,
                                method=self._postgres_copy_method
                                if conn.conn_type == "postgres"
                                else "multi",
                            )
                        except Exception:
                            logger.exception(
                                f"Batch {batch_num} failed at rows "
                                f"{total_rows_loaded + 1:,}–{total_rows_loaded + batch_rows:,}"
                            )
                            raise
                        first_batch = False
                        total_rows_loaded += batch_rows
                        batch_elapsed = time.monotonic() - batch_start
                        total_elapsed = time.monotonic() - load_start
                        rows_per_sec = (
                            total_rows_loaded / total_elapsed
                            if total_elapsed > 0
                            else 0
                        )
                        logger.info(
                            f"Batch {batch_num}: {batch_rows:,} rows | "
                            f"{batch_elapsed:.1f}s | "
                            f"total {total_rows_loaded:,} rows | "
                            f"{rows_per_sec:,.0f} rows/s"
                        )
                        del batch_df
                        gc.collect()
                        if batch_num % 10 == 0:
                            self._log_memory(f"batch {batch_num}")
                            try:
                                ctypes.CDLL("libc.so.6").malloc_trim(0)
                            except Exception:
                                pass

                total_elapsed = time.monotonic() - load_start
                avg_rows_per_sec = (
                    total_rows_loaded / total_elapsed if total_elapsed > 0 else 0
                )
                logger.info(
                    f"Load complete: {total_rows_loaded:,} rows in {total_elapsed:.1f}s "
                    f"({avg_rows_per_sec:,.0f} rows/s avg) | "
                    f"{batch_num} batches"
                )

            finally:
                os.unlink(tmp_path)

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

    def _iter_batches(self, path: str) -> Iterator[pd.DataFrame]:
        """Yields DataFrames of up to batch_size rows from the source file."""
        options: dict[str, Any] = dict(self.source_format_options or {})
        match self.source_format:
            case "parquet":
                import pyarrow as pa
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

    @staticmethod
    def _postgres_copy_method(table, conn, keys, data_iter):
        buf = io.StringIO()
        csv.writer(buf).writerows(data_iter)
        buf.seek(0)
        cols = ", ".join(f'"{k}"' for k in keys)
        schema = f'"{table.schema}".' if table.schema else ""
        with conn.connection.cursor() as cur:
            cur.copy_expert(
                sql=f'COPY {schema}"{table.name}" ({cols}) FROM STDIN WITH CSV',
                file=buf,
            )

    @staticmethod
    def _normalize_unicode_text(df: pd.DataFrame) -> None:
        # NFKC converts Arabic presentation forms (U+FE70-FEFF) to standard Arabic
        # (U+0600-U+06FF) and collapses other compatibility characters (ligatures,
        # full-width, superscripts). Zero Width Spaces (U+200B) are stripped separately
        # as they are valid Unicode but semantically empty in these fields.
        def _clean(value: Any) -> Any:
            if not isinstance(value, str):
                return value
            return unicodedata.normalize("NFKC", value).replace("​", "")

        for column in df.select_dtypes(include=["object", "string"]).columns:
            df[column] = df[column].map(_clean)

    def _check_and_fix_null_characters(self, df: pd.DataFrame) -> None:
        for column in df.select_dtypes(include=["object", "string"]).columns:
            col = df[column].astype(str)
            if col.str.contains("\x00").any():
                df[column] = col.str.replace("\x00", "", regex=False)
            if col.str.contains("\\u0000", regex=False).any():
                df[column] = (
                    df[column].astype(str).str.replace("\\u0000", "", regex=False)
                )

    def _delete_existing_run_data(self, engine: Engine) -> None:
        """Delete rows matching the current run's metadata values.

        Makes the operator idempotent: re-running with the same metadata
        (e.g. same _DS date) replaces data instead of duplicating it.
        """
        inspector = inspect(engine)
        schema = self.db_schema
        if self.db_table not in inspector.get_table_names(schema=schema):
            return

        conditions = []
        params: dict[str, Any] = {}
        for key, value in self.metadata.items():
            col_name = (
                key.upper() if self.metadata_columns_in_uppercase else key.lower()
            )
            param_key = f"meta_{col_name}"
            conditions.append(f'"{col_name}" = :{param_key}')
            params[param_key] = value

        if not conditions:
            return

        schema_prefix = f'"{schema}".' if schema else ""
        where_clause = " AND ".join(conditions)
        sql = f'DELETE FROM {schema_prefix}"{self.db_table}" WHERE {where_clause}'

        with engine.begin() as conn:
            result = conn.execute(text(sql), params)
            logger.info(
                f"Idempotent delete: removed {result.rowcount} existing rows "
                f"matching {params}"
            )

    def _check_and_fix_column_differences(
        self, source_columns: set[str], table_name: str, engine: Engine
    ) -> set[str]:
        """
        Reconciles source columns with the existing database table.

        - Columns in source but absent from the table  → ALTER TABLE ADD COLUMN TEXT.
        - Columns in the table but absent from source  → returned so each batch fills
          them with None.

        Returns the set of table columns that must be filled with None in every batch.
        """
        inspector = inspect(engine)
        if table_name not in inspector.get_table_names():
            return set()

        table_columns = {
            col["name"]
            for col in inspector.get_columns(table_name)
            if col["name"] not in self.metadata
        }

        only_in_source = source_columns - table_columns
        with engine.begin() as db_conn:
            for column in only_in_source:
                logger.warning(
                    f'Table "{table_name}" is missing column "{column}" from the source '
                    f"file — adding it as TEXT."
                )
                db_conn.execute(
                    text(f'ALTER TABLE "{table_name}" ADD COLUMN "{column}" TEXT')
                )

        only_in_table = table_columns - source_columns
        for column in only_in_table:
            logger.warning(
                f'Source file is missing column "{column}" present in table '
                f'"{table_name}" — will fill with NULL.'
            )

        return only_in_table

    def _log_memory(self, label: str) -> None:
        try:
            import pyarrow as pa

            arrow_mb = pa.default_memory_pool().bytes_allocated() / 1024 / 1024
        except Exception:
            arrow_mb = -1
        rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        logger.info(f"[mem:{label}] RSS={rss_mb:.0f} MB  Arrow={arrow_mb:.0f} MB")

    @staticmethod
    def _convert_to_datetime(value: pd.Series) -> pd.Series:
        try:
            return pd.to_datetime(value)
        except (ValueError, TypeError):
            return value

    def raw_content_to_pandas(
        self, path_or_buf: str | bytes | io.StringIO | io.BytesIO
    ) -> pd.DataFrame:
        options: dict[str, Any] = dict(self.source_format_options or {})

        match self.source_format:
            case "csv":
                return pd.read_csv(path_or_buf, **options)
            case "json":
                return pd.read_json(path_or_buf, **options)
            case "parquet":
                return pd.read_parquet(path_or_buf, **options)
            case "excel":
                return pd.read_excel(path_or_buf, **options)
            case "avro":
                import fastavro

                if isinstance(path_or_buf, (str, bytes)):
                    buf = io.BytesIO(
                        path_or_buf
                        if isinstance(path_or_buf, bytes)
                        else path_or_buf.encode()
                    )
                else:
                    buf = (
                        path_or_buf
                        if isinstance(path_or_buf, io.BytesIO)
                        else io.BytesIO(path_or_buf.read().encode())
                    )
                records = list(fastavro.reader(buf))
                return pd.DataFrame(records)
            case "fixed_width":
                return pd.read_fwf(path_or_buf, **options)
            case _:
                raise ValueError(f"Unknown source format {self.source_format}")
