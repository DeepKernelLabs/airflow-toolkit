from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from airflow_toolkit._compact.airflow_shim import BaseHook, BaseOperator, Context
from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory
from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol
from airflow_toolkit.providers.warehouse.dialects.base import WarehouseDialect
from airflow_toolkit.providers.warehouse.dialects.databricks import DatabricksDialect
from airflow_toolkit.providers.warehouse.dialects.snowflake import SnowflakeDialect
from airflow_toolkit.types import MetadataSpec, WarehouseSourceFormat

logger = logging.getLogger(__name__)

_DIALECTS: dict[str, WarehouseDialect] = {
    "databricks": DatabricksDialect(),
    "snowflake": SnowflakeDialect(),
}


class FilesystemToWarehouseOperator(BaseOperator):
    """Loads files already sitting in a data lake into a Databricks/Snowflake
    table via COPY INTO — a warehouse-side bulk load, not a streamed copy.

    Unlike FilesystemToDatabase/FilesystemToDeltalake/FilesystemToIceberg,
    this operator never downloads or chunks file bytes: COPY INTO's whole
    point is that the warehouse itself reads from cloud storage, so
    `source_location` is a plain templated string (a cloud URI for
    Databricks, `@stage/path` for Snowflake) — not derived from
    `filesystem_conn_id`, which is only used for an optional preflight
    existence check (see `preflight_check`).

    Table/schema creation is entirely the caller's responsibility: pass a
    `create_table_ddl` to have it run once before the COPY INTO (bundled into
    this same task, e.g. the Databricks style of one script per task), or
    leave it None and create the table in an earlier task of your own DAG
    (e.g. the Snowflake style of a separate CREATE TABLE task) — this
    operator never invents column types.

    `metadata` values are raw (Jinja-rendered) SQL value expressions, not
    plain literals — they're embedded verbatim as `<expr> AS <column>` in
    both the COPY INTO's SELECT list and the idempotent DELETE's WHERE
    clause, so you can add whatever cast your target column needs (e.g.
    `"DATE('{{ ds }}', 'yyyy-mm-dd')"`), the same way you would writing the
    SQL by hand. The default `{"_DS": "'{{ ds }}'"}` is a plain quoted string
    literal.

    When `idempotent=True` (the default) and `metadata` is non-empty, rows
    matching the current run's metadata values are deleted once, upfront,
    before the COPY INTO — this does not rely on either warehouse's native
    COPY INTO file-load-history dedup, which only prevents literally-the-
    same-file from reloading, not reprocessing the same logical partition
    after a corrected file.
    """

    template_fields = (
        "warehouse_conn_id",
        "database",
        "schema",
        "table",
        "source_location",
        "select_fragment",
        "create_table_ddl",
        "filesystem_path",
        "metadata",
    )

    def __init__(
        self,
        *,
        dialect: Literal["databricks", "snowflake"] | WarehouseDialect,
        warehouse_conn_id: str,
        database: str | None,
        schema: str,
        table: str,
        source_location: str,
        source_format: WarehouseSourceFormat = "json",
        select_fragment: str | None = None,
        file_format_options: Mapping[str, Any] | None = None,
        copy_options: Mapping[str, Any] | None = None,
        raw_column_name: str = "_raw",
        file_path_column: str = "_file_path",
        include_source_path: bool = True,
        metadata: MetadataSpec | None = None,
        metadata_columns_in_uppercase: bool = True,
        idempotent: bool = True,
        create_table_ddl: str | Sequence[str] | None = None,
        filesystem_conn_id: str | None = None,
        filesystem_path: str | None = None,
        preflight_check: bool = True,
        warehouse_hook_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.dialect: WarehouseDialect = (
            _DIALECTS[dialect] if isinstance(dialect, str) else dialect
        )
        self.warehouse_conn_id = warehouse_conn_id
        self.database = database
        self.schema = schema
        self.table = table
        self.source_location = source_location
        self.source_format = source_format
        self.select_fragment = select_fragment
        self.file_format_options = file_format_options
        self.copy_options = copy_options
        self.raw_column_name = raw_column_name
        self.file_path_column = file_path_column
        self.include_source_path = include_source_path
        self.metadata = {"_DS": "'{{ ds }}'"} if metadata is None else metadata
        self.metadata_columns_in_uppercase = metadata_columns_in_uppercase
        self.idempotent = idempotent
        self.create_table_ddl = create_table_ddl
        self.filesystem_conn_id = filesystem_conn_id
        self.filesystem_path = filesystem_path
        self.preflight_check = preflight_check
        self.warehouse_hook_kwargs = warehouse_hook_kwargs

    def execute(self, context: Context) -> None:
        if self.preflight_check and self.filesystem_conn_id:
            if self.filesystem_path is None:
                raise ValueError(
                    "filesystem_path must be set when filesystem_conn_id is "
                    "provided for the preflight check."
                )
            logger.info(
                f"Preflight check: {self.filesystem_conn_id}:{self.filesystem_path}"
            )
            filesystem: FilesystemProtocol = FilesystemFactory.get_data_lake_filesystem(
                connection=BaseHook.get_connection(self.filesystem_conn_id),
            )
            if not filesystem.check_prefix(self.filesystem_path):
                raise ValueError(
                    f"No files found under '{self.filesystem_path}' via "
                    f"connection '{self.filesystem_conn_id}' — refusing to run "
                    f"COPY INTO against an empty/missing source."
                )

        select_fragment = self.select_fragment
        if select_fragment is None:
            select_fragment = self.dialect.default_select_fragment(
                self.source_format, self.raw_column_name
            )

        metadata = self._cased_metadata()
        hook = self._get_warehouse_hook()

        if self.create_table_ddl:
            logger.info("Running create_table_ddl")
            hook.run(self.create_table_ddl)

        if self.idempotent and metadata:
            delete_sql = self.dialect.build_delete_statement(
                database=self.database,
                schema=self.schema,
                table=self.table,
                metadata=metadata,
            )
            logger.info(f"Idempotent delete:\n{delete_sql}")
            hook.run(delete_sql)

        copy_sql = self.dialect.build_copy_into(
            database=self.database,
            schema=self.schema,
            table=self.table,
            source_location=self.source_location,
            select_fragment=select_fragment,
            metadata=metadata,
            file_path_column=self.file_path_column,
            include_source_path=self.include_source_path,
            source_format=self.source_format,
            file_format_options=self.file_format_options,
            copy_options=self.copy_options,
        )
        logger.info(f"Running COPY INTO:\n{copy_sql}")
        columns, rows = self._run_copy_into(hook, copy_sql)

        result = self.dialect.parse_copy_result(columns, rows)
        logger.info(
            f"COPY INTO finished: rows_loaded={result.rows_loaded}, "
            f"rows_skipped={result.rows_skipped}, "
            f"files_processed={result.files_processed}"
        )
        logger.info(f"Raw result columns={result.raw_columns} rows={result.raw_rows}")
        if result.rows_skipped:
            logger.warning(f"COPY INTO skipped {result.rows_skipped} rows")

    def _cased_metadata(self) -> dict[str, str]:
        return {
            (key.upper() if self.metadata_columns_in_uppercase else key.lower()): value
            for key, value in self.metadata.items()
        }

    def _get_warehouse_hook(self) -> Any:
        hook_kwargs = dict(self.warehouse_hook_kwargs or {})
        if self.dialect.name == "databricks":
            from airflow.providers.databricks.hooks.databricks_sql import (
                DatabricksSqlHook,
            )

            return DatabricksSqlHook(
                databricks_conn_id=self.warehouse_conn_id, **hook_kwargs
            )
        if self.dialect.name == "snowflake":
            from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

            return SnowflakeHook(
                snowflake_conn_id=self.warehouse_conn_id, **hook_kwargs
            )
        raise NotImplementedError(
            f"No warehouse hook wiring for dialect {self.dialect.name!r} — "
            f"pass warehouse_hook_kwargs / extend _get_warehouse_hook for "
            f"external dialects."
        )

    def _run_copy_into(
        self, hook: Any, copy_sql: str
    ) -> tuple[list[str], list[tuple[Any, ...]]]:
        from airflow.providers.common.sql.hooks.sql import fetch_all_handler

        rows = hook.run(copy_sql, handler=fetch_all_handler) or []
        description = hook.last_description
        columns = [col[0] for col in description] if description else []
        return columns, list(rows)
