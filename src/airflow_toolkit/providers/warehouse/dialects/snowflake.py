from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from airflow_toolkit.providers.warehouse.dialects.base import CopyResult
from airflow_toolkit.types import WarehouseSourceFormat

_FILE_FORMAT_TYPE_MAP: dict[WarehouseSourceFormat, str] = {
    "csv": "CSV",
    "json": "JSON",
    "parquet": "PARQUET",
    "avro": "AVRO",
}


def _clause_tokens(options: Mapping[str, Any]) -> str:
    """Renders {"K": "'v'", "N": 1} as "K = 'v' N = 1".

    Snowflake's FILE_FORMAT/copy-option syntax mixes bare tokens (CSV, TRUE,
    1) and quoted string literals ('CONTINUE', ',') with no single quoting
    rule — unlike Databricks, where every value is always a quoted string.
    Values are used exactly as given: quote them yourself in the dict when
    you want a string literal (e.g. {"FIELD_DELIMITER": "','"}).
    """
    return " ".join(f"{key} = {value}" for key, value in options.items())


class SnowflakeDialect:
    """COPY INTO dialect for Snowflake.

    Source is always a named external stage (`@stage/path`) — the stage's
    own storage integration handles credentials, never inline in this
    operator's SQL. Tables are always fully qualified (database.schema.table
    or schema.table) rather than relying on a prior `USE SCHEMA` statement,
    to avoid a session-state dependency across tasks/connections.
    """

    name = "snowflake"

    def qualified_table_name(
        self, database: str | None, schema: str, table: str
    ) -> str:
        if database is None:
            return f"{schema}.{table}"
        return f"{database}.{schema}.{table}"

    def file_path_column_expression(self) -> str:
        return "METADATA$FILENAME"

    def default_select_fragment(
        self, source_format: WarehouseSourceFormat, raw_column_name: str
    ) -> str:
        if source_format == "json":
            # $1 is the entire parsed record as VARIANT when FILE_FORMAT
            # TYPE=JSON — schema-agnostic, no knowledge of the source's own
            # field names required.
            return f"$1 AS {raw_column_name}"
        # Unlike Databricks, there is no generic, schema-agnostic default for
        # typed loads: positional $1,$2,... needs the file's actual column
        # count/order, and SELECT * against a stage only returns named+typed
        # columns under stage-level configuration this operator doesn't
        # control. Callers must supply select_fragment explicitly for csv/
        # parquet/avro (e.g. "$1:id::INT AS id, $1:name::STRING AS name", or
        # "$1, $2, $3" for positional CSV).
        raise ValueError(
            f"select_fragment is required for the Snowflake dialect with "
            f"source_format={source_format!r} — no safe generic default exists "
            f"for typed loads."
        )

    def build_copy_into(
        self,
        *,
        database: str | None,
        schema: str,
        table: str,
        source_location: str,
        select_fragment: str,
        metadata: Mapping[str, str],
        file_path_column: str,
        include_source_path: bool,
        source_format: WarehouseSourceFormat,
        file_format_options: Mapping[str, Any] | None,
        copy_options: Mapping[str, Any] | None,
    ) -> str:
        qualified = self.qualified_table_name(database, schema, table)

        select_parts = [select_fragment]
        select_parts.extend(
            f"{value_expr} AS {column_name}"
            for column_name, value_expr in metadata.items()
        )
        if include_source_path:
            select_parts.append(
                f"{self.file_path_column_expression()} AS {file_path_column}"
            )
        select_clause = ",\n        ".join(select_parts)

        lines = [
            f"COPY INTO {qualified}",
            "FROM (",
            "    SELECT",
            f"        {select_clause}",
            f"    FROM {source_location}",
            ")",
        ]
        # Omitted entirely when not given — the stage's own FILE_FORMAT is
        # inherited, matching real-world usage where the format is defined
        # once on the stage rather than repeated on every COPY INTO.
        if file_format_options:
            type_token = f"TYPE = {_FILE_FORMAT_TYPE_MAP[source_format]}"
            lines.append(
                f"FILE_FORMAT = ({type_token} {_clause_tokens(file_format_options)})"
            )
        if copy_options:
            # Snowflake copy options (ON_ERROR, FORCE, PURGE, ...) are bare
            # top-level clauses, not wrapped in a COPY_OPTIONS(...) block
            # like Databricks.
            lines.append(_clause_tokens(copy_options))
        return "\n".join(lines)

    def build_delete_statement(
        self,
        *,
        database: str | None,
        schema: str,
        table: str,
        metadata: Mapping[str, str],
    ) -> str:
        qualified = self.qualified_table_name(database, schema, table)
        conditions = " AND ".join(
            f"{column_name} = {value_expr}"
            for column_name, value_expr in metadata.items()
        )
        return f"DELETE FROM {qualified} WHERE {conditions}"

    def parse_copy_result(
        self, columns: Sequence[str], rows: Sequence[tuple[Any, ...]]
    ) -> CopyResult:
        lower_columns = [c.lower() for c in columns]

        def _index(name: str) -> int | None:
            return lower_columns.index(name) if name in lower_columns else None

        loaded_idx = _index("rows_loaded")
        parsed_idx = _index("rows_parsed")

        rows_loaded = (
            sum(row[loaded_idx] for row in rows) if loaded_idx is not None else None
        )
        rows_skipped = (
            sum(row[parsed_idx] for row in rows) - rows_loaded
            if parsed_idx is not None and rows_loaded is not None
            else None
        )

        return CopyResult(
            rows_loaded=rows_loaded,
            rows_skipped=rows_skipped,
            files_processed=len(rows) or None,
            raw_columns=list(columns),
            raw_rows=[tuple(row) for row in rows],
        )
