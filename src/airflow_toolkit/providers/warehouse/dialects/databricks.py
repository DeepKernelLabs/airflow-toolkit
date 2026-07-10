from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from airflow_toolkit.providers.warehouse.dialects.base import CopyResult
from airflow_toolkit.types import WarehouseSourceFormat

_FILEFORMAT_MAP: dict[WarehouseSourceFormat, str] = {
    "csv": "CSV",
    "json": "JSON",
    "parquet": "PARQUET",
    "avro": "AVRO",
}

# COPY INTO's result-set column names have changed across DBR versions —
# matched by substring rather than an exact/fixed schema. Verify against the
# actual target runtime before relying on rows_loaded for alerting logic.
_ROWS_LOADED_MARKERS = ("num_inserted_rows", "num_affected_rows")
_ROWS_SKIPPED_MARKERS = ("num_skipped",)
_FILES_PROCESSED_MARKERS = ("num_target_files", "num_files", "num_source_files")


def _options_clause(options: Mapping[str, Any]) -> str:
    return ", ".join(f"'{key}' = '{value}'" for key, value in options.items())


class DatabricksDialect:
    """COPY INTO dialect for Databricks SQL Warehouses / Unity Catalog.

    Source is always a raw cloud storage URI/glob (credentials handled at
    the cluster/warehouse level via an instance profile or Unity Catalog
    external location — never inline in this operator's SQL). Tables are
    always fully qualified as catalog.schema.table in a single statement, no
    session-state dependency.
    """

    name = "databricks"

    def qualified_table_name(
        self, database: str | None, schema: str, table: str
    ) -> str:
        if database is None:
            raise ValueError(
                "DatabricksDialect requires `database` (the Unity Catalog "
                "catalog name) — COPY INTO needs a fully-qualified "
                "catalog.schema.table reference."
            )
        return f"{database}.{schema}.{table}"

    def file_path_column_expression(self) -> str:
        return "_metadata.file_path"

    def default_select_fragment(
        self, source_format: WarehouseSourceFormat, raw_column_name: str
    ) -> str:
        if source_format == "json":
            # struct(*) re-assembles every column COPY INTO infers from the
            # JSON file into one struct, then to_json() serializes it back to
            # a single string column — schema-agnostic, no knowledge of the
            # source's own field names required.
            return f"to_json(struct(*)) AS {raw_column_name}"
        if source_format in ("csv", "parquet", "avro"):
            # COPY INTO already exposes real named+typed columns from these
            # formats — "*" is a safe, zero-config typed-mode default.
            return "*"
        raise ValueError(
            f"No default select_fragment for source_format={source_format!r}"
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
            f"    FROM '{source_location}'",
            ")",
            f"FILEFORMAT = {_FILEFORMAT_MAP[source_format]}",
        ]
        if file_format_options:
            lines.append(f"FORMAT_OPTIONS ({_options_clause(file_format_options)})")
        if copy_options:
            lines.append(f"COPY_OPTIONS ({_options_clause(copy_options)})")
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

        def _sum_matching(markers: Sequence[str]) -> int | None:
            indices = [
                i
                for i, col in enumerate(lower_columns)
                if any(marker in col for marker in markers)
            ]
            if not indices:
                return None
            return sum(sum(row[i] for i in indices) for row in rows)

        files_processed = _sum_matching(_FILES_PROCESSED_MARKERS)
        if files_processed is None:
            # No recognizable per-file count column — fall back to counting
            # result rows (COPY INTO typically returns one row per file).
            files_processed = len(rows) or None

        return CopyResult(
            rows_loaded=_sum_matching(_ROWS_LOADED_MARKERS),
            rows_skipped=_sum_matching(_ROWS_SKIPPED_MARKERS),
            files_processed=files_processed,
            raw_columns=list(columns),
            raw_rows=[tuple(row) for row in rows],
        )
