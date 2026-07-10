"""Unit tests for WarehouseDialect implementations (Databricks / Snowflake).

Pure SQL-template tests — no mocks, no hooks, no operator involved.
"""

from __future__ import annotations

import pytest

from airflow_toolkit.providers.warehouse.dialects.databricks import DatabricksDialect
from airflow_toolkit.providers.warehouse.dialects.snowflake import SnowflakeDialect

# -----------------------------------------------------------------------------
# qualified_table_name
# -----------------------------------------------------------------------------


class TestQualifiedTableName:
    def test_databricks_requires_database(self):
        dialect = DatabricksDialect()
        assert (
            dialect.qualified_table_name("main", "raw", "orders") == "main.raw.orders"
        )
        with pytest.raises(ValueError, match="database"):
            dialect.qualified_table_name(None, "raw", "orders")

    def test_snowflake_falls_back_to_two_part(self):
        dialect = SnowflakeDialect()
        assert dialect.qualified_table_name("db", "raw", "orders") == "db.raw.orders"
        assert dialect.qualified_table_name(None, "raw", "orders") == "raw.orders"


# -----------------------------------------------------------------------------
# file_path_column_expression
# -----------------------------------------------------------------------------


def test_file_path_column_expressions():
    assert DatabricksDialect().file_path_column_expression() == "_metadata.file_path"
    assert SnowflakeDialect().file_path_column_expression() == "METADATA$FILENAME"


# -----------------------------------------------------------------------------
# default_select_fragment
# -----------------------------------------------------------------------------


class TestDefaultSelectFragment:
    def test_databricks_json(self):
        dialect = DatabricksDialect()
        assert (
            dialect.default_select_fragment("json", "_raw")
            == "to_json(struct(*)) AS _raw"
        )

    @pytest.mark.parametrize("source_format", ["csv", "parquet", "avro"])
    def test_databricks_typed_formats_default_to_star(self, source_format):
        dialect = DatabricksDialect()
        assert dialect.default_select_fragment(source_format, "_raw") == "*"

    def test_snowflake_json(self):
        dialect = SnowflakeDialect()
        assert dialect.default_select_fragment("json", "_raw") == "$1 AS _raw"

    @pytest.mark.parametrize("source_format", ["csv", "parquet", "avro"])
    def test_snowflake_typed_formats_raise(self, source_format):
        dialect = SnowflakeDialect()
        with pytest.raises(ValueError, match="select_fragment is required"):
            dialect.default_select_fragment(source_format, "_raw")


# -----------------------------------------------------------------------------
# build_copy_into — Databricks
# -----------------------------------------------------------------------------


class TestBuildCopyIntoDatabricks:
    def _base_kwargs(self, **overrides):
        kwargs = dict(
            database="main",
            schema="raw",
            table="orders",
            source_location="gs://bucket/raw/orders/*.json",
            select_fragment="to_json(struct(*)) AS _raw",
            metadata={"_DS": "'{{ ds }}'"},
            file_path_column="_file_path",
            include_source_path=True,
            source_format="json",
            file_format_options=None,
            copy_options=None,
        )
        kwargs.update(overrides)
        return kwargs

    def test_basic_statement_shape(self):
        sql = DatabricksDialect().build_copy_into(**self._base_kwargs())
        assert "COPY INTO main.raw.orders" in sql
        assert "FROM (" in sql
        assert "to_json(struct(*)) AS _raw" in sql
        assert "'{{ ds }}' AS _DS" in sql
        assert "_metadata.file_path AS _file_path" in sql
        assert "FROM 'gs://bucket/raw/orders/*.json'" in sql
        assert "FILEFORMAT = JSON" in sql

    def test_format_and_copy_options_emitted_when_present(self):
        sql = DatabricksDialect().build_copy_into(
            **self._base_kwargs(
                file_format_options={"mergeSchema": "true", "inferTimestamp": "true"},
                copy_options={"mergeSchema": "true"},
            )
        )
        assert (
            "FORMAT_OPTIONS ('mergeSchema' = 'true', 'inferTimestamp' = 'true')" in sql
        )
        assert "COPY_OPTIONS ('mergeSchema' = 'true')" in sql

    def test_options_omitted_when_absent(self):
        sql = DatabricksDialect().build_copy_into(**self._base_kwargs())
        assert "FORMAT_OPTIONS" not in sql
        assert "COPY_OPTIONS" not in sql

    def test_file_path_column_omitted_when_not_included(self):
        sql = DatabricksDialect().build_copy_into(
            **self._base_kwargs(include_source_path=False)
        )
        assert "_metadata.file_path" not in sql

    def test_no_metadata_columns_when_empty(self):
        sql = DatabricksDialect().build_copy_into(**self._base_kwargs(metadata={}))
        assert "_DS" not in sql


# -----------------------------------------------------------------------------
# build_copy_into — Snowflake
# -----------------------------------------------------------------------------


class TestBuildCopyIntoSnowflake:
    def _base_kwargs(self, **overrides):
        kwargs = dict(
            database="analytics",
            schema="raw",
            table="orders",
            source_location="@my_stage/raw/orders/2026/07/10",
            select_fragment="$1 AS _raw",
            metadata={"_DS": "'{{ ds }}'"},
            file_path_column="_file_path",
            include_source_path=True,
            source_format="json",
            file_format_options=None,
            copy_options=None,
        )
        kwargs.update(overrides)
        return kwargs

    def test_basic_statement_shape(self):
        sql = SnowflakeDialect().build_copy_into(**self._base_kwargs())
        assert "COPY INTO analytics.raw.orders" in sql
        assert "$1 AS _raw" in sql
        assert "'{{ ds }}' AS _DS" in sql
        assert "METADATA$FILENAME AS _file_path" in sql
        assert "FROM @my_stage/raw/orders/2026/07/10" in sql
        # No quotes around the stage reference (unlike Databricks' URI).
        assert "FROM '@my_stage" not in sql

    def test_no_use_schema_statement(self):
        sql = SnowflakeDialect().build_copy_into(**self._base_kwargs())
        assert "USE SCHEMA" not in sql

    def test_file_format_omitted_when_absent(self):
        sql = SnowflakeDialect().build_copy_into(**self._base_kwargs())
        assert "FILE_FORMAT" not in sql

    def test_file_format_emitted_when_present(self):
        sql = SnowflakeDialect().build_copy_into(
            **self._base_kwargs(file_format_options={"SKIP_HEADER": "1"})
        )
        assert "FILE_FORMAT = (TYPE = JSON SKIP_HEADER = 1)" in sql

    def test_copy_options_are_bare_clauses_not_wrapped(self):
        sql = SnowflakeDialect().build_copy_into(
            **self._base_kwargs(copy_options={"ON_ERROR": "'CONTINUE'"})
        )
        assert "ON_ERROR = 'CONTINUE'" in sql
        assert "COPY_OPTIONS" not in sql


# -----------------------------------------------------------------------------
# build_delete_statement
# -----------------------------------------------------------------------------


class TestBuildDeleteStatement:
    def test_databricks(self):
        sql = DatabricksDialect().build_delete_statement(
            database="main",
            schema="raw",
            table="orders",
            metadata={"_DS": "'2026-07-10'"},
        )
        assert sql == "DELETE FROM main.raw.orders WHERE _DS = '2026-07-10'"

    def test_snowflake_multiple_conditions(self):
        sql = SnowflakeDialect().build_delete_statement(
            database="analytics",
            schema="raw",
            table="orders",
            metadata={"_DS": "'2026-07-10'", "_RUN": "'abc'"},
        )
        assert sql == (
            "DELETE FROM analytics.raw.orders WHERE _DS = '2026-07-10' AND _RUN = 'abc'"
        )


# -----------------------------------------------------------------------------
# parse_copy_result
# -----------------------------------------------------------------------------


class TestParseCopyResultSnowflake:
    def test_known_columns_are_summed(self):
        dialect = SnowflakeDialect()
        columns = ["file", "status", "rows_parsed", "rows_loaded", "error_limit"]
        rows = [
            ("f1.json", "LOADED", 100, 100, 1),
            ("f2.json", "LOADED", 50, 40, 1),
        ]
        result = dialect.parse_copy_result(columns, rows)
        assert result.rows_loaded == 140
        assert result.rows_skipped == 10
        assert result.files_processed == 2
        assert result.raw_columns == columns
        assert result.raw_rows == rows

    def test_unrecognized_columns_leave_counts_none(self):
        dialect = SnowflakeDialect()
        columns = ["something_else"]
        rows = [("x",)]
        result = dialect.parse_copy_result(columns, rows)
        assert result.rows_loaded is None
        assert result.rows_skipped is None
        assert result.files_processed == 1
        assert result.raw_rows == rows


class TestParseCopyResultDatabricks:
    def test_known_columns_matched_by_substring(self):
        dialect = DatabricksDialect()
        columns = [
            "num_affected_rows",
            "num_inserted_rows",
            "num_skipped_corrupt_files",
        ]
        rows = [(10, 10, 0), (5, 5, 1)]
        result = dialect.parse_copy_result(columns, rows)
        assert result.rows_loaded == 30  # sums both matching columns
        assert result.rows_skipped == 1
        assert result.files_processed == 2  # falls back to row count

    def test_unrecognized_columns_fall_back_to_raw(self):
        dialect = DatabricksDialect()
        columns = ["totally_unknown_column"]
        rows = [("value",)]
        result = dialect.parse_copy_result(columns, rows)
        assert result.rows_loaded is None
        assert result.rows_skipped is None
        assert result.files_processed == 1
        assert result.raw_columns == columns
        assert result.raw_rows == rows
