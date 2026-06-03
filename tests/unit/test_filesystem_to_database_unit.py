"""Unit tests for FilesystemToDatabaseOperator.

All tests exercise class methods directly - no dag.test(), no Airflow supervisor.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from airflow_toolkit.providers.deltalake.operators.filesystem_to_database import (
    FilesystemToDatabaseOperator,
)

_MODULE = "airflow_toolkit.providers.deltalake.operators.filesystem_to_database"

# Special characters - constructed via chr() so the source file stays ASCII-clean.
_NULL = chr(0)  # U+0000 null character
_ZWS = chr(0x200B)  # U+200B Zero Width Space
_FULLWIDTH_A = chr(0xFF21)  # U+FF21 -> "A" after NFKC normalisation
# The 6-char literal escape string that the operator's second null-check strips.
# It is backslash + the four chars u000, NOT the null character itself.
_LITERAL_NUL_ESCAPE = chr(92) + "u0000"


def _make_op(**kwargs) -> FilesystemToDatabaseOperator:
    defaults = dict(
        task_id="test_op",
        filesystem_conn_id="fs_conn",
        database_conn_id="db_conn",
        filesystem_path="data/",
        db_table="test_table",
    )
    defaults.update(kwargs)
    return FilesystemToDatabaseOperator(**defaults)


# -----------------------------------------------------------------------------
# _postgres_copy_method
# -----------------------------------------------------------------------------


class TestPostgresCopyMethod:
    def _make_conn(self):
        cursor = MagicMock()
        conn = MagicMock()
        conn.connection.cursor.return_value.__enter__.return_value = cursor
        return conn, cursor

    def _call(self, schema, table_name, keys, rows):
        table = MagicMock()
        table.name = table_name
        table.schema = schema
        conn, cursor = self._make_conn()
        FilesystemToDatabaseOperator._postgres_copy_method(
            table, conn, keys, iter(rows)
        )
        return cursor

    def test_sql_includes_schema_when_present(self):
        cursor = self._call("myschema", "orders", ["id", "name"], [])
        sql = cursor.copy_expert.call_args.kwargs["sql"]
        assert 'COPY "myschema"."orders"' in sql
        assert "FROM STDIN WITH CSV" in sql

    def test_sql_omits_schema_when_absent(self):
        cursor = self._call(None, "orders", ["id"], [])
        sql = cursor.copy_expert.call_args.kwargs["sql"]
        assert '"myschema"' not in sql
        assert 'COPY "orders"' in sql

    def test_columns_are_quoted_in_sql(self):
        cursor = self._call(None, "t", ["first name", "value"], [])
        sql = cursor.copy_expert.call_args.kwargs["sql"]
        assert '"first name"' in sql
        assert '"value"' in sql

    def test_csv_content_matches_data(self):
        captured: list[str] = []

        table = MagicMock()
        table.name = "t"
        table.schema = None

        cursor = MagicMock()
        cursor.copy_expert.side_effect = lambda sql, file: captured.append(file.read())
        conn = MagicMock()
        conn.connection.cursor.return_value.__enter__.return_value = cursor

        FilesystemToDatabaseOperator._postgres_copy_method(
            table, conn, ["a", "b"], iter([(1, "hello"), (2, "world")])
        )

        assert len(captured) == 1
        assert "hello" in captured[0]
        assert "world" in captured[0]


# -----------------------------------------------------------------------------
# _normalize_unicode_text
# -----------------------------------------------------------------------------


class TestNormalizeUnicodeText:
    def test_nfkc_collapses_fullwidth_characters(self):
        df = pd.DataFrame({"col": [_FULLWIDTH_A + "hello"]})
        FilesystemToDatabaseOperator._normalize_unicode_text(df)
        assert df["col"][0] == "Ahello"

    def test_zero_width_space_is_stripped(self):
        df = pd.DataFrame({"col": ["before" + _ZWS + "after"]})
        FilesystemToDatabaseOperator._normalize_unicode_text(df)
        assert df["col"][0] == "beforeafter"

    def test_none_values_pass_through(self):
        df = pd.DataFrame({"txt": ["clean" + _ZWS, None, "normal"]})
        FilesystemToDatabaseOperator._normalize_unicode_text(df)
        assert df["txt"][0] == "clean"
        assert df["txt"][1] is None
        assert df["txt"][2] == "normal"

    def test_integer_columns_are_not_touched(self):
        df = pd.DataFrame({"num": [1, 2], "txt": ["x" + _ZWS + "y", "z"]})
        FilesystemToDatabaseOperator._normalize_unicode_text(df)
        assert df["num"].dtype.kind == "i"
        assert df["txt"][0] == "xy"


# -----------------------------------------------------------------------------
# _check_and_fix_null_characters
# -----------------------------------------------------------------------------


class TestCheckAndFixNullCharacters:
    def test_strips_null_byte(self):
        df = pd.DataFrame({"col": ["hello" + _NULL + "world"]})
        _make_op()._check_and_fix_null_characters(df)
        assert df["col"][0] == "helloworld"

    def test_strips_literal_unicode_escape_sequence(self):
        # The operator also strips the 6-char literal string (backslash + u0000),
        # which is different from the actual null character.
        df = pd.DataFrame({"col": ["val" + _LITERAL_NUL_ESCAPE + "ue"]})
        _make_op()._check_and_fix_null_characters(df)
        assert df["col"][0] == "value"

    def test_clean_string_is_unchanged(self):
        df = pd.DataFrame({"col": ["clean"]})
        _make_op()._check_and_fix_null_characters(df)
        assert df["col"][0] == "clean"

    def test_integer_columns_are_not_touched(self):
        df = pd.DataFrame({"num": [1, 2], "txt": ["a" + _NULL + "b", "c"]})
        _make_op()._check_and_fix_null_characters(df)
        assert list(df["num"]) == [1, 2]
        assert df["txt"][0] == "ab"


# -----------------------------------------------------------------------------
# _get_file_columns
# -----------------------------------------------------------------------------


class TestGetFileColumns:
    def test_csv_returns_header_columns(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("alpha,beta,gamma\n1,2,3\n")
        assert _make_op(source_format="csv")._get_file_columns(str(f)) == {
            "alpha",
            "beta",
            "gamma",
        }

    def test_csv_respects_format_options(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("a|b|c\n1|2|3\n")
        op = _make_op(source_format="csv", source_format_options={"sep": "|"})
        assert op._get_file_columns(str(f)) == {"a", "b", "c"}

    def test_json_lines(self, tmp_path):
        f = tmp_path / "data.json"
        f.write_text('{"x": 1, "y": 2}\n{"x": 3, "y": 4}\n')
        op = _make_op(source_format="json", source_format_options={"lines": True})
        assert op._get_file_columns(str(f)) == {"x", "y"}

    def test_parquet(self, tmp_path):
        pa = pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        f = tmp_path / "data.parquet"
        pq.write_table(pa.table({"p": [1, 2], "q": [3, 4]}), str(f))
        assert _make_op(source_format="parquet")._get_file_columns(str(f)) == {"p", "q"}


# -----------------------------------------------------------------------------
# _iter_batches
# -----------------------------------------------------------------------------


class TestIterBatches:
    def test_csv_splits_into_batches(self, tmp_path):
        f = tmp_path / "data.csv"
        rows = "\n".join(f"val{i}" for i in range(10))
        f.write_text(f"col\n{rows}\n")

        batches = list(
            _make_op(source_format="csv", batch_size=3)._iter_batches(str(f))
        )

        assert len(batches) == 4
        assert [len(b) for b in batches] == [3, 3, 3, 1]

    def test_csv_format_options_are_forwarded(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("a|b\n1|2\n3|4\n")

        op = _make_op(
            source_format="csv", source_format_options={"sep": "|"}, batch_size=10
        )
        batches = list(op._iter_batches(str(f)))

        assert len(batches) == 1
        assert set(batches[0].columns) == {"a", "b"}

    def test_parquet_total_rows_match(self, tmp_path):
        pa = pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        f = tmp_path / "data.parquet"
        pq.write_table(pa.table({"v": list(range(7))}), str(f))

        batches = list(
            _make_op(source_format="parquet", batch_size=3)._iter_batches(str(f))
        )

        assert sum(len(b) for b in batches) == 7
        assert all(len(b) <= 3 for b in batches)


# -----------------------------------------------------------------------------
# _check_and_fix_column_differences
# -----------------------------------------------------------------------------


class TestCheckAndFixColumnDifferences:
    def _sqlite_engine(self, tmp_path: Path, ddl: str | None = None):
        engine = create_engine(f"sqlite:///{tmp_path / 'test.db'}")
        if ddl:
            with engine.begin() as conn:
                conn.execute(text(ddl))
        return engine

    def test_returns_empty_set_when_table_does_not_exist(self, tmp_path):
        engine = self._sqlite_engine(tmp_path)
        result = _make_op()._check_and_fix_column_differences(
            {"a", "b"}, "nonexistent", engine
        )
        assert result == set()

    def test_returns_columns_absent_from_source(self, tmp_path):
        engine = self._sqlite_engine(
            tmp_path, "CREATE TABLE test_table (a TEXT, b TEXT, c TEXT, _DS TEXT)"
        )
        # Source only has "a" - b and c are in the table but not in source
        result = _make_op()._check_and_fix_column_differences(
            {"a"}, "test_table", engine
        )
        assert result == {"b", "c"}

    def test_issues_alter_table_for_new_source_columns(self):
        mock_engine = MagicMock()
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["test_table"]
        mock_inspector.get_columns.return_value = [{"name": "a"}]

        executed: list[str] = []
        mock_db_conn = MagicMock()
        mock_db_conn.execute.side_effect = lambda stmt: executed.append(str(stmt))
        mock_engine.begin.return_value.__enter__.return_value = mock_db_conn

        with patch(f"{_MODULE}.inspect", return_value=mock_inspector):
            _make_op()._check_and_fix_column_differences(
                {"a", "new_col"}, "test_table", mock_engine
            )

        assert any('"new_col"' in sql for sql in executed)

    def test_metadata_columns_excluded_from_comparison(self, tmp_path):
        engine = self._sqlite_engine(
            tmp_path, "CREATE TABLE test_table (a TEXT, _DS TEXT)"
        )
        # _DS is a metadata column and must not appear in the returned set
        result = _make_op(
            metadata={"_DS": "{{ ds }}"}
        )._check_and_fix_column_differences({"a"}, "test_table", engine)
        assert "_DS" not in result
        assert result == set()


# -----------------------------------------------------------------------------
# execute() - engine routing and to_sql method= selection
# -----------------------------------------------------------------------------


class TestExecuteEngineRouting:
    _CSV = b"col1,col2\n1,hello\n2,world\n"

    def _run(self, conn_type: str, host: str = "localhost"):
        db_conn = MagicMock()
        db_conn.conn_type = conn_type
        db_conn.login = "user"
        db_conn.password = "secret"
        db_conn.host = host
        db_conn.port = 5432
        db_conn.schema = "mydb"

        fs_conn = MagicMock()
        mock_fs = MagicMock()
        mock_fs.list_files.return_value = ["data/test.csv"]
        mock_fs.read.return_value = self._CSV

        mock_engine = MagicMock()

        with (
            patch(f"{_MODULE}.BaseHook") as mock_hook,
            patch(f"{_MODULE}.FilesystemFactory") as mock_factory,
            patch(f"{_MODULE}.create_engine", return_value=mock_engine) as mock_ce,
            patch(f"{_MODULE}.inspect") as mock_inspect,
            patch("pandas.DataFrame.to_sql") as mock_to_sql,
        ):
            mock_hook.get_connection.side_effect = [fs_conn, db_conn]
            mock_factory.get_data_lake_filesystem.return_value = mock_fs
            mock_inspect.return_value.get_table_names.return_value = []

            _make_op(source_format="csv").execute({})

            return mock_ce, mock_to_sql, db_conn

    def test_postgres_engine_has_keepalives(self):
        mock_ce, _, _ = self._run("postgres")
        connect_args = mock_ce.call_args.kwargs["connect_args"]
        assert connect_args["keepalives"] == 1
        assert connect_args["keepalives_idle"] == 60

    def test_postgres_to_sql_uses_copy_method(self):
        _, mock_to_sql, _ = self._run("postgres")
        assert (
            mock_to_sql.call_args.kwargs["method"]
            is FilesystemToDatabaseOperator._postgres_copy_method
        )

    def test_sqlite_engine_url_built_from_host(self):
        mock_ce, mock_to_sql, _ = self._run("sqlite", host="/data/my.db")
        assert mock_ce.call_args.args[0] == "sqlite:////data/my.db"
        assert mock_to_sql.call_args.kwargs["method"] == "multi"

    def test_other_conn_type_delegates_to_get_uri(self):
        mock_ce, mock_to_sql, db_conn = self._run("mysql")
        assert mock_ce.call_args.args[0] is db_conn.get_uri.return_value
        assert mock_to_sql.call_args.kwargs["method"] == "multi"
