"""Unit tests for SQLToFilesystem and FilesystemToFilesystem operators.

Uses LocalFilesystem backed by tmp_path so no external services are required.
BaseHook.get_connection and FilesystemFactory are patched to wire up local
filesystems without an Airflow metadata DB.
"""
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from airflow_toolkit.filesystems.impl.local_filesystem import LocalFilesystem
from airflow_toolkit.providers.filesystem.operators.filesystem import (
    FilesystemCheckOperator,
    FilesystemDeleteOperator,
    FilesystemToFilesystem,
    SQLToFilesystem,
)

_FACTORY = "airflow_toolkit.providers.filesystem.operators.filesystem.FilesystemFactory.get_data_lake_filesystem"
_GET_CONN = "airflow_toolkit.providers.filesystem.operators.filesystem.BaseHook.get_connection"


def _fs(base: Path) -> LocalFilesystem:
    base.mkdir(parents=True, exist_ok=True)
    hook = MagicMock()
    hook.get_path.return_value = str(base)
    return LocalFilesystem(hook)


# ---------------------------------------------------------------------------
# SQLToFilesystem
# ---------------------------------------------------------------------------

class TestSQLToFilesystem:
    def _make_op(self, **kwargs):
        defaults = dict(
            task_id="test_sql",
            source_sql_conn_id="sql_test",
            destination_fs_conn_id="fs_test",
            sql="SELECT * FROM t",
            destination_path="output/",
        )
        defaults.update(kwargs)
        return SQLToFilesystem(**defaults)

    def _mock_sql_hook(self, df: pd.DataFrame):
        hook = MagicMock()
        hook.get_pandas_df.return_value = df
        conn = MagicMock()
        conn.get_hook.return_value = hook
        return conn, hook

    def test_writes_single_parquet_file(self, tmp_path):
        dst = _fs(tmp_path / "dst")
        df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
        conn, sql_hook = self._mock_sql_hook(df)

        op = self._make_op()
        with patch(_GET_CONN, return_value=conn), patch(_FACTORY, return_value=dst):
            op.execute({})

        parquet_files = list((tmp_path / "dst" / "output").glob("*.parquet"))
        assert len(parquet_files) == 1
        assert parquet_files[0].name == "part0001.parquet"

    def test_files_list_is_populated_after_execute(self, tmp_path):
        dst = _fs(tmp_path / "dst")
        df = pd.DataFrame({"x": [1]})
        conn, _ = self._mock_sql_hook(df)

        op = self._make_op()
        with patch(_GET_CONN, return_value=conn), patch(_FACTORY, return_value=dst):
            op.execute({})

        assert op.files == ["output/part0001.parquet"]

    def test_batch_size_writes_one_file_per_chunk(self, tmp_path):
        dst = _fs(tmp_path / "dst")
        chunks = [pd.DataFrame({"id": [i]}) for i in range(3)]

        hook = MagicMock()
        hook.get_pandas_df_by_chunks.return_value = iter(chunks)
        conn = MagicMock()
        conn.get_hook.return_value = hook

        op = self._make_op(batch_size=1)
        with patch(_GET_CONN, return_value=conn), patch(_FACTORY, return_value=dst):
            op.execute({})

        parquet_files = sorted((tmp_path / "dst" / "output").glob("*.parquet"))
        assert len(parquet_files) == 3
        assert [f.name for f in parquet_files] == [
            "part0001.parquet", "part0002.parquet", "part0003.parquet"
        ]
        hook.get_pandas_df_by_chunks.assert_called_once_with(
            sql="SELECT * FROM t", chunksize=1
        )

    def test_creates_success_file_when_configured(self, tmp_path):
        dst = _fs(tmp_path / "dst")
        df = pd.DataFrame({"id": [1]})
        conn, _ = self._mock_sql_hook(df)

        op = self._make_op(create_file_on_success="__SUCCESS__")
        with patch(_GET_CONN, return_value=conn), patch(_FACTORY, return_value=dst):
            op.execute({})

        assert (tmp_path / "dst" / "output" / "__SUCCESS__").exists()

    def test_no_success_file_when_not_configured(self, tmp_path):
        dst = _fs(tmp_path / "dst")
        df = pd.DataFrame({"id": [1]})
        conn, _ = self._mock_sql_hook(df)

        op = self._make_op()
        with patch(_GET_CONN, return_value=conn), patch(_FACTORY, return_value=dst):
            op.execute({})

        assert not (tmp_path / "dst" / "output" / "__SUCCESS__").exists()

    def test_destination_path_trailing_slash_is_normalised(self, tmp_path):
        dst = _fs(tmp_path / "dst")
        df = pd.DataFrame({"id": [1]})
        conn, _ = self._mock_sql_hook(df)

        op = self._make_op(destination_path="output")  # no trailing slash
        with patch(_GET_CONN, return_value=conn), patch(_FACTORY, return_value=dst):
            op.execute({})

        assert (tmp_path / "dst" / "output" / "part0001.parquet").exists()

    def test_files_list_is_cleared_on_re_execute(self, tmp_path):
        dst = _fs(tmp_path / "dst")
        df = pd.DataFrame({"id": [1]})
        conn, _ = self._mock_sql_hook(df)

        op = self._make_op()
        with patch(_GET_CONN, return_value=conn), patch(_FACTORY, return_value=dst):
            op.execute({})
            op.execute({})  # second run

        # files list should reflect only the last run
        assert op.files == ["output/part0001.parquet"]


# ---------------------------------------------------------------------------
# FilesystemToFilesystem
# ---------------------------------------------------------------------------

class TestFilesystemToFilesystem:
    def _make_op(self, **kwargs):
        defaults = dict(
            task_id="test_copy",
            source_fs_conn_id="src_conn",
            destination_fs_conn_id="dst_conn",
            source_path="input/file.txt",
            destination_path="output/file.txt",
        )
        defaults.update(kwargs)
        return FilesystemToFilesystem(**defaults)

    def _patch(self, src_fs, dst_fs):
        """Return a context manager that wires source and destination filesystems."""
        mock_conn = MagicMock()
        return (
            patch(_GET_CONN, return_value=mock_conn),
            patch(_FACTORY, side_effect=[src_fs, dst_fs]),
        )

    def test_copies_single_file(self, tmp_path):
        src = _fs(tmp_path / "src")
        dst = _fs(tmp_path / "dst")
        src.write(b"hello", "input/file.txt")

        op = self._make_op()
        gc, fac = self._patch(src, dst)
        with gc, fac:
            op.execute({})

        assert dst.read("output/file.txt") == b"hello"

    def test_copies_all_files_when_source_is_directory(self, tmp_path):
        src = _fs(tmp_path / "src")
        dst = _fs(tmp_path / "dst")
        src.write(b"a", "data/file_a.txt")
        src.write(b"b", "data/file_b.txt")

        op = self._make_op(source_path="data/", destination_path="out/")
        gc, fac = self._patch(src, dst)
        with gc, fac:
            op.execute({})

        out_files = {f.name for f in (tmp_path / "dst" / "out").iterdir()}
        assert out_files == {"file_a.txt", "file_b.txt"}

    def test_destination_path_with_trailing_slash_appends_filename(self, tmp_path):
        src = _fs(tmp_path / "src")
        dst = _fs(tmp_path / "dst")
        src.write(b"content", "in/report.csv")

        op = self._make_op(source_path="in/report.csv", destination_path="out/")
        gc, fac = self._patch(src, dst)
        with gc, fac:
            op.execute({})

        assert dst.check_file("out/report.csv")

    def test_destination_path_without_slash_uses_exact_path(self, tmp_path):
        src = _fs(tmp_path / "src")
        dst = _fs(tmp_path / "dst")
        src.write(b"content", "in/report.csv")

        op = self._make_op(
            source_path="in/report.csv",
            destination_path="out/renamed.csv",
        )
        gc, fac = self._patch(src, dst)
        with gc, fac:
            op.execute({})

        assert dst.check_file("out/renamed.csv")

    def test_data_transformation_is_applied(self, tmp_path):
        src = _fs(tmp_path / "src")
        dst = _fs(tmp_path / "dst")
        src.write(b"original", "in/file.txt")

        def upper(data, filename, context):
            return data.upper()

        op = self._make_op(
            source_path="in/file.txt",
            destination_path="out/file.txt",
            data_transformation=upper,
        )
        gc, fac = self._patch(src, dst)
        with gc, fac:
            op.execute({})

        assert dst.read("out/file.txt") == b"ORIGINAL"

    def test_no_transformation_preserves_content(self, tmp_path):
        src = _fs(tmp_path / "src")
        dst = _fs(tmp_path / "dst")
        src.write(b"unchanged", "in/file.txt")

        op = self._make_op(source_path="in/file.txt", destination_path="out/file.txt")
        gc, fac = self._patch(src, dst)
        with gc, fac:
            op.execute({})

        assert dst.read("out/file.txt") == b"unchanged"

    def test_creates_success_file_when_configured(self, tmp_path):
        src = _fs(tmp_path / "src")
        dst = _fs(tmp_path / "dst")
        src.write(b"data", "in/file.txt")

        op = self._make_op(
            source_path="in/file.txt",
            destination_path="out/file.txt",
            create_file_on_success="__SUCCESS__",
        )
        gc, fac = self._patch(src, dst)
        with gc, fac:
            op.execute({})

        assert dst.check_file("out/__SUCCESS__")

    def test_no_success_file_when_not_configured(self, tmp_path):
        src = _fs(tmp_path / "src")
        dst = _fs(tmp_path / "dst")
        src.write(b"data", "in/file.txt")

        op = self._make_op(source_path="in/file.txt", destination_path="out/file.txt")
        gc, fac = self._patch(src, dst)
        with gc, fac:
            op.execute({})

        success_files = list((tmp_path / "dst" / "out").glob("__SUCCESS__"))
        assert success_files == []


# ---------------------------------------------------------------------------
# FilesystemCheckOperator — routing logic (file vs prefix)
# ---------------------------------------------------------------------------

class TestFilesystemCheckOperatorRouting:
    def test_path_ending_with_slash_calls_check_prefix(self):
        mock_fs = MagicMock()
        mock_fs.check_prefix.return_value = True
        mock_conn = MagicMock()

        op = FilesystemCheckOperator(
            task_id="check",
            filesystem_conn_id="conn",
            filesystem_path="some/prefix/",
        )
        with patch(_GET_CONN, return_value=mock_conn), patch(_FACTORY, return_value=mock_fs):
            result = op.execute({})

        mock_fs.check_prefix.assert_called_once_with("some/prefix/")
        mock_fs.check_file.assert_not_called()
        assert result is True

    def test_path_without_slash_calls_check_file(self):
        mock_fs = MagicMock()
        mock_fs.check_file.return_value = False
        mock_conn = MagicMock()

        op = FilesystemCheckOperator(
            task_id="check",
            filesystem_conn_id="conn",
            filesystem_path="some/file.txt",
        )
        with patch(_GET_CONN, return_value=mock_conn), patch(_FACTORY, return_value=mock_fs):
            result = op.execute({})

        mock_fs.check_file.assert_called_once_with("some/file.txt")
        mock_fs.check_prefix.assert_not_called()
        assert result is False


# ---------------------------------------------------------------------------
# FilesystemDeleteOperator — delegates to delete_prefix
# ---------------------------------------------------------------------------

class TestFilesystemDeleteOperator:
    def test_calls_delete_prefix(self):
        mock_fs = MagicMock()
        mock_conn = MagicMock()

        op = FilesystemDeleteOperator(
            task_id="delete",
            filesystem_conn_id="conn",
            filesystem_path="some/prefix/",
        )
        with patch(_GET_CONN, return_value=mock_conn), patch(_FACTORY, return_value=mock_fs):
            op.execute({})

        mock_fs.delete_prefix.assert_called_once_with("some/prefix/")
