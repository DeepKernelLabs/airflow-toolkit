from __future__ import annotations

import ftplib
from io import BytesIO
from unittest.mock import ANY, MagicMock

from airflow_toolkit.filesystems.impl.ftp_filesystem import FTPFilesystem


def _fs() -> tuple[FTPFilesystem, MagicMock]:
    conn = MagicMock(spec=ftplib.FTP)
    hook = MagicMock()
    hook.get_conn.return_value = conn
    return FTPFilesystem(hook), conn


class TestFTPRead:
    def test_retrbinary_called_with_correct_command(self):
        fs, conn = _fs()

        def fake_retr(cmd, callback):
            callback(b"hello world")

        conn.retrbinary.side_effect = fake_retr
        result = fs.read("/exports/data.csv")
        conn.retrbinary.assert_called_once_with("RETR exports/data.csv", ANY)
        assert result == b"hello world"

    def test_read_returns_bytes(self):
        fs, conn = _fs()

        def fake_retr(cmd, callback):
            callback(b"line1\n")
            callback(b"line2\n")

        conn.retrbinary.side_effect = fake_retr
        assert fs.read("/file.txt") == b"line1\nline2\n"


class TestFTPWrite:
    def test_write_bytes_calls_storbinary(self):
        fs, conn = _fs()
        fs.write(b"data", "/exports/file.csv")
        conn.storbinary.assert_called_once()
        assert conn.storbinary.call_args[0][0] == "STOR exports/file.csv"

    def test_write_str_encodes_utf8(self):
        fs, conn = _fs()
        fs.write("hello", "/file.txt")
        payload: BytesIO = conn.storbinary.call_args[0][1]
        assert payload.read() == b"hello"

    def test_write_bytesio_unwrapped(self):
        fs, conn = _fs()
        fs.write(BytesIO(b"bytes"), "/file.txt")
        payload: BytesIO = conn.storbinary.call_args[0][1]
        assert payload.read() == b"bytes"

    def test_write_creates_parent_dirs(self):
        fs, conn = _fs()
        fs.write(b"x", "/a/b/c.csv")
        mkd_calls = [c[0][0] for c in conn.mkd.call_args_list]
        assert "a" in mkd_calls
        assert "a/b" in mkd_calls

    def test_write_no_parent_dir_for_root_file(self):
        fs, conn = _fs()
        fs.write(b"x", "/file.csv")
        conn.mkd.assert_not_called()


class TestFTPDeleteFile:
    def test_delete_calls_conn_delete(self):
        fs, conn = _fs()
        fs.delete_file("/exports/old.csv")
        conn.delete.assert_called_once_with("exports/old.csv")


class TestFTPCheckFile:
    def test_returns_true_when_size_succeeds(self):
        fs, conn = _fs()
        conn.size.return_value = 1024
        assert fs.check_file("/file.csv") is True

    def test_returns_false_on_error_perm(self):
        fs, conn = _fs()
        conn.size.side_effect = ftplib.error_perm("550 No such file")
        assert fs.check_file("/missing.csv") is False


class TestFTPCheckPrefix:
    def test_returns_true_when_cwd_succeeds(self):
        fs, conn = _fs()
        conn.pwd.return_value = "/home/demo"
        assert fs.check_prefix("/exports/") is True
        conn.cwd.assert_any_call("exports/")
        conn.cwd.assert_any_call("/home/demo")

    def test_returns_false_on_error_perm(self):
        fs, conn = _fs()
        conn.cwd.side_effect = ftplib.error_perm("550 No such directory")
        assert fs.check_prefix("/missing/") is False


class TestFTPCreatePrefix:
    def test_creates_each_path_segment(self):
        fs, conn = _fs()
        fs.create_prefix("/a/b/c")
        mkd_paths = [c[0][0] for c in conn.mkd.call_args_list]
        assert mkd_paths == ["a", "a/b", "a/b/c"]

    def test_ignores_already_exists_error(self):
        fs, conn = _fs()
        conn.mkd.side_effect = ftplib.error_perm("550 Directory already exists")
        fs.create_prefix("/existing/dir")  # must not raise


class TestFTPListFiles:
    def test_flat_directory(self):
        fs, conn = _fs()
        conn.nlst.return_value = ["exports/file1.csv", "exports/file2.csv"]
        conn.size.return_value = 100
        result = fs.list_files("/exports")
        assert set(result) == {"exports/file1.csv", "exports/file2.csv"}

    def test_recursive_traversal(self):
        fs, conn = _fs()

        def nlst_side(path):
            if path == "exports":
                return ["exports/sub", "exports/root.csv"]
            if path == "exports/sub":
                return ["exports/sub/nested.csv"]
            return []

        def size_side(path):
            if path == "exports/sub":
                raise ftplib.error_perm("550 Could not get file size.")
            return 100

        conn.nlst.side_effect = nlst_side
        conn.size.side_effect = size_side
        result = fs.list_files("/exports")
        assert set(result) == {"exports/root.csv", "exports/sub/nested.csv"}

    def test_nlst_error_treats_path_as_file(self):
        fs, conn = _fs()
        conn.nlst.side_effect = ftplib.error_perm("550 Not a directory")
        result = fs.list_files("/exports/file.csv")
        assert result == ["exports/file.csv"]

    def test_nlst_returns_self_treats_as_file(self):
        fs, conn = _fs()
        conn.nlst.return_value = ["exports/file.csv"]
        result = fs.list_files("/exports/file.csv")
        assert result == ["exports/file.csv"]


class TestFTPDeletePrefix:
    def test_deletes_files_then_removes_dir(self):
        fs, conn = _fs()
        conn.nlst.return_value = ["exports/data.csv"]
        conn.size.return_value = 100
        fs.delete_prefix("/exports")
        conn.delete.assert_called_once_with("exports/data.csv")
        conn.rmd.assert_called_once_with("exports")

    def test_recursive_delete(self):
        fs, conn = _fs()

        def nlst_side(path):
            if path == "exports":
                return ["exports/sub"]
            if path == "exports/sub":
                return ["exports/sub/file.csv"]
            return []

        def size_side(path):
            if path == "exports/sub":
                raise ftplib.error_perm("550 Could not get file size.")
            return 100

        conn.nlst.side_effect = nlst_side
        conn.size.side_effect = size_side
        fs.delete_prefix("/exports")
        conn.delete.assert_called_once_with("exports/sub/file.csv")
        assert conn.rmd.call_count == 2
