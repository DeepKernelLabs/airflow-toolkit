from __future__ import annotations

from io import BytesIO
from unittest.mock import MagicMock


from airflow_toolkit.filesystems.impl.sharepoint_filesystem import SharePointFilesystem


def _fs(
    site_path: str = "/sites/MySite",
) -> tuple[SharePointFilesystem, MagicMock, MagicMock]:
    hook = MagicMock()
    hook.site_path = site_path
    ctx = MagicMock()
    hook.get_conn.return_value = ctx
    return SharePointFilesystem(hook), hook, ctx


class TestServerRelativeUrl:
    def test_prepends_site_path(self):
        fs, _, _ = _fs("/sites/MySite")
        assert (
            fs._server_relative_url("Documents/file.csv")
            == "/sites/MySite/Documents/file.csv"
        )

    def test_strips_leading_slash_from_path(self):
        fs, _, _ = _fs("/sites/MySite")
        assert (
            fs._server_relative_url("/Documents/file.csv")
            == "/sites/MySite/Documents/file.csv"
        )


class TestSharePointRead:
    def test_read_calls_get_file_by_server_relative_url(self):
        fs, hook, ctx = _fs()
        file_mock = MagicMock()
        file_mock.read.return_value = file_mock
        file_mock.value = b"content"
        ctx.web.get_file_by_server_relative_url.return_value = file_mock
        result = fs.read("Documents/data.csv")
        ctx.web.get_file_by_server_relative_url.assert_called_once_with(
            "/sites/MySite/Documents/data.csv"
        )
        ctx.execute_query.assert_called()
        assert result == b"content"


class TestSharePointWrite:
    def test_write_bytes_uploads_file(self):
        fs, hook, ctx = _fs()
        folder_mock = MagicMock()
        ctx.web.get_folder_by_server_relative_url.return_value = folder_mock
        fs.write(b"data", "Documents/exports/file.csv")
        ctx.web.ensure_folder_path.assert_called_once_with(
            "/sites/MySite/Documents/exports"
        )
        folder_mock.upload_file.assert_called_once_with("file.csv", b"data")
        assert ctx.execute_query.call_count == 2

    def test_write_str_encodes_utf8(self):
        fs, hook, ctx = _fs()
        folder_mock = MagicMock()
        ctx.web.get_folder_by_server_relative_url.return_value = folder_mock
        fs.write("hello", "Documents/file.txt")
        _, actual_data = folder_mock.upload_file.call_args[0]
        assert actual_data == b"hello"

    def test_write_bytesio(self):
        fs, hook, ctx = _fs()
        folder_mock = MagicMock()
        ctx.web.get_folder_by_server_relative_url.return_value = folder_mock
        fs.write(BytesIO(b"payload"), "Documents/file.bin")
        _, actual_data = folder_mock.upload_file.call_args[0]
        assert actual_data == b"payload"

    def test_write_root_level_uses_site_path(self):
        fs, hook, ctx = _fs("/sites/MySite")
        folder_mock = MagicMock()
        ctx.web.get_folder_by_server_relative_url.return_value = folder_mock
        fs.write(b"x", "file.csv")
        ctx.web.get_folder_by_server_relative_url.assert_called_once_with(
            "/sites/MySite"
        )


class TestSharePointDeleteFile:
    def test_delete_calls_delete_object(self):
        fs, hook, ctx = _fs()
        file_mock = MagicMock()
        ctx.web.get_file_by_server_relative_url.return_value = file_mock
        fs.delete_file("Documents/old.csv")
        ctx.web.get_file_by_server_relative_url.assert_called_once_with(
            "/sites/MySite/Documents/old.csv"
        )
        file_mock.delete_object.assert_called_once()
        ctx.execute_query.assert_called()


class TestSharePointCreatePrefix:
    def test_create_prefix_calls_ensure_folder_path(self):
        fs, hook, ctx = _fs()
        fs.create_prefix("Documents/exports/2024")
        ctx.web.ensure_folder_path.assert_called_once_with(
            "/sites/MySite/Documents/exports/2024"
        )
        ctx.execute_query.assert_called()


class TestSharePointDeletePrefix:
    def test_delete_prefix_calls_delete_object(self):
        fs, hook, ctx = _fs()
        folder_mock = MagicMock()
        ctx.web.get_folder_by_server_relative_url.return_value = folder_mock
        fs.delete_prefix("Documents/exports")
        ctx.web.get_folder_by_server_relative_url.assert_called_once_with(
            "/sites/MySite/Documents/exports"
        )
        folder_mock.delete_object.assert_called_once()
        ctx.execute_query.assert_called()


class TestSharePointCheckFile:
    def test_returns_true_when_execute_query_succeeds(self):
        fs, hook, ctx = _fs()
        assert fs.check_file("Documents/file.csv") is True

    def test_returns_false_when_execute_query_raises(self):
        fs, hook, ctx = _fs()
        ctx.execute_query.side_effect = Exception("not found")
        assert fs.check_file("Documents/missing.csv") is False


class TestSharePointCheckPrefix:
    def test_returns_true_when_folder_loads(self):
        fs, hook, ctx = _fs()
        assert fs.check_prefix("Documents/exports") is True

    def test_returns_false_when_execute_query_raises(self):
        fs, hook, ctx = _fs()
        ctx.execute_query.side_effect = Exception("not found")
        assert fs.check_prefix("Documents/missing") is False


class TestSharePointListFiles:
    def test_list_files_strips_site_path_prefix(self):
        fs, hook, ctx = _fs("/sites/MySite")
        file1 = MagicMock()
        file1.serverRelativeUrl = "/sites/MySite/Documents/exports/data.csv"
        file2 = MagicMock()
        file2.serverRelativeUrl = "/sites/MySite/Documents/exports/other.csv"
        files_mock = MagicMock()
        files_mock.__iter__ = MagicMock(return_value=iter([file1, file2]))
        ctx.web.get_folder_by_server_relative_url.return_value.get_files.return_value = files_mock
        ctx.load.return_value = None
        result = fs.list_files("Documents/exports")
        assert "Documents/exports/data.csv" in result
        assert "Documents/exports/other.csv" in result

    def test_list_files_calls_get_files_recursive(self):
        fs, hook, ctx = _fs()
        files_mock = MagicMock()
        files_mock.__iter__ = MagicMock(return_value=iter([]))
        folder_mock = ctx.web.get_folder_by_server_relative_url.return_value
        folder_mock.get_files.return_value = files_mock
        fs.list_files("Documents/exports")
        folder_mock.get_files.assert_called_once_with(True)
