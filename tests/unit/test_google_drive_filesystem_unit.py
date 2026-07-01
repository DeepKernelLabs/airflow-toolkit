from __future__ import annotations

import importlib.util
import sys
from unittest.mock import MagicMock, patch

import pytest

# Stub optional Google Drive dependencies only when not installed, so tests
# run without the extra but don't pollute sys.modules when the real package is present.
if importlib.util.find_spec("googleapiclient") is None:
    _http_stub = MagicMock()
    sys.modules["googleapiclient"] = MagicMock()
    sys.modules["googleapiclient.http"] = _http_stub
    sys.modules["googleapiclient.discovery"] = MagicMock()

if importlib.util.find_spec("google.oauth2") is None:
    sys.modules["google"] = MagicMock()
    sys.modules["google.oauth2"] = MagicMock()
    sys.modules["google.oauth2.service_account"] = MagicMock()

from airflow_toolkit.filesystems.impl.google_drive_filesystem import (
    GoogleDriveFilesystem,
)


def _mock_hook(shared_drive_id: str | None = None) -> MagicMock:
    hook = MagicMock()
    hook.shared_drive_id = shared_drive_id
    return hook


def _fs(
    shared_drive_id: str | None = None,
) -> tuple[GoogleDriveFilesystem, MagicMock, MagicMock]:
    hook = _mock_hook(shared_drive_id)
    service = MagicMock()
    hook.get_service.return_value = service
    fs = GoogleDriveFilesystem(hook)
    return fs, hook, service


def _list_result(files: list[dict]) -> MagicMock:
    result = MagicMock()
    result.execute.return_value = {"files": files}
    return result


class TestResolveId:
    def test_returns_none_when_not_found(self):
        fs, _, service = _fs()
        service.files.return_value.list.return_value = _list_result([])
        assert fs._resolve_id("folder/file.csv") is None

    def test_traverses_path_parts(self):
        fs, _, service = _fs()
        calls_iter = iter(
            [
                [{"id": "folder-id"}],
                [{"id": "file-id"}],
            ]
        )
        service.files.return_value.list.return_value.execute.side_effect = lambda: {
            "files": next(calls_iter)
        }
        result = fs._resolve_id("folder/file.csv")
        assert result == "file-id"

    def test_caches_intermediate_ids(self):
        fs, _, service = _fs()
        calls_iter = iter(
            [
                [{"id": "folder-id"}],
                [{"id": "file-id"}],
            ]
        )
        service.files.return_value.list.return_value.execute.side_effect = lambda: {
            "files": next(calls_iter)
        }
        fs._resolve_id("folder/file.csv")
        assert fs._id_cache.get("folder") == "folder-id"
        assert fs._id_cache.get("folder/file.csv") == "file-id"

    def test_returns_cached_result_without_api_call(self):
        fs, _, service = _fs()
        fs._id_cache["folder/file.csv"] = "cached-id"
        result = fs._resolve_id("folder/file.csv")
        assert result == "cached-id"
        service.files.return_value.list.assert_not_called()

    def test_shared_drive_adds_kwargs(self):
        fs, hook, service = _fs(shared_drive_id="drive-123")
        service.files.return_value.list.return_value = _list_result([{"id": "fid"}])
        fs._resolve_id("file.csv")
        call_kwargs = service.files.return_value.list.call_args[1]
        assert call_kwargs.get("driveId") == "drive-123"
        assert call_kwargs.get("corpora") == "drive"

    def test_folder_flag_adds_mimetype_filter_on_last_part(self):
        fs, _, service = _fs()
        service.files.return_value.list.return_value = _list_result([{"id": "fid"}])
        fs._resolve_id("folder", is_folder=True)
        q = service.files.return_value.list.call_args[1]["q"]
        assert "application/vnd.google-apps.folder" in q


class TestEnsureFolder:
    def test_creates_missing_folder(self):
        fs, _, service = _fs()
        service.files.return_value.list.return_value = _list_result([])
        service.files.return_value.create.return_value.execute.return_value = {
            "id": "new-id"
        }
        fs._ensure_folder("exports/2024")
        assert service.files.return_value.create.called

    def test_reuses_existing_folder(self):
        fs, _, service = _fs()
        service.files.return_value.list.return_value = _list_result(
            [{"id": "existing-id"}]
        )
        result = fs._ensure_folder("exports")
        service.files.return_value.create.assert_not_called()
        assert result == "existing-id"

    def test_empty_path_returns_root(self):
        fs, _, service = _fs()
        result = fs._ensure_folder("")
        assert result == "root"

    def test_shared_drive_returns_drive_id_for_root(self):
        fs, hook, service = _fs(shared_drive_id="drive-abc")
        result = fs._ensure_folder("")
        assert result == "drive-abc"


class TestCheckFile:
    def test_returns_true_when_resolve_id_succeeds(self):
        fs, _, service = _fs()
        service.files.return_value.list.return_value = _list_result([{"id": "fid"}])
        assert fs.check_file("folder/file.csv") is True

    def test_returns_false_when_not_found(self):
        fs, _, service = _fs()
        service.files.return_value.list.return_value = _list_result([])
        assert fs.check_file("folder/missing.csv") is False


class TestCheckPrefix:
    def test_returns_true_when_folder_found(self):
        fs, _, service = _fs()
        service.files.return_value.list.return_value = _list_result([{"id": "fid"}])
        assert fs.check_prefix("exports") is True

    def test_returns_false_when_folder_not_found(self):
        fs, _, service = _fs()
        service.files.return_value.list.return_value = _list_result([])
        assert fs.check_prefix("missing") is False


class TestRead:
    def test_raises_when_file_not_found(self):
        fs, _, service = _fs()
        service.files.return_value.list.return_value = _list_result([])
        with pytest.raises(FileNotFoundError):
            fs.read("folder/missing.csv")

    def test_downloads_file_content(self):
        fs, _, service = _fs()
        service.files.return_value.list.return_value = _list_result([{"id": "file-id"}])

        with patch("googleapiclient.http.MediaIoBaseDownload") as mock_dl_cls:
            mock_dl = MagicMock()
            mock_dl.next_chunk.side_effect = [(None, False), (None, True)]
            mock_dl_cls.return_value = mock_dl

            service.files.return_value.get_media.return_value = MagicMock()

            fs.read("folder/file.csv")
            service.files.return_value.get_media.assert_called_once_with(
                fileId="file-id", supportsAllDrives=True
            )


class TestWrite:
    def test_creates_new_file_when_not_exists(self):
        fs, _, service = _fs()
        # _resolve_id for parent folder: returns folder-id on first call
        # _ensure_folder: also uses list; we need separate responses
        # For simplicity, patch _ensure_folder and _resolve_id directly
        fs._ensure_folder = MagicMock(return_value="parent-id")
        fs._resolve_id = MagicMock(return_value=None)

        with patch("googleapiclient.http.MediaIoBaseUpload"):
            fs.write(b"content", "exports/file.csv")
            service.files.return_value.create.assert_called_once()

    def test_updates_existing_file(self):
        fs, _, service = _fs()
        fs._ensure_folder = MagicMock(return_value="parent-id")
        fs._resolve_id = MagicMock(return_value="existing-id")

        with patch("googleapiclient.http.MediaIoBaseUpload"):
            fs.write(b"new content", "exports/file.csv")
            service.files.return_value.update.assert_called_once()
            assert (
                service.files.return_value.update.call_args[1]["fileId"]
                == "existing-id"
            )

    def test_write_str_encodes(self):
        fs, _, service = _fs()
        fs._ensure_folder = MagicMock(return_value="parent-id")
        fs._resolve_id = MagicMock(return_value=None)

        captured = {}
        with patch("googleapiclient.http.MediaIoBaseUpload") as mock_media:

            def capture(*args, **kwargs):
                captured["buf"] = args[0]
                return MagicMock()

            mock_media.side_effect = capture
            fs.write("hello", "file.txt")
        assert captured["buf"].read() == b"hello"


class TestDeleteFile:
    def test_calls_delete_on_resolved_id(self):
        fs, _, service = _fs()
        fs._resolve_id = MagicMock(return_value="file-id")
        fs.delete_file("exports/file.csv")
        service.files.return_value.delete.assert_called_once_with(
            fileId="file-id", supportsAllDrives=True
        )

    def test_removes_from_cache(self):
        fs, _, service = _fs()
        fs._id_cache["exports/file.csv"] = "file-id"
        fs._resolve_id = MagicMock(return_value="file-id")
        fs.delete_file("exports/file.csv")
        assert "exports/file.csv" not in fs._id_cache

    def test_no_op_when_not_found(self):
        fs, _, service = _fs()
        fs._resolve_id = MagicMock(return_value=None)
        fs.delete_file("exports/missing.csv")
        service.files.return_value.delete.assert_not_called()


class TestListFiles:
    def test_returns_file_paths(self):
        fs, _, service = _fs()
        fs._resolve_id = MagicMock(return_value="folder-id")
        service.files.return_value.list.return_value.execute.return_value = {
            "files": [
                {"id": "f1", "name": "data.csv", "mimeType": "text/csv"},
                {"id": "f2", "name": "other.json", "mimeType": "application/json"},
            ]
        }
        result = fs.list_files("exports")
        assert "exports/data.csv" in result
        assert "exports/other.json" in result

    def test_recurses_into_subfolders(self):
        fs, _, service = _fs()
        fs._resolve_id = MagicMock(return_value="root-id")
        responses = iter(
            [
                {
                    "files": [
                        {
                            "id": "sub-id",
                            "name": "subdir",
                            "mimeType": "application/vnd.google-apps.folder",
                        },
                        {"id": "f1", "name": "top.csv", "mimeType": "text/csv"},
                    ]
                },
                {
                    "files": [
                        {"id": "f2", "name": "nested.csv", "mimeType": "text/csv"},
                    ]
                },
            ]
        )
        service.files.return_value.list.return_value.execute.side_effect = lambda: next(
            responses
        )
        result = fs.list_files("exports")
        assert "exports/top.csv" in result
        assert "exports/subdir/nested.csv" in result

    def test_returns_empty_when_folder_not_found(self):
        fs, _, service = _fs()
        fs._resolve_id = MagicMock(return_value=None)
        result = fs.list_files("missing")
        assert result == []

    def test_shared_drive_passes_extra_kwargs(self):
        fs, hook, service = _fs(shared_drive_id="drive-123")
        fs._resolve_id = MagicMock(return_value="folder-id")
        service.files.return_value.list.return_value.execute.return_value = {
            "files": []
        }
        fs.list_files("exports")
        call_kwargs = service.files.return_value.list.call_args[1]
        assert call_kwargs.get("driveId") == "drive-123"
