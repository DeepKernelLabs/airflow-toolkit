"""Unit tests for LocalFilesystem — no external services required."""
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from airflow_toolkit.filesystems.impl.local_filesystem import LocalFilesystem


def _make_fs(tmp_path: Path) -> LocalFilesystem:
    hook = MagicMock()
    hook.get_path.return_value = str(tmp_path)
    return LocalFilesystem(hook)


# ---------------------------------------------------------------------------
# write / read
# ---------------------------------------------------------------------------

def test_write_and_read_string(tmp_path):
    fs = _make_fs(tmp_path)
    fs.write("hello world", "file.txt")
    assert fs.read("file.txt") == b"hello world"


def test_write_and_read_bytes(tmp_path):
    fs = _make_fs(tmp_path)
    fs.write(b"\x00\x01\x02", "binary.bin")
    assert fs.read("binary.bin") == b"\x00\x01\x02"


def test_write_and_read_bytesio(tmp_path):
    fs = _make_fs(tmp_path)
    fs.write(BytesIO(b"bytesio content"), "bio.dat")
    assert fs.read("bio.dat") == b"bytesio content"


def test_write_creates_intermediate_directories(tmp_path):
    fs = _make_fs(tmp_path)
    fs.write("nested", "a/b/c/file.txt")
    assert (tmp_path / "a" / "b" / "c" / "file.txt").exists()


# ---------------------------------------------------------------------------
# delete_file
# ---------------------------------------------------------------------------

def test_delete_file(tmp_path):
    fs = _make_fs(tmp_path)
    fs.write("data", "to_delete.txt")
    fs.delete_file("to_delete.txt")
    assert not (tmp_path / "to_delete.txt").exists()


def test_delete_file_with_leading_slash(tmp_path):
    fs = _make_fs(tmp_path)
    fs.write("data", "slash.txt")
    fs.delete_file("/slash.txt")
    assert not (tmp_path / "slash.txt").exists()


# ---------------------------------------------------------------------------
# create_prefix / delete_prefix
# ---------------------------------------------------------------------------

def test_create_prefix_creates_directory(tmp_path):
    fs = _make_fs(tmp_path)
    fs.create_prefix("mydir/subdir/")
    assert (tmp_path / "mydir" / "subdir").is_dir()


def test_delete_prefix_removes_directory_tree(tmp_path):
    fs = _make_fs(tmp_path)
    fs.create_prefix("toremove/inner/")
    (tmp_path / "toremove" / "inner" / "file.txt").write_text("x")
    fs.delete_prefix("toremove/")
    assert not (tmp_path / "toremove").exists()


# ---------------------------------------------------------------------------
# check_file
# ---------------------------------------------------------------------------

def test_check_file_returns_true_for_existing_file(tmp_path):
    fs = _make_fs(tmp_path)
    fs.write("content", "exists.txt")
    assert fs.check_file("exists.txt") is True


def test_check_file_returns_false_for_missing_file(tmp_path):
    fs = _make_fs(tmp_path)
    assert fs.check_file("missing.txt") is False


def test_check_file_returns_false_for_directory(tmp_path):
    fs = _make_fs(tmp_path)
    fs.create_prefix("adir/")
    assert fs.check_file("adir") is False


# ---------------------------------------------------------------------------
# check_prefix
# ---------------------------------------------------------------------------

def test_check_prefix_returns_true_for_existing_directory(tmp_path):
    fs = _make_fs(tmp_path)
    fs.create_prefix("myprefix/")
    assert fs.check_prefix("myprefix/") is True


def test_check_prefix_returns_false_for_missing_directory(tmp_path):
    fs = _make_fs(tmp_path)
    assert fs.check_prefix("nonexistent/") is False


def test_check_prefix_returns_false_for_file(tmp_path):
    fs = _make_fs(tmp_path)
    fs.write("content", "afile.txt")
    assert fs.check_prefix("afile.txt") is False


# ---------------------------------------------------------------------------
# list_files
# ---------------------------------------------------------------------------

def test_list_files_returns_files_in_prefix(tmp_path):
    fs = _make_fs(tmp_path)
    fs.write("a", "data/file_a.txt")
    fs.write("b", "data/file_b.txt")
    files = fs.list_files("data/")
    assert len(files) == 2
    basenames = {Path(f).name for f in files}
    assert basenames == {"file_a.txt", "file_b.txt"}


def test_list_files_excludes_subdirectories(tmp_path):
    fs = _make_fs(tmp_path)
    fs.write("content", "root/file.txt")
    fs.create_prefix("root/subdir/")
    files = fs.list_files("root/")
    assert all(Path(f).is_file() for f in files)
    assert len(files) == 1


def test_list_files_empty_prefix(tmp_path):
    fs = _make_fs(tmp_path)
    fs.create_prefix("empty/")
    assert fs.list_files("empty/") == []
