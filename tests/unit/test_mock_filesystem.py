"""Tests for MockFilesystem — verifies it satisfies the FilesystemProtocol contract."""

from __future__ import annotations

from io import BytesIO

import pytest

from airflow_toolkit.testing import MockFilesystem


# ── Construction ──────────────────────────────────────────────────────────────


def test_starts_empty_by_default():
    fs = MockFilesystem()
    assert fs.files == {}


def test_seed_data_is_copied():
    seed = {"a/b.csv": b"hello"}
    fs = MockFilesystem(seed)
    assert fs.files == seed
    # mutations don't affect the original dict
    fs.files["new.csv"] = b"x"
    assert "new.csv" not in seed


# ── read ──────────────────────────────────────────────────────────────────────


def test_read_returns_seeded_bytes():
    fs = MockFilesystem({"data/file.csv": b"id,name\n1,Alice"})
    assert fs.read("data/file.csv") == b"id,name\n1,Alice"


def test_read_raises_file_not_found():
    fs = MockFilesystem()
    with pytest.raises(FileNotFoundError, match="no file at 'missing.csv'"):
        fs.read("missing.csv")


# ── write ─────────────────────────────────────────────────────────────────────


def test_write_bytes():
    fs = MockFilesystem()
    fs.write(b"hello", "out/result.csv")
    assert fs.files["out/result.csv"] == b"hello"


def test_write_str_encodes_to_utf8():
    fs = MockFilesystem()
    fs.write("héllo", "out/file.txt")
    assert fs.files["out/file.txt"] == "héllo".encode()


def test_write_bytesio():
    fs = MockFilesystem()
    buf = BytesIO(b"data")
    fs.write(buf, "out/file.bin")
    assert fs.files["out/file.bin"] == b"data"


def test_write_overwrites_existing():
    fs = MockFilesystem({"f.txt": b"old"})
    fs.write(b"new", "f.txt")
    assert fs.files["f.txt"] == b"new"


# ── delete_file ───────────────────────────────────────────────────────────────


def test_delete_file_removes_entry():
    fs = MockFilesystem({"a.csv": b"x"})
    fs.delete_file("a.csv")
    assert "a.csv" not in fs.files


def test_delete_file_missing_is_silent():
    fs = MockFilesystem()
    fs.delete_file("nonexistent.csv")  # must not raise


# ── create_prefix / delete_prefix ─────────────────────────────────────────────


def test_create_prefix_is_noop():
    fs = MockFilesystem()
    fs.create_prefix("some/prefix/")
    assert fs.files == {}


def test_delete_prefix_removes_matching_files():
    fs = MockFilesystem(
        {
            "data/a.csv": b"1",
            "data/b.csv": b"2",
            "other/c.csv": b"3",
        }
    )
    fs.delete_prefix("data/")
    assert "data/a.csv" not in fs.files
    assert "data/b.csv" not in fs.files
    assert fs.files == {"other/c.csv": b"3"}


def test_delete_prefix_empty_match_is_noop():
    fs = MockFilesystem({"x.csv": b"x"})
    fs.delete_prefix("zzz/")
    assert fs.files == {"x.csv": b"x"}


# ── check_file / check_prefix ─────────────────────────────────────────────────


def test_check_file_true_when_present():
    fs = MockFilesystem({"a.csv": b""})
    assert fs.check_file("a.csv") is True


def test_check_file_false_when_absent():
    fs = MockFilesystem()
    assert fs.check_file("a.csv") is False


def test_check_prefix_true_when_file_under_prefix():
    fs = MockFilesystem({"data/a.csv": b""})
    assert fs.check_prefix("data/") is True


def test_check_prefix_false_when_no_match():
    fs = MockFilesystem({"other/a.csv": b""})
    assert fs.check_prefix("data/") is False


# ── list_files ────────────────────────────────────────────────────────────────


def test_list_files_returns_matching_paths():
    fs = MockFilesystem(
        {
            "data/a.csv": b"",
            "data/b.csv": b"",
            "other/c.csv": b"",
        }
    )
    result = fs.list_files("data/")
    assert sorted(result) == ["data/a.csv", "data/b.csv"]


def test_list_files_empty_when_no_match():
    fs = MockFilesystem({"other/a.csv": b""})
    assert fs.list_files("data/") == []


def test_list_files_empty_filesystem():
    fs = MockFilesystem()
    assert fs.list_files("data/") == []


# ── round-trip ────────────────────────────────────────────────────────────────


def test_write_then_read_round_trip():
    fs = MockFilesystem()
    content = b"id,name\n1,Alice\n2,Bob"
    fs.write(content, "results/output.csv")
    assert fs.read("results/output.csv") == content


def test_full_workflow():
    fs = MockFilesystem()
    fs.write(b"a", "run/2024-01-01/file.csv")
    fs.write(b"b", "run/2024-01-02/file.csv")
    fs.write(b"c", "other/file.csv")

    assert fs.check_prefix("run/")
    assert sorted(fs.list_files("run/")) == [
        "run/2024-01-01/file.csv",
        "run/2024-01-02/file.csv",
    ]

    fs.delete_prefix("run/2024-01-01/")
    assert not fs.check_file("run/2024-01-01/file.csv")
    assert fs.check_file("run/2024-01-02/file.csv")
    assert fs.check_file("other/file.csv")
