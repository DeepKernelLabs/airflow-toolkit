"""Tests for new format support in FilesystemToDatabaseOperator: excel, avro, fixed_width.

All tests use MockFilesystem + in-memory SQLite — no Docker, no cloud credentials.
"""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any

import fastavro
import pandas as pd

from airflow_toolkit.providers.deltalake.operators.filesystem_to_database import (
    FilesystemToDatabaseOperator,
    _FORMAT_EXTENSIONS,
    _FORMAT_TEMP_SUFFIX,
)


# ── helpers ───────────────────────────────────────────────────────────────────


def _make_op(**kwargs: Any) -> FilesystemToDatabaseOperator:
    defaults: dict[str, Any] = dict(
        task_id="test_op",
        filesystem_conn_id="fs_conn",
        database_conn_id="db_conn",
        filesystem_path="data/",
        db_table="test_table",
        metadata={},
        include_source_path=False,
        idempotent=False,
    )
    defaults.update(kwargs)
    return FilesystemToDatabaseOperator(**defaults)


def _excel_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_excel(buf, index=False)
    return buf.getvalue()


def _avro_bytes(records: list[dict], schema: dict) -> bytes:
    buf = io.BytesIO()
    fastavro.writer(buf, schema, records)
    return buf.getvalue()


AVRO_SCHEMA = {
    "type": "record",
    "name": "Row",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
    ],
}

SAMPLE_RECORDS = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
SAMPLE_DF = pd.DataFrame(SAMPLE_RECORDS)


# ── _FORMAT_EXTENSIONS ────────────────────────────────────────────────────────


def test_format_extensions_contains_all_formats():
    for fmt in ("csv", "json", "parquet", "excel", "avro", "fixed_width"):
        assert fmt in _FORMAT_EXTENSIONS, f"Missing format: {fmt}"


def test_excel_extensions():
    assert ".xlsx" in _FORMAT_EXTENSIONS["excel"]
    assert ".xls" in _FORMAT_EXTENSIONS["excel"]


def test_avro_extension():
    assert ".avro" in _FORMAT_EXTENSIONS["avro"]


def test_fixed_width_extensions():
    exts = _FORMAT_EXTENSIONS["fixed_width"]
    assert ".fwf" in exts or ".txt" in exts


# ── _get_file_columns ─────────────────────────────────────────────────────────


class TestGetFileColumnsExcel:
    def test_returns_column_names(self, tmp_path):
        path = str(tmp_path / "data.xlsx")
        SAMPLE_DF.to_excel(path, index=False)
        op = _make_op(source_format="excel")
        assert op._get_file_columns(path) == {"id", "name"}

    def test_extra_columns_detected(self, tmp_path):
        path = str(tmp_path / "data.xlsx")
        df = pd.DataFrame({"a": [1], "b": [2], "c": [3]})
        df.to_excel(path, index=False)
        op = _make_op(source_format="excel")
        assert op._get_file_columns(path) == {"a", "b", "c"}


class TestGetFileColumnsAvro:
    def test_returns_column_names(self, tmp_path):
        path = str(tmp_path / "data.avro")
        Path(path).write_bytes(_avro_bytes(SAMPLE_RECORDS, AVRO_SCHEMA))
        op = _make_op(source_format="avro")
        assert op._get_file_columns(path) == {"id", "name"}


class TestGetFileColumnsFixedWidth:
    def test_returns_column_names(self, tmp_path):
        content = b"  1Alice\n  2Bob  \n"
        path = str(tmp_path / "data.fwf")
        Path(path).write_bytes(content)
        op = _make_op(
            source_format="fixed_width",
            source_format_options={
                "colspecs": [(0, 3), (3, 8)],
                "names": ["id", "name"],
            },
        )
        assert op._get_file_columns(path) == {"id", "name"}


# ── _iter_batches ─────────────────────────────────────────────────────────────


class TestIterBatchesExcel:
    def test_yields_all_rows(self, tmp_path):
        path = str(tmp_path / "data.xlsx")
        SAMPLE_DF.to_excel(path, index=False)
        op = _make_op(source_format="excel")
        rows = pd.concat(list(op._iter_batches(path)), ignore_index=True)
        assert len(rows) == 2
        assert list(rows.columns) == ["id", "name"]

    def test_respects_batch_size(self, tmp_path):
        path = str(tmp_path / "data.xlsx")
        df = pd.DataFrame({"x": range(10)})
        df.to_excel(path, index=False)
        op = _make_op(source_format="excel", batch_size=3)
        batches = list(op._iter_batches(path))
        assert len(batches) == 4  # 3+3+3+1
        assert sum(len(b) for b in batches) == 10

    def test_empty_file_yields_empty_batch(self, tmp_path):
        path = str(tmp_path / "empty.xlsx")
        pd.DataFrame({"id": [], "name": []}).to_excel(path, index=False)
        op = _make_op(source_format="excel")
        batches = list(op._iter_batches(path))
        assert len(batches) == 1
        assert len(batches[0]) == 0


class TestIterBatchesAvro:
    def test_yields_all_rows(self, tmp_path):
        path = str(tmp_path / "data.avro")
        Path(path).write_bytes(_avro_bytes(SAMPLE_RECORDS, AVRO_SCHEMA))
        op = _make_op(source_format="avro")
        rows = pd.concat(list(op._iter_batches(path)), ignore_index=True)
        assert len(rows) == 2
        assert set(rows.columns) == {"id", "name"}

    def test_respects_batch_size(self, tmp_path):
        records = [{"id": i, "name": f"user{i}"} for i in range(7)]
        path = str(tmp_path / "data.avro")
        Path(path).write_bytes(_avro_bytes(records, AVRO_SCHEMA))
        op = _make_op(source_format="avro", batch_size=3)
        batches = list(op._iter_batches(path))
        assert len(batches) == 3  # 3+3+1
        assert sum(len(b) for b in batches) == 7

    def test_values_are_correct(self, tmp_path):
        path = str(tmp_path / "data.avro")
        Path(path).write_bytes(_avro_bytes(SAMPLE_RECORDS, AVRO_SCHEMA))
        op = _make_op(source_format="avro")
        df = pd.concat(list(op._iter_batches(path)), ignore_index=True)
        assert df["name"].tolist() == ["Alice", "Bob"]


class TestIterBatchesFixedWidth:
    def test_yields_all_rows(self, tmp_path):
        content = b"  1Alice\n  2Bob  \n"
        path = str(tmp_path / "data.fwf")
        Path(path).write_bytes(content)
        op = _make_op(
            source_format="fixed_width",
            source_format_options={
                "colspecs": [(0, 3), (3, 8)],
                "names": ["id", "name"],
            },
        )
        rows = pd.concat(list(op._iter_batches(path)), ignore_index=True)
        assert len(rows) == 2


# ── raw_content_to_pandas ────────────────────────────────────────────────────


class TestRawContentToPandasExcel:
    def test_reads_from_bytesio(self):
        buf = io.BytesIO(_excel_bytes(SAMPLE_DF))
        op = _make_op(source_format="excel")
        df = op.raw_content_to_pandas(buf)
        assert list(df.columns) == ["id", "name"]
        assert len(df) == 2


class TestRawContentToPandasAvro:
    def test_reads_from_bytesio(self):
        buf = io.BytesIO(_avro_bytes(SAMPLE_RECORDS, AVRO_SCHEMA))
        op = _make_op(source_format="avro")
        df = op.raw_content_to_pandas(buf)
        assert set(df.columns) == {"id", "name"}
        assert len(df) == 2

    def test_reads_from_bytes(self):
        raw = _avro_bytes(SAMPLE_RECORDS, AVRO_SCHEMA)
        op = _make_op(source_format="avro")
        df = op.raw_content_to_pandas(raw)
        assert len(df) == 2


class TestRawContentToPandasFixedWidth:
    def test_reads_with_colspecs(self):
        content = b"  1Alice\n  2Bob  \n"
        op = _make_op(
            source_format="fixed_width",
            source_format_options={
                "colspecs": [(0, 3), (3, 8)],
                "names": ["id", "name"],
            },
        )
        df = op.raw_content_to_pandas(io.BytesIO(content))
        assert list(df.columns) == ["id", "name"]
        assert len(df) == 2


# ── extension filtering ───────────────────────────────────────────────────────


class TestExtensionFiltering:
    """Verify the extension matching logic used inside execute() to skip blobs."""

    def test_excel_matches_xlsx_and_xls(self):
        exts = _FORMAT_EXTENSIONS["excel"]
        assert "data/file.xlsx".endswith(exts)
        assert "data/file.xls".endswith(exts)
        assert not "data/file.csv".endswith(exts)
        assert not "data/file.parquet".endswith(exts)

    def test_avro_matches_avro_only(self):
        exts = _FORMAT_EXTENSIONS["avro"]
        assert "data/file.avro".endswith(exts)
        assert not "data/file.csv".endswith(exts)
        assert not "data/file.avro.gz".endswith(exts)

    def test_fixed_width_matches_expected_extensions(self):
        exts = _FORMAT_EXTENSIONS["fixed_width"]
        assert "data/file.fwf".endswith(exts) or "data/file.txt".endswith(exts)
        assert not "data/file.csv".endswith(exts)

    def test_csv_still_matches_gz(self):
        exts = _FORMAT_EXTENSIONS["csv"]
        assert "data/file.csv".endswith(exts)
        assert "data/file.csv.gz".endswith(exts)
        assert not "data/file.parquet".endswith(exts)

    def test_all_formats_have_at_least_one_extension(self):
        for fmt, exts in _FORMAT_EXTENSIONS.items():
            assert len(exts) >= 1, f"{fmt} has no extensions"


# ── temp suffix ───────────────────────────────────────────────────────────────


class TestFormatTempSuffix:
    """_FORMAT_TEMP_SUFFIX must use extensions that pandas/fastavro accept."""

    def test_excel_uses_xlsx_not_excel(self):
        # pd.read_excel() infers the engine from the file extension.
        # ".excel" is not recognized; ".xlsx" is.
        assert _FORMAT_TEMP_SUFFIX["excel"] == ".xlsx"

    def test_all_formats_covered(self):
        for fmt in _FORMAT_EXTENSIONS:
            assert fmt in _FORMAT_TEMP_SUFFIX, f"Missing temp suffix for format: {fmt}"

    def test_suffixes_start_with_dot(self):
        for fmt, suffix in _FORMAT_TEMP_SUFFIX.items():
            assert suffix.startswith("."), (
                f"{fmt} suffix '{suffix}' must start with '.'"
            )

    def test_excel_temp_file_readable_by_pandas(self, tmp_path):
        """End-to-end: write an xlsx file, rename to .xlsx (simulating temp suffix), read it."""
        path = str(tmp_path / "tmp_abc123.xlsx")
        SAMPLE_DF.to_excel(path, index=False)
        df = pd.read_excel(path)
        assert list(df.columns) == ["id", "name"]
