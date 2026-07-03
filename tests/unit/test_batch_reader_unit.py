"""Unit tests for FileBatchReaderMixin._get_file_schema() and dataframe_batches_to_arrow()."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from airflow_toolkit.providers.filesystem.batch_reader import (
    FileBatchReaderMixin,
    dataframe_batches_to_arrow,
)


class _Reader(FileBatchReaderMixin):
    def __init__(self, source_format: str, batch_size: int = 2, **options: Any):
        self.source_format = source_format
        self.source_format_options = options or None
        self.batch_size = batch_size


class TestGetFileSchema:
    def test_csv_schema_derived_from_first_batch(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("a,b\n1,x\n2,y\n3,z\n4,w\n")
        reader = _Reader("csv", batch_size=2)
        schema = reader._get_file_schema(str(f))
        assert schema.field("a").type == pa.int64()
        assert schema.field("b").type == pa.string()

    def test_parquet_schema_matches_file_schema(self, tmp_path):
        pa_ = pytest.importorskip("pyarrow")
        import pyarrow.parquet as pq

        f = tmp_path / "data.parquet"
        table = pa_.table({"p": [1, 2, 3], "q": ["a", "b", "c"]})
        pq.write_table(table, str(f))

        reader = _Reader("parquet", batch_size=2)
        schema = reader._get_file_schema(str(f))
        assert set(schema.names) == {"p", "q"}

    def test_header_only_file_yields_null_typed_schema(self, tmp_path):
        # pandas yields one empty chunk for a header-only CSV (not an empty
        # iterator), so this is a valid (if degenerate) schema, not an error.
        f = tmp_path / "empty.csv"
        f.write_text("a,b\n")
        reader = _Reader("csv")
        schema = reader._get_file_schema(str(f))
        assert schema.names == ["a", "b"]


class TestDataframeBatchesToArrow:
    def test_converts_consistent_batches(self):
        schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
        batches = [
            pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}),
            pd.DataFrame({"a": [3, 4], "b": ["z", "w"]}),
        ]
        result = list(dataframe_batches_to_arrow(iter(batches), schema))
        assert sum(b.num_rows for b in result) == 4
        assert all(b.schema == schema for b in result)

    def test_raises_clear_error_on_schema_drift(self, tmp_path):
        f = tmp_path / "drift.csv"
        f.write_text("a,b\n1,x\n2,y\nnotanumber,z\n4,w\n")
        reader = _Reader("csv", batch_size=2)
        schema = reader._get_file_schema(str(f))

        with pytest.raises(ValueError, match="does not match the schema"):
            list(dataframe_batches_to_arrow(reader._iter_batches(str(f)), schema))
