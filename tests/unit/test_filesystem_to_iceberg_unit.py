"""Unit tests for FilesystemToIcebergOperator.

Requires pyiceberg[pyarrow,sql-sqlite] to exercise a real SqlCatalog end to
end. That extra pulls in sqlalchemy>=2, which conflicts with
apache-airflow-providers-fab's sqlalchemy<2 pin on Python <3.13 — so it is
NOT installed in the project's 3 CI venvs, only in the dedicated
.venv-iceberg-test venv (no Airflow/FAB installed there). This module is
skipped entirely (not failed) when SqlCatalog isn't importable, so the
normal `pytest tests/unit` run in the CI venvs collects but skips it.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

pytest.importorskip("pyiceberg.catalog.sql")

from pyiceberg.catalog import load_catalog  # noqa: E402

from airflow_toolkit.providers.iceberg.operators.filesystem_to_iceberg import (  # noqa: E402
    FilesystemToIcebergOperator,
)
from airflow_toolkit.testing import MockFilesystem  # noqa: E402

_MODULE = "airflow_toolkit.providers.iceberg.operators.filesystem_to_iceberg"


def _catalog_properties(tmp_path) -> dict[str, str]:
    return {
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": f"file://{tmp_path}/warehouse",
    }


def _make_op(catalog_properties: dict[str, str], **kwargs: Any):
    defaults: dict[str, Any] = dict(
        task_id="test_op",
        filesystem_conn_id="fs_conn",
        filesystem_path="data/",
        catalog_name="test_catalog",
        catalog_properties=catalog_properties,
        table_identifier="ns.tbl",
    )
    defaults.update(kwargs)
    return FilesystemToIcebergOperator(**defaults)


def _run_with_mock_filesystem(op: FilesystemToIcebergOperator, files: dict[str, bytes]):
    mock_fs = MockFilesystem(files)
    with (
        patch(f"{_MODULE}.FilesystemFactory") as mock_factory,
        patch(f"{_MODULE}.BaseHook"),
    ):
        mock_factory.get_data_lake_filesystem.return_value = mock_fs
        op.execute({})


class TestExecuteWritesToIcebergTable:
    def test_creates_table_with_metadata_columns(self, tmp_path):
        catalog_properties = _catalog_properties(tmp_path)
        op = _make_op(catalog_properties, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(op, {"data/file1.csv": b"id,name\n1,alice\n2,bob\n"})

        catalog = load_catalog("test_catalog", **catalog_properties)
        df = catalog.load_table("ns.tbl").scan().to_pandas()
        assert set(df.columns) == {"id", "name", "_DS", "_LOADED_FROM"}
        assert len(df) == 2
        assert (df["_DS"] == "2026-07-01").all()

    def test_multiple_files_same_run_do_not_clobber_each_other(self, tmp_path):
        catalog_properties = _catalog_properties(tmp_path)
        op = _make_op(catalog_properties, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(
            op,
            {
                "data/file1.csv": b"id,name\n1,alice\n2,bob\n",
                "data/file2.csv": b"id,name\n3,carol\n",
            },
        )

        catalog = load_catalog("test_catalog", **catalog_properties)
        df = catalog.load_table("ns.tbl").scan().to_pandas()
        assert len(df) == 3

    def test_idempotent_rerun_replaces_instead_of_duplicating(self, tmp_path):
        catalog_properties = _catalog_properties(tmp_path)
        op1 = _make_op(catalog_properties, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(op1, {"data/file1.csv": b"id,name\n1,alice\n2,bob\n"})

        op2 = _make_op(catalog_properties, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(op2, {"data/file1.csv": b"id,name\n3,carol\n"})

        catalog = load_catalog("test_catalog", **catalog_properties)
        df = catalog.load_table("ns.tbl").scan().to_pandas()
        assert len(df) == 1
        assert df.iloc[0]["name"] == "carol"

    def test_idempotent_rerun_preserves_other_run_data(self, tmp_path):
        catalog_properties = _catalog_properties(tmp_path)
        op1 = _make_op(catalog_properties, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(op1, {"data/f1.csv": b"id,name\n1,alice\n"})

        op2 = _make_op(catalog_properties, metadata={"_DS": "2026-07-02"})
        _run_with_mock_filesystem(op2, {"data/f2.csv": b"id,name\n2,bob\n"})

        op3 = _make_op(catalog_properties, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(op3, {"data/f1.csv": b"id,name\n1,alice2\n"})

        catalog = load_catalog("test_catalog", **catalog_properties)
        df = catalog.load_table("ns.tbl").scan().to_pandas().sort_values("id")
        assert len(df) == 2
        assert set(df["name"]) == {"alice2", "bob"}

    def test_schema_evolution_adds_new_column(self, tmp_path):
        catalog_properties = _catalog_properties(tmp_path)
        op1 = _make_op(catalog_properties, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(op1, {"data/f1.csv": b"id,name\n1,alice\n"})

        op2 = _make_op(catalog_properties, metadata={"_DS": "2026-07-02"})
        _run_with_mock_filesystem(
            op2, {"data/f2.csv": b"id,name,extra_col\n2,bob,hello\n"}
        )

        catalog = load_catalog("test_catalog", **catalog_properties)
        df = catalog.load_table("ns.tbl").scan().to_pandas().sort_values("id")
        assert "extra_col" in df.columns
        assert df.iloc[0]["extra_col"] is None
        assert df.iloc[1]["extra_col"] == "hello"

    def test_non_idempotent_overwrite_only_affects_first_batch(self, tmp_path):
        catalog_properties = _catalog_properties(tmp_path)
        csv = "id,name\n" + "\n".join(f"{i},name{i}" for i in range(10)) + "\n"
        op = _make_op(
            catalog_properties,
            metadata={},
            idempotent=False,
            write_mode="overwrite",
            batch_size=3,
        )
        _run_with_mock_filesystem(
            op,
            {
                "data/f1.csv": csv.encode(),
                "data/f2.csv": b"id,name\n99,last\n",
            },
        )

        catalog = load_catalog("test_catalog", **catalog_properties)
        df = catalog.load_table("ns.tbl").scan().to_pandas()
        assert len(df) == 11

    def test_schema_drift_raises_clear_error(self, tmp_path):
        catalog_properties = _catalog_properties(tmp_path)
        op = _make_op(catalog_properties, metadata={}, batch_size=2)
        with pytest.raises(ValueError, match="does not match the schema"):
            _run_with_mock_filesystem(
                op, {"data/file1.csv": b"a,b\n1,x\n2,y\nnotanumber,z\n4,w\n"}
            )
