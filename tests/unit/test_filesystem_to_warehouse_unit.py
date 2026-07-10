"""Unit tests for FilesystemToWarehouseOperator.

There is no local/in-process equivalent of a Databricks SQL Warehouse or
Snowflake account (unlike Delta/Iceberg, which can run against a plain local
table) — the warehouse hook itself is mocked, and assertions check the exact
SQL text + call order passed to `hook.run()`.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from airflow_toolkit.providers.warehouse.dialects.databricks import DatabricksDialect
from airflow_toolkit.providers.warehouse.operators.filesystem_to_warehouse import (
    FilesystemToWarehouseOperator,
)
from airflow_toolkit.testing import MockFilesystem

_MODULE = "airflow_toolkit.providers.warehouse.operators.filesystem_to_warehouse"


def _make_op(**kwargs: Any) -> FilesystemToWarehouseOperator:
    defaults: dict[str, Any] = dict(
        task_id="test_op",
        dialect="databricks",
        warehouse_conn_id="wh_conn",
        database="main",
        schema="raw",
        table="orders",
        source_location="gs://bucket/raw/orders/*.json",
        preflight_check=False,
    )
    defaults.update(kwargs)
    return FilesystemToWarehouseOperator(**defaults)


def _mock_hook() -> MagicMock:
    hook = MagicMock()
    hook.run.return_value = []
    hook.last_description = None
    return hook


def _run(op: FilesystemToWarehouseOperator, hook: MagicMock | None = None) -> MagicMock:
    hook = hook or _mock_hook()
    with patch.object(
        FilesystemToWarehouseOperator, "_get_warehouse_hook", return_value=hook
    ):
        op.execute({})
    return hook


def _run_with_filesystem(
    op: FilesystemToWarehouseOperator, files: dict[str, bytes]
) -> MagicMock:
    hook = _mock_hook()
    with (
        patch.object(
            FilesystemToWarehouseOperator, "_get_warehouse_hook", return_value=hook
        ),
        patch(f"{_MODULE}.FilesystemFactory") as mock_factory,
        patch(f"{_MODULE}.BaseHook"),
    ):
        mock_factory.get_data_lake_filesystem.return_value = MockFilesystem(files)
        op.execute({})
    return hook


def _sql_calls(hook: MagicMock) -> list[str]:
    return [call.args[0] for call in hook.run.call_args_list]


# -----------------------------------------------------------------------------
# Call order: create_table_ddl -> delete -> copy into
# -----------------------------------------------------------------------------


class TestCallOrder:
    def test_default_run_is_delete_then_copy(self):
        op = _make_op()  # idempotent=True by default, default metadata={"_DS": ...}
        hook = _run(op)
        calls = _sql_calls(hook)
        assert len(calls) == 2
        assert calls[0].startswith("DELETE FROM main.raw.orders")
        assert calls[1].startswith("COPY INTO main.raw.orders")

    def test_idempotent_false_skips_delete(self):
        op = _make_op(idempotent=False)
        hook = _run(op)
        calls = _sql_calls(hook)
        assert len(calls) == 1
        assert calls[0].startswith("COPY INTO")

    def test_empty_metadata_skips_delete_even_if_idempotent(self):
        op = _make_op(idempotent=True, metadata={})
        hook = _run(op)
        calls = _sql_calls(hook)
        assert len(calls) == 1
        assert calls[0].startswith("COPY INTO")

    def test_create_table_ddl_runs_first(self):
        op = _make_op(
            create_table_ddl="CREATE TABLE IF NOT EXISTS main.raw.orders (...)"
        )
        hook = _run(op)
        calls = _sql_calls(hook)
        assert len(calls) == 3
        assert calls[0].startswith("CREATE TABLE")
        assert calls[1].startswith("DELETE FROM")
        assert calls[2].startswith("COPY INTO")

    def test_no_create_table_ddl_by_default(self):
        op = _make_op()
        hook = _run(op)
        calls = _sql_calls(hook)
        assert not any(c.startswith("CREATE TABLE") for c in calls)


# -----------------------------------------------------------------------------
# select_fragment resolution
# -----------------------------------------------------------------------------


class TestSelectFragmentResolution:
    def test_databricks_json_default_used_when_not_supplied(self):
        op = _make_op(source_format="json")
        hook = _run(op)
        copy_sql = _sql_calls(hook)[-1]
        assert "to_json(struct(*)) AS _raw" in copy_sql

    def test_explicit_select_fragment_overrides_default(self):
        op = _make_op(select_fragment="GET_JSON_OBJECT(value, '$.id') AS id")
        hook = _run(op)
        copy_sql = _sql_calls(hook)[-1]
        assert "GET_JSON_OBJECT(value, '$.id') AS id" in copy_sql

    def test_raw_column_name_customization(self):
        op = _make_op(source_format="json", raw_column_name="payload")
        hook = _run(op)
        copy_sql = _sql_calls(hook)[-1]
        assert "AS payload" in copy_sql

    def test_snowflake_typed_without_select_fragment_raises_before_any_hook_call(self):
        op = _make_op(
            dialect="snowflake",
            source_format="csv",
            select_fragment=None,
            database="analytics",
            source_location="@stage/raw/orders",
        )
        # No hook patch at all: this must fail before _get_warehouse_hook()
        # is ever called.
        with pytest.raises(ValueError, match="select_fragment is required"):
            op.execute({})

    def test_snowflake_typed_with_explicit_select_fragment_succeeds(self):
        op = _make_op(
            dialect="snowflake",
            source_format="csv",
            select_fragment="$1, $2, $3",
            database="analytics",
            source_location="@stage/raw/orders",
        )
        hook = _run(op)
        copy_sql = _sql_calls(hook)[-1]
        assert "$1, $2, $3" in copy_sql


# -----------------------------------------------------------------------------
# Metadata casing
# -----------------------------------------------------------------------------


class TestMetadataCasing:
    def test_uppercased_by_default(self):
        op = _make_op(metadata={"loaded_at": "'2026-07-10'"})
        hook = _run(op)
        copy_sql = _sql_calls(hook)[-1]
        assert "AS LOADED_AT" in copy_sql

    def test_lowercase_when_disabled(self):
        op = _make_op(
            metadata={"LOADED_AT": "'2026-07-10'"}, metadata_columns_in_uppercase=False
        )
        hook = _run(op)
        copy_sql = _sql_calls(hook)[-1]
        assert "AS loaded_at" in copy_sql


# -----------------------------------------------------------------------------
# Preflight check (optional, filesystem-side only)
# -----------------------------------------------------------------------------


class TestPreflightCheck:
    def test_raises_and_skips_copy_when_no_files(self):
        op = _make_op(
            preflight_check=True, filesystem_conn_id="fs_conn", filesystem_path="data/"
        )
        with pytest.raises(ValueError, match="No files found"):
            _run_with_filesystem(op, files={})

    def test_proceeds_when_files_exist(self):
        op = _make_op(
            preflight_check=True, filesystem_conn_id="fs_conn", filesystem_path="data/"
        )
        hook = _run_with_filesystem(op, files={"data/x.json": b"{}"})
        assert hook.run.called

    def test_disabled_ignores_empty_filesystem(self):
        op = _make_op(
            preflight_check=False, filesystem_conn_id="fs_conn", filesystem_path="data/"
        )
        hook = _run_with_filesystem(op, files={})
        assert hook.run.called

    def test_skipped_entirely_without_filesystem_conn_id(self):
        op = _make_op(preflight_check=True, filesystem_conn_id=None)
        hook = _run(op)
        assert hook.run.called


# -----------------------------------------------------------------------------
# Dialect selection
# -----------------------------------------------------------------------------


class TestDialectSelection:
    def test_string_resolves_to_registered_dialect(self):
        op = _make_op(dialect="databricks")
        assert isinstance(op.dialect, DatabricksDialect)

    def test_accepts_prebuilt_dialect_instance(self):
        op = _make_op(dialect=DatabricksDialect())
        assert isinstance(op.dialect, DatabricksDialect)
        hook = _run(op)
        assert hook.run.called


# -----------------------------------------------------------------------------
# COPY INTO result parsing / logging
# -----------------------------------------------------------------------------


class TestResultParsing:
    def test_databricks_result_columns_logged(self, caplog):
        hook = _mock_hook()
        hook.run.return_value = [(10, 0)]
        hook.last_description = [("num_affected_rows",), ("num_skipped_rows",)]
        op = _make_op(idempotent=False)
        with caplog.at_level("INFO"):
            _run(op, hook=hook)
        assert "rows_loaded=10" in caplog.text
