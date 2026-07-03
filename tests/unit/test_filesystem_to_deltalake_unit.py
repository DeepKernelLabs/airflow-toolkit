"""Unit tests for FilesystemToDeltalakeOperator and deltalake_storage_options()."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from deltalake import DeltaTable

from airflow_toolkit.providers.deltalake.operators.filesystem_to_deltalake import (
    FilesystemToDeltalakeOperator,
)
from airflow_toolkit.providers.deltalake.storage_options import (
    deltalake_storage_options,
)
from airflow_toolkit.testing import MockFilesystem

_MODULE = "airflow_toolkit.providers.deltalake.operators.filesystem_to_deltalake"


def _conn(conn_type: str, **kwargs: Any) -> MagicMock:
    conn = MagicMock()
    conn.conn_type = conn_type
    conn.conn_id = kwargs.pop("conn_id", "test_conn")
    conn.login = kwargs.pop("login", None)
    conn.password = kwargs.pop("password", None)
    conn.extra_dejson = kwargs.pop("extra_dejson", {})
    return conn


def _make_op(**kwargs: Any) -> FilesystemToDeltalakeOperator:
    defaults: dict[str, Any] = dict(
        task_id="test_op",
        filesystem_conn_id="fs_conn",
        filesystem_path="data/",
        table_path="/tmp/doesnotmatter",
    )
    defaults.update(kwargs)
    return FilesystemToDeltalakeOperator(**defaults)


def _run_with_mock_filesystem(
    op: FilesystemToDeltalakeOperator, files: dict[str, bytes]
):
    mock_fs = MockFilesystem(files)
    with (
        patch(f"{_MODULE}.FilesystemFactory") as mock_factory,
        patch(f"{_MODULE}.BaseHook"),
    ):
        mock_factory.get_data_lake_filesystem.return_value = mock_fs
        op.execute({})


# -----------------------------------------------------------------------------
# deltalake_storage_options() — AWS
# -----------------------------------------------------------------------------


class TestDeltalakeStorageOptionsAws:
    @patch("airflow.providers.amazon.aws.hooks.s3.S3Hook")
    def test_basic_credentials(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.get_credentials.return_value = MagicMock(
            access_key="AKIA...", secret_key="secret", token=None
        )
        mock_hook.conn_region_name = "eu-west-1"
        mock_hook.conn_config.get_service_endpoint_url.return_value = None

        options = deltalake_storage_options(_conn("aws"))

        assert options["AWS_ACCESS_KEY_ID"] == "AKIA..."
        assert options["AWS_SECRET_ACCESS_KEY"] == "secret"
        assert options["AWS_REGION"] == "eu-west-1"
        assert options["AWS_S3_ALLOW_UNSAFE_RENAME"] == "true"
        assert "AWS_SESSION_TOKEN" not in options
        assert "AWS_ENDPOINT_URL" not in options

    @patch("airflow.providers.amazon.aws.hooks.s3.S3Hook")
    def test_session_token_included_when_present(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.get_credentials.return_value = MagicMock(
            access_key="AKIA...", secret_key="secret", token="session-tok"
        )
        mock_hook.conn_region_name = None
        mock_hook.conn_config.get_service_endpoint_url.return_value = None

        options = deltalake_storage_options(_conn("aws"))

        assert options["AWS_SESSION_TOKEN"] == "session-tok"

    @patch("airflow.providers.amazon.aws.hooks.s3.S3Hook")
    def test_custom_http_endpoint_sets_minio_flags(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.get_credentials.return_value = MagicMock(
            access_key="AKIA...", secret_key="secret", token=None
        )
        mock_hook.conn_region_name = None
        mock_hook.conn_config.get_service_endpoint_url.return_value = (
            "http://localhost:9090"
        )

        options = deltalake_storage_options(_conn("aws"))

        assert options["AWS_ENDPOINT_URL"] == "http://localhost:9090"
        assert options["AWS_ALLOW_HTTP"] == "true"
        assert options["AWS_S3_ADDRESSING_STYLE"] == "path"

    @patch("airflow.providers.amazon.aws.hooks.s3.S3Hook")
    def test_https_endpoint_does_not_allow_http(self, mock_hook_cls):
        mock_hook = mock_hook_cls.return_value
        mock_hook.get_credentials.return_value = MagicMock(
            access_key="AKIA...", secret_key="secret", token=None
        )
        mock_hook.conn_region_name = None
        mock_hook.conn_config.get_service_endpoint_url.return_value = (
            "https://s3.custom.example.com"
        )

        options = deltalake_storage_options(_conn("aws"))

        assert options["AWS_ALLOW_HTTP"] == "false"


# -----------------------------------------------------------------------------
# deltalake_storage_options() — Azure
# -----------------------------------------------------------------------------


class TestDeltalakeStorageOptionsAzure:
    def test_uses_connection_string_when_present(self):
        conn = _conn(
            "wasb",
            extra_dejson={
                "connection_string": (
                    "DefaultEndpointsProtocol=https;AccountName=myacct;"
                    "AccountKey=mykey;EndpointSuffix=core.windows.net"
                )
            },
        )
        options = deltalake_storage_options(conn)
        assert options == {
            "azure_storage_account_name": "myacct",
            "azure_storage_account_key": "mykey",
        }

    def test_falls_back_to_login_password(self):
        conn = _conn("wasb", login="myacct", password="mykey")
        options = deltalake_storage_options(conn)
        assert options == {
            "azure_storage_account_name": "myacct",
            "azure_storage_account_key": "mykey",
        }

    def test_raises_when_nothing_available(self):
        conn = _conn("wasb")
        with pytest.raises(ValueError, match="neither"):
            deltalake_storage_options(conn)


# -----------------------------------------------------------------------------
# deltalake_storage_options() — GCP
# -----------------------------------------------------------------------------


class TestDeltalakeStorageOptionsGcp:
    def test_keyfile_dict_as_dict(self):
        conn = _conn(
            "google_cloud_platform",
            extra_dejson={
                "keyfile_dict": {"type": "service_account", "project_id": "p"}
            },
        )
        options = deltalake_storage_options(conn)
        assert '"project_id": "p"' in options["google_service_account_key"]

    def test_keyfile_dict_as_json_string(self):
        conn = _conn(
            "google_cloud_platform",
            extra_dejson={"keyfile_dict": '{"type": "service_account"}'},
        )
        options = deltalake_storage_options(conn)
        assert options["google_service_account_key"] == '{"type": "service_account"}'

    def test_key_path_reads_file(self, tmp_path):
        keyfile = tmp_path / "key.json"
        keyfile.write_text('{"type": "service_account"}')
        conn = _conn("google_cloud_platform", extra_dejson={"key_path": str(keyfile)})
        options = deltalake_storage_options(conn)
        assert options["google_service_account_key"] == '{"type": "service_account"}'

    def test_raises_when_nothing_available(self):
        conn = _conn("google_cloud_platform")
        with pytest.raises(ValueError, match="neither"):
            deltalake_storage_options(conn)


class TestDeltalakeStorageOptionsUnsupported:
    def test_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            deltalake_storage_options(_conn("sftp"))


# -----------------------------------------------------------------------------
# FilesystemToDeltalakeOperator.execute() — real local Delta tables, no mocks
# for delta-rs itself (it works fine against a plain filesystem path).
# -----------------------------------------------------------------------------


class TestExecuteWritesToDeltaTable:
    def test_creates_table_with_metadata_columns(self, tmp_path):
        table_path = str(tmp_path / "delta_table")
        op = _make_op(table_path=table_path, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(op, {"data/file1.csv": b"id,name\n1,alice\n2,bob\n"})

        df = DeltaTable(table_path).to_pandas()
        assert set(df.columns) == {"id", "name", "_DS", "_LOADED_FROM"}
        assert len(df) == 2
        assert (df["_DS"] == "2026-07-01").all()

    def test_multiple_files_same_run_do_not_clobber_each_other(self, tmp_path):
        table_path = str(tmp_path / "delta_table")
        op = _make_op(table_path=table_path, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(
            op,
            {
                "data/file1.csv": b"id,name\n1,alice\n2,bob\n",
                "data/file2.csv": b"id,name\n3,carol\n",
            },
        )

        df = DeltaTable(table_path).to_pandas()
        assert len(df) == 3

    def test_idempotent_rerun_replaces_instead_of_duplicating(self, tmp_path):
        table_path = str(tmp_path / "delta_table")
        op1 = _make_op(table_path=table_path, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(op1, {"data/file1.csv": b"id,name\n1,alice\n2,bob\n"})

        op2 = _make_op(table_path=table_path, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(op2, {"data/file1.csv": b"id,name\n3,carol\n"})

        df = DeltaTable(table_path).to_pandas()
        assert len(df) == 1
        assert df.iloc[0]["name"] == "carol"

    def test_idempotent_rerun_preserves_other_run_data(self, tmp_path):
        table_path = str(tmp_path / "delta_table")
        op1 = _make_op(table_path=table_path, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(op1, {"data/f1.csv": b"id,name\n1,alice\n"})

        op2 = _make_op(table_path=table_path, metadata={"_DS": "2026-07-02"})
        _run_with_mock_filesystem(op2, {"data/f2.csv": b"id,name\n2,bob\n"})

        # Rerun for _DS=2026-07-01 only — 2026-07-02 rows must survive untouched.
        op3 = _make_op(table_path=table_path, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(op3, {"data/f1.csv": b"id,name\n1,alice2\n"})

        df = DeltaTable(table_path).to_pandas().sort_values("id")
        assert len(df) == 2
        assert set(df["name"]) == {"alice2", "bob"}

    def test_schema_evolution_adds_new_column(self, tmp_path):
        table_path = str(tmp_path / "delta_table")
        op1 = _make_op(table_path=table_path, metadata={"_DS": "2026-07-01"})
        _run_with_mock_filesystem(op1, {"data/f1.csv": b"id,name\n1,alice\n"})

        op2 = _make_op(table_path=table_path, metadata={"_DS": "2026-07-02"})
        _run_with_mock_filesystem(
            op2, {"data/f2.csv": b"id,name,extra_col\n2,bob,hello\n"}
        )

        df = DeltaTable(table_path).to_pandas().sort_values("id")
        assert "extra_col" in df.columns
        assert df.iloc[0]["extra_col"] is None
        assert df.iloc[1]["extra_col"] == "hello"

    def test_non_idempotent_second_file_appends_even_if_write_mode_is_overwrite(
        self, tmp_path
    ):
        table_path = str(tmp_path / "delta_table")
        op = _make_op(
            table_path=table_path,
            metadata={},
            idempotent=False,
            write_mode="overwrite",
        )
        _run_with_mock_filesystem(
            op,
            {
                "data/file1.csv": b"id,name\n1,alice\n",
                "data/file2.csv": b"id,name\n2,bob\n",
            },
        )

        df = DeltaTable(table_path).to_pandas()
        assert len(df) == 2

    def test_schema_drift_raises_clear_error(self, tmp_path):
        table_path = str(tmp_path / "delta_table")
        op = _make_op(table_path=table_path, metadata={}, batch_size=2)
        with pytest.raises(ValueError, match="does not match the schema"):
            _run_with_mock_filesystem(
                op,
                {"data/file1.csv": b"a,b\n1,x\n2,y\nnotanumber,z\n4,w\n"},
            )
