"""Unit tests for FilesystemFactory — verifies correct routing by conn_type."""

from unittest.mock import MagicMock, patch

import pytest

from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory
from airflow_toolkit.filesystems.impl.blob_storage_filesystem import (
    BlobStorageFilesystem,
)
from airflow_toolkit.filesystems.impl.google_cloud_storage_filesystem import (
    GCSFilesystem,
)
from airflow_toolkit.filesystems.impl.local_filesystem import LocalFilesystem
from airflow_toolkit.filesystems.impl.s3_filesystem import S3Filesystem
from airflow_toolkit.filesystems.impl.sftp_filesystem import SFTPFilesystem
from airflow_toolkit.filesystems.impl.azure_file_share_filesystem import (
    AzureFileShareFilesystem,
)
from airflow_toolkit.filesystems.impl.azure_databricks_volume_filesystem import (
    AzureDatabricksVolumeFilesystem,
)


def _conn(conn_type: str, conn_id: str = "test_conn") -> MagicMock:
    conn = MagicMock()
    conn.conn_type = conn_type
    conn.conn_id = conn_id
    return conn


# ---------------------------------------------------------------------------
# Happy-path: each conn_type maps to the right filesystem class
# ---------------------------------------------------------------------------


@patch("airflow.providers.amazon.aws.hooks.s3.S3Hook")
def test_aws_returns_s3_filesystem(mock_hook):
    fs = FilesystemFactory.get_data_lake_filesystem(_conn("aws"))
    assert isinstance(fs, S3Filesystem)
    mock_hook.assert_called_once_with(aws_conn_id="test_conn")


@patch("airflow.providers.microsoft.azure.hooks.wasb.WasbHook")
def test_wasb_returns_blob_storage_filesystem(mock_hook):
    fs = FilesystemFactory.get_data_lake_filesystem(_conn("wasb"))
    assert isinstance(fs, BlobStorageFilesystem)
    mock_hook.assert_called_once_with(wasb_conn_id="test_conn")


@patch("airflow.providers.google.cloud.hooks.gcs.GCSHook")
def test_gcp_returns_gcs_filesystem(mock_hook):
    fs = FilesystemFactory.get_data_lake_filesystem(_conn("google_cloud_platform"))
    assert isinstance(fs, GCSFilesystem)
    mock_hook.assert_called_once_with(gcp_conn_id="test_conn")


@patch("airflow.providers.sftp.hooks.sftp.SFTPHook")
def test_sftp_returns_sftp_filesystem(mock_hook):
    fs = FilesystemFactory.get_data_lake_filesystem(_conn("sftp"))
    assert isinstance(fs, SFTPFilesystem)
    mock_hook.assert_called_once_with(ssh_conn_id="test_conn")


@patch("airflow_toolkit.filesystems.filesystem_factory.FSHook")
def test_fs_returns_local_filesystem(mock_hook):
    fs = FilesystemFactory.get_data_lake_filesystem(_conn("fs"))
    assert isinstance(fs, LocalFilesystem)
    mock_hook.assert_called_once_with(fs_conn_id="test_conn")


@patch(
    "airflow_toolkit.providers.azure.hooks.azure_file_share.AzureFileShareServicePrincipalHook"
)
def test_azure_file_share_sp_returns_azure_file_share_filesystem(mock_hook):
    fs = FilesystemFactory.get_data_lake_filesystem(_conn("azure_file_share_sp"))
    assert isinstance(fs, AzureFileShareFilesystem)
    mock_hook.assert_called_once_with(conn_id="test_conn")


@patch(
    "airflow_toolkit.providers.azure.hooks.azure_databricks.AzureDatabricksVolumeHook"
)
def test_azure_databricks_volume_returns_databricks_filesystem(mock_hook):
    fs = FilesystemFactory.get_data_lake_filesystem(_conn("azure_databricks_volume"))
    assert isinstance(fs, AzureDatabricksVolumeFilesystem)
    mock_hook.assert_called_once_with(azure_databricks_volume_conn_id="test_conn")


# ---------------------------------------------------------------------------
# Error path: unsupported type raises NotImplementedError
# ---------------------------------------------------------------------------


def test_unsupported_conn_type_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="not supported"):
        FilesystemFactory.get_data_lake_filesystem(_conn("postgres"))


def test_error_message_contains_conn_type():
    with pytest.raises(NotImplementedError, match="ftp"):
        FilesystemFactory.get_data_lake_filesystem(_conn("ftp"))
