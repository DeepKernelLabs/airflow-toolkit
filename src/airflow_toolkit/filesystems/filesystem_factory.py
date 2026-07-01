from airflow_toolkit._compact.airflow_shim import Connection, FSHook
from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol


class FilesystemFactory:
    @staticmethod
    def get_data_lake_filesystem(connection: Connection) -> FilesystemProtocol:
        if connection.conn_type == "wasb":
            from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

            from airflow_toolkit.filesystems.impl.blob_storage_filesystem import (
                BlobStorageFilesystem,
            )

            hook = WasbHook(wasb_conn_id=connection.conn_id)
            return BlobStorageFilesystem(hook)
        elif connection.conn_type == "aws":
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            from airflow_toolkit.filesystems.impl.s3_filesystem import S3Filesystem

            hook = S3Hook(aws_conn_id=connection.conn_id)
            return S3Filesystem(hook)
        elif connection.conn_type == "google_cloud_platform":
            from airflow.providers.google.cloud.hooks.gcs import GCSHook

            from airflow_toolkit.filesystems.impl.google_cloud_storage_filesystem import (
                GCSFilesystem,
            )

            hook = GCSHook(gcp_conn_id=connection.conn_id)
            return GCSFilesystem(hook)
        elif connection.conn_type == "sftp":
            from airflow.providers.sftp.hooks.sftp import SFTPHook

            from airflow_toolkit.filesystems.impl.sftp_filesystem import SFTPFilesystem

            hook = SFTPHook(ssh_conn_id=connection.conn_id)
            return SFTPFilesystem(hook)
        elif connection.conn_type == "fs":
            from airflow_toolkit.filesystems.impl.local_filesystem import (
                LocalFilesystem,
            )

            hook = FSHook(fs_conn_id=connection.conn_id)
            return LocalFilesystem(hook)
        elif connection.conn_type == "azure_file_share_sp":
            from airflow_toolkit.filesystems.impl.azure_file_share_filesystem import (
                AzureFileShareFilesystem,
            )
            from airflow_toolkit.providers.azure.hooks.azure_file_share import (
                AzureFileShareServicePrincipalHook,
            )

            hook = AzureFileShareServicePrincipalHook(conn_id=connection.conn_id)
            return AzureFileShareFilesystem(hook)
        elif connection.conn_type == "azure_databricks_volume":
            from airflow_toolkit.filesystems.impl.azure_databricks_volume_filesystem import (
                AzureDatabricksVolumeFilesystem,
            )
            from airflow_toolkit.providers.azure.hooks.azure_databricks import (
                AzureDatabricksVolumeHook,
            )

            hook = AzureDatabricksVolumeHook(
                azure_databricks_volume_conn_id=connection.conn_id
            )
            return AzureDatabricksVolumeFilesystem(hook)
        elif connection.conn_type == "ftp":
            from airflow.providers.ftp.hooks.ftp import FTPHook

            from airflow_toolkit.filesystems.impl.ftp_filesystem import FTPFilesystem

            hook = FTPHook(ftp_conn_id=connection.conn_id)
            return FTPFilesystem(hook)
        elif connection.conn_type == "sharepoint":
            from airflow_toolkit.filesystems.impl.sharepoint_filesystem import (
                SharePointFilesystem,
            )
            from airflow_toolkit.providers.microsoft.hooks.sharepoint import (
                SharePointHook,
            )

            hook = SharePointHook(conn_id=connection.conn_id)
            return SharePointFilesystem(hook)
        elif connection.conn_type == "google_drive":
            from airflow_toolkit.filesystems.impl.google_drive_filesystem import (
                GoogleDriveFilesystem,
            )
            from airflow_toolkit.providers.google.hooks.drive import GoogleDriveHook

            hook = GoogleDriveHook(conn_id=connection.conn_id)
            return GoogleDriveFilesystem(hook)
        else:
            raise NotImplementedError(
                f"Data Lake type {connection.conn_type} is not supported"
            )
