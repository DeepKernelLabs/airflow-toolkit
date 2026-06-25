import logging
from io import BytesIO

from databricks.sdk.errors.platform import NotFound

from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol
from airflow_toolkit.providers.azure.hooks.azure_databricks import (
    AzureDatabricksVolumeHook,
)

logger = logging.getLogger(__name__)


class AzureDatabricksVolumeFilesystem(FilesystemProtocol):
    def __init__(self, hook: AzureDatabricksVolumeHook):
        self.hook = hook

    def read(self, path: str) -> bytes:
        response = self.hook.get_conn().files.download(path)
        if response.contents is None:
            return b""
        return response.contents.read()

    def write(self, data: str | bytes | BytesIO, path: str):
        if isinstance(data, str):
            data = data.encode()
        elif isinstance(data, BytesIO):
            data = data.getvalue()
        self.hook.get_conn().files.upload(path, BytesIO(data))

    def delete_file(self, path: str):
        self.hook.get_conn().files.delete(path)

    def create_prefix(self, prefix: str):
        self.hook.get_conn().files.create_directory(prefix)

    def delete_prefix(self, prefix: str):
        conn = self.hook.get_conn()

        try:
            entries = list(conn.files.list_directory_contents(prefix))
        except NotFound:
            return

        for entry in entries:
            if entry.path is None:
                continue
            if entry.is_directory:
                self.delete_prefix(entry.path)
            else:
                conn.files.delete(entry.path)
        conn.files.delete_directory(prefix)

    def check_file(self, path: str) -> bool:
        try:
            self.hook.get_conn().files.get_metadata(path)
            return True
        except NotFound:
            return False

    def check_prefix(self, prefix: str) -> bool:
        try:
            _ = [
                f
                for f in self.hook.get_conn().files.list_directory_contents(
                    prefix, page_size=1
                )
            ]
            return True

        except NotFound:
            return False

    def list_files(self, prefix: str) -> list[str]:
        results: list[str] = []
        try:
            for entry in self.hook.get_conn().files.list_directory_contents(prefix):
                if entry.path is None:
                    continue
                if entry.is_directory:
                    results.extend(self.list_files(entry.path))
                else:
                    results.append(entry.path)
        except NotFound:
            return []
        return results
