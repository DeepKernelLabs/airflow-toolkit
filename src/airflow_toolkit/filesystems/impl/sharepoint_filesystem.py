from __future__ import annotations

from io import BytesIO

from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol
from airflow_toolkit.providers.microsoft.hooks.sharepoint import SharePointHook


class SharePointFilesystem(FilesystemProtocol):
    def __init__(self, hook: SharePointHook):
        self.hook = hook

    def _server_relative_url(self, path: str) -> str:
        return f"{self.hook.site_path}/{path.lstrip('/')}"

    def read(self, path: str) -> bytes:
        ctx = self.hook.get_conn()
        url = self._server_relative_url(path)
        result = ctx.web.get_file_by_server_relative_url(url).read()
        ctx.execute_query()
        return result.value

    def write(self, data: str | bytes | BytesIO, path: str):
        if isinstance(data, str):
            data = data.encode()
        elif isinstance(data, BytesIO):
            data = data.getvalue()
        ctx = self.hook.get_conn()
        parts = path.lstrip("/").split("/")
        filename = parts[-1]
        folder_path = "/".join(parts[:-1])
        folder_url = (
            self._server_relative_url(folder_path)
            if folder_path
            else self.hook.site_path
        )
        ctx.web.ensure_folder_path(folder_url)
        ctx.execute_query()
        folder = ctx.web.get_folder_by_server_relative_url(folder_url)
        folder.upload_file(filename, data)
        ctx.execute_query()

    def delete_file(self, path: str):
        ctx = self.hook.get_conn()
        url = self._server_relative_url(path)
        ctx.web.get_file_by_server_relative_url(url).delete_object()
        ctx.execute_query()

    def create_prefix(self, prefix: str):
        ctx = self.hook.get_conn()
        url = self._server_relative_url(prefix)
        ctx.web.ensure_folder_path(url)
        ctx.execute_query()

    def delete_prefix(self, prefix: str):
        ctx = self.hook.get_conn()
        url = self._server_relative_url(prefix)
        ctx.web.get_folder_by_server_relative_url(url).delete_object()
        ctx.execute_query()

    def check_file(self, path: str) -> bool:
        ctx = self.hook.get_conn()
        url = self._server_relative_url(path)
        try:
            f = ctx.web.get_file_by_server_relative_url(url)
            ctx.load(f)
            ctx.execute_query()
            return True
        except Exception:
            return False

    def check_prefix(self, prefix: str) -> bool:
        ctx = self.hook.get_conn()
        url = self._server_relative_url(prefix)
        try:
            folder = ctx.web.get_folder_by_server_relative_url(url)
            ctx.load(folder)
            ctx.execute_query()
            return True
        except Exception:
            return False

    def list_files(self, prefix: str) -> list[str]:
        ctx = self.hook.get_conn()
        url = self._server_relative_url(prefix)
        folder = ctx.web.get_folder_by_server_relative_url(url)
        files = folder.get_files(True)
        ctx.load(files)
        ctx.execute_query()
        site_path = self.hook.site_path
        return [f.serverRelativeUrl.removeprefix(site_path).lstrip("/") for f in files]
