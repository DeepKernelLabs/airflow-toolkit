from contextlib import contextmanager
from io import BytesIO
from typing import Generator

from airflow.providers.sftp.hooks.sftp import SFTPHook

from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol


class SFTPFilesystem(FilesystemProtocol):
    def __init__(self, hook: SFTPHook):
        self.hook = hook

    @contextmanager
    def _conn(self) -> Generator:
        """Yield an SFTP client, using managed connections when available.

        Provider >=5.x (AF3) decorates hook methods with @handle_connection_management,
        which opens and closes the underlying SFTP transport per call. After each call
        self.conn is left pointing to a closed client, so get_conn() returns a stale
        object. Using get_managed_conn() (reference-counted) avoids this by always
        opening a fresh connection. Provider <5.x (AF2) lacks get_managed_conn, so
        we fall back to get_conn().
        """
        if hasattr(self.hook, "get_managed_conn"):
            with self.hook.get_managed_conn() as conn:
                yield conn
        else:
            yield self.hook.get_conn()

    def read(self, path: str) -> bytes:
        out = BytesIO()
        with self._conn() as conn:
            conn.getfo(remotepath=path, fl=out)
        out.seek(0)
        return out.getvalue()

    def write(self, data: str | bytes | BytesIO, path: str):
        if isinstance(data, str):
            data = data.encode()
        if isinstance(data, bytes):
            data = BytesIO(data)
        with self._conn() as conn:
            conn.putfo(fl=data, remotepath=path, confirm=True)

    def delete_file(self, path: str):
        self.hook.delete_file(path)

    def create_prefix(self, prefix: str):
        self.hook.create_directory(prefix)

    def delete_prefix(self, prefix: str):
        for file in self.hook.list_directory(prefix):
            self.hook.delete_file(prefix.rstrip("/") + "/" + file)
        self.hook.delete_directory(prefix)

    def check_file(self, path: str) -> bool:
        return self.hook.isfile(path)

    def check_prefix(self, prefix: str) -> bool:
        return self.hook.isdir(prefix)

    def list_files(self, prefix: str) -> list[str]:
        return self.hook.list_directory(prefix)
