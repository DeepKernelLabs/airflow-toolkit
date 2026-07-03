import logging
from contextlib import contextmanager
from io import BytesIO
from typing import Generator

from airflow.providers.sftp.hooks.sftp import SFTPHook

from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol

logger = logging.getLogger(__name__)


class SFTPFilesystem(FilesystemProtocol):
    def __init__(self, hook: SFTPHook):
        self.hook = hook

    @contextmanager
    def _conn(self) -> Generator:
        """Yield an SFTP client via get_managed_conn() (reference-counted).

        providers-sftp >=5.x decorates hook methods with @handle_connection_management,
        which closes the underlying transport after each call. get_managed_conn() keeps
        the connection open for the duration of the context manager.
        """
        with self.hook.get_managed_conn() as conn:
            yield conn

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
        logger.info(f'Deleting file "{path}"')
        self.hook.delete_file(path)

    def create_prefix(self, prefix: str):
        self.hook.create_directory(prefix)

    def delete_prefix(self, prefix: str):
        files = self.hook.list_directory(prefix)
        logger.info(f'Deleting {len(files)} file(s) under directory "{prefix}"')
        for file in files:
            self.hook.delete_file(prefix.rstrip("/") + "/" + file)
        self.hook.delete_directory(prefix)

    def check_file(self, path: str) -> bool:
        return self.hook.isfile(path)

    def check_prefix(self, prefix: str) -> bool:
        return self.hook.isdir(prefix)

    def list_files(self, prefix: str) -> list[str]:
        files, _, _ = self.hook.get_tree_map(prefix)
        return files
