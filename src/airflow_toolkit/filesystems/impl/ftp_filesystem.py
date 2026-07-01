from __future__ import annotations

import ftplib
from io import BytesIO
from typing import TYPE_CHECKING

from airflow_toolkit.filesystems.filesystem_protocol import FilesystemProtocol

if TYPE_CHECKING:
    from airflow.providers.ftp.hooks.ftp import FTPHook


class FTPFilesystem(FilesystemProtocol):
    def __init__(self, hook: FTPHook):
        self.hook = hook

    @staticmethod
    def _rel(path: str) -> str:
        """Strip leading slash — FTP paths are relative to the user's home (chrooted or not)."""
        return path.lstrip("/")

    def read(self, path: str) -> bytes:
        out = BytesIO()
        conn = self.hook.get_conn()
        conn.retrbinary(f"RETR {self._rel(path)}", out.write)
        out.seek(0)
        return out.getvalue()

    def write(self, data: str | bytes | BytesIO, path: str):
        if isinstance(data, str):
            data = data.encode()
        elif isinstance(data, BytesIO):
            data = data.getvalue()
        rel_path = self._rel(path)
        parent = "/".join(rel_path.rstrip("/").split("/")[:-1])
        if parent:
            self.create_prefix(parent)
        conn = self.hook.get_conn()
        conn.storbinary(f"STOR {rel_path}", BytesIO(data))

    def delete_file(self, path: str):
        conn = self.hook.get_conn()
        conn.delete(self._rel(path))

    def create_prefix(self, prefix: str):
        conn = self.hook.get_conn()
        parts = [p for p in prefix.split("/") if p]
        current = ""
        for part in parts:
            current = f"{current}/{part}" if current else part
            try:
                conn.mkd(current)
            except ftplib.error_perm:
                pass

    def delete_prefix(self, prefix: str):
        conn = self.hook.get_conn()
        _delete_recursive(conn, self._rel(prefix))

    def check_file(self, path: str) -> bool:
        conn = self.hook.get_conn()
        try:
            conn.size(self._rel(path))
            return True
        except ftplib.error_perm:
            return False

    def check_prefix(self, prefix: str) -> bool:
        conn = self.hook.get_conn()
        try:
            original = conn.pwd()
            conn.cwd(self._rel(prefix))
            conn.cwd(original)
            return True
        except ftplib.error_perm:
            return False

    def list_files(self, prefix: str) -> list[str]:
        conn = self.hook.get_conn()
        results: list[str] = []
        _list_recursive(conn, self._rel(prefix), results)
        return results


def _is_file(conn: ftplib.FTP, path: str) -> bool:
    try:
        conn.size(path)
        return True
    except ftplib.error_perm:
        return False


def _delete_recursive(conn: ftplib.FTP, path: str) -> None:
    try:
        entries = conn.nlst(path)
    except ftplib.error_perm:
        return
    for entry in entries:
        if _is_file(conn, entry):
            conn.delete(entry)
        else:
            _delete_recursive(conn, entry)
    conn.rmd(path)


def _list_recursive(conn: ftplib.FTP, path: str, results: list[str]) -> None:
    try:
        entries = conn.nlst(path)
    except ftplib.error_perm:
        results.append(path)
        return
    # nlst returns [path] itself when path is a file, not a directory
    if entries == [path]:
        results.append(path)
        return
    for entry in entries:
        if _is_file(conn, entry):
            results.append(entry)
        else:
            _list_recursive(conn, entry, results)
