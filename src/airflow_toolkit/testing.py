"""Testing utilities for airflow-toolkit.

Import from here in your unit tests — no Docker, no cloud credentials needed.

    from airflow_toolkit.testing import MockFilesystem

    fs = MockFilesystem({"data/2024-01-01.csv": b"id,name\\n1,Alice"})
    fs.write(b"id,name\\n2,Bob", "data/2024-01-02.csv")
    assert fs.check_file("data/2024-01-01.csv")
"""

from __future__ import annotations

from io import BytesIO


class MockFilesystem:
    """In-memory implementation of FilesystemProtocol for unit testing.

    Stores all files in a plain dict — no network, no Docker, no credentials.
    Inspect ``fs.files`` directly in assertions.

    Args:
        files: Optional seed data mapping path → bytes.
    """

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self.files: dict[str, bytes] = dict(files or {})

    def read(self, path: str) -> bytes:
        if path not in self.files:
            raise FileNotFoundError(f"MockFilesystem: no file at '{path}'")
        return self.files[path]

    def write(self, data: str | bytes | BytesIO, path: str) -> None:
        if isinstance(data, str):
            data = data.encode()
        elif isinstance(data, BytesIO):
            data = data.getvalue()
        self.files[path] = data

    def delete_file(self, path: str) -> None:
        self.files.pop(path, None)

    def create_prefix(self, prefix: str) -> None:
        pass

    def delete_prefix(self, prefix: str) -> None:
        for key in [k for k in self.files if k.startswith(prefix)]:
            del self.files[key]

    def check_file(self, path: str) -> bool:
        return path in self.files

    def check_prefix(self, prefix: str) -> bool:
        return any(k.startswith(prefix) for k in self.files)

    def list_files(self, prefix: str) -> list[str]:
        return [k for k in self.files if k.startswith(prefix)]
