from io import BytesIO
from typing import Protocol


class FilesystemProtocol(Protocol):
    def read(self, path: str) -> bytes: ...

    def write(self, data: str | bytes | BytesIO, path: str): ...

    def delete_file(self, path: str): ...

    def create_prefix(self, prefix: str): ...

    def delete_prefix(self, prefix: str): ...

    def check_file(self, path: str) -> bool:
        """Return True if *path* refers to an existing file.

        *path* must name a file, not a directory/prefix — use ``check_prefix()``
        for that. Passing a directory path here is undefined: implementations
        may return False or raise, and must not be relied upon either way.
        """
        ...

    def check_prefix(self, prefix: str) -> bool: ...

    def list_files(self, prefix: str) -> list[str]:
        """Return paths of all files under *prefix*, recursively.

        Directories are never included. Returned paths must be usable
        with ``read()``, ``delete_file()``, etc.
        """
        ...
