from __future__ import annotations

from io import BytesIO
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from airflow_toolkit._compact.airflow_shim import Context


class FilesystemTransformation(Protocol):
    """Transform file content during a FilesystemToFilesystem copy."""

    def __call__(self, data: bytes, filename: str, context: "Context") -> bytes: ...


class HttpTransformation(Protocol):
    """Transform HTTP response data before writing to a filesystem."""

    def __call__(
        self, data: Any, *args: Any, **kwargs: Any
    ) -> BytesIO | bytes | str: ...
