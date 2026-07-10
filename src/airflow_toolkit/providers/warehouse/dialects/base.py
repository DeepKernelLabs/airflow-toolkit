from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from airflow_toolkit.types import WarehouseSourceFormat


@dataclass
class CopyResult:
    """Best-effort normalized result of a COPY INTO statement.

    Any field may be None if the dialect couldn't confidently map the
    warehouse's result-set columns to it — callers must treat these as
    telemetry for logging, not as a contract to build alerting logic on.
    raw_columns/raw_rows are always populated so nothing is silently lost.
    """

    rows_loaded: int | None
    rows_skipped: int | None
    files_processed: int | None
    raw_columns: list[str] = field(default_factory=list)
    raw_rows: list[tuple[Any, ...]] = field(default_factory=list)


class WarehouseDialect(Protocol):
    """Structural protocol for a warehouse's COPY INTO SQL dialect.

    Mirrors FilesystemProtocol's convention: a Protocol, not an ABC, so
    third-party dialects (e.g. Redshift) can be added without importing a
    toolkit base class or subclassing anything.
    """

    name: str

    def qualified_table_name(
        self, database: str | None, schema: str, table: str
    ) -> str: ...

    def file_path_column_expression(self) -> str:
        """Dialect-native SQL expression yielding the source file's path,
        for use inside a COPY INTO ... FROM (SELECT ...) subquery."""

    def default_select_fragment(
        self, source_format: WarehouseSourceFormat, raw_column_name: str
    ) -> str:
        """Zero-config SELECT-list fragment (no trailing comma, no alias for
        metadata/file-path columns) for source_format.

        Raises ValueError if this dialect has no safe generic default for
        that format — callers must then supply select_fragment explicitly.
        """

    def build_copy_into(
        self,
        *,
        database: str | None,
        schema: str,
        table: str,
        source_location: str,
        select_fragment: str,
        metadata: Mapping[str, str],
        file_path_column: str,
        include_source_path: bool,
        source_format: WarehouseSourceFormat,
        file_format_options: Mapping[str, Any] | None,
        copy_options: Mapping[str, Any] | None,
    ) -> str: ...

    def build_delete_statement(
        self,
        *,
        database: str | None,
        schema: str,
        table: str,
        metadata: Mapping[str, str],
    ) -> str:
        """Returns a full DELETE statement, ready to run as-is.

        metadata values are raw (already Jinja-rendered) SQL value
        expressions — the same ones embedded in the COPY INTO's SELECT list
        — not literal Python values, so they can't be passed as DBAPI bind
        parameters (a bind param would compare the column against the
        expression's source text, not its evaluated value). The predicate is
        built by directly embedding each expression, mirroring
        FilesystemToDeltalakeOperator's raw-predicate approach (DataFusion
        has no bind params either) rather than
        FilesystemToDatabaseOperator's parameterized one (which assumes
        plain literal values, not expressions).
        """

    def parse_copy_result(
        self, columns: Sequence[str], rows: Sequence[tuple[Any, ...]]
    ) -> CopyResult: ...
