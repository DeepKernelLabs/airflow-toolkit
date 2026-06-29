from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Type, TypedDict

if TYPE_CHECKING:
    from requests.auth import AuthBase

# ── Compression ────────────────────────────────────────────────────────────

CompressionOptions = Literal["infer", "gzip", "bz2", "zip", "xz", "zstd"] | None

# ── Filesystem / format ────────────────────────────────────────────────────

SaveFormat = Literal["jsonl"]

# ── Metadata columns ───────────────────────────────────────────────────────
# Passed to FilesystemToDatabaseOperator as extra columns added to every row.
# Key = column name; value = Airflow template string (e.g. "{{ ds }}").
# Keys prefixed with "_" are coerced to datetime at load time.

MetadataSpec = dict[str, str]

# ── HTTP multi-request ─────────────────────────────────────────────────────


class RequestSpec(TypedDict, total=False):
    """User-provided per-request overrides for MultiHttpToFilesystem (all keys optional)."""

    endpoint: str
    method: str
    data: Any
    headers: dict[str, str]
    auth_type: Type["AuthBase"] | None
    jmespath_expression: str | None
    save_format: SaveFormat
    source_format: SaveFormat
    compression: CompressionOptions


class RequestState(TypedDict):
    """Fully-resolved runtime state for a single HTTP request (all keys required)."""

    endpoint: str | None
    method: str
    data: Any
    headers: dict[str, str] | None
    auth_type: Type["AuthBase"] | None
    jmespath_expression: str | None
    save_format: SaveFormat
    source_format: SaveFormat
    compression: CompressionOptions
