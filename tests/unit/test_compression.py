from typing import Callable

import pytest

from airflow_toolkit.compression_utils import (
    compress,
    decompress,
    ungzip_data,
    CompressionOptions,
)
from airflow_toolkit.providers.http_to_data_lake.operators.http_to_data_lake import (
    list_to_jsonl,
)


@pytest.mark.parametrize("compression", ["gzip", "zip", None])
def test_compression(compression: CompressionOptions) -> None:
    data = b"hello world"
    assert decompress(compression, compress(compression, data)) == data


@pytest.mark.parametrize(
    "compression_type, uncompress_fn",
    [("gzip", ungzip_data), (None, lambda x: x)],
)
def test_compresion_list_to_jsonl(
    compression_type: CompressionOptions, uncompress_fn: Callable[[bytes], bytes]
) -> None:
    data = [{"test": 1.1, "other": "string1"}, {"test": 2.2, "other": "string2"}]
    result = list_to_jsonl(data=data, compression=compression_type)
    uncompressed_data = uncompress_fn(result.read())
    assert (
        uncompressed_data
        == b'{"test":1.1,"other":"string1"}\n{"test":2.2,"other":"string2"}\n'
    )
