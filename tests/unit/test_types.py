"""Tests for airflow_toolkit.types — centralized type definitions."""

from __future__ import annotations


from airflow_toolkit.types import (
    MetadataSpec,
    RequestSpec,
    RequestState,
    SaveFormat,
)


def test_request_spec_accepts_partial_dict():
    spec: RequestSpec = {"endpoint": "/api/v1", "method": "GET"}
    assert spec["endpoint"] == "/api/v1"


def test_request_spec_accepts_empty_dict():
    spec: RequestSpec = {}
    assert spec == {}


def test_request_state_requires_all_keys():
    state: RequestState = {
        "endpoint": "/api/v1",
        "method": "GET",
        "data": None,
        "headers": None,
        "auth_type": None,
        "jmespath_expression": None,
        "save_format": "jsonl",
        "source_format": "jsonl",
        "compression": None,
    }
    assert state["method"] == "GET"


def test_metadata_spec_is_str_to_str_dict():
    meta: MetadataSpec = {"_DS": "{{ ds }}", "_RUN_ID": "{{ run_id }}"}
    assert meta["_DS"] == "{{ ds }}"


def test_compression_options_values():
    valid_values = {"infer", "gzip", "bz2", "zip", "xz", "zstd", None}
    assert "gzip" in valid_values
    assert None in valid_values


def test_save_format_value():
    fmt: SaveFormat = "jsonl"
    assert fmt == "jsonl"


def test_request_spec_keys_match_annotations():
    expected_keys = {
        "endpoint",
        "method",
        "data",
        "headers",
        "auth_type",
        "jmespath_expression",
        "save_format",
        "source_format",
        "compression",
    }
    assert set(RequestSpec.__annotations__.keys()) == expected_keys


def test_request_state_keys_match_annotations():
    expected_keys = {
        "endpoint",
        "method",
        "data",
        "headers",
        "auth_type",
        "jmespath_expression",
        "save_format",
        "source_format",
        "compression",
    }
    assert set(RequestState.__annotations__.keys()) == expected_keys


def test_request_spec_and_state_have_same_keys():
    assert set(RequestSpec.__annotations__) == set(RequestState.__annotations__)
