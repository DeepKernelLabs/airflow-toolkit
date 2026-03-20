"""Unit tests for DuckdbToDeltalakeOperator — focuses on the pure static method."""

from airflow_toolkit.providers.deltalake.operators.duckdb_to_deltalake import (
    DuckdbToDeltalakeOperator,
)

parse = DuckdbToDeltalakeOperator.parse_storage_options


# ---------------------------------------------------------------------------
# parse_storage_options — pure static method, no mocking needed
# ---------------------------------------------------------------------------


def test_extracts_account_name_and_key():
    conn = "DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey123;EndpointSuffix=core.windows.net"
    result = parse(conn)
    assert result == {"account_name": "myaccount", "account_key": "mykey123"}


def test_missing_account_key_returns_only_name():
    conn = "DefaultEndpointsProtocol=https;AccountName=onlyname;EndpointSuffix=core.windows.net"
    result = parse(conn)
    assert result == {"account_name": "onlyname"}


def test_missing_account_name_returns_only_key():
    conn = "AccountKey=onlykey;EndpointSuffix=core.windows.net"
    result = parse(conn)
    assert result == {"account_key": "onlykey"}


def test_empty_connection_string_returns_empty_dict():
    assert parse("") == {}


def test_unrelated_segments_are_ignored():
    conn = "SomeOtherProp=value;AnotherProp=xyz"
    assert parse(conn) == {}


def test_values_with_equals_sign_inside_are_handled():
    # AccountKey values are base64 and may contain '=' padding
    conn = "AccountName=myaccount;AccountKey=abc123=="
    result = parse(conn)
    assert result["account_name"] == "myaccount"
    assert result["account_key"] == "abc123=="


def test_whitespace_around_values_is_stripped():
    conn = "AccountName= spacedname ;AccountKey= spacedkey "
    result = parse(conn)
    assert result["account_name"] == "spacedname"
    assert result["account_key"] == "spacedkey"
