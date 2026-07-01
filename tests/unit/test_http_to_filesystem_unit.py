from __future__ import annotations

import time
from io import BytesIO
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from airflow_toolkit.exceptions import ApiResponseTypeError
from airflow_toolkit.providers.filesystem.operators.auth import OAuth2ClientCredentials
from airflow_toolkit.providers.filesystem.operators.http_to_filesystem import (
    HttpBatchOperator,
    HttpToFilesystem,
    MultiHttpToFilesystem,
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _op(**kwargs: Any) -> HttpToFilesystem:
    defaults: dict[str, Any] = dict(
        task_id="test",
        http_conn_id="http_test",
        filesystem_conn_id="fs_test",
        filesystem_path="bucket/path/",
        save_format="jsonl",
    )
    defaults.update(kwargs)
    return HttpToFilesystem(**defaults)  # type: ignore[arg-type]


def _multi_op(**kwargs: Any) -> MultiHttpToFilesystem:
    defaults: dict[str, Any] = dict(
        task_id="test",
        http_conn_id="http_test",
        filesystem_conn_id="fs_test",
        filesystem_path="bucket/path/",
        save_format="jsonl",
        multi_requests=[{"endpoint": "/a"}],
    )
    defaults.update(kwargs)
    return MultiHttpToFilesystem(**defaults)  # type: ignore[arg-type]


def _mock_response(json_data=None, text: str = "", content: bytes = b"") -> MagicMock:
    r = MagicMock()
    r.json.return_value = json_data
    r.text = text
    r.content = content
    return r


# ── OAuth2ClientCredentials ──────────────────────────────────────────────────


class TestOAuth2ClientCredentials:
    def test_returns_auth_base_subclass(self):
        from requests.auth import AuthBase

        AuthClass = OAuth2ClientCredentials.client_credentials(
            "https://auth.example.com/token", "id", "secret"
        )
        assert issubclass(AuthClass, AuthBase)

    def test_each_call_returns_independent_class(self):
        A = OAuth2ClientCredentials.client_credentials("url1", "id1", "s1")
        B = OAuth2ClientCredentials.client_credentials("url2", "id2", "s2")
        assert A is not B
        assert (
            A._token is B._token is None
        )  # both start empty but are independent attrs

    @patch("airflow_toolkit.providers.filesystem.operators.auth.requests.post")
    def test_fetches_token_on_first_call(self, mock_post):
        mock_post.return_value.json.return_value = {
            "access_token": "tok123",
            "expires_in": 3600,
        }
        mock_post.return_value.raise_for_status = MagicMock()

        AuthClass = OAuth2ClientCredentials.client_credentials(
            "https://auth.example.com/token", "client_id", "client_secret"
        )
        r = MagicMock()
        r.headers = {}
        AuthClass()(r)

        mock_post.assert_called_once_with(
            "https://auth.example.com/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "client_id",
                "client_secret": "client_secret",
            },
        )
        assert r.headers["Authorization"] == "Bearer tok123"

    @patch("airflow_toolkit.providers.filesystem.operators.auth.requests.post")
    def test_scope_included_when_provided(self, mock_post):
        mock_post.return_value.json.return_value = {
            "access_token": "t",
            "expires_in": 3600,
        }
        mock_post.return_value.raise_for_status = MagicMock()

        AuthClass = OAuth2ClientCredentials.client_credentials(
            "https://token.url", "id", "secret", scope="read write"
        )
        r = MagicMock()
        r.headers = {}
        AuthClass()(r)

        assert mock_post.call_args[1]["data"]["scope"] == "read write"

    @patch("airflow_toolkit.providers.filesystem.operators.auth.requests.post")
    def test_scope_omitted_when_not_provided(self, mock_post):
        mock_post.return_value.json.return_value = {
            "access_token": "t",
            "expires_in": 3600,
        }
        mock_post.return_value.raise_for_status = MagicMock()

        AuthClass = OAuth2ClientCredentials.client_credentials("url", "id", "secret")
        r = MagicMock()
        r.headers = {}
        AuthClass()(r)

        assert "scope" not in mock_post.call_args[1]["data"]

    @patch("airflow_toolkit.providers.filesystem.operators.auth.requests.post")
    def test_token_cached_no_second_fetch(self, mock_post):
        mock_post.return_value.json.return_value = {
            "access_token": "tok",
            "expires_in": 3600,
        }
        mock_post.return_value.raise_for_status = MagicMock()

        AuthClass = OAuth2ClientCredentials.client_credentials("url", "id", "secret")
        auth = AuthClass()
        r = MagicMock()
        r.headers = {}
        auth(r)
        auth(r)

        assert mock_post.call_count == 1

    @patch("airflow_toolkit.providers.filesystem.operators.auth.requests.post")
    def test_token_refreshed_when_expired(self, mock_post):
        mock_post.return_value.json.return_value = {
            "access_token": "new_tok",
            "expires_in": 3600,
        }
        mock_post.return_value.raise_for_status = MagicMock()

        AuthClass = OAuth2ClientCredentials.client_credentials("url", "id", "secret")
        AuthClass._token = "old_tok"
        AuthClass._expiry = time.time() - 1  # already expired

        r = MagicMock()
        r.headers = {}
        AuthClass()(r)

        assert r.headers["Authorization"] == "Bearer new_tok"
        mock_post.assert_called_once()

    @patch("airflow_toolkit.providers.filesystem.operators.auth.requests.post")
    def test_token_refreshed_within_30s_buffer(self, mock_post):
        mock_post.return_value.json.return_value = {
            "access_token": "refreshed",
            "expires_in": 3600,
        }
        mock_post.return_value.raise_for_status = MagicMock()

        AuthClass = OAuth2ClientCredentials.client_credentials("url", "id", "secret")
        AuthClass._token = "old_tok"
        AuthClass._expiry = time.time() + 20  # 20s left → within 30s buffer

        r = MagicMock()
        r.headers = {}
        AuthClass()(r)

        assert r.headers["Authorization"] == "Bearer refreshed"
        mock_post.assert_called_once()


# ── HttpToFilesystem — constructor validations ────────────────────────────────


class TestHttpToFilesystemConstructor:
    @pytest.mark.parametrize("fmt", ["parquet", "excel", "avro"])
    def test_binary_with_compression_raises(self, fmt):
        with pytest.raises(ValueError, match="Compression is not supported"):
            _op(source_format=fmt, save_format=fmt, compression="gzip")

    def test_source_differs_from_save_without_transform_raises(self):
        with pytest.raises(ValueError, match="data_transformation must be provided"):
            _op(source_format="json", save_format="csv")

    def test_kwargs_without_transform_raises(self):
        with pytest.raises(ValueError, match="data_transformation must be provided"):
            _op(data_transformation_kwargs={"key": "val"})

    def test_non_callable_transform_raises(self):
        with pytest.raises(ValueError, match="data_transformation must be a callable"):
            _op(data_transformation="not_a_function")

    def test_valid_construction_defaults(self):
        op = _op()
        assert op.save_format == "jsonl"
        assert op.source_format == "jsonl"
        assert op.requests_per_second is None

    def test_source_format_defaults_to_save_format(self):
        op = _op(save_format="json")
        assert op.source_format == "json"

    def test_requests_per_second_stored(self):
        op = _op(requests_per_second=5.0)
        assert op.requests_per_second == 5.0


# ── HttpToFilesystem._file_name() ────────────────────────────────────────────


class TestFileName:
    @pytest.mark.parametrize(
        "save_format, expected_ext",
        [
            ("json", "json"),
            ("jsonl", "jsonl"),
            ("xml", "xml"),
            ("csv", "csv"),
            ("parquet", "parquet"),
            ("excel", "xlsx"),
            ("avro", "avro"),
            ("fixed_width", "fwf"),
        ],
    )
    def test_correct_extension(self, save_format, expected_ext):
        if save_format in ("parquet", "excel", "avro"):
            op = _op(source_format=save_format, save_format=save_format)
        else:
            op = _op(save_format=save_format)
        assert op._file_name(1) == f"part0001.{expected_ext}"

    def test_sequential_numbering(self):
        op = _op()
        assert op._file_name(1) == "part0001.jsonl"
        assert op._file_name(42) == "part0042.jsonl"
        assert op._file_name(999) == "part0999.jsonl"

    def test_compression_appended(self):
        op = _op(compression="gzip")
        assert op._file_name(1) == "part0001.jsonl.gzip"

    def test_unknown_format_uses_format_as_extension(self):
        op = _op(save_format="jsonl")
        op.save_format = "custom_fmt"
        assert op._file_name(1) == "part0001.custom_fmt"


# ── HttpToFilesystem._ensure_bytesio() ───────────────────────────────────────


class TestEnsureBytesio:
    def setup_method(self):
        self.op = _op()

    def test_bytesio_passthrough(self):
        buf = BytesIO(b"data")
        assert self.op._ensure_bytesio(buf) is buf

    def test_bytes_wrapped(self):
        result = self.op._ensure_bytesio(b"hello")
        assert isinstance(result, BytesIO)
        assert result.read() == b"hello"

    def test_str_encoded(self):
        result = self.op._ensure_bytesio("hello")
        assert isinstance(result, BytesIO)
        assert result.read() == b"hello"

    def test_invalid_type_raises(self):
        with pytest.raises(TypeError, match="Unsupported"):
            self.op._ensure_bytesio(12345)


# ── HttpToFilesystem._response_filter() ──────────────────────────────────────


class TestResponseFilter:
    def test_json_with_jmespath(self):
        op = _op(save_format="json", jmespath_expression="page")
        result = op._response_filter(_mock_response(json_data={"page": 1, "data": []}))
        assert isinstance(result, BytesIO)
        assert b"1" in result.read()

    def test_json_without_jmespath(self):
        op = _op(save_format="json")
        result = op._response_filter(_mock_response(json_data={"page": 1}))
        assert isinstance(result, BytesIO)

    def test_jsonl_list_response(self):
        op = _op(save_format="jsonl")
        result = op._response_filter(_mock_response(json_data=[{"id": 1}, {"id": 2}]))
        assert isinstance(result, BytesIO)
        content = result.read().decode()
        assert '"id"' in content

    def test_jsonl_dict_strict_raises(self):
        op = _op(save_format="jsonl", strict_response_schema=True)
        with pytest.raises(ApiResponseTypeError):
            op._response_filter(_mock_response(json_data={"id": 1}))

    def test_jsonl_dict_non_strict_returns_empty(self):
        op = _op(save_format="jsonl", strict_response_schema=False)
        result = op._response_filter(_mock_response(json_data={"id": 1}))
        assert result.read() == b""

    def test_xml_response(self):
        op = _op(save_format="xml")
        result = op._response_filter(_mock_response(text="<root><item>1</item></root>"))
        assert isinstance(result, BytesIO)
        assert b"<root>" in result.read()

    def test_csv_response(self):
        op = _op(save_format="csv")
        result = op._response_filter(_mock_response(text="id,name\n1,Alice\n2,Bob\n"))
        assert isinstance(result, BytesIO)
        assert result.read()

    def test_parquet_binary_passthrough(self):
        op = _op(source_format="parquet", save_format="parquet")
        raw = b"PAR1\x00\x01\x02\x03"
        result = op._response_filter(_mock_response(content=raw))
        assert result.read() == raw

    def test_excel_binary_passthrough(self):
        op = _op(source_format="excel", save_format="excel")
        raw = b"PK\x03\x04xlsx_magic"
        result = op._response_filter(_mock_response(content=raw))
        assert result.read() == raw

    def test_avro_binary_passthrough(self):
        op = _op(source_format="avro", save_format="avro")
        raw = b"Obj\x01avro_magic"
        result = op._response_filter(_mock_response(content=raw))
        assert result.read() == raw

    def test_fixed_width_text_passthrough(self):
        op = _op(save_format="fixed_width")
        fwf = "00001Alice    30\n00002Bob      25\n"
        result = op._response_filter(_mock_response(text=fwf))
        assert isinstance(result, BytesIO)
        assert b"Alice" in result.read()

    def test_unknown_format_raises(self):
        op = _op(save_format="jsonl")
        op.source_format = "unknown_fmt"
        with pytest.raises(NotImplementedError, match="unknown_fmt"):
            op._response_filter(_mock_response(text="data"))

    def test_jmespath_on_non_json_raises(self):
        op = _op(save_format="xml", jmespath_expression="$.data")
        with pytest.raises(ApiResponseTypeError):
            op._response_filter(_mock_response(text="<xml/>"))

    def test_custom_transformation_called(self):
        transform = MagicMock(return_value=b"transformed")
        op = _op(
            source_format="json",
            save_format="csv",
            data_transformation=transform,
        )
        result = op._response_filter(_mock_response(json_data={"id": 1}))
        transform.assert_called_once_with({"id": 1})
        assert result.read() == b"transformed"

    def test_custom_transformation_with_kwargs(self):
        transform = MagicMock(return_value=b"out")
        op = _op(
            source_format="json",
            save_format="csv",
            data_transformation=transform,
            data_transformation_kwargs={"sep": ";"},
        )
        op._response_filter(_mock_response(json_data={"id": 1}))
        transform.assert_called_once_with({"id": 1}, {"sep": ";"})


# ── Rate limiting ─────────────────────────────────────────────────────────────


class TestRateLimiting:
    @patch(
        "airflow_toolkit.providers.filesystem.operators.http_to_filesystem.time.sleep"
    )
    def test_paginate_sync_sleeps_before_each_page(self, mock_sleep):
        op = MagicMock(spec=HttpBatchOperator)
        op.pagination_function = (
            lambda r: None
        )  # returns None → one iteration then break
        op.endpoint = "/api"
        op.data = {}
        op.headers = {}
        op.extra_options = {}

        list(HttpBatchOperator.paginate_sync(op, _mock_response(), delay=0.5))

        mock_sleep.assert_called_once_with(0.5)

    @patch(
        "airflow_toolkit.providers.filesystem.operators.http_to_filesystem.time.sleep"
    )
    def test_paginate_sync_no_sleep_without_delay(self, mock_sleep):
        op = MagicMock(spec=HttpBatchOperator)
        op.pagination_function = lambda r: None

        list(HttpBatchOperator.paginate_sync(op, _mock_response(), delay=0.0))

        mock_sleep.assert_not_called()

    @patch(
        "airflow_toolkit.providers.filesystem.operators.http_to_filesystem.time.sleep"
    )
    def test_multi_http_sleeps_between_sequential_requests(self, mock_sleep):
        op = _multi_op(
            requests_per_second=2.0,
            multi_requests=[
                {"endpoint": "/a"},
                {"endpoint": "/b"},
                {"endpoint": "/c"},
            ],
        )

        with (
            patch.object(op, "_apply_request_overrides"),
            patch.object(op, "_restore_request_state"),
            patch(
                "airflow_toolkit.providers.filesystem.operators.http_to_filesystem.HttpToFilesystem.execute"
            ),
        ):
            op.execute({})

        # 3 requests → 2 sleeps (no sleep before first request)
        assert mock_sleep.call_count == 2
        for c in mock_sleep.call_args_list:
            assert c == call(0.5)  # 1.0 / 2.0

    @patch(
        "airflow_toolkit.providers.filesystem.operators.http_to_filesystem.time.sleep"
    )
    def test_multi_http_no_sleep_without_rate_limit(self, mock_sleep):
        op = _multi_op(
            multi_requests=[{"endpoint": "/a"}, {"endpoint": "/b"}],
        )

        with (
            patch.object(op, "_apply_request_overrides"),
            patch.object(op, "_restore_request_state"),
            patch(
                "airflow_toolkit.providers.filesystem.operators.http_to_filesystem.HttpToFilesystem.execute"
            ),
        ):
            op.execute({})

        mock_sleep.assert_not_called()


# ── MultiHttpToFilesystem — constructor & state management ────────────────────


class TestMultiHttpState:
    def test_pagination_function_raises(self):
        with pytest.raises(ValueError, match="Pagination is not supported"):
            _multi_op(pagination_function=lambda r: None)

    def test_empty_multi_requests_raises(self):
        with pytest.raises(ValueError, match="non-empty list"):
            _multi_op(multi_requests=[])

    def test_max_workers_stored(self):
        op = _multi_op(max_workers=4)
        assert op.max_workers == 4

    def test_capture_and_restore_state(self):
        op = _multi_op(endpoint="/base", method="GET")
        state = op._capture_request_state()
        op.endpoint = "/modified"
        op.method = "POST"
        op._restore_request_state(state)
        assert op.endpoint == "/base"
        assert op.method == "GET"

    def test_apply_overrides_simple_field(self):
        op = _multi_op(endpoint="/base", method="GET")
        base = op._capture_request_state()
        op._apply_request_overrides({"endpoint": "/override"}, base)
        assert op.endpoint == "/override"
        assert op.method == "GET"  # unchanged

    def test_apply_overrides_dict_merge_headers(self):
        op = _multi_op(headers={"Auth": "base", "X-Keep": "yes"})
        base = op._capture_request_state()
        op._apply_request_overrides({"headers": {"Auth": "new"}}, base)
        assert op.headers == {"Auth": "new", "X-Keep": "yes"}

    def test_apply_overrides_dict_merge_data(self):
        op = _multi_op(method="POST", data={"a": 1, "b": 2})
        base = op._capture_request_state()
        op._apply_request_overrides({"data": {"b": 99}}, base)
        assert op.data == {"a": 1, "b": 99}

    def test_apply_overrides_unknown_key_raises(self):
        op = _multi_op()
        base = op._capture_request_state()
        with pytest.raises(ValueError, match="Unknown keys"):
            op._apply_request_overrides({"unknown_key": "val"}, base)

    @pytest.mark.parametrize(
        "base_val, override_val, expected",
        [
            ({"a": 1}, {"b": 2}, {"a": 1, "b": 2}),
            ({"a": 1}, {"a": 99}, {"a": 99}),
            ("base", "new", "new"),
            (None, "val", "val"),
            (42, {"k": "v"}, {"k": "v"}),
        ],
    )
    def test_merge_or_replace(self, base_val, override_val, expected):
        assert (
            MultiHttpToFilesystem._merge_or_replace(base_val, override_val) == expected
        )

    def test_validate_binary_with_compression_raises(self):
        op = _multi_op()
        op.source_format = "excel"
        op.compression = "gzip"
        with pytest.raises(ValueError, match="Compression is not supported"):
            op._validate_current_request_state()


# ── MultiHttpToFilesystem — parallel execution ────────────────────────────────


class TestMultiHttpParallel:
    def test_sequential_mode_when_max_workers_is_none(self):
        op = _multi_op(max_workers=None)
        assert op.max_workers is None

    def test_parallel_all_requests_complete(self):
        completed = []

        def fake_execute(base_op, context, spec, base_state, file_number):
            completed.append(file_number)

        op = _multi_op(
            max_workers=3,
            multi_requests=[
                {"endpoint": "/a"},
                {"endpoint": "/b"},
                {"endpoint": "/c"},
            ],
        )

        with patch.object(
            MultiHttpToFilesystem, "_execute_one_request", new=fake_execute
        ):
            op.execute({})

        assert sorted(completed) == [1, 2, 3]

    def test_parallel_exception_propagates(self):
        def failing_execute(base_op, context, spec, base_state, file_number):
            raise RuntimeError("request failed")

        op = _multi_op(
            max_workers=2,
            multi_requests=[{"endpoint": "/a"}],
        )

        with patch.object(
            MultiHttpToFilesystem, "_execute_one_request", new=failing_execute
        ):
            with pytest.raises(RuntimeError, match="request failed"):
                op.execute({})
