import json

import pendulum
import pytest
import requests_mock as req_mock_module
from botocore.exceptions import ClientError as BotoClientError

from airflow_toolkit.exceptions import ApiResponseTypeError
from airflow_toolkit.providers.http_to_data_lake.operators.http_to_data_lake import (
    HttpToDataLake,
)
from airflow_toolkit._compact.airflow_shim import run_dag

# ---------------------------------------------------------------------------
# Mock HTTP server constants — no real network calls
# ---------------------------------------------------------------------------
MOCK_HOST = "http://mock.reqres.test"

_USERS_PAGE_1 = {
    "page": 1,
    "per_page": 6,
    "total": 12,
    "total_pages": 2,
    "data": [
        {"id": 1, "email": "george.bluth@reqres.in"},
        {"id": 2, "email": "janet.weaver@reqres.in"},
        {"id": 3, "email": "emma.wong@reqres.in"},
        {"id": 4, "email": "eve.holt@reqres.in"},
        {"id": 5, "email": "charles.morris@reqres.in"},
        {"id": 6, "email": "tracey.ramos@reqres.in"},
    ],
}

_USERS_PAGE_2 = {
    "page": 2,
    "per_page": 6,
    "total": 12,
    "total_pages": 2,
    "data": [
        {"id": 7, "email": "michael.lawson@reqres.in"},
        {"id": 8, "email": "lindsay.ferguson@reqres.in"},
        {"id": 9, "email": "tobias.funke@reqres.in"},
        {"id": 10, "email": "byron.fields@reqres.in"},
        {"id": 11, "email": "george.edwards@reqres.in"},
        {"id": 12, "email": "rachel.howell@reqres.in"},
    ],
}

# Single-user response — data is a dict, not a list (triggers ApiResponseTypeError
# when save_format=jsonl without a list-returning jmespath expression)
_USER_2 = {"data": {"id": 2, "email": "janet.weaver@reqres.in"}}


def _conn(monkeypatch):
    monkeypatch.setenv(
        "AIRFLOW_CONN_HTTP_TEST",
        json.dumps({"conn_type": "http", "host": MOCK_HOST}),
    )


def test_http_to_data_lake(dag, s3_bucket, s3_resource, monkeypatch):
    _conn(monkeypatch)
    with req_mock_module.Mocker() as m:
        m.get(f"{MOCK_HOST}/api/users", json=_USERS_PAGE_1)
        with dag:
            HttpToDataLake(
                task_id="test_http_to_data_lake",
                http_conn_id="http_test",
                data_lake_conn_id="data_lake_test",
                data_lake_path=s3_bucket + "/source1/entity1/{{ ds }}/",
                endpoint="/api/users",
                method="GET",
                jmespath_expression="data[:2].{id: id, email: email}",
            )
        run_dag(dag, pendulum.datetime(2023, 10, 1))

    content = (
        s3_resource.Object(s3_bucket, "source1/entity1/2023-10-01/part0001.jsonl")
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
    assert (
        content
        == """\
{"id":1,"email":"george.bluth@reqres.in"}
{"id":2,"email":"janet.weaver@reqres.in"}
"""
    )


def test_http_to_data_lake_response_format_jsonl_with_jmespath_expression(
    s3_bucket,
    monkeypatch,
):
    _conn(monkeypatch)
    with req_mock_module.Mocker() as m:
        m.get(f"{MOCK_HOST}/api/users", json=_USERS_PAGE_1)
        m.get(f"{MOCK_HOST}/api/users/2", json=_USER_2)

        http_to_data_lake_op = HttpToDataLake(
            task_id="test_http_to_data_lake",
            http_conn_id="http_test",
            data_lake_conn_id="data_lake_test",
            data_lake_path=s3_bucket + "/source1/entity1/{{ ds }}/",
            endpoint="/api/users",
            method="GET",
            save_format="jsonl",
            jmespath_expression="data[:2].{id: id, email: email}",
        )
        http_to_data_lake_op.execute({"ds": "2024-01-03"})

        assert isinstance(http_to_data_lake_op.data, list)
        assert len(http_to_data_lake_op.data) == 2
        assert (
            "id" in http_to_data_lake_op.data[0]
            and "email" in http_to_data_lake_op.data[0]
        )

        with pytest.raises(ApiResponseTypeError):
            response_origin_no_list = HttpToDataLake(
                task_id="test_http_to_data_lake_no_list",
                http_conn_id="http_test",
                data_lake_conn_id="data_lake_test",
                data_lake_path=s3_bucket + "/source1/entity1/{{ ds }}/",
                endpoint="/api/users/2",
                method="GET",
                save_format="jsonl",
                jmespath_expression="data.{id: id, email: email}",
            )
            response_origin_no_list.execute({"ds": "2024-01-03"})


def test_http_to_data_lake_response_format_jsonl_without_jmespath_expression(
    s3_bucket,
    monkeypatch,
):
    _conn(monkeypatch)
    # Top-level response is a dict (page object), not a list → ApiResponseTypeError
    with req_mock_module.Mocker() as m:
        m.get(f"{MOCK_HOST}/api/users", json=_USERS_PAGE_1)
        with pytest.raises(ApiResponseTypeError):
            http_to_data_lake_op = HttpToDataLake(
                task_id="test_http_to_data_lake",
                http_conn_id="http_test",
                data_lake_conn_id="data_lake_test",
                data_lake_path=s3_bucket + "/source1/entity1/{{ ds }}/",
                endpoint="/api/users",
                method="GET",
                save_format="jsonl",
                jmespath_expression=None,
            )
            http_to_data_lake_op.execute({"ds": "2024-01-03"})

    # A response that IS a list at the top level should pass without jmespath
    monkeypatch.setenv(
        "AIRFLOW_CONN_HTTP_TEST_LIST",
        json.dumps({"conn_type": "http", "host": "http://test-airflow-toolkit.test"}),
    )
    with req_mock_module.Mocker() as m:
        m.get(
            "http://test-airflow-toolkit.test/api/v2/test",
            text="""[{"id": 1, "email": "user1@test.com"}, {"id": 2, "email": "user2@test.com"}]""",
        )
        http_to_data_lake_list_op = HttpToDataLake(
            task_id="test_http_to_data_lake_list",
            http_conn_id="http_test_list",
            data_lake_conn_id="data_lake_test",
            data_lake_path=s3_bucket + "/source1/entity1/{{ ds }}/",
            endpoint="/api/v2/test",
            method="GET",
            save_format="jsonl",
            jmespath_expression=None,
        )
        http_to_data_lake_list_op.execute({"ds": "2024-01-03"})

    assert isinstance(http_to_data_lake_list_op.data, list)


def test_http_to_data_lake_response_format_json_with_jmespath_expression(
    s3_bucket, monkeypatch
):
    _conn(monkeypatch)
    with req_mock_module.Mocker() as m:
        m.get(f"{MOCK_HOST}/api/users", json=_USERS_PAGE_1)
        http_to_data_lake_op = HttpToDataLake(
            task_id="test_http_to_data_lake",
            http_conn_id="http_test",
            data_lake_conn_id="data_lake_test",
            data_lake_path=s3_bucket + "/source1/entity1/{{ ds }}/",
            endpoint="/api/users",
            method="GET",
            save_format="json",
            jmespath_expression="{page:page,total:total}",
        )
        http_to_data_lake_op.execute({"ds": "2024-01-03"})

    assert isinstance(http_to_data_lake_op.data, dict)
    assert http_to_data_lake_op.data == {"page": 1, "total": 12}


def test_http_to_data_lake_response_format_json_without_jmespath_expression(
    s3_bucket, monkeypatch
):
    _conn(monkeypatch)
    with req_mock_module.Mocker() as m:
        m.get(f"{MOCK_HOST}/api/users", json=_USERS_PAGE_1)
        http_to_data_lake_op = HttpToDataLake(
            task_id="test_http_to_data_lake",
            http_conn_id="http_test",
            data_lake_conn_id="data_lake_test",
            data_lake_path=s3_bucket + "/source1/entity1/{{ ds }}/",
            endpoint="/api/users",
            method="GET",
            save_format="json",
            jmespath_expression=None,
        )
        http_to_data_lake_op.execute({"ds": "2024-01-03"})

    assert True


def test_http_to_data_lake_response_wrong_format(s3_bucket, monkeypatch):
    _conn(monkeypatch)
    with req_mock_module.Mocker() as m:
        m.get(f"{MOCK_HOST}/api/users", json=_USERS_PAGE_1)
        with pytest.raises(NotImplementedError, match=r".*wrong_format.*"):
            http_to_data_lake_op = HttpToDataLake(
                task_id="test_http_to_data_lake",
                http_conn_id="http_test",
                data_lake_conn_id="data_lake_test",
                data_lake_path=s3_bucket + "/source1/entity1/{{ ds }}/",
                endpoint="/api/users",
                method="GET",
                save_format="wrong_format",
                jmespath_expression=None,
            )
            http_to_data_lake_op.execute({"ds": "2024-01-03"})
            assert False, "This try should fail"


def reqres_pagination_function(response):
    current_page = response.json()["page"]
    if current_page < response.json()["total_pages"]:
        return {"data": {"page": current_page + 1}}


def test_http_to_datalake_pagination_jsonl(dag, s3_bucket, s3_resource, monkeypatch):
    _conn(monkeypatch)
    with req_mock_module.Mocker() as m:
        m.get(
            f"{MOCK_HOST}/api/users",
            [{"json": _USERS_PAGE_1}, {"json": _USERS_PAGE_2}],
        )
        with dag:
            HttpToDataLake(
                task_id="test_http_to_data_lake",
                http_conn_id="http_test",
                data_lake_conn_id="data_lake_test",
                data_lake_path=s3_bucket + "/source1/entity1/{{ ds }}/",
                endpoint="/api/users",
                method="GET",
                data={"page": 1},
                save_format="jsonl",
                jmespath_expression="data[:2].{id: id, email: email}",
                pagination_function=reqres_pagination_function,
            )
        run_dag(dag, pendulum.datetime(2023, 10, 1))

    content_part_1 = (
        s3_resource.Object(s3_bucket, "source1/entity1/2023-10-01/part0001.jsonl")
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
    content_part_2 = (
        s3_resource.Object(s3_bucket, "source1/entity1/2023-10-01/part0002.jsonl")
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
    assert (
        content_part_1
        == """\
{"id":1,"email":"george.bluth@reqres.in"}
{"id":2,"email":"janet.weaver@reqres.in"}
"""
    )
    assert (
        content_part_2
        == """\
{"id":7,"email":"michael.lawson@reqres.in"}
{"id":8,"email":"lindsay.ferguson@reqres.in"}
"""
    )


def test_http_to_datalake_pagination_json(dag, s3_bucket, s3_resource, monkeypatch):
    _conn(monkeypatch)
    with req_mock_module.Mocker() as m:
        m.get(
            f"{MOCK_HOST}/api/users",
            [{"json": _USERS_PAGE_1}, {"json": _USERS_PAGE_2}],
        )
        with dag:
            HttpToDataLake(
                task_id="test_http_to_data_lake",
                http_conn_id="http_test",
                data_lake_conn_id="data_lake_test",
                data_lake_path=s3_bucket + "/source1/entity1/{{ ds }}/",
                endpoint="/api/users",
                method="GET",
                data={"page": 1},
                save_format="json",
                jmespath_expression="{page:page,total:total}",
                pagination_function=reqres_pagination_function,
            )
        run_dag(dag, pendulum.datetime(2023, 10, 1))

    content_part1 = (
        s3_resource.Object(s3_bucket, "source1/entity1/2023-10-01/part0001.json")
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
    content_part2 = (
        s3_resource.Object(s3_bucket, "source1/entity1/2023-10-01/part0002.json")
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
    assert content_part1 == """{"page": 1, "total": 12}"""
    assert content_part2 == """{"page": 2, "total": 12}"""


def test_http_to_data_lake_check_one_page_data_is_duplicated(
    dag, s3_bucket, s3_resource, monkeypatch
):
    # Check if a second file (`part002.jsonl`) is created calling the API
    # for avoiding duplicates. If it exists, the test fails.
    _conn(monkeypatch)
    with req_mock_module.Mocker() as m:
        m.get(f"{MOCK_HOST}/api/users", json=_USERS_PAGE_1)
        with dag:
            HttpToDataLake(
                task_id="test_http_to_data_lake",
                http_conn_id="http_test",
                data_lake_conn_id="data_lake_test",
                data_lake_path=s3_bucket + "/source1/entity1/{{ ds }}/",
                endpoint="/api/users",
                method="GET",
                jmespath_expression="data[:2].{id: id, email: email}",
            )
        run_dag(dag, pendulum.datetime(2023, 10, 1))

    with pytest.raises(BotoClientError, match=r".*NoSuchKey.*"):
        _ = (
            s3_resource.Object(s3_bucket, "source1/entity1/2023-10-01/part0002.jsonl")
            .get()["Body"]
            .read()
            .decode("utf-8")
        )
