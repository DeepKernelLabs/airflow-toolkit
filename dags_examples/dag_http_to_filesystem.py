"""
DAG: http_to_filesystem_test
--------------------------------
This DAG tests HttpToFilesystem by fetching from an HTTP API and writing to an S3-backed data lake (via S3Mock).
It validates that:
  - HTTP requests can be issued to a test API.
  - Responses are filtered with JMESPath.
  - Transformed data is saved into the filesystem (S3).

Requirements
------------
- Docker: adobe/s3mock
- Connections:
  data_lake_test → AWS (endpoint_url=http://localhost:9090)
  http_test → HTTP (host=https://reqres.in)

- Environment variables:
  export AIRFLOW_CONN_DATA_LAKE_TEST='{"conn_type": "aws", "extra": {"endpoint_url": "http://localhost:9090"}}'
  export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
  export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  export AWS_DEFAULT_REGION=us-east-1
  export TEST_BUCKET=data_lake
  export S3_ENDPOINT_URL=http://localhost:9090
  export AIRFLOW_CONN_HTTP_TEST='{"conn_type": "http","host": "https://reqres.in"}'

- Airflow connections to create:
  uv run airflow connections add data_lake_test --conn-type aws --conn-extra '{"endpoint_url": "http://localhost:9090"}'
  uv run airflow connections add http_test --conn-type http --conn-host https://reqres.in

Setup Instructions
------------------
1. Start mock S3:
   docker run -d --name test-s3mock --rm -p 9090:9090 -e initialBuckets=data_lake -e debug=true -t adobe/s3mock

2. Configure awscli:
   aws configure set default.s3.addressing_style path
   aws configure set default.s3.use_expect_header false

3. Reset Airflow if needed:
   uv run airflow db init #for airflow2
   uv run airflow db migrate #for airflow3

4. Symlink DAG into airflow:
   ln -s <absolute_path_to_project>/a_testing ~/airflow/dags/a_testing

Execution
---------
- Run DAG in local test mode:
  .venv/bin/python a_testing/dag_http_to_filesystem_test.py

- Or trigger from Airflow UI / CLI:
  uv run airflow dags trigger http_to_filesystem_test

Expected Outcomes
-----------------
- HttpToFilesystem sends a GET to https://reqres.in/api/users.
- Response is filtered (`data[:2].{id: id, email: email}`).
- Resulting JSON is saved in `data_lake/source1/entity1/{{ ds }}/` on mock S3.
"""

import pendulum
from airflow.operators.empty import EmptyOperator

from airflow_toolkit.providers.filesystem.operators.http_to_filesystem import (
    HttpToFilesystem,
)
from airflow_toolkit._compact.airflow_shim import DAG


with DAG(
    dag_id="http_to_filesystem_test",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")
    HttpToFilesystem(
        task_id="test_http_to_data_lake",
        http_conn_id="http_test",
        headers={"x-api-key": "reqres-free-v1"},
        filesystem_conn_id="data_lake_test",
        filesystem_path="data_lake/source1/entity1/{{ ds }}/",
        endpoint="/api/users",
        method="GET",
        jmespath_expression="data[:2].{id: id, email: email}",
    )
    end = EmptyOperator(task_id="end")


if __name__ == "__main__":
    dag.test(logical_date=pendulum.datetime(2023, 10, 1))
