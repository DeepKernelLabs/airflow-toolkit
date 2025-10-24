"""
DAG: data_lake_operator_test
--------------------------------
This DAG tests the DataLake operators with an S3-backed data lake (mocked via S3Mock).
It checks that:
  - Files can be ingested from HTTP into the data lake.
  - Marker files (__SUCCESS__, __FAILURE__) are uploaded.
  - DataLakeCheckOperator validates prefixes and files.
  - DataLakeDeleteOperator cleans up paths.

Requirements
------------
- Requires mock S3 (`adobe/s3mock`) running with bucket `data_lake`.

- Connections:
  data_lake_test → AWS (endpoint_url=http://localhost:9090)
  http_test → HTTP (https://reqres.in)

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
- Local run:
  .venv/bin/python dags_examples/dag_data_lake_operator_test.py

- Or trigger in Airflow:
  uv run airflow dags trigger data_lake_operator_test

Expected Outcomes
-----------------
- HttpToFilesystem writes API response into S3.
- Marker files are created.
- Checks return True/False depending on prefix and file presence.
- Cleanup operators remove test files.
"""

import pendulum

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow_toolkit._compact.airflow_shim import PythonOperator, DAG
from airflow_toolkit.providers.data_lake.operators.data_lake import (
    DataLakeCheckOperator,
    DataLakeDeleteOperator,
)
from airflow_toolkit.providers.filesystem.operators.http_to_filesystem import (
    HttpToFilesystem,
)


def print_res(task_id, expected_result, **context):
    print("\n\n\n-------------------------")
    print("Task id: ", task_id)
    print("Expected result: ", expected_result)
    print("Real result: ", context["ti"].xcom_pull(task_ids=task_id))
    print("-------------------------\n\n\n")
    assert context["ti"].xcom_pull(task_ids=task_id) == expected_result


def create_marker(filename, **context):
    hook = S3Hook(aws_conn_id="data_lake_test")
    key = f"source1/entity1/{context['ds']}/{filename}"
    # upload empty content
    hook.load_string("", key=key, bucket_name="data_lake", replace=True)


with DAG(
    dag_id="data_lake_operator_test",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:
    http_to_filesystem = HttpToFilesystem(
        task_id="test_http_to_data_lake",
        http_conn_id="http_test",
        headers={"x-api-key": "reqres-free-v1"},
        filesystem_conn_id="data_lake_test",
        filesystem_path="data_lake/source1/entity1/{{ ds }}/",
        endpoint="/api/users",
        method="GET",
        jmespath_expression="data[:2].{id: id, email: email}",
    )

    create_success_marker = PythonOperator(
        task_id="create_success_marker",
        python_callable=create_marker,
        op_args=["__SUCCESS__"],
    )

    # # Check correct prefix, no extra file
    res_correct_prefix = DataLakeCheckOperator(
        task_id="test_data_lake_correct_prefix",
        data_lake_conn_id="data_lake_test",
        data_lake_path="data_lake/source1/entity1/{{ ds }}/",
    )
    check_res_correct_prefix = PythonOperator(
        task_id="check_res_correct_prefix",
        python_callable=print_res,
        op_args=["test_data_lake_correct_prefix", True],
    )

    # Check incorrect prefix, no extra file
    res_incorrect_prefix = DataLakeCheckOperator(
        task_id="test_data_lake_incorrect_prefix",
        data_lake_conn_id="data_lake_test",
        data_lake_path="data_lake/source1/entity2/{{ ds }}/",
    )
    check_res_incorrect_prefix = PythonOperator(
        task_id="check_res_incorrect_prefix",
        python_callable=print_res,
        op_args=["test_data_lake_incorrect_prefix", False],
    )

    # Check correct prefix, extra file
    res_correct_prefix_extra_file = DataLakeCheckOperator(
        task_id="test_data_lake_correct_prefix_extra_file",
        data_lake_conn_id="data_lake_test",
        data_lake_path="data_lake/source1/entity1/{{ ds }}/",
        check_specific_filename="__SUCCESS__",
    )
    check_res_correct_prefix_extra_file = PythonOperator(
        task_id="check_correct_extra_file",
        python_callable=print_res,
        op_args=["test_data_lake_correct_prefix_extra_file", True],
    )

    # # Check correct prefix, incorrect extra file
    res_prefix_incorrect_extra_file = DataLakeCheckOperator(
        task_id="test_data_lake_prefix_incorrect_extra_file",
        data_lake_conn_id="data_lake_test",
        data_lake_path="data_lake/source1/entity1/{{ ds }}/",
        check_specific_filename="__FAILURE__",
    )
    check_res_incorrect_extra_file = PythonOperator(
        task_id="check_res_incorrect_extra_file",
        python_callable=print_res,
        op_args=["test_data_lake_prefix_incorrect_extra_file", False],
    )

    # Delete source
    data_lake_delete = DataLakeDeleteOperator(
        task_id="test_data_lake",
        data_lake_conn_id="data_lake_test",
        data_lake_path="data_lake/source1/entity1/{{ ds }}/",
    )

    delete_success_marker = DataLakeDeleteOperator(
        task_id="delete_success_marker",
        data_lake_conn_id="data_lake_test",
        data_lake_path="data_lake/source1/entity1/{{ ds }}/__SUCCESS__",
    )

    (
        http_to_filesystem
        >> [
            res_correct_prefix >> check_res_correct_prefix,
            res_incorrect_prefix >> check_res_incorrect_prefix,
            create_success_marker
            >> res_correct_prefix_extra_file
            >> check_res_correct_prefix_extra_file
            >> delete_success_marker,
            res_prefix_incorrect_extra_file >> check_res_incorrect_extra_file,
        ]
        >> data_lake_delete
    )


if __name__ == "__main__":
    print("Tasks in DAG:", dag.task_ids)
    dag.test(logical_date=pendulum.datetime(2023, 10, 1))
