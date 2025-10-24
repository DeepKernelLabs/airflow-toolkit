"""
DAG: file_system_operator_test
--------------------------------
This DAG tests the Filesystem operators with an S3-backed data lake (mocked via S3Mock).
It verifies that:
  - Data is ingested from HTTP into the filesystem.
  - Marker files (__SUCCESS__, __FAILURE__) are created.
  - FilesystemCheckOperator detects valid/invalid prefixes and files.
  - FilesystemDeleteOperator cleans up the test directory.

Requirements
------------
- Requires `adobe/s3mock` running with bucket `data_lake`.
- Connections:
  data_lake_test → AWS (endpoint_url=http://localhost:9090)
  http_test → HTTP (https://reqres.in)   (optional, if testing SFTP scenarios)

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
   ln -s ./airflow-toolkit/dags_examples ~/airflow/dags/dags_examples

Execution
---------
- Local run:
  .venv/bin/python dags_examples/dag_file_system_operator_test.py

- Or trigger in Airflow:
  uv run airflow dags trigger file_system_operator_test

Expected Outcomes
-----------------
- HttpToFilesystem writes API response into S3.
- Marker files are uploaded.
- Checks return True/False depending on file presence.
- Delete operator removes the test directory.
"""

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow_toolkit._compact.airflow_shim import PythonOperator, DAG
from airflow_toolkit.providers.filesystem.operators.filesystem import (
    FilesystemCheckOperator,
    FilesystemDeleteOperator,
)
from airflow_toolkit.providers.filesystem.operators.http_to_filesystem import (
    HttpToFilesystem,
)


def assert_result(task_id, expected_result, **context):
    """Helper to validate XCom results from check tasks."""
    print("\n\n\n-------------------------")
    print("Task id: ", task_id)
    print("Expected result: ", expected_result)
    print("Real result: ", context["ti"].xcom_pull(task_ids=task_id))
    print("-------------------------\n\n\n")
    return context["ti"].xcom_pull(task_ids=task_id) == expected_result


def create_marker(filename, **context):
    """Upload an empty marker file to the data lake for the given date partition."""
    hook = S3Hook(aws_conn_id="data_lake_test")
    key = f"data_lake/source1/entity1/{context['ds']}/{filename}"
    hook.load_string("", key=key, bucket_name="data_lake", replace=True)


with DAG(
    dag_id="file_system_operator_test",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:
    # Step 1: Write HTTP data to the data lake
    http_to_filesystem_task = HttpToFilesystem(
        task_id="http_to_data_lake",
        http_conn_id="http_test",
        headers={"x-api-key": "reqres-free-v1"},
        filesystem_conn_id="data_lake_test",
        filesystem_path="data_lake/source1/entity1/{{ ds }}/",
        endpoint="/api/users",
        method="GET",
        jmespath_expression="data[:2].{id: id, email: email}",
    )

    # Step 2: Create marker files
    create_success_marker_task = PythonOperator(
        task_id="create_success_marker",
        python_callable=create_marker,
        op_args=["__SUCCESS__"],
    )
    create_failure_marker_task = PythonOperator(
        task_id="create_failure_marker",
        python_callable=create_marker,
        op_args=["__FAILURE__"],
    )

    # Step 3: Run checks
    check_correct_prefix_task = FilesystemCheckOperator(
        task_id="check_correct_prefix",
        filesystem_conn_id="data_lake_test",
        filesystem_path="data_lake/source1/entity1/{{ ds }}/",
    )
    assert_correct_prefix_task = PythonOperator(
        task_id="assert_correct_prefix",
        python_callable=assert_result,
        op_args=["check_correct_prefix", True],
    )

    check_incorrect_prefix_task = FilesystemCheckOperator(
        task_id="check_incorrect_prefix",
        filesystem_conn_id="data_lake_test",
        filesystem_path="data_lake/source1/entity2/{{ ds }}/",
    )
    assert_incorrect_prefix_task = PythonOperator(
        task_id="assert_incorrect_prefix",
        python_callable=assert_result,
        op_args=["check_incorrect_prefix", False],
    )

    check_success_marker_task = FilesystemCheckOperator(
        task_id="check_success_marker",
        filesystem_conn_id="data_lake_test",
        filesystem_path="data_lake/source1/entity1/{{ ds }}/__SUCCESS__",
    )
    assert_success_marker_task = PythonOperator(
        task_id="assert_success_marker",
        python_callable=assert_result,
        op_args=["check_success_marker", True],
    )

    check_failure_marker_task = FilesystemCheckOperator(
        task_id="check_failure_marker",
        filesystem_conn_id="data_lake_test",
        filesystem_path="data_lake/source1/entity1/{{ ds }}/__FAILURE__",
    )
    assert_failure_marker_task = PythonOperator(
        task_id="assert_failure_marker",
        python_callable=assert_result,
        op_args=["check_failure_marker", False],
    )

    # Step 4: Clean up
    delete_files_task = FilesystemDeleteOperator(
        task_id="delete_from_data_lake",
        filesystem_conn_id="data_lake_test",
        filesystem_path="data_lake/source1/entity1/{{ ds }}/",
    )

    # DAG wiring
    (
        http_to_filesystem_task
        >> create_success_marker_task
        >> create_failure_marker_task
        >> [
            check_correct_prefix_task >> assert_correct_prefix_task,
            check_incorrect_prefix_task >> assert_incorrect_prefix_task,
            check_success_marker_task >> assert_success_marker_task,
            check_failure_marker_task >> assert_failure_marker_task,
        ]
        >> delete_files_task
    )


if __name__ == "__main__":
    print("Tasks in DAG:", dag.task_ids)
    dag.test(logical_date=pendulum.datetime(2023, 10, 1))
