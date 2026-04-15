"""
Demo pipeline: JSONPlaceholder → MinIO (raw) → PostgreSQL (ELT pattern)

Source:  https://jsonplaceholder.typicode.com  (public, no auth)
Tables:  posts · comments · users · todos · albums

DAG flow:
    setup (TaskGroup)  — tareas en paralelo
        ensure_datalake_prefix · ensure_database_schemas
        ↓
    extract (TaskGroup)  — 5 entities en paralelo
        check_file (branch) → delete_file → extract_to_datalake
                            ↘              ↗  (if file doesn't exist yet)
        ↓
    load (TaskGroup)  — 5 entities en paralelo
        check_file → drop_partition → load_to_raw
        ↓
    transform (DbtTaskGroup via Cosmos)
        bronze__*  →  silver__*  →  gold__user_activity
"""
from __future__ import annotations

import io
from datetime import timedelta

import pandas as pd
from airflow.decorators import dag
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.task_group import TaskGroup
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior
from cosmos.profiles import PostgresUserPasswordProfileMapping

from airflow_toolkit._compact.airflow_shim import BaseHook
from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory
from airflow_toolkit.providers.deltalake.operators.filesystem_to_database import (
    FilesystemToDatabaseOperator,
)
from airflow_toolkit.providers.filesystem.operators.filesystem import (
    FilesystemCheckOperator,
    FilesystemDeleteOperator,
)
from airflow_toolkit.providers.filesystem.operators.http_to_filesystem import (
    HttpToFilesystem,
)

# ---------------------------------------------------------------------------
# Connection IDs  (pre-configured via AIRFLOW_CONN_* env vars in docker-compose)
# ---------------------------------------------------------------------------
HTTP_CONN_ID = "demo_http"
S3_CONN_ID = "demo_minio"
DB_CONN_ID = "demo_postgres"
BUCKET = "raw"
SOURCE = "jsonplaceholder"
DBT_PROJECT_PATH = "/opt/airflow/dbt/jsonplaceholder"
DBT_EXECUTABLE = "/home/airflow/.local/bin/dbt"

ENTITIES = ["posts", "comments", "users", "todos", "albums"]

# Base path in MinIO — ds is rendered by Airflow at runtime (e.g. 2025/01/15)
# Format: bucket/key  (no s3:// prefix — required by airflow-toolkit filesystem operators)
BASE_PATH = f"{BUCKET}/{SOURCE}/{{{{ ds.replace('-', '/') }}}}"

METADATA = {
    "_ds": "{{ ds }}",
    "_interval_start": "{{ data_interval_start }}",
    "_interval_end": "{{ data_interval_end }}",
    "_loaded_at": "{{ dag_run.start_date.isoformat() }}",
}

# Safe DELETE: only runs if the table already exists
DROP_PARTITION_SQL = """
DO $$
BEGIN
    IF EXISTS (
        SELECT FROM information_schema.tables
        WHERE table_schema = '{{ params.table_schema }}'
          AND table_name   = '{{ params.table_name }}'
    ) THEN
        DELETE FROM {{ params.table_schema }}.{{ params.table_name }}
        WHERE _ds = '{{ ds }}';
    ELSE
        RAISE NOTICE 'Table %.% does not exist yet — skipping partition drop',
            '{{ params.table_schema }}', '{{ params.table_name }}';
    END IF;
END $$;
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def json_to_csv(data: list) -> str:
    """Convert a JSON array (from the API) into a CSV string."""
    buf = io.StringIO()
    pd.DataFrame(data).to_csv(buf, index=False)
    return buf.getvalue()


def s3_key(entity: str, ds: str) -> str:
    """Return the S3 key for a given entity and execution date."""
    return f"{SOURCE}/{ds.replace('-', '/')}/{entity}/part0001.csv"


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
@dag(
    dag_id="demo_jsonplaceholder",
    schedule=None,
    tags=["demo", "elt", "jsonplaceholder"],
    description="JSONPlaceholder API → MinIO → PostgreSQL (raw → bronze → silver → gold)",
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
)
def demo_pipeline():

    # ------------------------------------------------------------------ #
    #  0. Setup — garantías previas al pipeline                           #
    #     Las dos tareas son independientes y corren en paralelo           #
    # ------------------------------------------------------------------ #
    with TaskGroup("setup") as tg_setup:

        def _ensure_datalake_prefix(**_):
            # Verify the bucket is reachable. Prefixes are virtual in S3/MinIO
            # and don't need explicit creation — the bucket is created by minio-init.
            fs = FilesystemFactory.get_data_lake_filesystem(
                connection=BaseHook.get_connection(S3_CONN_ID)
            )
            fs.check_prefix(f"{BUCKET}/")

        ensure_datalake_prefix = PythonOperator(
            task_id="ensure_datalake_prefix",
            python_callable=_ensure_datalake_prefix,
        )

        ensure_database_schemas = SQLExecuteQueryOperator(
            task_id="ensure_database_schemas",
            conn_id=DB_CONN_ID,
            sql="""
                CREATE SCHEMA IF NOT EXISTS raw;
                CREATE SCHEMA IF NOT EXISTS bronze;
                CREATE SCHEMA IF NOT EXISTS silver;
                CREATE SCHEMA IF NOT EXISTS gold;
            """,
            autocommit=True,
        )

        ensure_datalake_prefix >> ensure_database_schemas

    # ------------------------------------------------------------------ #
    #  1. Extract TaskGroup                                                #
    #     For each entity:                                                 #
    #       check_file → delete_file (if exists) → extract                #
    #       check_file →                           extract (if not exists) #
    # ------------------------------------------------------------------ #
    with TaskGroup("extract") as tg_extract:
        for entity in ENTITIES:
            with TaskGroup(entity):
                entity_path = f"{BASE_PATH}/{entity}/"

                def _branch_check_file(entity_name, **context):
                    fs = FilesystemFactory.get_data_lake_filesystem(
                        connection=BaseHook.get_connection(S3_CONN_ID)
                    )
                    ds = context.get("ds") or context["logical_date"].strftime("%Y-%m-%d")
                    file_path = f"{BUCKET}/{s3_key(entity_name, ds)}"
                    if fs.check_file(file_path):
                        return f"extract.{entity_name}.delete_file"
                    return f"extract.{entity_name}.extract_to_datalake"

                check_file = BranchPythonOperator(
                    task_id="check_file",
                    python_callable=_branch_check_file,
                    op_kwargs={"entity_name": entity},
                )

                delete_file = FilesystemDeleteOperator(
                    task_id="delete_file",
                    filesystem_conn_id=S3_CONN_ID,
                    filesystem_path=f"{BASE_PATH}/{entity}/part0001.csv",
                )

                fetch = HttpToFilesystem(
                    task_id="extract_to_datalake",
                    http_conn_id=HTTP_CONN_ID,
                    filesystem_conn_id=S3_CONN_ID,
                    filesystem_path=entity_path,
                    endpoint=f"/{entity}",
                    method="GET",
                    source_format="json",
                    save_format="csv",
                    data_transformation=json_to_csv,
                    # Runs whether delete_file ran or was skipped
                    trigger_rule="none_failed_min_one_success",
                    execution_timeout=timedelta(minutes=5),
                )

                check_file >> [delete_file, fetch]
                delete_file >> fetch

    # ------------------------------------------------------------------ #
    #  2. Load TaskGroup                                                   #
    #     For each entity:                                                 #
    #       check_file → drop_partition → load_to_raw                     #
    # ------------------------------------------------------------------ #
    with TaskGroup("load") as tg_load:

        for entity in ENTITIES:
            with TaskGroup(entity):
                entity_path = f"{BASE_PATH}/{entity}/"

                check_file = FilesystemCheckOperator(
                    task_id="check_file",
                    filesystem_conn_id=S3_CONN_ID,
                    filesystem_path=f"{BASE_PATH}/{entity}/part0001.csv",
                )

                drop_partition = SQLExecuteQueryOperator(
                    task_id="drop_partition",
                    conn_id=DB_CONN_ID,
                    sql=DROP_PARTITION_SQL,
                    params={"table_schema": "raw", "table_name": entity},
                    autocommit=True,
                )

                load_to_raw = FilesystemToDatabaseOperator(
                    task_id="load_to_raw",
                    filesystem_conn_id=S3_CONN_ID,
                    database_conn_id=DB_CONN_ID,
                    filesystem_path=entity_path,
                    db_schema="raw",
                    db_table=entity,
                    source_format="csv",
                    source_format_options={"low_memory": False},
                    table_aggregation_type="append",
                    metadata=METADATA,
                    metadata_columns_in_uppercase=False,
                    include_source_path=True,
                    execution_timeout=timedelta(minutes=10),
                )

                check_file >> drop_partition >> load_to_raw

    # ------------------------------------------------------------------ #
    #  3. Transform — dbt via Cosmos (bronze → silver → gold)             #
    # ------------------------------------------------------------------ #
    dbt_transform = DbtTaskGroup(
        group_id="transform",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=ProfileConfig(
            profile_name="demo_profile",
            target_name="dev",
            profile_mapping=PostgresUserPasswordProfileMapping(
                conn_id=DB_CONN_ID,
                profile_args={"schema": "public"},
            ),
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            test_behavior=TestBehavior.AFTER_ALL,
        ),
        operator_args={"install_deps": True},
    )

    # ------------------------------------------------------------------ #
    #  DAG-level dependencies                                              #
    # ------------------------------------------------------------------ #
    tg_setup >> tg_extract >> tg_load >> dbt_transform


demo_pipeline()
