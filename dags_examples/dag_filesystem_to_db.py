"""
DAG: filesystem_to_db_local_test
--------------------------------
This DAG tests FilesystemToDatabaseOperator with a local CSV and SQLite.
It validates that:
  - A file can be read from the filesystem.
  - Data is loaded into a database table.
  - Metadata fields (_DS, _INTERVAL_START, _INTERVAL_END, _LOADED_AT) are attached.

Requirements
------------
- No containers required (uses local CSV + SQLite).
- Connections:
  sqlite_test → SQLite (host=<absolute_path_to_project>/dags_examples/output.sqlite)
  local_fs_test → Filesystem (path=<absolute_path_to_project>/dags_examples)

- Environment variables:
  export AIRFLOW_CONN_SQLITE_TEST='{"conn_type": "sqlite", "host": "<absolute_path_to_project>/dags_examples/output.sqlite"}'
  export AIRFLOW_CONN_LOCAL_FS_TEST='{"conn_type": "fs", "extra": {"path": "<absolute_path_to_project>/dags_examples"}}'

- Airflow connections to create:
  uv run airflow connections add sqlite_test --conn-type sqlite --conn-host <absolute_path_to_project>/dags_examples/output.sqlite
  uv run airflow connections add local_fs_test --conn-type fs --conn-extra '{"path": "<absolute_path_to_project>/dags_examples"}'

Setup Instructions
------------------
1. Place a test CSV file in the dags_examples directory:
   echo -e "a,b\n1,x\n2,y\n3,z" > ./dags_examples/test.csv

2. Reset Airflow if needed:
   uv run airflow db init     # for airflow2
   uv run airflow db migrate  # for airflow3

3. Symlink DAG into airflow:
   ln -s ./airflow-toolkit/dags_examples ~/airflow/dags/dags_examples

Execution
---------
- Prepare CSV:
  echo -e "a,b\n1,x\n2,y\n3,z" > ./dags_examples/test.csv
- Run locally:
  .venv/bin/python dags_examples/dag_filesystem_to_db_local_test.py
- Or trigger in Airflow:
  uv run airflow dags trigger filesystem_to_db_local_test

Expected Outcomes
-----------------
- CSV is read from filesystem.
- Data is inserted into SQLite table `test_table`.
- Rows can be queried back successfully.
"""

from __future__ import annotations

import io
import os
import pendulum
import pandas as pd

from airflow_toolkit.providers.deltalake.operators.filesystem_to_database import (
    FilesystemToDatabaseOperator,
)
from airflow_toolkit._compact.airflow_shim import (
    Context,
    BaseHook,
    BaseOperator,
    DAG,
    is_airflow3,
)
from airflow_toolkit.filesystems.filesystem_factory import FilesystemFactory
from sqlalchemy.engine import URL, create_engine

if is_airflow3:
    from airflow.providers.standard.operators.empty import EmptyOperator
else:
    from airflow.operators.empty import EmptyOperator


class DebugFilesystemOperator(BaseOperator):
    """Simple operator to test filesystem connection and read a file."""

    template_fields = ("filesystem_conn_id", "filepath")

    def __init__(self, filesystem_conn_id: str, filepath: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filesystem_conn_id = filesystem_conn_id
        self.filepath = filepath

    def execute(self, context: Context):
        # Resolve connection
        conn = BaseHook.get_connection(self.filesystem_conn_id)
        fs = FilesystemFactory.get_data_lake_filesystem(conn)

        # Read raw bytes
        raw = fs.read(self.filepath)
        df = pd.read_csv(io.BytesIO(raw))
        print("=== DataFrame Preview ===")
        print(df)
        return df


class DebugDatabaseOperator(BaseOperator):
    """Simple operator to test database connection and insert a dataframe."""

    template_fields = ("database_conn_id", "table_name")

    def __init__(self, database_conn_id: str, table_name: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.database_conn_id = database_conn_id
        self.table_name = table_name

    def execute(self, context: Context):
        # Resolve connection
        conn = BaseHook.get_connection(self.database_conn_id)

        if conn.conn_type == "sqlite" or conn.conn_type == "generic":
            print(30 * "======")
            print("conn.host:", conn.host)
            print("conn.schema:", conn.schema)
            print("conn.get_uri:", conn.get_uri())
            db_path = conn.host
            url = URL.create("sqlite", database=db_path)
        else:
            url = URL.create(
                conn.conn_type,
                username=conn.login,
                password=conn.password,
                host=conn.host,
                port=conn.port,
                database=conn.schema,
            )

        engine = create_engine(url)

        # Create sample dataframe
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        # Write to DB
        df.to_sql(self.table_name, engine, if_exists="replace", index=False)

        # Read back
        df2 = pd.read_sql(f"SELECT * FROM {self.table_name}", engine)

        print("=== Written & Readback from DB ===")
        print(df2)
        print("Connecting to DB:", url)

        return df2


# --- Local paths ---
CURRENT_DIR = "./dags_examples/"
CSV_PATH = os.path.join(CURRENT_DIR, "test.csv")
DB_PATH = os.path.join(CURRENT_DIR, "output.sqlite")


# --- DAG definition ---
with DAG(
    dag_id="filesystem_to_db_local_test",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:
    start = EmptyOperator(task_id="start")

    fs_to_db = FilesystemToDatabaseOperator(
        task_id="filesystem_to_database_test_",
        filesystem_conn_id="local_fs_test",
        database_conn_id="sqlite_test",
        filesystem_path=CSV_PATH,
        db_table="test_table",
        source_format="csv",
        table_aggregation_type="replace",
        metadata={
            "_DS": "{{ ds }}",
            "_INTERVAL_START": "{{ data_interval_start }}",
            "_INTERVAL_END": "{{ data_interval_end }}",
            "_LOADED_AT": "{{ data_interval_end }}",
        },
    )

    end = EmptyOperator(task_id="end")

    start >> fs_to_db >> end


if __name__ == "__main__":
    dag.test(logical_date=pendulum.datetime(2023, 10, 1))
