import json
import textwrap
from pathlib import Path
import numpy as np
import pendulum
from airflow.hooks.base import BaseHook

from airflow_toolkit.providers.deltalake.operators.filesystem_to_database import (
    FilesystemToDatabaseOperator,
)


def _write_csv_for_ds(root: Path, exec_date: pendulum.DateTime, filename: str = "test.csv") -> Path:
    folder = root 
    folder.mkdir(parents=True, exist_ok=True)
    (folder / filename).write_text(textwrap.dedent("""\
        a,b,c
        1,2,3
        4,5,6
        7,8,9
    """))
    return folder

def test_source_file_to_database(dag, sa_session, tmp_path, sqlite_connection, local_fs_connection):
    execution_date = pendulum.datetime(2023, 10, 1)

    fs_root = Path(BaseHook.get_connection("local_fs_test").extra_dejson["path"])
    folder = _write_csv_for_ds(fs_root, execution_date)
    loaded_at = pendulum.now()
    # sanity: don’t over-constrain sqlite URI, just ensure it’s sqlite
    assert BaseHook.get_connection("local_fs_test").extra_dejson["path"] == str(folder)
    assert BaseHook.get_connection("sqlite_test").conn_type == "sqlite"

    src = BaseHook.get_connection("sqlite_test").get_hook()
    src.run(sql=[
        """
        CREATE TABLE IF NOT EXISTS test_table (
            a INTEGER, b INTEGER, c INTEGER,
            _DS TEXT, _INTERVAL_START TEXT, _INTERVAL_END TEXT,
            _LOADED_AT TEXT, _LOADED_FROM TEXT
        )
        """
    ])
    with dag:
        FilesystemToDatabaseOperator(
            filesystem_conn_id="local_fs_test",
            database_conn_id="sqlite_test",
            filesystem_path=str(folder),
            db_table="test_table",
            task_id="filesystem_to_database_test",
            metadata={
                "_DS": "{{ ds }}",
                "_INTERVAL_START": "{{ data_interval_start }}",
                "_INTERVAL_END": "{{ data_interval_end }}",
                "_LOADED_AT": loaded_at.isoformat(),
            },
        )

    dag.test(execution_date=execution_date, session=sa_session)

    df = src.get_pandas_df(
        "SELECT * FROM test_table",
        parse_dates=["_DS", "_INTERVAL_START", "_INTERVAL_END", "_LOADED_AT"],
    )

    # Example of expected result
    #    a  b  c                         _DS            _INTERVAL_START              _INTERVAL_END                        _LOADED_AT       _LOADED_FROM
    # 0  1  2  3  2023-10-01 00:00:00.000000  2023-09-30T00:00:00+00:00  2023-10-01T00:00:00+00:00  2024-10-25T12:54:13.211896+02:00  /tmp/.../test.csv
    # 1  4  5  6  2023-10-01 00:00:00.000000  2023-09-30T00:00:00+00:00  2023-10-01T00:00:00+00:00  2024-10-25T12:54:13.211896+02:00  /tmp/.../test.csv
    # 2  7  8  9  2023-10-01 00:00:00.000000  2023-09-30T00:00:00+00:00  2023-10-01T00:00:00+00:00  2024-10-25T12:54:13.211896+02:00  /tmp/.../test.csv
    

    assert len(df) == 3
    assert str(df.iloc[0]["_DS"]) == execution_date.to_datetime_string()
    assert (
        str(df.iloc[0]["_INTERVAL_START"])
        == (execution_date - pendulum.duration(days=1)).to_datetime_string()
    )
    assert str(df.iloc[0]["_INTERVAL_END"]) == execution_date.to_datetime_string()
    assert df.iloc[0]["_LOADED_FROM"] == str(folder / "test.csv")
    assert (
        str(df.iloc[0]["_LOADED_AT"])
        == loaded_at.isoformat().replace("T", " ").split("+")[0]
    )


def test_source_file_with_less_columns_that_database(dag, sa_session, sqlite_connection, local_fs_connection):
    """
    Check behavior when source file has less columns than the database table.
    Example:
        - Source file has 3 columns ([a, b, c] + metadata)
        - Table in the database has 4 columns ([a, b, c, d] + metadata)

    The FilesystemToDatabaseOperator should add the columns using a null value on columns
    not defined in the csv file.
    """

    exec_date = pendulum.datetime(2023, 10, 1)
    folder = _write_csv_for_ds(Path(BaseHook.get_connection("local_fs_test").extra_dejson["path"]), exec_date)

    sqlite_hook = BaseHook.get_connection("sqlite_test").get_hook()
    sqlite_hook.run(
        sql=[
            'CREATE TABLE test_csv_with_less_columns_that_database (a int, b int, c int, d int, "_DS" date);',
            'INSERT INTO test_csv_with_less_columns_that_database VALUES (0, 0, 0, 0, "2024-07-31 00:00:00.000000");',
        ]
    )

    with dag:
        FilesystemToDatabaseOperator(
            filesystem_conn_id="local_fs_test",
            database_conn_id="sqlite_test",
            filesystem_path=str(folder),
            db_table="test_csv_with_less_columns_that_database",
            task_id="filesystem_to_database_test",
            metadata={"_DS": "{{ ds }}"},
            include_source_path=False,
        )

    dag.test(execution_date=exec_date, session=sa_session)

    df = sqlite_hook.get_pandas_df(
        sql="SELECT * FROM test_csv_with_less_columns_that_database"
    )

    # Expected result
    #    a  b  c    d                         _DS
    # 0  0  0  0  0.0  2024-07-31 00:00:00.000000  # Original row
    # 1  1  2  3  NaN  2023-10-01 00:00:00.000000  # \
    # 2  4  5  6  NaN  2023-10-01 00:00:00.000000  #  |> Added from csv
    # 3  7  8  9  NaN  2023-10-01 00:00:00.000000  # /

    assert set(df.columns) == {"a", "b", "c", "d", "_DS"}
    assert len(df) == 4
    assert df.iloc[0].d == 0
    assert all([np.isnan(i) for i in [df.iloc[1].d, df.iloc[2].d, df.iloc[3].d]])

def test_source_file_with_more_columns_than_database(dag, sa_session, sqlite_connection, local_fs_connection):
    """
    Check behavior when source file has more columns than the database table.
    Example:
        - Source file has 3 columns ([a, b, c] + metadata)
        - Table in the database has 2 columns ([a, b] + metadata)

    The FilesystemToDatabaseOperator should add the columns using a null value on columns
    not defined in the source file.
    """

    exec_date = pendulum.datetime(2023, 10, 1)
    folder = _write_csv_for_ds(Path(BaseHook.get_connection("local_fs_test").extra_dejson["path"]), exec_date)

    sqlite_hook = BaseHook.get_connection("sqlite_test").get_hook()
    sqlite_hook.run(
        sql=[
            'CREATE TABLE test_csv_with_more_columns_than_database (a int, b int, "_DS" date);',
            'INSERT INTO test_csv_with_more_columns_than_database VALUES (0, 0, "2024-07-31 00:00:00.000000");',
        ]
    )

    with dag:
        FilesystemToDatabaseOperator(
            filesystem_conn_id="local_fs_test",
            database_conn_id="sqlite_test",
            filesystem_path=str(folder),
            db_table="test_csv_with_more_columns_than_database",
            task_id="filesystem_to_database_test",
            metadata={"_DS": "{{ ds }}"},
            include_source_path=False,
        )

    dag.test(execution_date=exec_date, session=sa_session)

    df = sqlite_hook.get_pandas_df(sql="SELECT * FROM test_csv_with_more_columns_than_database")

    # Expected result
    #    a  b                         _DS    c
    # 0  0  0  2024-07-31 00:00:00.000000  NaN  # Original row
    # 1  1  2  2023-10-01 00:00:00.000000  3.0  # \
    # 2  4  5  2023-10-01 00:00:00.000000  6.0  #  |> Added from csv
    # 3  7  8  2023-10-01 00:00:00.000000  9.0  # /

    assert set(df.columns) == {"a", "b", "c", "_DS"}
    assert len(df) == 4
    assert np.isnan(df.iloc[0].c)


def test_source_file_and_database_with_different_columns(dag, sa_session, sqlite_connection, local_fs_connection):
    """
    Check behavior when source file has columns not present in the database and the source
    file has columns not present in source file.
    Example:
        - Source file has 3 columns ([a, b, c] + metadata)
        - Table in the database has 2 columns ([a, d] + metadata)

    The FilesystemToDatabaseOperator should add the columns using a null value on columns
    not defined in the source file.
    """
    exec_date = pendulum.datetime(2023, 10, 1)
    folder = _write_csv_for_ds(Path(BaseHook.get_connection("local_fs_test").extra_dejson["path"]), exec_date)

    sqlite_hook = BaseHook.get_connection("sqlite_test").get_hook()
    sqlite_hook.run(
        sql=[
            "CREATE TABLE test_csv_with_more_columns_than_database (a int, d int, _DS date);",
            'INSERT INTO test_csv_with_more_columns_than_database VALUES (0, 0, "2024-07-31 00:00:00.000000");',
        ]
    )

    with dag:
        FilesystemToDatabaseOperator(
            filesystem_conn_id="local_fs_test",
            database_conn_id="sqlite_test",
            filesystem_path=str(folder),
            db_table="test_csv_with_more_columns_than_database",
            task_id="filesystem_to_database_test",
            metadata={"_DS": "{{ ds }}"},
            include_source_path=False,
        )

    dag.test(execution_date=exec_date, session=sa_session)

    df = sqlite_hook.get_pandas_df(sql="SELECT * FROM test_csv_with_more_columns_than_database")

    # Expected result
    #    a    d                         _DS    c    b
    # 0  0  0.0  2024-07-31 00:00:00.000000  NaN  NaN # Original row
    # 1  1  NaN  2023-10-01 00:00:00.000000  3.0  2.0  # \
    # 2  4  NaN  2023-10-01 00:00:00.000000  6.0  5.0  # |> Added from csv
    # 3  7  NaN  2023-10-01 00:00:00.000000  9.0  8.0  # /

    assert set(df.columns) == {"a", "b", "c", "d", "_DS"}
    assert len(df) == 4
    assert all([np.isnan(i) for i in [df.iloc[0].b, df.iloc[0].c]])  # Original row
    assert all(
        [np.isnan(i) for i in [df.iloc[1].d, df.iloc[2].d, df.iloc[3].d]]
    )  # Added from source file
