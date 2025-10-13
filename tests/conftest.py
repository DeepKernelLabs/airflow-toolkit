import os
from pathlib import Path
import uuid
import subprocess

import boto3
import pendulum
import pytest

from airflow import DAG
from airflow.utils.db import provide_session

import json
from airflow.models import Connection
from airflow.utils.session import create_session

# ---------- Paths ----------
@pytest.fixture
def project_path() -> Path:
    return Path(__file__).parent.parent.absolute()


@pytest.fixture
def source_path(project_path) -> Path:
    return project_path / 'src'


@pytest.fixture
def tests_path(project_path) -> Path:
    return project_path / 'tests'


@pytest.fixture
def load_airflow_test_config() -> Path:
    from airflow.configuration import conf

    return conf.load_test_config()

# ---------- Airflow DB isolation ----------
@pytest.fixture(autouse=True)
def fresh_airflow_db_per_test(tmp_path, monkeypatch):
    """
    Full Airflow + FAB schema, brand-new per test.
    Uses a per-test AIRFLOW_HOME and sqlite file DB,
    then runs `airflow db reset -y`.
    """
    home = tmp_path / "af_home"
    home.mkdir(parents=True, exist_ok=True)
    db_path = home / "airflow.db"

    # Minimal, isolated config
    monkeypatch.setenv("AIRFLOW_HOME", str(home))
    monkeypatch.setenv("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
    monkeypatch.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    monkeypatch.setenv("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", f"sqlite:///{db_path}")
    # Make sure we don't accidentally inherit any external config
    monkeypatch.delenv("AIRFLOW__CORE__SQL_ALCHEMY_CONN", raising=False)

    # Initialize a clean DB with all tables (Airflow + FAB)
    subprocess.run(["airflow", "db", "reset", "-y"], check=True)

    yield
    # nothing to teardown; tmp_path is cleaned by pytest

@pytest.fixture
def sa_session():
    @provide_session
    def create_airflow_session(session=None):
        return session

    session = create_airflow_session()
    yield session
    session.expunge_all()
    session.close()


@pytest.fixture
def dag():
    """
    Fresh DAG per test with a unique dag_id so .test() runs never collide.
    Use as:
        with dag:
            ...
        dag.test(execution_date=pendulum.datetime(YYYY, M, D))
    """
    dag_id = f"test_{uuid.uuid4().hex[:8]}"
    default_args = {"start_date": pendulum.datetime(2023, 1, 1)}
    with DAG(dag_id=dag_id, schedule="@daily", default_args=default_args) as d:
        yield d
    # with DAG(
    #     dag_id='test-dag',
    #     schedule="@daily",
    #     start_date=pendulum.datetime(2020, 1, 1),
    #     catchup=False,
    # ) as dag:
    #     yield dag


@pytest.fixture
def s3_bucket(s3_resource):
    """Delete all objects in the specified S3 bucket."""
    bucket_name = os.environ['TEST_BUCKET']
    bucket = s3_resource.Bucket(bucket_name)
    bucket.objects.all().delete()
    return bucket_name


@pytest.fixture
def s3_resource():
    endpoint_url = os.environ['S3_ENDPOINT_URL']
    return boto3.resource('s3', endpoint_url=endpoint_url)


@pytest.fixture
def s3_client():
    endpoint_url = os.environ['S3_ENDPOINT_URL']
    return boto3.client('s3', endpoint_url=endpoint_url)


@pytest.fixture
def local_fs_conn_params(tmp_path):
    return {
        'conn_type': 'fs',
        'extra': {
            'path': str(tmp_path),
        },
    }

# -----
@pytest.fixture
def sqlite_database(tmp_path: Path):
    db = tmp_path / "test_db.sqlite"
    db.touch()
    return db

@pytest.fixture
def sqlite_connection(sqlite_database: Path):
    abs_path = sqlite_database.resolve().as_posix()
    with create_session() as session:
        existing = (
            session.query(Connection)
            .filter(Connection.conn_id == "sqlite_test")
            .one_or_none()
        )
        if existing is None:
            session.add(Connection(conn_id="sqlite_test", conn_type="sqlite", host=abs_path))
        else:
            existing.conn_type = "sqlite"
            existing.host = abs_path
            existing.schema = existing.login = existing.password = None
            existing.port = None
            existing.extra = None
        session.commit()
    return abs_path  # not used by the operator; you’ll use the conn_id


@pytest.fixture
def local_fs_connection(tmp_path: Path):
    """Ensure conn_id=local_fs_test exists and points to <tmp>/data_lake."""
    dl_root = tmp_path / "data_lake"
    dl_root.mkdir(parents=True, exist_ok=True)

    extra = json.dumps({"path": str(dl_root)})
    with create_session() as session:
        existing = (
            session.query(Connection)
            .filter(Connection.conn_id == "local_fs_test")
            .one_or_none()
        )
        if existing is None:
            session.add(Connection(conn_id="local_fs_test", conn_type="fs", extra=extra))
        else:
            existing.conn_type = "fs"
            existing.extra = extra
        session.commit()

    return dl_root