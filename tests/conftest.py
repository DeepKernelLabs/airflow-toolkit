import os
import sys
from pathlib import Path
import uuid
import subprocess

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pendulum
import pytest

import json

from airflow_toolkit._compact.airflow_shim import (
    Connection,
    DAG,
    create_session,
    is_airflow3,
    provide_session,
)


# ---------- Paths ----------
@pytest.fixture
def project_path() -> Path:
    return Path(__file__).parent.parent.absolute()


@pytest.fixture
def source_path(project_path) -> Path:
    return project_path / "src"


@pytest.fixture
def tests_path(project_path) -> Path:
    return project_path / "tests"


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
    # export AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
    try:
        import connexion  # noqa: F401

        monkeypatch.setenv(
            "AIRFLOW__CORE__AUTH_MANAGER",
            "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
        )
    except ImportError:
        pass

    monkeypatch.setenv("AIRFLOW__CORE__STORE_SERIALIZED_DAGS", "False")
    monkeypatch.setenv("AIRFLOW__CORE__STORE_DAG_CODE", "False")
    monkeypatch.setenv("AIRFLOW__CORE__EXECUTOR", "SequentialExecutor")
    # Initialize DB correctly for AF2 or AF3
    airflow_bin = Path(sys.executable).parent / "airflow"
    if is_airflow3:
        subprocess.run([str(airflow_bin), "db", "migrate"], check=True)
        # Airflow 3 caches the DB connection at module-import time, so we must
        # re-initialise the ORM after monkeypatching the connection URL.
        import airflow.settings as _af_settings

        _af_settings.configure_vars()
        _af_settings.configure_orm(disable_connection_pool=True)
    else:
        subprocess.run([str(airflow_bin), "db", "reset", "-y"], check=True)
        # Airflow 2 also caches the DB connection at import time; force reconnect.
        import airflow.settings as _af_settings

        _af_settings.configure_vars()
        _af_settings.configure_orm(disable_connection_pool=True)

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
        run_dag(dag, pendulum.datetime(YYYY, M, D))
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
    bucket_name = os.environ["TEST_BUCKET"]
    bucket = s3_resource.Bucket(bucket_name)
    try:
        bucket.objects.all().delete()
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchBucket":
            bucket.create()
        else:
            raise
    return bucket_name


@pytest.fixture
def s3_resource():
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    if not endpoint_url:
        pytest.skip("S3_ENDPOINT_URL not set — skipping S3 integration test")
    return boto3.resource("s3", endpoint_url=endpoint_url)


@pytest.fixture
def s3_client():
    endpoint_url = os.environ["S3_ENDPOINT_URL"]
    s3_config = {
        "addressing_style": "path",
        "use_expect_header": False,
    }
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        config=Config(s3=s3_config),
    )
    # return boto3.client('s3', endpoint_url=endpoint_url)


@pytest.fixture
def local_fs_conn_params(tmp_path: Path) -> dict[str, str | dict[str, str]]:
    return {
        "conn_type": "fs",
        "extra": {
            "path": str(tmp_path),
        },
    }


# -----
@pytest.fixture
def sqlite_database(tmp_path: Path) -> Path:
    db = tmp_path / "test_db.sqlite"
    db.touch()
    return db


@pytest.fixture
def sqlite_connection(sqlite_database: Path) -> str:
    abs_path = sqlite_database.resolve().as_posix()
    with create_session() as session:
        existing = (
            session.query(Connection)
            .filter(Connection.conn_id == "sqlite_test")
            .one_or_none()
        )
        if existing is None:
            session.add(
                Connection(conn_id="sqlite_test", conn_type="sqlite", host=abs_path)
            )
        else:
            existing.conn_type = "sqlite"
            existing.host = abs_path
            existing.schema = existing.login = existing.password = None
            existing.port = None
            existing.extra = None
        session.commit()
    return abs_path  # not used by the operator; you’ll use the conn_id


@pytest.fixture
def local_fs_connection(tmp_path: Path) -> Path:
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
            session.add(
                Connection(conn_id="local_fs_test", conn_type="fs", extra=extra)
            )
        else:
            existing.conn_type = "fs"
            existing.extra = extra
        session.commit()

    return dl_root
