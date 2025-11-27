import logging
import os
import subprocess
from pathlib import Path

import pytest
import virtualenv

logger = logging.getLogger(__file__)


@pytest.fixture
def project_path():
    return Path(__file__).resolve().parents[2]  # Adjust if needed


@pytest.fixture
def virtual_environment(tmp_path):
    """Create a temp virtualenv and yield its path."""
    venv_dir = tmp_path / "venv"
    virtualenv.cli_run([str(venv_dir)])
    logger.info(f"Created temp virtualenv at {venv_dir}")
    yield venv_dir


def install_package(venv_path: Path, package: str, cwd: Path, af_version: str):
    """Install the given package using `uv` inside the given virtualenv."""
    spec = f"{package}[{af_version}]"
    # pdb.set_trace()
    subprocess.check_call(
        [
            "uv",
            "pip",
            "install",
            "--python",
            f"{venv_path}/bin/python",
            package,
            "-e",
            spec,
        ],
        cwd=cwd,
    )


def make_env(venv_path: Path, airflow_home: Path) -> dict:
    env = os.environ.copy()
    env["PATH"] = f"{venv_path}/bin:" + env["PATH"]
    env["VIRTUAL_ENV"] = str(venv_path)
    env["AIRFLOW_HOME"] = str(airflow_home)
    env["AIRFLOW__CORE__EXECUTOR"] = "SequentialExecutor"
    env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{airflow_home}/airflow.db"

    # Optionally bring over connection environment variables from GitHub Actions
    for var in [
        "AIRFLOW_CONN_DATA_LAKE_TEST",
        "AIRFLOW_CONN_SFTP_TEST",
        "AIRFLOW_CONN_GCP_DATA_LAKE_TEST",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_DEFAULT_REGION",
        "TEST_BUCKET",
        "S3_ENDPOINT_URL",
    ]:
        if var in os.environ:
            env[var] = os.environ[var]

    return env


@pytest.mark.parametrize("af_version", ["airflow2"])
def test_import_package(virtual_environment, project_path, tmp_path, af_version):
    venv_path = virtual_environment
    airflow_home = tmp_path / "af_home"
    airflow_home.mkdir(exist_ok=True)
    env = make_env(venv_path, airflow_home)

    # Install the project
    install_package(
        venv_path, str(project_path), cwd=project_path, af_version=af_version
    )

    # Run import and provider checks inside the temp venv
    def run_python(code: str) -> str:
        return subprocess.check_output(
            [f"{venv_path}/bin/python", "-c", code],
            env=env,
            universal_newlines=True,
            stderr=subprocess.STDOUT,
        )

    try:
        # Entry point check
        result = run_python(
            "from airflow.utils.entry_points import entry_points_with_dist; "
            "print(list(entry_points_with_dist('apache_airflow_provider')))"
        )
        assert "airflow_toolkit.providers.package:get_provider_info" in result

        # ProvidersManager check
        result = run_python(
            "from airflow.providers_manager import ProvidersManager; "
            "pm = ProvidersManager(); "
            "print(pm.providers['airflow-toolkit'].data['package-name'])"
        )
        assert "airflow-toolkit" in result

        # Import a real operator
        result = run_python(
            "from airflow_toolkit.providers.filesystem.operators.http_to_filesystem import HttpToFilesystem; "
            "print('Import Ok')"
        )
        assert "Import Ok" in result

    except subprocess.CalledProcessError as e:
        logger.exception(e)
        pytest.fail(
            f"Failed to import or register the package in clean env: {e.output}"
        )


# TODO, test connection in both env, airflow2 and airflow3
def test_s3_connection_available(virtual_environment, tmp_path):
    airflow_home = tmp_path / "af_home"
    airflow_home.mkdir(exist_ok=True)
    env = make_env(virtual_environment, airflow_home)

    # init DB (if needed explicitly here)
    subprocess.check_call(["airflow", "db", "init"], env=env)

    output = subprocess.check_output(
        ["airflow", "connections", "get", "data_lake_test"],
        env=env,
        universal_newlines=True,
    )

    print("AIRFLOW OUTPUT:", output)  # Helpful for debugging
    assert "data_lake_test" in output
