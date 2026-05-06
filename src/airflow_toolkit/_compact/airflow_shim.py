"""Airflow 3 imports centralised for airflow-toolkit."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.sdk import DAG  # noqa: F401
from airflow.sdk import BaseHook  # noqa: F401
from airflow.providers.standard.hooks.filesystem import FSHook  # noqa: F401
from airflow.models.connection import Connection  # noqa: F401
from airflow.sdk.bases.notifier import BaseNotifier  # noqa: F401
from airflow.sdk import BaseOperator  # noqa: F401
from airflow.sdk import BaseSensorOperator  # noqa: F401
from airflow.sdk.definitions.context import Context  # noqa: F401
from airflow.providers.standard.operators.python import PythonOperator  # noqa: F401
from airflow.providers.standard.operators.bash import BashOperator  # noqa: F401
from airflow.sdk import task as _task
from airflow.utils.session import create_session  # noqa: F401
from airflow.utils.db import provide_session  # noqa: F401

branch_task = _task.branch

is_airflow3 = True


def run_dag(dag, logical_date):
    """Register an in-memory DAG and run dag.test() on Airflow 3.

    dag.test() requires the DAG to be serialised into the metadata DB (via the
    bundle system) before it can create a DagRun. In-memory test DAGs are not
    loaded from a file bundle, so we serialise manually first.
    """
    from airflow import settings as _af_settings
    from airflow.dag_processing.bundles.manager import DagBundlesManager
    from airflow.dag_processing.collection import update_dag_parsing_results_in_db
    from airflow.serialization.serialized_objects import LazyDeserializedDAG

    session = _af_settings.Session()
    try:
        manager = DagBundlesManager()
        manager.sync_bundles_to_db(session=session)
        session.commit()
        update_dag_parsing_results_in_db(
            bundle_name="dags-folder",
            bundle_version=None,
            dags=[LazyDeserializedDAG.from_dag(dag)],
            import_errors={},
            parse_duration=None,
            warnings=set(),
            session=session,
        )
        session.commit()
    finally:
        session.close()
    return dag.test(logical_date=logical_date)


def get_connection_hook(conn_id: str):
    """Return a hook for *conn_id*."""
    return BaseHook.get_hook(conn_id)


if TYPE_CHECKING:
    pass
