"""
shim module for Airflow compatibility

This module centralizes all conditional imports between Airflow 2.x and 3.x.
Code should import core symbols (e.g. BaseOperator, Context, get_current_context) only from this shim.
Internally it inspects the installed Airflow version and redirects to the correct internal packages.
"""

from __future__ import annotations

from typing import TYPE_CHECKING


def _af_major() -> int:
    try:
        from airflow import __version__ as _v
    except Exception:
        return 0
    try:
        return int((_v.split(".", 1))[0])
    except Exception:
        return 0


_M = _af_major()

if _M >= 3:
    from airflow.sdk import DAG as _DAG
    from airflow.sdk import BaseHook as _BaseHook
    from airflow.providers.standard.hooks.filesystem import FSHook as _FSHook
    from airflow.models.connection import Connection as _Connection
    from airflow.sdk.bases.notifier import BaseNotifier as _BaseNotifier
    from airflow.sdk import BaseOperator as _BaseOperator
    from airflow.sdk import (
        BaseSensorOperator as _BaseSensorOperator,
    )
    from airflow.sdk.definitions.context import Context as _Context
    from airflow.providers.standard.operators.python import (
        PythonOperator as _PythonOperator,
    )
    from airflow.providers.standard.operators.bash import BashOperator as _BashOperator
    from airflow.sdk import task as _branch_task

    _branch_task = _branch_task.branch

else:
    from airflow import DAG as _DAG
    from airflow.hooks.base import BaseHook as _BaseHook
    from airflow.decorators import branch_task as _branch_task
    from airflow.hooks.filesystem import FSHook as _FSHook
    from airflow.operators.python import PythonOperator as _PythonOperator
    from airflow.operators.bash import BashOperator as _BashOperator
    from airflow.notifications.basenotifier import BaseNotifier as _BaseNotifier
    from airflow.models.baseoperator import BaseOperator as _BaseOperator
    from airflow.models.connection import Connection as _Connection
    from airflow.sensors.base import BaseSensorOperator as _BaseSensorOperator
    from airflow.utils.context import Context as _Context

DAG = _DAG
BaseHook = _BaseHook
BaseOperator = _BaseOperator
BaseSensorOperator = _BaseSensorOperator
FSHook = _FSHook
Context = _Context
Connection = _Connection
BaseNotifier = _BaseNotifier
PythonOperator = _PythonOperator
BashOperator = _BashOperator
branch_task = _branch_task

is_airflow3 = _M >= 3

# Session utilities — paths differ between Airflow 2 and 3
from airflow.utils.session import create_session  # noqa: E402, F401

try:
    from airflow.utils.db import provide_session  # noqa: E402, F401
except ImportError:

    def provide_session(func):  # noqa: E402
        return func


def run_dag(dag, logical_date):
    """Run dag.test() compatible with both Airflow 2 and 3.

    Airflow 2 uses ``execution_date``, Airflow 3 renamed it to ``logical_date``.

    In Airflow 3, dag.test() requires the DAG to be serialized into the metadata
    DB (via the bundle system) before it can create a DagRun.  When running
    in-memory test DAGs (not loaded from a file bundle) we serialize the DAG
    manually so dag.test() can proceed.
    """
    if is_airflow3:
        from airflow import settings as _af_settings
        from airflow.dag_processing.bundles.manager import DagBundlesManager
        from airflow.dag_processing.collection import update_dag_parsing_results_in_db
        from airflow.serialization.serialized_objects import LazyDeserializedDAG

        session = _af_settings.Session()
        try:
            # Airflow 3 dag.test() needs DagVersion in the DB.  Sync bundles
            # first (provides the FK parent), then register the in-memory DAG.
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
    else:
        return dag.test(execution_date=logical_date)


def get_connection_hook(conn_id: str):
    """Return a hook for *conn_id*, compatible with Airflow 2 and 3.

    Airflow 2: ``Connection.get_hook()`` exists.
    Airflow 3: ``Connection.get_hook()`` was removed; fall back to
    ``BaseHook.get_hook()``.
    """
    conn = _BaseHook.get_connection(conn_id)
    if hasattr(conn, "get_hook"):
        return conn.get_hook()
    return _BaseHook.get_hook(conn_id)


# Improve editor typing without importing both variants at runtime
if TYPE_CHECKING:
    try:
        from airflow.sdk import BaseOperator as _TBase
    except Exception:
        from airflow.models.baseoperator import BaseOperator as _TBase
    BaseOperator = _TBase

    try:
        from airflow.sdk.definitions.context import Context as _TContext
    except Exception:
        from airflow.utils.context import Context as _TContext
    Context = _TContext
