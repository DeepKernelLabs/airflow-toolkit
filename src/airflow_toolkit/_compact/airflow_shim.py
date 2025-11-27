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
