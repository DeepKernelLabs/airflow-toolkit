"""
shim module for Airflow compatibility

This module centralizes all conditional imports between Airflow 2.x and 3.x.
Code should import core symbols (e.g. BaseOperator, Context, get_current_context) only from this shim.
Internally it inspects the installed Airflow version and redirects to the correct internal packages.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable


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
    from airflow.operators.python import get_current_context as _get_current_context
    from airflow.sdk import Connection as _Connection
    from airflow.sdk.BaseHook import BaseHook as _BaseHook
    from airflow.sdk.core.baseoperator import BaseOperator as _BaseOperator
    from airflow.sdk.core.basesensoroperator import (
        BaseSensorOperator as _BaseSensorOperator,
    )
    from airflow.sdk.definitions.context import Context as _Context
    from airflow.sdk.definitions.trigger_rule import TriggerRule as _TriggerRule
else:
    from airflow.hooks.base import BaseHook as _BaseHook
    from airflow.models.baseoperator import BaseOperator as _BaseOperator
    from airflow.models.connection import Connection as _Connection  # noqa: F401
    from airflow.operators.python import get_current_context as _get_current_context
    from airflow.sensors.base import BaseSensorOperator as _BaseSensorOperator
    from airflow.utils.context import Context as _Context
    from airflow.utils.trigger_rule import TriggerRule as _TriggerRule


BaseOperator = _BaseOperator
BaseSensorOperator = _BaseSensorOperator
BaseHook = _BaseHook
Context = _Context
Connection = _Connection
TriggerRule = _TriggerRule
get_current_context: Callable[[], _Context] = _get_current_context

is_airflow3 = _M >= 3

# Improve editor typing without importing both variants at runtime
if TYPE_CHECKING:
    try:
        from airflow.sdk.core.baseoperator import BaseOperator as _TBase
    except Exception:
        from airflow.models.baseoperator import BaseOperator as _TBase
    BaseOperator = _TBase

    try:
        from airflow.sdk.definitions.context import Context as _TContext
    except Exception:
        from airflow.utils.context import Context as _TContext
    Context = _TContext
