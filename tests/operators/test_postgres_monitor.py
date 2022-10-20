import datetime
import time
from typing import Callable
from unittest import mock

import pytest
from airflow.models import DAG
from psycopg2 import errors as psycopg2_errors

from pipelines.hooks.postgres import PostgresMonitorHook
from pipelines.operators.exceptions import (
    PostgresMonitorOperatorCommandException,
)
from pipelines.operators.postgres import PostgresMonitorOperator


@pytest.fixture
def args():
    return {
        "owner": "data_discovery",
        "start_date": datetime.datetime(2021, 1, 1),
    }


@pytest.fixture
def dag(args):
    return DAG("test_monitor", default_args=args, schedule_interval="@once")


@pytest.fixture
def monitor_simple(dag):
    return PostgresMonitorOperator(
        task_id="monitor_simple", sql="SELECT 1", work_mem="1GB", dag=dag
    )


@pytest.fixture
def monitor_with_no_sql(dag):
    return PostgresMonitorOperator(task_id="monitor_with_no_sql", sql="", dag=dag)


@pytest.fixture
def monitor_jinja_params(dag):
    return PostgresMonitorOperator(
        task_id="monitor_jinja",
        sql="SELECT 1",
        dag=dag,
        params={"test_param": ""},
    )


@pytest.fixture
def monitor_fail_if_no_result(dag):
    return PostgresMonitorOperator(
        task_id="monitor_fail_if_no_result",
        sql="SELECT 1",
        fail_if_no_result=True,
        dag=dag,
    )


@pytest.fixture
def context(dag):
    return {
        "dag": dag,
        "run_id": "scheduled__2021-01-01T00:00:00+00:00",
        "ti": (mock.Mock(**{"xcom_push.return_value": None})),
    }


def test_init(monitor_simple, monitor_with_no_sql):
    assert isinstance(monitor_simple, PostgresMonitorOperator)
    assert isinstance(monitor_with_no_sql, PostgresMonitorOperator)


@mock.patch.object(PostgresMonitorHook, "run", return_value=42)
@mock.patch.object(time, "perf_counter", side_effect=(39666.66092718, 39691.200262516))
def test_successful_execution(
    mock_ssh: PostgresMonitorHook, mock_time: Callable, context, monitor_simple
):
    xcoms_push = {}
    monitor_simple.execute(context)
    for ti_calls in context["ti"].mock_calls:
        xcoms_push.update({ti_calls.kwargs["key"]: ti_calls.kwargs["value"]})
    assert 42 == xcoms_push["affected_rows"]
    assert 24539.34 == xcoms_push["execution_duration"]


@mock.patch.object(PostgresMonitorHook, "run", return_value=0)
def test_no_result_execution(
    mock_ssh: PostgresMonitorHook, context, monitor_simple, monitor_fail_if_no_result
):
    monitor_simple.execute(context)

    with pytest.raises(PostgresMonitorOperatorCommandException):
        monitor_fail_if_no_result.execute(context)


@mock.patch.object(PostgresMonitorHook, "run", side_effect=psycopg2_errors.SyntaxError)
@mock.patch.object(time, "perf_counter", side_effect=(39666.66092718, 39691.200262516))
def test_failed_execution(
    mock_ssh: PostgresMonitorHook, mock_time: Callable, context, monitor_simple
):
    with pytest.raises(PostgresMonitorOperatorCommandException):
        monitor_simple.execute(context)


def test_rendering_params_are_passed_to_operator(monitor_jinja_params):
    assert "test_param" in monitor_jinja_params.params


@mock.patch.object(PostgresMonitorHook, "run", return_value=42)
def test_custom_workmem(mock_ssh: PostgresMonitorHook, context, monitor_simple):
    monitor_simple.execute(context)

    assert mock_ssh.call_args_list[0].args[0] == "SET work_mem='1GB';"
