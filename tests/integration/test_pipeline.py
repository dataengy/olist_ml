"""Integration: полный граф in-process — dbt build (seeds → staging → marts,
тесты) + ML.

Всё во временной DuckDB (DUCKDB_PATH из conftest) и временном инстансе
Dagster.
Один запуск на модуль (~30–60 с): остальные тесты модуля проверяют его
результат.
"""

from __future__ import annotations

import os

import dagster as dg
import duckdb
import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def pipeline_run(defs):
    with dg.instance_for_test() as instance:
        job = defs.resolve_implicit_global_asset_job_def()
        result = job.execute_in_process(
            instance=instance,
            raise_on_error=False,
        )
        yield result


@pytest.fixture(scope="module")
def warehouse(pipeline_run):
    with duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True) as con:
        yield con


def test_run_succeeds(pipeline_run) -> None:
    failed = [
        e.step_key for e in pipeline_run.all_events if e.is_step_failure
    ]
    assert pipeline_run.success, failed


def test_all_asset_checks_pass(pipeline_run) -> None:
    evaluations = pipeline_run.get_asset_check_evaluations()
    assert evaluations, "ни одной проверки не выполнено"
    failed = [
        f"{e.asset_key.to_user_string()}:{e.check_name}"
        for e in evaluations
        if not e.passed
    ]
    assert not failed, failed


def test_writes_to_isolated_duckdb() -> None:
    assert "olist-ml-test-run-" in os.environ["DUCKDB_PATH"]
    assert os.path.exists(os.environ["DUCKDB_PATH"])


def test_feature_mart_is_built(warehouse) -> None:
    (rows,) = warehouse.sql(
        "select count(*) from marts.mart_order_features",
    ).fetchone()
    assert rows > 1000
    (dupes,) = warehouse.sql(
        "select count(*) - count(distinct order_id)"
        " from marts.mart_order_features",
    ).fetchone()
    assert dupes == 0


def test_python_model_is_built(warehouse) -> None:
    days, anomalies = warehouse.sql(
        "select count(*), sum(is_anomaly)"
        " from marts.mart_daily_order_anomalies",
    ).fetchone()
    assert days > 300
    assert 0 < anomalies < days


def test_ml_outputs(pipeline_run) -> None:
    evaluation = pipeline_run.output_for_node("model_evaluation")
    assert 0.5 < evaluation["roc_auc"] <= 1.0
    dataset = pipeline_run.output_for_node("training_dataset")
    assert len(dataset["train"]) > len(dataset["holdout"]) > 0
