"""E2E: настоящий `dagster dev` в подпроцессе, запуск всех ассетов по
GraphQL, reload локации.

Помечен e2e и по умолчанию не запускается: `pytest -m e2e` (поднимает
webserver + демон + код-локацию, 1–3 минуты).

`dagster dev -m olist_ml.definitions` из временного cwd, а не `dg dev` из
корня: Dagster подхватывает .env из cwd и перебил бы им временный DUCKDB_PATH
из conftest.
Наш загрузчик (olist_ml.defs.env) читает .env-файлы по абсолютному пути от
корня проекта с override=False, поэтому остальные переменные приходят как
обычно.
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess
import time
from pathlib import Path

import pytest
import requests

from tests.conftest import DAGSTER_BIN

pytestmark = pytest.mark.e2e


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait(predicate, timeout: float, what: str, step: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception:  # noqa: BLE001 — сервер ещё поднимается
            pass
        time.sleep(step)
    raise TimeoutError(f"не дождались: {what}")


@pytest.fixture(scope="module")
def dagster_dev(run_dir: Path):
    port = _free_port()
    log = (run_dir / "dagster_dev.log").open("w")
    proc = subprocess.Popen(
        [
            str(DAGSTER_BIN),
            "dev",
            "-m",
            "olist_ml.definitions",
            "--port",
            str(port),
        ],
        cwd=run_dir,
        env=dict(os.environ),
        start_new_session=True,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        _wait(
            lambda: requests.get(
                f"http://127.0.0.1:{port}/server_info",
                timeout=2,
            ).ok,
            240,
            "webserver",
        )
        yield port
    finally:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        log.close()


def _location_name(port: int) -> str:
    entries = requests.post(
        f"http://127.0.0.1:{port}/graphql",
        json={
            "query": (
                "{ workspaceOrError { ... on Workspace"
                " { locationEntries { name } } } }"
            ),
        },
        timeout=10,
    ).json()["data"]["workspaceOrError"]["locationEntries"]
    assert len(entries) == 1, entries
    return entries[0]["name"]


def test_materialize_all_via_graphql_and_reload(dagster_dev: int) -> None:
    from dagster import DagsterRunStatus
    from dagster_graphql import DagsterGraphQLClient

    client = DagsterGraphQLClient("127.0.0.1", port_number=dagster_dev)
    location = _location_name(dagster_dev)

    # __ASSET_JOB — неявный job «все ассеты» (то же, что Materialize all в
    # UI).
    run_id = client.submit_job_execution(
        "__ASSET_JOB",
        repository_location_name=location,
        repository_name="__repository__",
    )
    terminal = {
        DagsterRunStatus.SUCCESS,
        DagsterRunStatus.FAILURE,
        DagsterRunStatus.CANCELED,
    }
    _wait(
        lambda: client.get_run_status(run_id) in terminal,
        900,
        "run terminal",
        step=5,
    )
    assert client.get_run_status(run_id) == DagsterRunStatus.SUCCESS

    status = client.reload_repository_location(location)
    assert status.status.name == "SUCCESS", status

    # Данные легли во временную БД сессии тестов, а не в репозиторную data/.
    import duckdb

    with duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True) as con:
        row = con.sql(
            "select count(*) from marts.mart_order_features",
        ).fetchone()
    assert row is not None
    assert row[0] > 1000
