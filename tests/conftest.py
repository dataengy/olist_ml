"""Общая обвязка тестов.

Главное правило: тесты работают во ВРЕМЕННОМ каталоге и не трогают
репозиторные data/ и DAGSTER_HOME. Переменные выставляются в pytest_configure
— до импорта olist_ml: модуль olist_ml.defs.env грузит .env-файлы с
override=False, поэтому заданное здесь значение DUCKDB_PATH выигрывает у
значения из ./.env. Остальные переменные (DBT_TARGET, CH_RAW_DB, ENV_FILES,
REQUIRED_ENV_VARS) берутся из .env-файлов как обычно — в CI их создают из
.env.template. Поэтому в этом модуле нет импортов olist_ml на верхнем уровне.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
BIN = Path(sys.executable).parent
DG_BIN = BIN / "dg"
DAGSTER_BIN = BIN / "dagster"
# Временный каталог сессии — в типизированном stash конфигурации pytest.
RUN_DIR = pytest.StashKey[Path]()


def pytest_configure(config: pytest.Config) -> None:
    run_dir = Path(tempfile.mkdtemp(prefix="olist-ml-test-run-"))
    config.stash[RUN_DIR] = run_dir
    (run_dir / "dagster_home").mkdir()
    os.environ.update(
        {
            # Изолированная БД и инстанс Dagster на сессию тестов.
            "DUCKDB_PATH": str(run_dir / "olist.duckdb"),
            "DAGSTER_HOME": str(run_dir / "dagster_home"),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "DAGSTER_DISABLE_TELEMETRY": "true",
        },
    )


def pytest_unconfigure(config: pytest.Config) -> None:
    run_dir = config.stash.get(RUN_DIR, None)
    if run_dir and not os.environ.get("KEEP_TEST_RUN_DIR"):
        shutil.rmtree(run_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def root() -> Path:
    return ROOT


@pytest.fixture(scope="session")
def run_dir(pytestconfig: pytest.Config) -> Path:
    return pytestconfig.stash[RUN_DIR]


@pytest.fixture(scope="session")
def defs():
    """Definitions code location: dbt-компонент (manifest из defs state) +
    ML-ассеты.
    """
    from olist_ml.definitions import defs as lazy_defs

    return lazy_defs()


@pytest.fixture
def instance():
    """Временный инстанс Dagster на тест — журнал событий не течёт между
    тестами.
    """
    import dagster as dg

    with dg.instance_for_test() as inst:
        yield inst
