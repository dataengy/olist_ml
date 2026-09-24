"""Переменные окружения — только из ./.env и dbt/.env (уже заданные в shell не перетираются).

Дефолтов в коде нет: отсутствующая переменная — ошибка при загрузке definitions.
Шаблоны — .env.template и dbt/.env.template.

DUCKDB_PATH приводится к абсолютному от корня проекта: dbt резолвит относительный path
от своего cwd (dbt/), а ml.py — от cwd процесса Dagster.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[3]
ROOT_ENV = PROJECT_ROOT / ".env"  # точка входа: в нём ENV_FILES и REQUIRED_ENV_VARS

if not ROOT_ENV.exists():
    raise RuntimeError(f"Нет {ROOT_ENV}: скопируйте .env.template в .env")
load_dotenv(ROOT_ENV, override=False)

for name in ("ENV_FILES", "REQUIRED_ENV_VARS"):
    if not os.environ.get(name):
        raise RuntimeError(f"В {ROOT_ENV} не задан {name} (см. .env.template)")


def _csv(name: str) -> list[str]:
    return [item.strip() for item in os.environ[name].split(",") if item.strip()]


ENV_FILES = [PROJECT_ROOT / rel for rel in _csv("ENV_FILES")]
REQUIRED = _csv("REQUIRED_ENV_VARS")

for env_file in ENV_FILES:
    if not env_file.exists():
        raise RuntimeError(f"Нет {env_file}: скопируйте соседний .env.template")
    load_dotenv(env_file, override=False)

missing = [name for name in REQUIRED if not os.environ.get(name)]
if missing:
    raise RuntimeError(
        f"Не заданы переменные {missing}: заполните .env / dbt/.env по шаблонам .env.template"
    )

_duckdb = Path(os.environ["DUCKDB_PATH"])
if not _duckdb.is_absolute():
    os.environ["DUCKDB_PATH"] = str(PROJECT_ROOT / _duckdb)

DUCKDB_PATH = Path(os.environ["DUCKDB_PATH"])
# DuckDB создаёт файл БД, но не родительский каталог.
DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
