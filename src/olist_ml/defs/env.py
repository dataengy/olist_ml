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
ENV_FILES = (PROJECT_ROOT / ".env", PROJECT_ROOT / "dbt" / ".env")
REQUIRED = ("DUCKDB_PATH", "DBT_TARGET", "CH_RAW_DB")

for env_file in ENV_FILES:
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
