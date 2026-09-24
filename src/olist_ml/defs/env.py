"""Переменные окружения из ./.env и dbt/.env (уже заданные в shell не перетираются).

DUCKDB_PATH приводится к абсолютному от корня проекта: dbt резолвит относительный path
от своего cwd (dbt/), а ml.py — от cwd процесса Dagster.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[3]

for env_file in (PROJECT_ROOT / ".env", PROJECT_ROOT / "dbt" / ".env"):
    load_dotenv(env_file, override=False)

os.environ.setdefault("DUCKDB_PATH", "data/olist.duckdb")
_duckdb = Path(os.environ["DUCKDB_PATH"])
if not _duckdb.is_absolute():
    os.environ["DUCKDB_PATH"] = str(PROJECT_ROOT / _duckdb)

DUCKDB_PATH = Path(os.environ["DUCKDB_PATH"])
# DuckDB создаёт файл БД, но не родительский каталог.
DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
