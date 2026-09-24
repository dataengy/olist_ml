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


def load_env_file(path: Path) -> None:
    """Загрузить .env-файл; нет файла — ошибка с подсказкой про соседний .env.template."""
    if not path.exists():
        raise RuntimeError(f"Нет {path}: скопируйте {path.name}.template рядом с ним")
    load_dotenv(path, override=False)


def require(*names: str, source: str = ".env-файлах") -> None:
    """Все переменные заданы и непусты, иначе ошибка со списком недостающих."""
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise RuntimeError(f"Не заданы {missing} в {source} (см. *.env.template)")


def env_list(name: str) -> list[str]:
    """Значение через запятую → список непустых элементов."""
    return [item.strip() for item in os.environ[name].split(",") if item.strip()]


def env_path(name: str) -> Path:
    """Путь из переменной; относительный — от корня проекта (и записывается обратно абсолютным)."""
    path = Path(os.environ[name])
    if not path.is_absolute():
        path = PROJECT_ROOT / path
        os.environ[name] = str(path)
    return path


load_env_file(ROOT_ENV)
require("ENV_FILES", "REQUIRED_ENV_VARS", source=str(ROOT_ENV))

ENV_FILES = [PROJECT_ROOT / rel for rel in env_list("ENV_FILES")]
REQUIRED = env_list("REQUIRED_ENV_VARS")

for env_file in ENV_FILES:
    load_env_file(env_file)
require(*REQUIRED)

DUCKDB_PATH = env_path("DUCKDB_PATH")
# DuckDB создаёт файл БД, но не родительский каталог.
DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
