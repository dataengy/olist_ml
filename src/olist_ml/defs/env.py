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


# --- Загрузка окружения: выполняется один раз, при первом импорте модуля ----------------------
#
# Шаг 1. Корневой ./.env — единственный файл с фиксированным путём («бутстрап»): только в нём
# описано, какие ещё файлы грузить и какие переменные обязательны. Нет файла — сразу ошибка.
load_env_file(ROOT_ENV)

# Шаг 2. Без этих двух ключей дальше идти нельзя: неизвестно, что грузить и что проверять.
# source= — чтобы в тексте ошибки был указан именно корневой .env, а не «.env-файлы» вообще.
require("ENV_FILES", "REQUIRED_ENV_VARS", source=str(ROOT_ENV))

# Шаг 3. Список файлов окружения (через запятую, пути от корня проекта),
# например ".env,dbt/.env" → [<root>/.env, <root>/dbt/.env].
# Корневой .env в списке может повторяться: повторная загрузка безвредна (override=False).
ENV_FILES = [PROJECT_ROOT / rel for rel in env_list("ENV_FILES")]

# Шаг 4. Имена обязательных переменных, например ["DUCKDB_PATH", "DBT_TARGET", "CH_RAW_DB"].
REQUIRED = env_list("REQUIRED_ENV_VARS")

# Шаг 5. Грузим все файлы по порядку. override=False внутри load_env_file: значение, уже
# заданное в shell/CI или в более раннем файле, не перетирается — выигрывает первый источник.
for env_file in ENV_FILES:
    load_env_file(env_file)

# Шаг 6. Fail fast: проверяем обязательные переменные разом и перечисляем все недостающие,
# чтобы не чинить их по одной. Дефолтов нет — пустое значение тоже считается отсутствующим.
require(*REQUIRED)

DUCKDB_PATH = env_path("DUCKDB_PATH")
# DuckDB создаёт файл БД, но не родительский каталог.
DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
