"""Точка входа кода Dagster (code location olist_ml).

dg / dagster находят её по [tool.dg] в pyproject.toml и вызывают defs() при
каждой загрузке кода:
dg dev, dg list defs, dg launch, перезагрузка location в UI.
"""

from pathlib import Path

from dagster import definitions, load_from_defs_folder

# Импорт ради побочного эффекта: defs/env.py загружает ./.env и dbt/.env в
# os.environ и падает, если чего-то не хватает. Должен выполниться ДО сборки
# dbt-компонента: dbt читает профиль (DBT_TARGET, DUCKDB_PATH, CH_RAW_DB) из
# окружения процесса. Импорт на уровне модуля гарантирует порядок: он
# выполняется раньше, чем Dagster вызовет defs().
from olist_ml.defs import (
    env,  # noqa: F401 — .env и dbt/.env до сборки dbt-компонента
)


# @definitions — «ленивые» определения: функция вызывается при загрузке кода,
# а не при импорте, и возвращает объект Definitions со всеми ассетами,
# checks, jobs и т.п.
@definitions
def defs():
    # Обходит папку defs/ рядом с этим файлом и собирает всё, что там найдёт:
    #   defs/dbt/defs.yaml — dbt-компонент (ассеты dbt-проекта и их тесты);
    #   defs/ml.py         — ML-ассеты и asset checks (training_dataset,
    #   model, …);
    #   defs/env.py        — определений не содержит, только загружает
    #   окружение.
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
