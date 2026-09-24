from pathlib import Path

from dagster import definitions, load_from_defs_folder

from olist_ml.defs import env  # noqa: F401 — .env и dbt/.env до сборки dbt-компонента


@definitions
def defs():
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
