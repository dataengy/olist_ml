"""Показать переменные, собранные из ./.env и dbt/.env."""
import os
from olist_ml.defs import env  # noqa: F401
print({k: os.environ.get(k) for k in ["DUCKDB_PATH", "DBT_TARGET", "CH_RAW_DB"]})
