"""Проверить, что dbt-витрина есть в DuckDB (после dbt build)."""
import duckdb
from olist_ml.defs.env import DUCKDB_PATH
with duckdb.connect(str(DUCKDB_PATH), read_only=True) as c:
    print(c.sql("select count(*) from marts.mart_order_features").fetchall())
