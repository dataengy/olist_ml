"""Убрать meta.dagster.asset_key [raw, <table>] у источников olist_raw (дубль с seed raw/<table>)."""
import re, pathlib
p = pathlib.Path("dbt/models/sources/sources.yml")
s, n = re.subn(r"\n        meta:\n          dagster:\n            asset_key: \[raw, \w+\]", "", p.read_text())
p.write_text(s); print(f"removed {n}")
