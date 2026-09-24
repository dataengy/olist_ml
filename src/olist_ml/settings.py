"""Shared paths for Dagster assets and the dbt DuckDB profile."""

import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env", override=False)

_duckdb_path = Path(
    os.getenv("DUCKDB_PATH", PROJECT_ROOT / "data" / "olist.duckdb")
).expanduser()
if not _duckdb_path.is_absolute():
    _duckdb_path = PROJECT_ROOT / _duckdb_path
DUCKDB_PATH = _duckdb_path.resolve()

# dbt reads this variable from profiles.yml; Python assets use the same resolved path.
os.environ["DUCKDB_PATH"] = str(DUCKDB_PATH)
