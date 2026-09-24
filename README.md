# olist_ml

## Getting started

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Install the dbt adapter and Dagster integration with the project dependencies,
then fetch dbt package dependencies:

```bash
uv sync
uv run dbt deps --project-dir dbt --profiles-dir dbt
```

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

The dbt project is loaded from `dbt/` through a Dagster YAML component. Its
source definitions use `source_*` logical names and keep the original database
table names through `identifier`, so source and seed asset keys stay distinct.
Materialize the dbt assets first; the `training_dataset` asset then reads `marts.mart_order_features`
from the same DuckDB file configured by `DUCKDB_PATH` in the root `.env` file
(defaults to `data/olist.duckdb`; see `.env.example`).

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)
