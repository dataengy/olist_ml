"""Combine regular Python asset definitions with the dbt YAML component."""

from pathlib import Path

import dagster as dg

from olist_ml import integrations_yaml, settings


def build_defs() -> dg.Definitions:
    shared = dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
    dbt_defs = dg.ComponentTree.from_module(
        defs_module=integrations_yaml,
        project_root=settings.PROJECT_ROOT,
    ).build_defs()
    return dg.Definitions.merge(shared, _without_component_tree(dbt_defs))


def _without_component_tree(defs: dg.Definitions) -> dg.Definitions:
    """Keep the root project's component tree when merging YAML component defs."""
    return dg.Definitions(
        assets=defs.assets,
        asset_checks=defs.asset_checks,
        jobs=defs.jobs,
        schedules=defs.schedules,
        sensors=defs.sensors,
        resources=defs.resources,
        loggers=defs.loggers,
        executor=defs.executor,
        metadata=defs.metadata,
    )


@dg.definitions
def defs() -> dg.Definitions:
    return build_defs()
