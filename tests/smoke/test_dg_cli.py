"""Smoke: определения грузятся через сам dg — то же, что `dg check defs` /
`dg list defs`.
"""

from __future__ import annotations

import json
import os
import subprocess

import pytest

from tests.conftest import DG_BIN, ROOT

pytestmark = pytest.mark.smoke

# dbt: seeds (raw + справочники), staging, intermediate, marts (в т.ч.
# python-модель);
# ML: training_dataset → model → model_evaluation. Источники olist_raw/* —
# внешние, в списке материализуемых ассетов их нет.
EXPECTED_ASSETS = {
    # seeds
    "customers",
    "geolocation",
    "order_items",
    "order_payments",
    "order_reviews",
    "orders",
    "products",
    "sellers",
    "product_category_name_translation",
    "seed_state_region",
    # staging
    "stg_customers",
    "stg_geolocation",
    "stg_order_items",
    "stg_order_payments",
    "stg_order_reviews",
    "stg_orders",
    "stg_products",
    "stg_sellers",
    # intermediate
    "int_order_distance",
    "int_order_items_enriched",
    "int_orders_enriched",
    # marts
    "mart_customer_value_profile",
    "mart_daily_order_anomalies",
    "mart_daily_state_metrics",
    "mart_delivery_quality_by_category",
    "mart_delivery_quality_by_state",
    "mart_hourly_order_pattern",
    "mart_order_features",
    "mart_seller_analytics",
    # ML
    "training_dataset",
    "model",
    "model_evaluation",
}


def _dg(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(DG_BIN), *args],
        cwd=ROOT,
        env=dict(os.environ),
        capture_output=True,
        text=True,
        timeout=300,
    )


def test_dg_check_defs() -> None:
    proc = _dg("check", "defs")
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "All definitions loaded successfully" in proc.stdout + proc.stderr


def test_dg_list_defs_json() -> None:
    proc = _dg("list", "defs", "--json")
    assert proc.returncode == 0, proc.stderr
    listing = json.loads(proc.stdout)

    assets = {str(item["asset_key"]) for item in listing["assets"]}
    assert assets == EXPECTED_ASSETS

    checks = {str(item["key"]) for item in listing["asset_checks"]}
    # ML-проверки + dbt-тесты витрины признаков и python-модели.
    assert "training_dataset:no_leakage" in checks
    assert "model_evaluation:quality_gate" in checks
    assert any(key.startswith("mart_order_features:") for key in checks)
    assert any(
        key.startswith("mart_daily_order_anomalies:") for key in checks
    )


def test_asset_groups(defs) -> None:
    """Группы из defs.yaml: seeds / staging / intermediate / marts + ml."""
    groups = {
        key.to_user_string(): spec.group_name
        for key, spec in ((s.key, s) for s in defs.resolve_all_asset_specs())
    }
    assert groups["orders"] == "seeds"
    assert groups["stg_orders"] == "staging"
    assert groups["int_orders_enriched"] == "intermediate"
    assert groups["mart_order_features"] == "marts"
    assert groups["training_dataset"] == "ml"


def test_training_dataset_depends_on_dbt_mart(defs) -> None:
    import dagster as dg

    graph = defs.resolve_asset_graph()
    parents = graph.get(dg.AssetKey("training_dataset")).parent_keys
    assert dg.AssetKey("mart_order_features") in parents


def test_staging_reads_seeds_without_duplicate_keys(defs) -> None:
    """Регрессия дубля ключей seed/source: staging зависит прямо от
    seed-ассета, а отдельных ассетов-источников olist_raw/* (с тем же ключом,
    что у seed) нет.
    """
    import dagster as dg

    graph = defs.resolve_asset_graph()
    assert (
        dg.AssetKey("orders")
        in graph.get(dg.AssetKey("stg_orders")).parent_keys
    )
    assert not [
        k for k in graph.get_all_asset_keys() if k.path[0] == "olist_raw"
    ]
