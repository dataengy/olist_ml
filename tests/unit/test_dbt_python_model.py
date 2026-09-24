"""Unit: dbt python-модель mart_daily_order_anomalies на синтетике, без dbt и
DuckDB.

Модель — обычная функция model(dbt, session); подменяем объект dbt заглушкой,
которая отдаёт config и ref(...).df(), и проверяем логику окна/z-score
напрямую.
"""

from __future__ import annotations

import importlib.util
from types import SimpleNamespace

import pandas as pd
import pytest

from tests.conftest import ROOT

pytestmark = pytest.mark.unit

MODEL_PATH = (
    ROOT / "dbt" / "models" / "marts" / "mart_daily_order_anomalies.py"
)


def _load_model():
    spec = importlib.util.spec_from_file_location(
        "mart_daily_order_anomalies",
        MODEL_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.model


class _Config(dict):
    def __call__(self, **kwargs):  # dbt.config(materialized=...)
        self.update(kwargs)

    def get(self, key, default=None):  # как dbt.config.get: без ключа — None
        return super().get(key, default)


def _fake_dbt(orders: pd.DataFrame, window: int = 3, z: float = 3.0):
    return SimpleNamespace(
        config=_Config(rolling_window_days=window, anomaly_z_threshold=z),
        ref=lambda name: SimpleNamespace(df=lambda: orders),
    )


def _orders(counts: dict[str, int]) -> pd.DataFrame:
    """Строки заказов: counts[day] заказов в день, первый заказ дня —
    отменён.
    """
    rows = [
        {
            "order_id": f"{day}-{i}",
            "order_purchase_date": day,
            "is_canceled": int(i == 0),
        }
        for day, n in counts.items()
        for i in range(n)
    ]
    return pd.DataFrame(rows)


def test_spike_is_flagged_and_gaps_are_filled() -> None:
    counts = {
        "2018-01-01": 10,
        "2018-01-02": 11,
        "2018-01-03": 9,
        "2018-01-04": 10,
        # 2018-01-05 — нет заказов вовсе
        "2018-01-06": 60,
    }
    out = _load_model()(_fake_dbt(_orders(counts)), session=None).set_index(
        "order_date",
    )

    # непрерывный ряд: пропущенный день появился с нулём
    assert len(out) == 6
    assert out.loc[pd.Timestamp("2018-01-05"), "orders_cnt"] == 0
    # первые window дней без статистики окна
    assert out["rolling_mean"].iloc[:3].isna().all()
    # всплеск 60 при норме ~7 — аномалия вверх
    spike = out.loc[pd.Timestamp("2018-01-06")]
    assert spike["is_anomaly"] == 1
    assert spike["anomaly_direction"] == "spike"
    # обычные дни — не аномалии
    assert out.loc[: pd.Timestamp("2018-01-04"), "is_anomaly"].eq(0).all()
    assert set(out.columns) >= {"canceled_cnt", "z_score", "rolling_std"}


def test_window_uses_only_previous_days() -> None:
    """Сам день не входит в свою норму: при постоянном ряде σ=0 → z NULL, не
    аномалия.
    """
    counts = {f"2018-02-0{d}": 5 for d in range(1, 8)}
    out = _load_model()(_fake_dbt(_orders(counts), window=3), session=None)
    assert out["z_score"].isna().all()
    assert out["is_anomaly"].eq(0).all()


def test_missing_config_fails_fast() -> None:
    dbt = _fake_dbt(_orders({"2018-01-01": 1}))
    dbt.config.pop("rolling_window_days")
    with pytest.raises(TypeError):
        _load_model()(dbt, session=None)
