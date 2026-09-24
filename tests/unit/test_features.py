"""Unit: контракт признаков и чистые функции обучения (olist_ml.features) —
без I/O.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from olist_ml import features as F

pytestmark = pytest.mark.unit


def _frame(n: int = 400, seed: int = 0) -> pd.DataFrame:
    """Синтетическая витрина с колонками контракта и таргетом, зависящим от
    признаков.
    """
    rng = np.random.default_rng(seed)
    df = pd.DataFrame({col: rng.normal(size=n) for col in F.NUMERIC})
    df["customer_state"] = rng.choice(["SP", "RJ", "MG"], size=n)
    df["main_payment_type"] = rng.choice(["credit_card", "boleto"], size=n)
    df[F.KEY] = [f"order-{i:05d}" for i in range(n)]
    signal = df["estimated_delivery_span_days"] + (
        df["customer_state"] == "RJ"
    )
    df[F.TARGET] = (signal > 0.5).astype(int)
    return df


def test_contract_has_no_overlap_and_no_target() -> None:
    assert not set(F.NUMERIC) & set(F.CATEGORICAL)
    assert F.TARGET not in F.FEATURES
    assert F.FEATURES == F.NUMERIC + F.CATEGORICAL


def test_split_is_deterministic_and_order_independent() -> None:
    df = _frame()
    train_a, test_a = F.split_by_hash(df, test_frac=0.2, seed=42)
    shuffled = df.sample(frac=1.0, random_state=7)
    train_b, test_b = F.split_by_hash(shuffled, test_frac=0.2, seed=42)
    pd.testing.assert_frame_equal(train_a, train_b)
    pd.testing.assert_frame_equal(test_a, test_b)
    assert len(train_a) + len(test_a) == len(df)
    assert not set(train_a[F.KEY]) & set(test_a[F.KEY])


def test_split_is_nested_by_test_frac() -> None:
    df = _frame()
    _, small = F.split_by_hash(df, test_frac=0.1, seed=42)
    _, large = F.split_by_hash(df, test_frac=0.3, seed=42)
    assert set(small[F.KEY]) <= set(large[F.KEY])


def test_split_seed_changes_partition() -> None:
    df = _frame()
    _, a = F.split_by_hash(df, test_frac=0.2, seed=1)
    _, b = F.split_by_hash(df, test_frac=0.2, seed=2)
    assert set(a[F.KEY]) != set(b[F.KEY])


@pytest.mark.parametrize("bad", [0.0, 1.0, -0.1, 1.5])
def test_split_rejects_bad_fraction(bad: float) -> None:
    with pytest.raises(ValueError, match="test_frac"):
        F.split_by_hash(_frame(), test_frac=bad, seed=0)


def test_pipeline_learns_and_evaluates() -> None:
    train, holdout = F.split_by_hash(_frame(n=800), test_frac=0.25, seed=0)
    pipe = F.build_pipeline(random_state=0)
    pipe.fit(train[list(F.FEATURES)], train[F.TARGET])
    metrics = F.evaluate(
        pipe,
        holdout[list(F.FEATURES)],
        holdout[F.TARGET],
        threshold=0.5,
    )
    assert set(metrics) == {
        "roc_auc",
        "pr_auc",
        "accuracy",
        "positive_rate",
        "n_rows",
    }
    assert metrics["n_rows"] == len(holdout)
    assert metrics["roc_auc"] > 0.8  # сигнал в данных сильный


def test_pipeline_handles_missing_numbers_and_unknown_categories() -> None:
    train = _frame(n=300)
    pipe = F.build_pipeline(random_state=0).fit(
        train[list(F.FEATURES)],
        train[F.TARGET],
    )
    probe = train.head(3).copy()
    probe.loc[:, "order_value"] = np.nan  # → медиана
    probe.loc[:, "customer_state"] = "XX"  # незнакомая категория → нули
    proba = pipe.predict_proba(probe[list(F.FEATURES)])[:, 1]
    assert np.isfinite(proba).all()


def test_fingerprint_order_independent_and_param_sensitive() -> None:
    df = _frame(n=50)
    fp = F.dataset_fingerprint(df, {"seed": 1})
    assert fp == F.dataset_fingerprint(df.iloc[::-1], {"seed": 1})
    assert fp != F.dataset_fingerprint(df, {"seed": 2})


@pytest.mark.parametrize(
    ("value", "passed"),
    [(0.61, True), (0.7, True), (0.6, False)],
)
def test_gate_boundary_inclusive(value: float, passed: bool) -> None:
    assert F.gate(value, threshold=0.61) is passed
