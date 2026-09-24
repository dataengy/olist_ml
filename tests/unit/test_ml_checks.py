"""Unit: asset checks ML — утечка признаков и quality gate (без
материализации).
"""

from __future__ import annotations

import dagster as dg
import pytest

from olist_ml import features as F
from olist_ml.defs import ml

pytestmark = pytest.mark.unit


def test_contract_has_no_leaky_features() -> None:
    assert not set(F.FEATURES) & ml.LEAKY


def test_no_leakage_check_passes() -> None:
    result = ml.no_leakage(training_dataset={})
    assert isinstance(result, dg.AssetCheckResult)
    assert result.passed
    assert result.metadata["leaked"].value == []


@pytest.mark.parametrize(
    ("roc_auc", "passed"),
    [(0.9, True), (0.61, True), (0.5, False)],
)
def test_quality_gate(roc_auc: float, passed: bool) -> None:
    result = ml.quality_gate(
        model_evaluation={"roc_auc": roc_auc},
        config=ml.GateConfig(min_roc_auc=0.61),
    )
    assert isinstance(result, dg.AssetCheckResult)
    assert result.passed is passed
    assert result.severity == dg.AssetCheckSeverity.ERROR
    assert result.metadata["threshold"].value == 0.61


def test_quality_gate_is_blocking() -> None:
    spec = next(iter(ml.quality_gate.check_specs))
    assert spec.blocking
