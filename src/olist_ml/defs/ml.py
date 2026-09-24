"""ML-ассеты поверх dbt-витрины mart_order_features."""

import dagster as dg
import duckdb

from olist_ml import features as F
from olist_ml.settings import DUCKDB_PATH


@dg.asset(
    group_name="ml",
    kinds={"pandas"},
    deps=[dg.AssetKey("mart_order_features")],
)
def training_dataset() -> dg.Output[dict]:
    """Датасет из dbt-таблицы; split train/holdout по md5(order_id)."""
    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as conn:
        mart_order_features = conn.execute(
            "select * from marts.mart_order_features"
        ).fetchdf()
    df = mart_order_features[mart_order_features[F.TARGET].notna()]
    train, holdout = F.split_by_hash(df, test_frac=0.2, seed=42)
    return dg.Output(
        {"train": train, "holdout": holdout},
        metadata={
            "rows_train": len(train),
            "rows_holdout": len(holdout),
            "positive_rate": round(
                float(df[F.TARGET].astype(int).mean()), 4
            ),
            "features": dg.MetadataValue.json(list(F.FEATURES)),
            "path": str(DUCKDB_PATH),
        },
    )


@dg.asset(group_name="ml", kinds={"sklearn"}, code_version="v2")
def model(training_dataset: dict):
    """sklearn Pipeline (preprocessing + LogisticRegression), fit только на train."""
    train = training_dataset["train"]
    pipe = F.build_pipeline(random_state=42).set_params(clf__C=0.1)
    pipe.fit(
        train[list(F.FEATURES)],
        train[F.TARGET].astype(int),
    )
    return pipe


@dg.asset(
    group_name="ml",
    kinds={"sklearn"},
)
def model_evaluation(
    model,
    training_dataset: dict,
) -> dg.Output[dict]:
    """Метрики на holdout — в metadata: UI строит график по истории материализаций."""
    h = training_dataset["holdout"]
    m = F.evaluate(
        model, h[list(F.FEATURES)], h[F.TARGET].astype(int), threshold=0.5
    )
    return dg.Output(
        m,
        metadata={
            "roc_auc": round(m["roc_auc"], 4),
            "pr_auc": round(m["pr_auc"], 4),
            "accuracy": round(m["accuracy"], 4),
            "rows_holdout": m["n_rows"],
        },
    )


# --- asset checks -----------------------------------------------------------------------------

# Поля, неизвестные в момент оформления заказа: в признаки попадать не должны.
LEAKY = {
    F.TARGET,
    "delivery_delay_days",
    "order_delivered_customer_date",
    "review_score",
}


@dg.asset_check(
    asset=training_dataset,
    description="Нет утечки: target и поля «из будущего» не в признаках.",
)
def no_leakage(training_dataset: dict) -> dg.AssetCheckResult:
    leaked = sorted(set(F.FEATURES) & LEAKY)
    return dg.AssetCheckResult(
        passed=not leaked, metadata={"leaked": dg.MetadataValue.json(leaked)}
    )


class GateConfig(dg.Config):
    min_roc_auc: float = 0.61


@dg.asset_check(
    asset=model_evaluation,
    blocking=True,
    description="Quality gate: ROC AUC на holdout ≥ порога (порог — через run config).",
)
def quality_gate(
    model_evaluation: dict, config: GateConfig
) -> dg.AssetCheckResult:
    value = model_evaluation["roc_auc"]
    return dg.AssetCheckResult(
        passed=value >= config.min_roc_auc,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "roc_auc_holdout": round(value, 4),
            "threshold": config.min_roc_auc,
        },
    )
