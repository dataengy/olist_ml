"""Демо 1, шаг 5 — v2 + asset checks: утечка на training_dataset, quality
gate на model_evaluation.

Граф ML-ассетов (группа "ml"):

    mart_order_features (dbt)
      → training_dataset      ← check no_leakage
      → model
      → model_evaluation      ← check quality_gate (blocking)

Вся логика обучения — чистые функции в olist_ml.features; здесь только
Dagster-обвязка:
чтение витрины, передача данных между ассетами и metadata для UI.
"""

import dagster as dg
import duckdb

# F — контракт признаков и функции обучения (без Dagster и I/O).
from olist_ml import features as F

# Абсолютный путь к DuckDB из .env; импорт заодно гарантирует, что окружение
# загружено.
from olist_ml.defs.env import DUCKDB_PATH

# Та же БД, куда пишет dbt (dbt/profiles.yml, таргет duck); путь — из .env
# (defs/env.py).
# Схема marts задаётся в dbt_project.yml (+schema: marts), имя таблицы = имя
# dbt-модели.
MART_TABLE = "marts.mart_order_features"


@dg.asset(
    group_name="ml",
    kinds={"pandas"},  # значок в UI; на исполнение не влияет
    # Зависимость без передачи данных: Dagster знает, что ассет строится
    # после dbt-модели (и материализует её раньше в общем запуске), а сами
    # данные читаем из DuckDB ниже.
    deps=[
        dg.AssetKey("mart_order_features"),
    ],  # dbt-модель marts/mart_order_features
)
def training_dataset() -> dg.Output[dict]:
    """Строки с известным target, детерминированный сплит train/holdout по
    md5(order_id).
    """
    # read_only: не блокируем файл БД для записи — dbt может работать
    # параллельно.
    # Контекстный менеджер закрывает соединение сразу после чтения.
    with duckdb.connect(str(DUCKDB_PATH), read_only=True) as con:
        mart_order_features = con.sql(f"select * from {MART_TABLE}").df()
    # Таргет NULL у ещё не доставленных заказов: для обучения и оценки они не
    # годятся.
    df = mart_order_features[mart_order_features[F.TARGET].notna()]
    # 20% в holdout; сплит по хешу order_id — воспроизводим при любом порядке
    # строк.
    train, holdout = F.split_by_hash(df, test_frac=0.2, seed=42)
    # Значение ассета (dict с двумя DataFrame) сохраняет IO manager (по
    # умолчанию pickle в DAGSTER_HOME/storage) и передаёт в зависимые ассеты;
    # metadata видна в UI.
    return dg.Output(
        {"train": train, "holdout": holdout},
        metadata={
            "rows_train": len(train),
            "rows_holdout": len(holdout),
            # Доля опозданий: контроль дисбаланса классов между запусками.
            "positive_rate": round(
                float(df[F.TARGET].astype(int).mean()),
                4,
            ),
            "features": dg.MetadataValue.json(list(F.FEATURES)),
        },
    )


# code_version: при изменении логики обучения увеличить — Dagster пометит
# модель устаревшей.
@dg.asset(group_name="ml", kinds={"sklearn"}, code_version="v1")
def model(training_dataset: dict):
    """sklearn Pipeline (preprocessing + LogisticRegression), fit только на
    train.
    """
    # Аргумент training_dataset — значение одноимённого ассета (Dagster
    # связывает их по имени).
    train = training_dataset["train"]
    pipe = F.build_pipeline(random_state=42)
    # Препроцессинг внутри Pipeline обучается тоже только на train — holdout
    # не «подглядывает».
    # Таргет может прийти как float/nullable int после pandas — приводим к
    # int для sklearn.
    pipe.fit(train[list(F.FEATURES)], train[F.TARGET].astype(int))
    return pipe


@dg.asset(group_name="ml", kinds={"sklearn"})
def model_evaluation(
    model,
    training_dataset: dict,
) -> dg.Output[dict]:
    """Метрики на holdout — в metadata: UI строит график по истории
    материализаций.
    """
    h = training_dataset["holdout"]
    # threshold=0.5 влияет только на accuracy; ROC AUC и PR AUC от порога не
    # зависят.
    m = F.evaluate(
        model,
        h[list(F.FEATURES)],
        h[F.TARGET].astype(int),
        threshold=0.5,
    )
    # Сам словарь метрик — значение ассета (его читает quality_gate);
    # округлённые копии — в metadata, чтобы видеть динамику метрик от запуска
    # к запуску.
    return dg.Output(
        m,
        metadata={
            "roc_auc": round(m["roc_auc"], 4),
            "pr_auc": round(m["pr_auc"], 4),
            "accuracy": round(m["accuracy"], 4),
            "rows_holdout": m["n_rows"],
        },
    )


# --- asset checks ----------------------------------------------------------

# Поля, неизвестные в момент оформления заказа: в признаки попадать не
# должны.
# target — очевидно; delivery_delay_days и дата доставки — известны только
# после доставки; review_score — отзыв ставят после получения заказа.
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
    # Проверяем контракт признаков (F.FEATURES), а не колонки датасета: в
    # датасете эти поля могут быть (для анализа), важно лишь, чтобы модель их
    # не использовала.
    leaked = sorted(set(F.FEATURES) & LEAKY)
    return dg.AssetCheckResult(
        passed=not leaked,
        metadata={"leaked": dg.MetadataValue.json(leaked)},
    )


# Параметры проверки задаются в run config при запуске (Launchpad в UI), без
# правки кода.
class GateConfig(dg.Config):
    # Минимально допустимый ROC AUC на holdout.
    min_roc_auc: float = 0.61


@dg.asset_check(
    asset=model_evaluation,
    # blocking: если проверка не прошла, ассеты ниже по графу в этом запуске
    # не строятся.
    blocking=True,
    description=(
        "Quality gate: ROC AUC на holdout ≥ порога "
        "(порог — через run config)."
    ),
)
def quality_gate(
    model_evaluation: dict,
    config: GateConfig,
) -> dg.AssetCheckResult:
    value = model_evaluation["roc_auc"]
    return dg.AssetCheckResult(
        passed=value >= config.min_roc_auc,
        # ERROR (а не WARN): провал — красный статус в UI и срабатывание
        # blocking.
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "roc_auc_holdout": round(value, 4),
            "threshold": config.min_roc_auc,
        },
    )
