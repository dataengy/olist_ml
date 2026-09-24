"""Feature contract и чистые функции обучения (ADR-06, ADR-14). Никакого
Dagster и I/O.

Признаки — 10 колонок витрины, известных в момент оформления заказа
(docs/contracts/features.md).
Модель — один sklearn Pipeline: preprocessing (impute + scale / one-hot)
внутри, fit только на train.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

# --- Контракт признаков (docs/contracts/features.md) -----------------------

# Целевая переменная: 1 — заказ доставлен позже обещанной даты, 0 — вовремя,
# NULL — не доставлен.
TARGET = "is_late_delivery"
# Числовые признаки: в пайплайне — заполнение пропусков медианой и
# стандартизация.
NUMERIC: tuple[str, ...] = (
    "items_cnt",
    "order_value",
    "freight_share",
    "max_installments",
    "customer_seller_distance_km",
    "estimated_delivery_span_days",
    "purchase_month",
    "total_weight_g",
)
# Категориальные признаки: one-hot кодирование.
CATEGORICAL: tuple[str, ...] = ("customer_state", "main_payment_type")
# Все признаки модели в фиксированном порядке — именно эти колонки подаются в
# Pipeline.
FEATURES: tuple[str, ...] = NUMERIC + CATEGORICAL
# Ключ строки витрины: на нём основаны сплит и отпечаток датасета.
KEY = "order_id"

# Число «корзин» хеша для сплита: точность доли test_frac — 1/10 000.
_HASH_SPACE = 10_000


# --- Сплит train/holdout ---------------------------------------------------


def _bucket(order_id: str, seed: int) -> int:
    # md5 от "order_id|split|seed" → первые 8 hex-символов (32 бита) → число
    # → остаток по корзинам.
    # Зависит только от самого order_id и seed, поэтому заказ всегда попадает
    # в одну и ту же корзину; смена seed даёт другое, но тоже воспроизводимое
    # разбиение. md5 — не для безопасности, а как быстрый равномерный хеш,
    # одинаковый на всех платформах (в отличие от hash()).
    digest = hashlib.md5(f"{order_id}|split|{seed}".encode()).hexdigest()
    return int(digest[:8], 16) % _HASH_SPACE


def split_by_hash(
    df: pd.DataFrame,
    test_frac: float,
    seed: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Детерминированный сплит по md5(order_id): не зависит от порядка строк;
    вложен по test_frac.
    """
    if not (0.0 < test_frac < 1.0):
        raise ValueError(f"test_frac must be in (0, 1), got {test_frac!r}")
    # Стабильная сортировка по ключу: порядок строк на выходе не зависит от
    # порядка на входе.
    ordered = df.sort_values(KEY, kind="mergesort").reset_index(drop=True)
    # Номер корзины 0.._HASH_SPACE-1 для каждого заказа.
    buckets = ordered[KEY].astype(str).map(lambda v: _bucket(v, seed))
    # В holdout — корзины ниже порога. «Вложенность»: holdout при
    # test_frac=0.1 целиком входит в holdout при test_frac=0.2 — увеличение
    # доли только добавляет заказы, не перемешивая старые.
    is_test = buckets < int(test_frac * _HASH_SPACE)
    # (train, holdout) с чистыми индексами 0..n-1.
    train = ordered[~is_test].reset_index(drop=True)
    holdout = ordered[is_test].reset_index(drop=True)
    return train, holdout


# --- Модель ----------------------------------------------------------------


def build_pipeline(
    numeric: tuple[str, ...] = NUMERIC,
    categorical: tuple[str, ...] = CATEGORICAL,
    random_state: int = 0,
) -> Pipeline:
    """Необученный Pipeline: препроцессинг признаков + логистическая
    регрессия.

    Препроцессинг внутри Pipeline, поэтому его параметры (медианы, среднее/σ,
    словарь категорий) вычисляются только на данных fit — на holdout утечки
    нет, а на инференсе применяются те же.
    """
    # ColumnTransformer применяет свою ветку к своим колонкам и склеивает
    # результат; колонки, не перечисленные ни в одной ветке, отбрасываются.
    pre = ColumnTransformer(
        [
            # Числа: пропуски → медиана (устойчива к выбросам), затем масштаб
            # к mean=0, std=1 — иначе признаки с большими значениями
            # (order_value, total_weight_g) доминируют в регрессии.
            (
                "num",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="median")),
                        ("scale", StandardScaler()),
                    ],
                ),
                list(numeric),
            ),
            # Категории: one-hot; незнакомая на инференсе категория → все
            # нули вместо ошибки.
            (
                "cat",
                OneHotEncoder(handle_unknown="ignore"),
                list(categorical),
            ),
        ],
    )
    # max_iter с запасом, чтобы solver сошёлся; class_weight="balanced" —
    # опозданий заметно меньше, чем своевременных доставок, и веса классов
    # выравнивают вклад редкого класса.
    model = LogisticRegression(
        max_iter=1000,
        class_weight="balanced",
        random_state=random_state,
    )
    # Шаги по порядку: сначала препроцессинг, затем классификатор.
    return Pipeline([("pre", pre), ("clf", model)])


# --- Оценка и служебные функции --------------------------------------------


def evaluate(
    pipe: Pipeline,
    x: pd.DataFrame,
    y: pd.Series,
    threshold: float,
) -> dict[str, float]:
    """Метрики обученного Pipeline на выборке (x, y)."""
    # Вероятность класса 1 (опоздание) для каждой строки.
    proba = pipe.predict_proba(x)[:, 1]
    return {
        # Качество ранжирования, от порога не зависит: 0.5 — случайно, 1.0 —
        # идеально.
        "roc_auc": float(roc_auc_score(y, proba)),
        # Площадь под precision-recall: информативнее ROC AUC при редком
        # положительном классе.
        "pr_auc": float(average_precision_score(y, proba)),
        # Доля верных ответов при пороге threshold (единственная метрика,
        # зависящая от порога).
        "accuracy": float(
            accuracy_score(y, (proba >= threshold).astype(int)),
        ),
        # Доля опозданий в выборке и её размер — контекст для интерпретации
        # метрик.
        "positive_rate": float(np.mean(y)),
        "n_rows": int(len(y)),
    }


def dataset_fingerprint(df: pd.DataFrame, params: dict[str, Any]) -> str:
    """md5 отсортированных order_id + параметров: не зависит от порядка
    строк, меняется с params.
    """
    # Сортировка делает отпечаток независимым от порядка строк.
    ids = "|".join(sorted(df[KEY].astype(str)))
    # sort_keys — одинаковый JSON при любом порядке ключей; default=str — для
    # дат, Path и т.п.
    payload = ids + "#" + json.dumps(params, sort_keys=True, default=str)
    return hashlib.md5(payload.encode()).hexdigest()


def gate(value: float, threshold: float) -> bool:
    """Quality gate: метрика не ниже порога (граница включается)."""
    return value >= threshold
