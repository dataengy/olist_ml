"""Python-модель dbt: дневной объём заказов и аномалии по скользящему z-score.

Почему Python, а не SQL: скользящие окна со сдвигом, заполнение пропущенных дней и
z-score удобнее и нагляднее в pandas. dbt-duckdb выполняет модель внутри DuckDB-процесса:
dbt.ref() отдаёт relation, .df() — pandas DataFrame, а возвращённый DataFrame dbt
материализует таблицей marts.mart_daily_order_anomalies.

Параметры окна и порога — в config модели (_mart_daily_order_anomalies.yml), без дефолтов в коде.
"""

import pandas as pd


def model(dbt, session):
    # Python-модели в dbt-duckdb материализуются только как table (или incremental).
    dbt.config(materialized="table")

    # Параметры из yml-конфига модели; отсутствие ключа — ошибка (без дефолтов).
    window = int(dbt.config.get("rolling_window_days"))
    z_threshold = float(dbt.config.get("anomaly_z_threshold"))

    # Зависимость через ref: dbt строит граф (stg_orders → эта модель), Dagster — тоже.
    orders = dbt.ref("stg_orders").df()

    # Заказы по дням покупки; отменённые считаем отдельно — всплеск отмен тоже сигнал.
    daily = (
        orders.groupby("order_purchase_date")
        .agg(orders_cnt=("order_id", "count"), canceled_cnt=("is_canceled", "sum"))
        .sort_index()
    )
    daily.index = pd.to_datetime(daily.index)

    # Дни без заказов в источнике отсутствуют — дозаполняем нулями, иначе окно «сожмётся».
    full_range = pd.date_range(daily.index.min(), daily.index.max(), freq="D")
    daily = daily.reindex(full_range, fill_value=0)
    daily.index.name = "order_date"

    # Статистики окна по ПРЕДЫДУЩИМ дням (shift(1)): текущий день не влияет на свою же норму,
    # иначе сильный всплеск частично «маскирует» сам себя.
    history = daily["orders_cnt"].shift(1).rolling(window, min_periods=window)
    daily["rolling_mean"] = history.mean()
    daily["rolling_std"] = history.std()

    # z-score: на сколько σ день отклонился от нормы. Нет σ (начало ряда, σ=0) → NULL.
    std = daily["rolling_std"].where(daily["rolling_std"] > 0)
    daily["z_score"] = (daily["orders_cnt"] - daily["rolling_mean"]) / std

    # Флаг аномалии в обе стороны (провал тоже важен); где z неизвестен — 0.
    daily["is_anomaly"] = (daily["z_score"].abs() >= z_threshold).astype(int)
    daily["anomaly_direction"] = (
        daily["z_score"]
        .where(daily["is_anomaly"] == 1)
        .map(lambda z: None if pd.isna(z) else ("spike" if z > 0 else "drop"))
    )

    # dbt ожидает DataFrame; дата — обычной колонкой, а не индексом.
    return daily.reset_index()
