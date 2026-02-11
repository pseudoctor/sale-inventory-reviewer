from __future__ import annotations

import numpy as np
import pandas as pd


def overlap_days(start_date: pd.Timestamp, end_date: pd.Timestamp, data_min_date: pd.Timestamp, data_max_date: pd.Timestamp) -> int:
    overlap_start = max(start_date, data_min_date)
    overlap_end = min(end_date, data_max_date)
    if overlap_start > overlap_end:
        return 0
    return (overlap_end - overlap_start).days + 1


def combine_daily_sales(daily_sales_3m_mtd: pd.Series, daily_sales_30d: pd.Series, use_peak_mode: bool) -> pd.Series:
    if use_peak_mode:
        return pd.concat([daily_sales_3m_mtd, daily_sales_30d], axis=1).max(axis=1)
    return pd.concat([daily_sales_3m_mtd, daily_sales_30d], axis=1).min(axis=1)


def classify_risk_levels(turnover_days: pd.Series, low_days: float, high_days: float) -> pd.Series:
    return pd.Series(
        np.select([turnover_days > high_days, turnover_days < low_days], ["高", "低"], default="中"),
        index=turnover_days.index,
    )


def apply_inventory_metrics(df: pd.DataFrame, low_days: float, high_days: float) -> pd.DataFrame:
    out = df.copy()
    out["inventory_sales_ratio"] = np.where(out["forecast_daily_sales"] > 0, out["inventory_qty"] / out["forecast_daily_sales"], float("inf"))
    out["turnover_rate"] = np.where(out["inventory_qty"] > 0, (out["forecast_daily_sales"] * 30) / out["inventory_qty"], 0)
    turnover_precise = np.where(out["forecast_daily_sales"] > 0, out["inventory_qty"] / out["forecast_daily_sales"], float("inf"))
    out["turnover_days"] = np.where(np.isfinite(turnover_precise), np.round(turnover_precise), float("inf"))
    out["risk_level"] = classify_risk_levels(pd.Series(turnover_precise, index=out.index), low_days, high_days)
    return out
