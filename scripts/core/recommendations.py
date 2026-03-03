from __future__ import annotations

import numpy as np
import pandas as pd


def compute_case_counts(qty: pd.Series, factor: pd.Series, use_peak_mode: bool) -> pd.Series:
    qty_num = pd.to_numeric(qty, errors="coerce").fillna(0)
    factor_num = pd.to_numeric(factor, errors="coerce")
    valid_factor = factor_num > 0
    raw_cases = np.where(valid_factor, qty_num / factor_num, np.nan)
    rounded = np.where(valid_factor, np.ceil(raw_cases) if use_peak_mode else np.floor(raw_cases), np.nan)
    min_one_case_mask = valid_factor & (qty_num > 0) & (raw_cases < 1)
    rounded = np.where(min_one_case_mask, 1, rounded)
    return pd.Series(rounded, index=qty.index).astype("Int64")


def map_province_by_supplier_card(card: str | None, mapping: dict[str, str]) -> str:
    if card is None:
        return "其他/未知"
    return mapping.get(str(card), "其他/未知")
