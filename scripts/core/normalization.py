from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re
from typing import Optional

import pandas as pd


def normalize_barcode_value(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, float):
        if pd.isna(value):
            return None
        return format(value, ".0f")
    if isinstance(value, int):
        return str(value)
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none"}:
        return None
    if re.fullmatch(r"[+-]?\d+(?:\.\d+)?[eE][+-]?\d+", text):
        try:
            text = format(Decimal(text), "f")
        except (InvalidOperation, ValueError):
            return text
    if re.fullmatch(r"\d+\.0+", text):
        return text.split(".", 1)[0]
    if re.fullmatch(r"\d+\.\d+", text):
        integer, decimal = text.split(".", 1)
        if set(decimal) == {"0"}:
            return integer
    return text


def normalize_supplier_card_value(value) -> Optional[str]:
    return normalize_barcode_value(value)


def normalize_numeric_value(value) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none"}:
        return None
    cleaned = text.replace(",", "").replace("，", "")
    numeric = pd.to_numeric(pd.Series([cleaned]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return None
    return float(numeric)


def normalize_numeric_series(values: pd.Series) -> tuple[pd.Series, int]:
    raw_text = values.astype(str).str.strip()
    raw_non_empty = values.notna() & ~raw_text.str.lower().isin({"", "nan", "none"})
    cleaned = raw_text.str.replace(",", "", regex=False).str.replace("，", "", regex=False)
    normalized = pd.to_numeric(cleaned, errors="coerce")
    invalid_rows = int((raw_non_empty & normalized.isna()).sum())
    return normalized.fillna(0), invalid_rows


def pick_first_non_empty(series: pd.Series) -> Optional[str]:
    for value in series:
        normalized = normalize_supplier_card_value(value)
        if normalized is not None:
            return normalized
    return None
