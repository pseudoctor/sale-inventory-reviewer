from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

from .column_detection import extract_month_key as extract_month_key
from .column_detection import find_column as find_column
from .column_detection import find_sales_amount_column as find_sales_amount_column
from .column_detection import format_ignored_sales_files as format_ignored_sales_files
from .column_detection import list_ignored_sales_files as list_ignored_sales_files
from .column_detection import resolve_sales_candidates as resolve_sales_candidates
from .normalization import (
    normalize_barcode_value as normalize_barcode_value,
    normalize_numeric_series as normalize_numeric_series,
    normalize_numeric_value as normalize_numeric_value,
    normalize_supplier_card_value as normalize_supplier_card_value,
    pick_first_non_empty as pick_first_non_empty,
)


def read_excel_first_sheet(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".xls" and importlib.util.find_spec("xlrd") is None:
        raise ModuleNotFoundError(
            f"Reading .xls requires 'xlrd'. Install with: python3 -m pip install xlrd (file: {path})"
        )
    return pd.read_excel(path, sheet_name=0, dtype=str)


def build_unambiguous_barcode_map(df: pd.DataFrame, group_cols: List[str], value_col: str, output_col: str) -> pd.DataFrame:
    base = df[group_cols + [value_col]].copy()
    base[value_col] = base[value_col].apply(normalize_barcode_value)
    base = base[base[value_col].notna()]
    if base.empty:
        return pd.DataFrame(columns=group_cols + [output_col])
    dedup = base.drop_duplicates(subset=group_cols + [value_col], keep="first")
    counts = dedup.groupby(group_cols, as_index=False)[value_col].nunique().rename(columns={value_col: "_nuniq"})
    one_to_one = dedup.merge(counts, on=group_cols, how="left")
    one_to_one = one_to_one[one_to_one["_nuniq"] == 1].drop(columns=["_nuniq"])
    one_to_one = one_to_one.drop_duplicates(subset=group_cols, keep="first")
    return one_to_one.rename(columns={value_col: output_col})


def build_unambiguous_source_to_target_map(
    df: pd.DataFrame,
    source_col: str,
    target_col: str,
    output_col: str,
) -> tuple[pd.DataFrame, int]:
    base = df[[source_col, target_col]].copy()
    base[source_col] = base[source_col].apply(normalize_barcode_value)
    base[target_col] = base[target_col].apply(normalize_barcode_value)
    base = base[base[source_col].notna() & base[target_col].notna()]
    if base.empty:
        return pd.DataFrame(columns=[source_col, output_col]), 0

    dedup = base.drop_duplicates(subset=[source_col, target_col], keep="first")
    counts = dedup.groupby(source_col, as_index=False)[target_col].nunique().rename(columns={target_col: "_nuniq"})
    ambiguous_sources = int((counts["_nuniq"] > 1).sum())
    one_to_one = dedup.merge(counts, on=source_col, how="left")
    one_to_one = one_to_one[one_to_one["_nuniq"] == 1].drop(columns=["_nuniq"])
    one_to_one = one_to_one.drop_duplicates(subset=[source_col], keep="first")
    return one_to_one.rename(columns={target_col: output_col}), ambiguous_sources


def normalize_sales_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, Optional[str], str, str, str, str, Optional[str]]:
    """标准化销售表核心字段，并返回匹配到的列名。"""
    df = df.copy()
    df.columns = df.columns.str.strip()

    store_col = find_column(df.columns.tolist(), ["门店名称", "门店", "store"])
    brand_col = find_column(df.columns.tolist(), ["品牌", "brand"])
    product_col = find_column(df.columns.tolist(), ["商品名称", "商品", "product"])
    barcode_col = find_column(df.columns.tolist(), ["国条码", "商品条码", "条码", "商品编码.1", "商品编码", "barcode"])
    qty_col = find_column(df.columns.tolist(), ["销售数量", "数量", "sales_qty", "qty"])
    date_col = find_column(df.columns.tolist(), ["销售时间", "日期", "date", "sales_date"])
    supplier_card_col = find_column(df.columns.tolist(), ["供商卡号", "供应商卡号", "supplier_card", "supplier_code"])

    if not all([store_col, product_col, barcode_col, qty_col, date_col]):
        missing = [
            name
            for name, col in [
                ("门店名称", store_col),
                ("商品名称", product_col),
                ("商品条码", barcode_col),
                ("销售数量", qty_col),
                ("销售时间", date_col),
            ]
            if col is None
        ]
        raise ValueError(f"Missing required sales columns: {', '.join(missing)}")

    qty_series, invalid_qty_rows = normalize_numeric_series(df[qty_col])
    df[qty_col] = qty_series
    df.attrs["invalid_qty_rows"] = invalid_qty_rows
    return df, store_col, brand_col, product_col, barcode_col, qty_col, date_col, supplier_card_col


def normalize_inventory_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, Optional[str], str, str, str, Optional[str]]:
    df = df.copy()
    df.columns = df.columns.str.strip()

    store_col = find_column(df.columns.tolist(), ["门店名称", "门店", "store"])
    brand_col = find_column(df.columns.tolist(), ["品牌", "brand"])
    product_col = find_column(df.columns.tolist(), ["商品名称", "商品", "product"])
    barcode_col = find_column(df.columns.tolist(), ["国条码", "商品条码", "条码", "商品编码.1", "商品编码", "barcode"])
    qty_col = find_column(df.columns.tolist(), ["数量", "库存数量", "当前库存", "inventory_qty", "qty"])
    supplier_card_col = find_column(df.columns.tolist(), ["供商卡号", "供应商卡号", "supplier_card", "supplier_code"])

    if not all([store_col, product_col, barcode_col, qty_col]):
        missing = [
            name
            for name, col in [
                ("门店名称", store_col),
                ("商品名称", product_col),
                ("商品条码", barcode_col),
                ("数量", qty_col),
            ]
            if col is None
        ]
        raise ValueError(f"Missing required inventory columns: {', '.join(missing)}")

    qty_series, invalid_qty_rows = normalize_numeric_series(df[qty_col])
    df[qty_col] = qty_series
    df.attrs["invalid_qty_rows"] = invalid_qty_rows
    return df, store_col, brand_col, product_col, barcode_col, qty_col, supplier_card_col


def parse_sales_dates(raw_dates: pd.Series, date_format: str, dayfirst: bool) -> pd.Series:
    if date_format.strip():
        return pd.to_datetime(raw_dates, format=date_format.strip(), errors="coerce")
    return pd.to_datetime(raw_dates, errors="coerce", dayfirst=dayfirst)


def load_carton_factor_df(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Carton factor file not found: {path}")
    df = read_excel_first_sheet(path)
    df.columns = df.columns.astype(str).str.strip()

    barcode_col = find_column(df.columns.tolist(), ["商品条码", "条码", "商品编码.1", "商品编码", "barcode"])
    product_col = find_column(df.columns.tolist(), ["商品名称", "商品", "product"])
    factor_col = find_column(df.columns.tolist(), ["装箱数（因子）", "装箱数(因子)", "装箱数", "因子", "factor"])
    if not all([barcode_col, product_col, factor_col]):
        missing = [name for name, col in [("商品条码", barcode_col), ("商品名称", product_col), ("装箱数（因子）", factor_col)] if col is None]
        raise ValueError(f"Carton factor file missing columns: {', '.join(missing)}")

    out = df[[barcode_col, product_col, factor_col]].copy()
    out.columns = ["商品条码", "商品名称", "装箱数（因子）"]
    out["商品条码"] = out["商品条码"].apply(normalize_barcode_value)
    out["商品名称"] = out["商品名称"].astype(str).str.strip()
    out["装箱数（因子）"] = pd.to_numeric(out["装箱数（因子）"], errors="coerce")
    out = out[out["装箱数（因子）"].notna() & (out["装箱数（因子）"] > 0)]
    out["装箱数（因子）"] = out["装箱数（因子）"].astype(int)
    return out


def extract_brand_from_product(product: Optional[str], brands: List[str]) -> str:
    if product is None:
        return "其他"
    text = str(product)
    best_brand: Optional[str] = None
    best_pos: Optional[int] = None
    for brand in brands:
        keyword = str(brand).strip()
        if keyword == "":
            continue
        pos = text.find(keyword)
        if pos == -1:
            continue
        if best_pos is None or pos < best_pos:
            best_pos = pos
            best_brand = keyword
    if best_brand is not None:
        return best_brand
    return "其他"


def _normalize_brand_value(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none"}:
        return None
    return text


def fill_brand_from_product(
    df: pd.DataFrame,
    *,
    product_col: str,
    brands: List[str],
    brand_col: str = "品牌",
    insert_before_col: Optional[str] = None,
) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    if product_col not in df.columns:
        raise ValueError(f"Missing {product_col}; cannot derive {brand_col} column.")
    if brand_col not in df.columns:
        insert_anchor = insert_before_col or product_col
        insert_at = list(df.columns).index(insert_anchor)
        df.insert(insert_at, brand_col, None)

    existing_brand = df[brand_col].apply(_normalize_brand_value)
    fallback_brand = df[product_col].apply(lambda v: extract_brand_from_product(v, brands))
    df[brand_col] = existing_brand.fillna(fallback_brand)
    return df


def ensure_inventory_brand_column(df: pd.DataFrame, brands: List[str]) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    product_col = find_column(df.columns.tolist(), ["商品名称", "商品", "product"])
    if product_col is None:
        raise ValueError("Missing 商品名称/商品/product; cannot derive 品牌 column.")
    return fill_brand_from_product(
        df,
        product_col=product_col,
        brands=brands,
        brand_col="品牌",
        insert_before_col=product_col,
    )


def ensure_sales_brand_column(df: pd.DataFrame, brands: List[str]) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    return fill_brand_from_product(
        df,
        product_col="product",
        brands=brands,
        brand_col="brand",
        insert_before_col="product",
    )


def extract_inventory_date(df: pd.DataFrame) -> str:
    df = df.copy()
    df.columns = df.columns.str.strip()
    date_col = find_column(df.columns.tolist(), ["库存日期", "日期", "盘点日期", "库存时间", "时间", "inventory_date", "date"])
    if not date_col:
        return "未知"
    raw_series = df[date_col]
    parsed = pd.to_datetime(raw_series, errors="coerce").dropna()
    if not parsed.empty:
        return parsed.max().date().isoformat()
    non_null = raw_series.dropna()
    if non_null.empty:
        return "未知"
    return str(non_null.iloc[0]).strip()
