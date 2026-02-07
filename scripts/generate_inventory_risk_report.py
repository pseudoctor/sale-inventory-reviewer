#!/usr/bin/env python3
"""Generate inventory risk Excel report from windowed daily sales and inventory data."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re

import numpy as np
import pandas as pd
import yaml
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill

BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"

DEFAULT_CONFIG = {
    "raw_data_dir": "./raw_data",
    "output_file": "./reports/inventory_risk_report.xlsx",
    "sales_files": [],
    "inventory_file": "",
    "risk_days_high": 60,
    "risk_days_low": 45,
    "sales_window_full_months": 3,
    "sales_window_include_mtd": True,
    "sales_window_recent_days": 30,
    "season_mode": False,
    "brand_keywords": [],
}


def load_config() -> Dict:
    config = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
        config.update({k: loaded.get(k, v) for k, v in config.items()})
    return config


def extract_month_key(filename: str) -> Optional[int]:
    match = re.search(r"(\d{4})(\d{2})", filename)
    if not match:
        return None
    return int(match.group(1) + match.group(2))


def find_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in columns:
            return c
    return None


def normalize_sales_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, str, str, str, str]:
    df = df.copy()
    df.columns = df.columns.str.strip()

    store_col = find_column(df.columns, ["门店名称", "门店", "store"])
    brand_col = find_column(df.columns, ["品牌", "brand"])
    product_col = find_column(df.columns, ["商品名称", "商品", "product"])
    barcode_col = find_column(df.columns, ["商品条码", "条码", "商品编码.1", "商品编码", "barcode"])
    qty_col = find_column(df.columns, ["销售数量", "数量", "sales_qty", "qty"])
    date_col = find_column(df.columns, ["销售时间", "日期", "date", "sales_date"])

    if not all([store_col, brand_col, product_col, barcode_col, qty_col, date_col]):
        missing = [
            name
            for name, col in [
                ("门店名称", store_col),
                ("品牌", brand_col),
                ("商品名称", product_col),
                ("商品条码", barcode_col),
                ("销售数量", qty_col),
                ("销售时间", date_col),
            ]
            if col is None
        ]
        raise ValueError(f"Missing required sales columns: {', '.join(missing)}")

    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df[df[date_col].notna()].copy()

    return df, store_col, brand_col, product_col, barcode_col, qty_col, date_col


def overlap_days(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    data_min_date: pd.Timestamp,
    data_max_date: pd.Timestamp,
) -> int:
    overlap_start = max(start_date, data_min_date)
    overlap_end = min(end_date, data_max_date)
    if overlap_start > overlap_end:
        return 0
    return (overlap_end - overlap_start).days + 1


def combine_daily_sales(
    daily_sales_3m_mtd: pd.Series,
    daily_sales_30d: pd.Series,
    use_peak_mode: bool,
) -> pd.Series:
    if use_peak_mode:
        return pd.concat([daily_sales_3m_mtd, daily_sales_30d], axis=1).max(axis=1)
    return pd.concat([daily_sales_3m_mtd, daily_sales_30d], axis=1).min(axis=1)


def classify_risk_levels(turnover_days: pd.Series, low_days: float, high_days: float) -> pd.Series:
    return pd.Series(
        np.select(
            [turnover_days > high_days, turnover_days < low_days],
            ["高", "低"],
            default="中",
        ),
        index=turnover_days.index,
    )


def normalize_inventory_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, str, str, str, str]:
    df = df.copy()
    df.columns = df.columns.str.strip()

    store_col = find_column(df.columns, ["门店名称", "门店", "store"])
    brand_col = find_column(df.columns, ["品牌", "brand"])
    product_col = find_column(df.columns, ["商品名称", "商品", "product"])
    barcode_col = find_column(df.columns, ["商品条码", "条码", "商品编码.1", "商品编码", "barcode"])
    qty_col = find_column(df.columns, ["数量", "库存数量", "inventory_qty", "qty"])

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

    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)

    return df, store_col, brand_col, product_col, barcode_col, qty_col


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
    # Normalize common spreadsheet artifacts such as "6907...0.0"
    if re.fullmatch(r"\d+\.0+", text):
        return text.split(".", 1)[0]
    return text


def extract_brand_from_product(product: Optional[str], brands: List[str]) -> str:
    if product is None:
        return "其他"
    text = str(product)
    for brand in brands:
        if brand in text:
            return brand
    return "其他"


def ensure_inventory_brand_column(df: pd.DataFrame, brands: List[str]) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip()
    if "品牌" in df.columns:
        return df
    if "商品名称" not in df.columns:
        raise ValueError("Inventory file missing 商品名称; cannot derive 品牌 column.")
    brand_series = df["商品名称"].apply(lambda v: extract_brand_from_product(v, brands))
    insert_at = list(df.columns).index("商品名称")
    df.insert(insert_at, "品牌", brand_series)
    return df


def extract_inventory_date(df: pd.DataFrame) -> str:
    df = df.copy()
    df.columns = df.columns.str.strip()

    date_col = find_column(df.columns, ["库存日期", "日期", "盘点日期", "库存时间", "时间", "inventory_date", "date"])
    if not date_col:
        return "未知"

    raw_series = df[date_col]
    parsed = pd.to_datetime(raw_series, errors="coerce").dropna()
    if not parsed.empty:
        date_value = parsed.max().date()
        return date_value.isoformat()

    non_null = raw_series.dropna()
    if non_null.empty:
        return "未知"

    return str(non_null.iloc[0]).strip()


def main() -> None:
    config = load_config()

    raw_data_dir = Path(config["raw_data_dir"])
    if not raw_data_dir.is_absolute():
        raw_data_dir = (BASE_DIR / raw_data_dir).resolve()

    output_file = Path(config["output_file"])
    if not output_file.is_absolute():
        output_file = (BASE_DIR / output_file).resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)

    configured_sales_files = [f for f in config.get("sales_files") or []]
    inventory_file = config.get("inventory_file") or ""
    brand_keywords = [str(b).strip() for b in (config.get("brand_keywords") or []) if str(b).strip()]
    if not brand_keywords:
        brand_keywords = [
            "伊利",
            "蒙牛",
            "南国",
            "阿宝乐",
            "畅鲜选",
            "蜀珑珠",
            "永璞",
            "雅士利",
            "大漠银根",
            "谷栗村",
            "君乐宝",
            "秦俑",
            "红色拖拉机",
            "飞鹤",
            "雀巢",
            "Seesaw",
            "佳贝艾特",
        ]

    # Auto-detect sales files by YYYYMM and keep all available for window calculations
    if configured_sales_files:
        candidate_files = [raw_data_dir / f for f in configured_sales_files]
    else:
        candidate_files = list(raw_data_dir.glob("*.xlsx"))

    sales_candidates = []
    for path in candidate_files:
        month_key = extract_month_key(path.name)
        if month_key is None:
            continue
        sales_candidates.append((month_key, path))

    sales_candidates.sort(key=lambda x: x[0])

    # Inventory data
    inv_path = raw_data_dir / inventory_file
    if not inv_path.exists():
        raise FileNotFoundError(f"Inventory file not found: {inv_path}")

    inv_df = pd.read_excel(inv_path, sheet_name=0, dtype=str)
    inv_df = ensure_inventory_brand_column(inv_df, brand_keywords)
    inventory_date_text = extract_inventory_date(inv_df)
    parsed_inventory_date = pd.to_datetime(inventory_date_text, errors="coerce")
    if pd.isna(parsed_inventory_date):
        raise ValueError(f"Invalid inventory date: {inventory_date_text}")
    inventory_date_ts = pd.Timestamp(parsed_inventory_date).normalize()
    inventory_date = inventory_date_ts.date().isoformat()
    inv_df, inv_store, inv_brand, inv_product, inv_barcode, inv_qty = normalize_inventory_df(inv_df)
    if inv_brand is None:
        inv_df = inv_df.copy()
        inv_df["品牌"] = inv_df[inv_product].apply(lambda v: extract_brand_from_product(v, brand_keywords))
        inv_brand = "品牌"
    inv_df = inv_df[[inv_store, inv_brand, inv_product, inv_barcode, inv_qty]].copy()
    inv_df.columns = ["store", "brand", "product", "barcode", "inventory_qty"]
    inv_df["barcode"] = inv_df["barcode"].apply(normalize_barcode_value)

    sales_data = []
    for _, filepath in sales_candidates:
        filename = filepath.name
        filepath = raw_data_dir / filename
        if not filepath.exists():
            print(f"Warning: missing sales file {filepath}")
            continue
        df = pd.read_excel(filepath, sheet_name=0, dtype=str)
        df, store_col, brand_col, product_col, barcode_col, qty_col, date_col = normalize_sales_df(df)
        df = df[[store_col, brand_col, product_col, barcode_col, qty_col, date_col]].copy()
        df.columns = ["store", "brand", "product", "barcode", "sales_qty", "sales_date"]
        df["barcode"] = df["barcode"].apply(normalize_barcode_value)
        sales_data.append(df)

    if not sales_data:
        raise FileNotFoundError("No sales files were loaded.")

    sales_df = pd.concat(sales_data, ignore_index=True)

    full_months = int(config.get("sales_window_full_months", 3))
    include_mtd = bool(config.get("sales_window_include_mtd", True))
    recent_days = int(config.get("sales_window_recent_days", 30))
    season_mode_raw = config.get("season_mode", False)
    if isinstance(season_mode_raw, bool):
        use_peak_mode = season_mode_raw
    else:
        mode_text = str(season_mode_raw).strip().lower()
        if mode_text in {"true", "peak"}:
            use_peak_mode = True
        elif mode_text in {"false", "off_peak"}:
            use_peak_mode = False
        else:
            raise ValueError("season_mode must be true/false (or legacy peak/off_peak)")
    if full_months < 0:
        raise ValueError("sales_window_full_months must be >= 0")
    if recent_days <= 0:
        raise ValueError("sales_window_recent_days must be > 0")

    mtd_end = inventory_date_ts if include_mtd else (inventory_date_ts.replace(day=1) - pd.Timedelta(days=1))
    mtd_start = (inventory_date_ts.replace(day=1) - pd.DateOffset(months=full_months)).normalize()
    recent_start = (inventory_date_ts - pd.Timedelta(days=recent_days - 1)).normalize()

    sales_df["sales_date"] = pd.to_datetime(sales_df["sales_date"], errors="coerce")
    sales_df = sales_df[sales_df["sales_date"].notna()].copy()
    if sales_df.empty:
        raise ValueError("Sales data has no valid dates after parsing 销售时间.")

    data_min_date = sales_df["sales_date"].min().normalize()
    data_max_date = sales_df["sales_date"].max().normalize()
    mtd_days = overlap_days(mtd_start, mtd_end, data_min_date, data_max_date)
    recent_days_effective = overlap_days(recent_start, inventory_date_ts, data_min_date, data_max_date)
    has_mtd_window_data = mtd_days > 0
    has_recent_window_data = recent_days_effective > 0

    sales_mtd = sales_df[(sales_df["sales_date"] >= mtd_start) & (sales_df["sales_date"] <= mtd_end)]
    sales_30d = sales_df[(sales_df["sales_date"] >= recent_start) & (sales_df["sales_date"] <= inventory_date_ts)]

    sales_totals = (
        sales_df.groupby(["store", "brand", "product", "barcode"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "sales_qty_total"})
    )
    daily_3m_mtd = (
        sales_mtd.groupby(["store", "brand", "product", "barcode"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "sales_qty_3m_mtd"})
    )
    daily_30d = (
        sales_30d.groupby(["store", "brand", "product", "barcode"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "sales_qty_30d"})
    )
    sales_totals = sales_totals.merge(daily_3m_mtd, on=["store", "brand", "product", "barcode"], how="left")
    sales_totals = sales_totals.merge(daily_30d, on=["store", "brand", "product", "barcode"], how="left")
    sales_totals["sales_qty_3m_mtd"] = sales_totals["sales_qty_3m_mtd"].fillna(0)
    sales_totals["sales_qty_30d"] = sales_totals["sales_qty_30d"].fillna(0)
    sales_totals["daily_sales_3m_mtd"] = (
        sales_totals["sales_qty_3m_mtd"] / mtd_days if has_mtd_window_data else 0
    )
    sales_totals["daily_sales_30d"] = (
        sales_totals["sales_qty_30d"] / recent_days_effective if has_recent_window_data else 0
    )
    sales_totals["forecast_daily_sales"] = combine_daily_sales(
        sales_totals["daily_sales_3m_mtd"],
        sales_totals["daily_sales_30d"],
        use_peak_mode,
    )
    sales_totals["barcode_key"] = sales_totals["barcode"].apply(normalize_barcode_value)

    inv_totals = (
        inv_df.groupby(["store", "brand", "product", "barcode"], as_index=False)["inventory_qty"]
        .sum()
    )
    inv_totals["barcode_key"] = inv_totals["barcode"].apply(normalize_barcode_value)

    # Match sales to inventory: barcode match first, fallback to store+brand+product
    sales_barcode = sales_totals[sales_totals["barcode_key"].notna()].copy()
    inv_barcode = inv_totals[inv_totals["barcode_key"].notna()].copy()

    detail = inv_totals.copy()
    detail = detail.merge(
        sales_barcode[["store", "barcode_key", "daily_sales_3m_mtd", "daily_sales_30d", "forecast_daily_sales"]],
        on=["store", "barcode_key"],
        how="left",
    )

    fallback_sales = sales_totals.groupby(["store", "brand", "product"], as_index=False).agg({
        "daily_sales_3m_mtd": "sum",
        "daily_sales_30d": "sum",
        "forecast_daily_sales": "sum",
    })
    missing_mask = detail["forecast_daily_sales"].isna()
    if missing_mask.any():
        fallback = detail.loc[missing_mask].merge(
            fallback_sales,
            on=["store", "brand", "product"],
            how="left",
            suffixes=("", "_fallback"),
        )
        detail.loc[missing_mask, "daily_sales_3m_mtd"] = fallback["daily_sales_3m_mtd_fallback"].values
        detail.loc[missing_mask, "daily_sales_30d"] = fallback["daily_sales_30d_fallback"].values
        detail.loc[missing_mask, "forecast_daily_sales"] = fallback["forecast_daily_sales_fallback"].values

    detail["daily_sales_3m_mtd"] = detail["daily_sales_3m_mtd"].fillna(0)
    detail["daily_sales_30d"] = detail["daily_sales_30d"].fillna(0)
    detail["forecast_daily_sales"] = detail["forecast_daily_sales"].fillna(0)

    detail["inventory_sales_ratio"] = np.where(
        detail["forecast_daily_sales"] > 0,
        detail["inventory_qty"] / detail["forecast_daily_sales"],
        float("inf"),
    )
    detail["turnover_rate"] = np.where(
        detail["inventory_qty"] > 0,
        (detail["forecast_daily_sales"] * 30) / detail["inventory_qty"],
        0,
    )
    detail["turnover_days"] = np.where(
        detail["forecast_daily_sales"] > 0,
        np.round(detail["inventory_qty"] / detail["forecast_daily_sales"]),
        float("inf"),
    )

    high_days = float(config.get("risk_days_high", 60))
    low_days = float(config.get("risk_days_low", 45))

    detail["risk_level"] = classify_risk_levels(detail["turnover_days"], low_days, high_days)
    
    detail = detail[[
        "store",
        "brand",
        "barcode",
        "product",
        "daily_sales_3m_mtd",
        "daily_sales_30d",
        "forecast_daily_sales",
        "inventory_qty",
        "risk_level",
        "inventory_sales_ratio",
        "turnover_rate",
        "turnover_days",
    ]]

    detail = detail.sort_values(["store", "brand", "product", "barcode"]).reset_index(drop=True)

    # Store summary
    store_summary = detail.groupby("store", as_index=False).agg({
        "daily_sales_3m_mtd": "sum",
        "daily_sales_30d": "sum",
        "forecast_daily_sales": "sum",
        "inventory_qty": "sum",
    })
    store_summary["inventory_sales_ratio"] = np.where(
        store_summary["forecast_daily_sales"] > 0,
        store_summary["inventory_qty"] / store_summary["forecast_daily_sales"],
        float("inf"),
    )
    store_summary["turnover_rate"] = np.where(
        store_summary["inventory_qty"] > 0,
        (store_summary["forecast_daily_sales"] * 30) / store_summary["inventory_qty"],
        0,
    )
    store_summary["turnover_days"] = np.where(
        store_summary["forecast_daily_sales"] > 0,
        np.round(store_summary["inventory_qty"] / store_summary["forecast_daily_sales"]),
        float("inf"),
    )
    store_summary["risk_level"] = classify_risk_levels(store_summary["turnover_days"], low_days, high_days)

    # Brand summary
    brand_summary = detail.groupby("brand", as_index=False).agg({
        "daily_sales_3m_mtd": "sum",
        "daily_sales_30d": "sum",
        "forecast_daily_sales": "sum",
        "inventory_qty": "sum",
    })
    brand_summary["inventory_sales_ratio"] = np.where(
        brand_summary["forecast_daily_sales"] > 0,
        brand_summary["inventory_qty"] / brand_summary["forecast_daily_sales"],
        float("inf"),
    )
    brand_summary["turnover_rate"] = np.where(
        brand_summary["inventory_qty"] > 0,
        (brand_summary["forecast_daily_sales"] * 30) / brand_summary["inventory_qty"],
        0,
    )
    brand_summary["turnover_days"] = np.where(
        brand_summary["forecast_daily_sales"] > 0,
        np.round(brand_summary["inventory_qty"] / brand_summary["forecast_daily_sales"]),
        float("inf"),
    )
    brand_summary["risk_level"] = classify_risk_levels(brand_summary["turnover_days"], low_days, high_days)

    # Missing-in-inventory list: sales with qty but no inventory match
    inv_barcode_keys = set(zip(inv_totals["store"], inv_totals["barcode_key"]))
    inv_fallback_keys = set(zip(inv_totals["store"], inv_totals["brand"], inv_totals["product"]))

    def is_missing(row) -> bool:
        if row["barcode_key"] is not None:
            if (row["store"], row["barcode_key"]) in inv_barcode_keys:
                return False
        if (row["store"], row["brand"], row["product"]) in inv_fallback_keys:
            return False
        return True

    missing_sales = sales_totals.copy()
    missing_sales["is_missing"] = missing_sales.apply(is_missing, axis=1)
    missing_sales = missing_sales[missing_sales["is_missing"] & (missing_sales["forecast_daily_sales"] > 0)]

    # Append missing items into detail table
    if not missing_sales.empty:
        missing_detail = missing_sales.copy()
        missing_detail["inventory_qty"] = 0
        missing_detail["inventory_sales_ratio"] = float("inf")
        missing_detail["turnover_rate"] = 0
        missing_detail["turnover_days"] = float("inf")
        missing_detail["risk_level"] = "高"
        missing_detail = missing_detail[[
            "store",
            "brand",
            "barcode",
            "product",
            "daily_sales_3m_mtd",
            "daily_sales_30d",
            "forecast_daily_sales",
            "inventory_qty",
                "risk_level",
            "inventory_sales_ratio",
            "turnover_rate",
            "turnover_days",
        ]]
        detail = pd.concat([detail, missing_detail], ignore_index=True)

    detail = detail.sort_values(["store", "brand", "product", "barcode"]).reset_index(drop=True)

    detail["out_of_stock"] = np.where(
        (detail["forecast_daily_sales"] > 0) & (detail["inventory_qty"] == 0),
        "是",
        "否",
    )

    # Suggested outbound/replenishment quantities (store-SKU only, no lane matching)
    detail["daily_demand"] = detail["forecast_daily_sales"]
    detail["low_target_qty"] = detail["daily_demand"] * low_days
    detail["high_keep_qty"] = detail["daily_demand"] * high_days
    detail["need_qty"] = np.where(
        detail["forecast_daily_sales"] > 0,
        np.ceil(np.maximum(0, detail["low_target_qty"] - detail["inventory_qty"])),
        0,
    )
    detail["suggest_outbound_qty"] = np.where(
        (detail["risk_level"] == "高") & (detail["forecast_daily_sales"] > 0),
        np.floor(np.maximum(0, detail["inventory_qty"] - detail["high_keep_qty"])),
        0,
    )
    detail["suggest_replenish_qty"] = np.where(
        (detail["risk_level"] == "低") | (detail["out_of_stock"] == "是"),
        detail["need_qty"],
        0,
    )
    detail["suggest_outbound_qty"] = detail["suggest_outbound_qty"].astype(int)
    detail["suggest_replenish_qty"] = detail["suggest_replenish_qty"].astype(int)

    # Rename columns to Chinese for output
    detail_out = detail.rename(columns={
        "store": "门店名称",
        "brand": "品牌",
        "barcode": "商品条码",
        "product": "商品名称",
        "daily_sales_3m_mtd": "近三月+本月迄今平均日销",
        "daily_sales_30d": "近30天平均日销售",
        "inventory_qty": "库存数量",
        "out_of_stock": "缺货",
        "risk_level": "风险等级",
        "inventory_sales_ratio": "库存/销售比",
        "turnover_rate": "库存周转率",
        "turnover_days": "库存周转天数",
        "suggest_outbound_qty": "建议调出数量",
        "suggest_replenish_qty": "建议补货数量",
    })
    detail_out["商品条码"] = detail_out["商品条码"].astype(str)
    detail_out = detail_out[[
        "门店名称",
        "品牌",
        "商品条码",
        "商品名称",
        "近三月+本月迄今平均日销",
        "近30天平均日销售",
        "库存数量",
        "缺货",
        "风险等级",
        "库存/销售比",
        "库存周转率",
        "库存周转天数",
        "建议调出数量",
        "建议补货数量",
    ]]

    store_summary_out = store_summary.rename(columns={
        "store": "门店名称",
        "daily_sales_3m_mtd": "近三月+本月迄今平均日销",
        "daily_sales_30d": "近30天平均日销售",
        "forecast_daily_sales": "预测平均日销(季节模式后)",
        "inventory_qty": "库存数量",
        "risk_level": "风险等级",
        "inventory_sales_ratio": "库存/销售比",
        "turnover_rate": "库存周转率",
        "turnover_days": "库存周转天数",
    })
    store_summary_out = store_summary_out[[
        "门店名称",
        "近三月+本月迄今平均日销",
        "近30天平均日销售",
        "预测平均日销(季节模式后)",
        "库存数量",
        "库存/销售比",
        "库存周转率",
        "库存周转天数",
        "风险等级",
    ]]

    brand_summary_out = brand_summary.rename(columns={
        "brand": "品牌",
        "daily_sales_3m_mtd": "近三月+本月迄今平均日销",
        "daily_sales_30d": "近30天平均日销售",
        "forecast_daily_sales": "预测平均日销(季节模式后)",
        "inventory_qty": "库存数量",
        "risk_level": "风险等级",
        "inventory_sales_ratio": "库存/销售比",
        "turnover_rate": "库存周转率",
        "turnover_days": "库存周转天数",
    })
    brand_summary_out = brand_summary_out[[
        "品牌",
        "近三月+本月迄今平均日销",
        "近30天平均日销售",
        "预测平均日销(季节模式后)",
        "库存数量",
        "库存/销售比",
        "库存周转率",
        "库存周转天数",
        "风险等级",
    ]]

    missing_out = missing_sales[[
        "store",
        "brand",
        "barcode",
        "product",
        "daily_sales_3m_mtd",
        "daily_sales_30d",
    ]].rename(columns={
        "store": "门店名称",
        "brand": "品牌",
        "barcode": "商品条码",
        "product": "商品名称",
        "daily_sales_3m_mtd": "近三月+本月迄今平均日销",
        "daily_sales_30d": "近30天平均日销售",
    })
    missing_out["商品条码"] = missing_out["商品条码"].astype(str)
    replenish_lookup_exact = (
        detail_out[["门店名称", "品牌", "商品条码", "商品名称", "建议补货数量"]]
        .groupby(["门店名称", "品牌", "商品条码", "商品名称"], as_index=False)["建议补货数量"]
        .max()
    )
    missing_out = missing_out.merge(
        replenish_lookup_exact,
        on=["门店名称", "品牌", "商品条码", "商品名称"],
        how="left",
    )
    replenish_lookup_fallback = (
        detail_out[["门店名称", "品牌", "商品名称", "建议补货数量"]]
        .groupby(["门店名称", "品牌", "商品名称"], as_index=False)["建议补货数量"]
        .max()
        .rename(columns={"建议补货数量": "建议补货数量_fallback"})
    )
    missing_out = missing_out.merge(
        replenish_lookup_fallback,
        on=["门店名称", "品牌", "商品名称"],
        how="left",
    )
    missing_out["建议补货数量"] = (
        missing_out["建议补货数量"].fillna(missing_out["建议补货数量_fallback"]).fillna(0).astype(int)
    )
    missing_out = missing_out.drop(columns=["建议补货数量_fallback"])
    missing_out = missing_out[[
        "门店名称",
        "品牌",
        "商品条码",
        "商品名称",
        "近三月+本月迄今平均日销",
        "近30天平均日销售",
        "建议补货数量",
    ]]

    replenish_out = detail_out[detail_out["建议补货数量"] > 0].copy()
    replenish_out = replenish_out[[
        "门店名称",
        "品牌",
        "商品条码",
        "商品名称",
        "近三月+本月迄今平均日销",
        "近30天平均日销售",
        "库存数量",
        "缺货",
        "风险等级",
        "建议补货数量",
    ]]

    transfer_out = detail_out[detail_out["建议调出数量"] > 0].copy()
    transfer_out = transfer_out[[
        "门店名称",
        "品牌",
        "商品条码",
        "商品名称",
        "近三月+本月迄今平均日销",
        "近30天平均日销售",
        "库存数量",
        "风险等级",
        "建议调出数量",
    ]]

    # Summary sheet
    summary_rows = [
        ["风险等级-高", int((detail_out["风险等级"] == "高").sum())],
        ["风险等级-中", int((detail_out["风险等级"] == "中").sum())],
        ["风险等级-低", int((detail_out["风险等级"] == "低").sum())],
        ["缺货/库存缺失SKU数", int(len(missing_out))],
        ["库存总量", float(detail_out["库存数量"].sum())],
        ["近三月+本月迄今平均日销总量", float(detail_out["近三月+本月迄今平均日销"].sum())],
        ["近30天平均日销售总量", float(detail_out["近30天平均日销售"].sum())],
        ["预测平均日销总量(季节模式后)", float(detail["forecast_daily_sales"].sum())],
        ["季节模式", "旺季(取高值)" if use_peak_mode else "淡季(取低值)"],
        [
            "窗口数据状态",
            "正常" if (has_mtd_window_data and has_recent_window_data)
            else f"警告: 3M+MTD有效={has_mtd_window_data}, 30天有效={has_recent_window_data}",
        ],
    ]
    summary_out = pd.DataFrame(summary_rows, columns=["指标", "数值"])

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        detail_out.to_excel(writer, sheet_name="明细", index=False)
        store_summary_out.to_excel(writer, sheet_name="门店汇总", index=False)
        brand_summary_out.to_excel(writer, sheet_name="品牌汇总", index=False)
        missing_out.to_excel(writer, sheet_name="缺货清单", index=False)
        replenish_out.to_excel(writer, sheet_name="建议补货清单", index=False)
        transfer_out.to_excel(writer, sheet_name="建议调货清单", index=False)
        summary_out.to_excel(writer, sheet_name="汇总", index=False)

    # Apply styling and merging
    wb = load_workbook(output_file)

    header_fill = PatternFill(start_color="E8EAF6", end_color="E8EAF6", fill_type="solid")
    header_font = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center")
    left = Alignment(horizontal="left", vertical="center")

    risk_fills = {
        "高": PatternFill(start_color="F44336", end_color="F44336", fill_type="solid"),
        "中": PatternFill(start_color="F59E0B", end_color="F59E0B", fill_type="solid"),
        "低": PatternFill(start_color="4CAF50", end_color="4CAF50", fill_type="solid"),
    }
    out_of_stock_fill = PatternFill(start_color="7C3AED", end_color="7C3AED", fill_type="solid")

    def style_sheet(ws, sheet_name: str):
        ws.insert_rows(1)
        title = f"库存日期：{inventory_date}"
        ws.cell(row=1, column=1, value=title)
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=ws.max_column)
        ws.cell(row=1, column=1).font = Font(bold=True)
        ws.cell(row=1, column=1).alignment = center

        ws.freeze_panes = "E3" if sheet_name == "明细" else "A3"
        last_col = ws.max_column
        last_col_letter = ws.cell(row=2, column=last_col).column_letter
        ws.auto_filter.ref = f"A2:{last_col_letter}{ws.max_row}"
        for cell in ws[2]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center

        # Auto width based on cell content (with caps for readability)
        headers = [c.value for c in ws[2]]
        preferred_widths = {
            "门店名称": 18,
            "品牌": 10,
            "商品条码": 16,
            "商品名称": 36,
            "近三月+本月迄今平均日销": 22,
            "近30天平均日销售": 16,
            "库存数量": 10,
            "风险等级": 8,
            "库存/销售比": 12,
            "库存周转率": 12,
            "库存周转天数": 12,
            "建议调出数量": 12,
            "建议补货数量": 12,
        }

        for col in ws.columns:
            max_len = 0
            col_letter = ws.cell(row=2, column=col[0].column).column_letter
            header = ws.cell(row=2, column=col[0].column).value
            for cell in col:
                if cell.value is None:
                    continue
                max_len = max(max_len, len(str(cell.value)))
            auto_width = max_len + 2
            if header in preferred_widths:
                ws.column_dimensions[col_letter].width = max(preferred_widths[header], min(auto_width, 40))
            else:
                ws.column_dimensions[col_letter].width = min(auto_width, 40)

        # Align text columns to left, numbers to center
        for row in ws.iter_rows(min_row=3, max_row=ws.max_row):
            for cell in row:
                if isinstance(cell.value, (int, float)):
                    cell.alignment = center
                else:
                    cell.alignment = left

        # Wrap long text columns for readability
        wrap_columns = {"门店名称", "商品名称"}
        for col_idx, header in enumerate(headers, start=1):
            if header in wrap_columns:
                for row in ws.iter_rows(min_row=3, max_row=ws.max_row):
                    row[col_idx - 1].alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)

        # Risk color
        headers = [c.value for c in ws[2]]
        if "风险等级" in headers:
            risk_idx = headers.index("风险等级") + 1
            for row in ws.iter_rows(min_row=3, max_row=ws.max_row):
                cell = row[risk_idx - 1]
                fill = risk_fills.get(cell.value)
                if fill:
                    cell.fill = fill
                    cell.font = Font(color="FFFFFF", bold=True)
                    cell.alignment = center

        # Barcode column as text
        if "商品条码" in headers:
            barcode_idx = headers.index("商品条码") + 1
            for row in ws.iter_rows(min_row=3, max_row=ws.max_row):
                row[barcode_idx - 1].number_format = "@"

        # Out-of-stock highlight (avg sales > 0, inventory == 0)
        if "近三月+本月迄今平均日销" in headers and "库存数量" in headers:
            avg_idx = headers.index("近三月+本月迄今平均日销") + 1
            inv_idx = headers.index("库存数量") + 1
            for row in ws.iter_rows(min_row=3, max_row=ws.max_row):
                avg_val = row[avg_idx - 1].value or 0
                inv_val = row[inv_idx - 1].value or 0
                if avg_val > 0 and inv_val == 0:
                    for cell in row:
                        cell.fill = out_of_stock_fill
                        cell.font = Font(color="FFFFFF", bold=True)

        # Number format
        number_formats = {
            "近三月+本月迄今平均日销": "0.000",
            "近30天平均日销售": "0.000",
            "库存数量": "0",
            "库存/销售比": "0.0",
            "库存周转率": "0.0%",
            "库存周转天数": "0",
            "建议调出数量": "0",
            "建议补货数量": "0",
        }
        for name, fmt in number_formats.items():
            if name in headers:
                idx = headers.index(name) + 1
                for row in ws.iter_rows(min_row=3, max_row=ws.max_row):
                    row[idx - 1].number_format = fmt

    for sheet in ["明细", "门店汇总", "品牌汇总", "缺货清单", "建议补货清单", "建议调货清单", "汇总"]:
        style_sheet(wb[sheet], sheet)

    # Merge same store in Detail sheet
    ws_detail = wb["明细"]
    headers = [c.value for c in ws_detail[2]]
    if "门店名称" in headers:
        store_idx = headers.index("门店名称") + 1
        start_row = 3
        current = ws_detail.cell(row=3, column=store_idx).value
        for row in range(4, ws_detail.max_row + 2):
            value = ws_detail.cell(row=row, column=store_idx).value if row <= ws_detail.max_row else None
            if value != current:
                end_row = row - 1
                if end_row > start_row:
                    ws_detail.merge_cells(start_row=start_row, start_column=store_idx, end_row=end_row, end_column=store_idx)
                    ws_detail.cell(row=start_row, column=store_idx).alignment = center
                start_row = row
                current = value

    # Apply borders after merges for all active cells
    from openpyxl.styles import Border, Side
    thin = Side(style="thin", color="000000")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for ws in wb.worksheets:
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column):
            for cell in row:
                cell.border = border

    wb.save(output_file)

    print(f"Report saved: {output_file}")


if __name__ == "__main__":
    main()
