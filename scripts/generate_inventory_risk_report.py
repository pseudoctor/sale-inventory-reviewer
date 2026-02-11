#!/usr/bin/env python3
"""Generate inventory risk Excel report from windowed daily sales and inventory data."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import sys

import numpy as np
import pandas as pd

if __package__ in {None, ""}:
    # Allow running as `python3 scripts/generate_inventory_risk_report.py`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.core import batch as core_batch
from scripts.core import config as core_config
from scripts.core import io as core_io
from scripts.core import metrics as core_metrics
from scripts.core import recommendations as core_recommendations
from scripts.core import report_writer as core_report_writer

BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"
PROGRAM_VERSION = "1.1.0"

DEFAULT_CONFIG = {
    "run_mode": "single",
    "display_name": "",
    "system_id": "",
    "raw_data_dir": "./raw_data",
    "output_file": "./reports/inventory_risk_report.xlsx",
    "sales_files": [],
    "inventory_file": "",
    "risk_days_high": 60,
    "risk_days_low": 45,
    "sales_window_full_months": 3,
    "sales_window_include_mtd": True,
    "sales_window_recent_days": 30,
    "sales_date_dayfirst": False,
    "sales_date_format": "",
    "season_mode": False,
    "fail_on_empty_window": False,
    "strict_auto_scan": False,
    "carton_factor_file": "./data/sku装箱数.xlsx",
    "brand_keywords": [],
    "batch": {
        "continue_on_error": True,
        "summary_output_file": "./reports/batch_run_summary.xlsx",
        "systems": [],
    },
    "province_column_enabled": None,
}

PREFERRED_WIDTHS = {
    "门店名称": 18,
    "品牌": 10,
    "商品条码": 16,
    "商品名称": 36,
    "省份": 10,
    "装箱数（因子）": 12,
    "近三月+本月迄今平均日销": 22,
    "近30天平均日销售": 16,
    "库存数量": 10,
    "风险等级": 8,
    "库存/销售比": 12,
    "库存周转率": 12,
    "库存周转天数": 12,
    "建议调出数量": 12,
    "建议补货数量": 12,
    "建议补货箱数": 12,
}

NUMBER_FORMATS = {
    "近三月+本月迄今平均日销": "0.000",
    "近30天平均日销售": "0.000",
    "库存数量": "0",
    "装箱数（因子）": "0",
    "库存/销售比": "0.0",
    "库存周转率": "0.0%",
    "库存周转天数": "0",
    "建议调出数量": "0",
    "建议补货数量": "0",
}

SUMMARY_ONE_DECIMAL_METRICS = {
    "近三月+本月迄今平均日销总量",
    "近30天平均日销售总量",
    "预测平均日销总量(季节模式后)",
}

DEFAULT_LEGACY_OUTPUT_FILES = {
    "./reports/inventory_risk_report.xlsx",
    "reports/inventory_risk_report.xlsx",
    "inventory_risk_report.xlsx",
}

AUTO_OUTPUT_DATE_PLACEHOLDER = "{库存日期}"

SUPPLIER_CARD_PROVINCE_MAP = {
    "153085": "宁夏",
    "680249": "甘肃",
    "153412": "宁夏",
    "152901": "监狱系统",
}


def compute_config_snapshot(config: Dict[str, Any]) -> str:
    keys = [
        "risk_days_high",
        "risk_days_low",
        "sales_window_full_months",
        "sales_window_include_mtd",
        "sales_window_recent_days",
        "season_mode",
        "strict_auto_scan",
        "sales_date_dayfirst",
        "sales_date_format",
    ]
    payload = "|".join(f"{k}={config.get(k)}" for k in keys)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def stage_error(stage: str, exc: Exception) -> RuntimeError:
    return RuntimeError(f"[{stage}] {exc}")


def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return core_config.validate_config(config)


def validate_batch_config(config: Dict[str, Any]) -> None:
    core_config.validate_batch_config(config, BASE_DIR)


def build_system_config(system_cfg: Dict[str, Any], global_cfg: Dict[str, Any]) -> Dict[str, Any]:
    return core_config.build_system_config(system_cfg, global_cfg)


def resolve_system_raw_data_dir(config: Dict[str, Any]) -> Path:
    return core_config.resolve_system_raw_data_dir(config, BASE_DIR)


def resolve_output_file_path(config: Dict[str, Any], display_name: str, inventory_date: str) -> Path:
    return core_config.resolve_output_file_path(config, display_name, inventory_date, BASE_DIR)


def resolve_expected_output_for_status(config: Dict[str, Any], display_name: str) -> str:
    return core_config.resolve_expected_output_for_status(config, display_name, BASE_DIR)


def load_config() -> Dict[str, Any]:
    return core_config.load_config(CONFIG_PATH)


def extract_month_key(filename: str) -> Optional[int]:
    return core_io.extract_month_key(filename)


def find_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    return core_io.find_column(columns, candidates)


def read_excel_first_sheet(path: Path) -> pd.DataFrame:
    return core_io.read_excel_first_sheet(path)


def resolve_sales_candidates(raw_data_dir: Path, configured_sales_files: List[str]) -> List[Path]:
    return core_io.resolve_sales_candidates(raw_data_dir, configured_sales_files)


def list_ignored_sales_files(raw_data_dir: Path, configured_sales_files: List[str]) -> List[str]:
    return core_io.list_ignored_sales_files(raw_data_dir, configured_sales_files)


def normalize_sales_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, Optional[str], str, str, str, str, Optional[str]]:
    return core_io.normalize_sales_df(df)


def overlap_days(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    data_min_date: pd.Timestamp,
    data_max_date: pd.Timestamp,
) -> int:
    return core_metrics.overlap_days(start_date, end_date, data_min_date, data_max_date)


def combine_daily_sales(
    daily_sales_3m_mtd: pd.Series,
    daily_sales_30d: pd.Series,
    use_peak_mode: bool,
) -> pd.Series:
    return core_metrics.combine_daily_sales(daily_sales_3m_mtd, daily_sales_30d, use_peak_mode)


def classify_risk_levels(turnover_days: pd.Series, low_days: float, high_days: float) -> pd.Series:
    return core_metrics.classify_risk_levels(turnover_days, low_days, high_days)


def normalize_inventory_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, Optional[str], str, str, str, Optional[str]]:
    return core_io.normalize_inventory_df(df)


def normalize_barcode_value(value) -> Optional[str]:
    return core_io.normalize_barcode_value(value)


def normalize_supplier_card_value(value) -> Optional[str]:
    return core_io.normalize_supplier_card_value(value)


def pick_first_non_empty(series: pd.Series) -> Optional[str]:
    return core_io.pick_first_non_empty(series)


def build_unambiguous_barcode_map(
    df: pd.DataFrame,
    group_cols: List[str],
    value_col: str,
    output_col: str,
) -> pd.DataFrame:
    return core_io.build_unambiguous_barcode_map(df, group_cols, value_col, output_col)


def format_ignored_sales_files(ignored_sales_files: List[str], limit: int = 20, max_chars: int = 2000) -> str:
    return core_io.format_ignored_sales_files(ignored_sales_files, limit=limit, max_chars=max_chars)


def map_province_by_supplier_card(card: Optional[str]) -> str:
    normalized = normalize_supplier_card_value(card)
    return core_recommendations.map_province_by_supplier_card(normalized, SUPPLIER_CARD_PROVINCE_MAP)


def parse_sales_dates(raw_dates: pd.Series, date_format: str, dayfirst: bool) -> pd.Series:
    return core_io.parse_sales_dates(raw_dates, date_format, dayfirst)


def compute_case_counts(qty: pd.Series, factor: pd.Series, use_peak_mode: bool) -> pd.Series:
    return core_recommendations.compute_case_counts(qty, factor, use_peak_mode)


def load_carton_factor_df(path: Path) -> pd.DataFrame:
    return core_io.load_carton_factor_df(path)


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


def apply_inventory_metrics(df: pd.DataFrame, low_days: float, high_days: float) -> pd.DataFrame:
    return core_metrics.apply_inventory_metrics(df, low_days, high_days)


def generate_report_for_system(system_cfg: Dict[str, Any], global_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    config = dict(system_cfg)
    _ = global_cfg
    system_id = str(config.get("system_id", "single")).strip() or "single"
    display_name = str(config.get("display_name", system_id)).strip() or system_id
    is_wumei_system = "物美" in display_name
    province_column_enabled_cfg = config.get("province_column_enabled", None)
    enable_province_column = (
        bool(province_column_enabled_cfg)
        if isinstance(province_column_enabled_cfg, bool)
        else is_wumei_system
    )

    try:
        raw_data_dir = resolve_system_raw_data_dir(config)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("config", exc) from exc

    configured_sales_files = [f for f in config.get("sales_files") or []]
    inventory_file = config.get("inventory_file") or ""
    carton_factor_file = config.get("carton_factor_file") or ""
    strict_auto_scan = bool(config.get("strict_auto_scan", False))
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

    try:
        sales_candidates = resolve_sales_candidates(raw_data_dir, configured_sales_files)
        ignored_sales_files = list_ignored_sales_files(raw_data_dir, configured_sales_files)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("input_read", exc) from exc

    if not configured_sales_files and not sales_candidates:
        ignored_text = format_ignored_sales_files(ignored_sales_files)
        detail = f" Ignored files: {ignored_text}" if ignored_text else ""
        if strict_auto_scan and ignored_sales_files:
            raise RuntimeError(
                "[input_read] strict_auto_scan=true and no valid sales files were detected."
                f" Ignored candidates: {ignored_text}"
            )
        raise FileNotFoundError(
            f"No auto-detected sales files in {raw_data_dir}. "
            "Expected filename with sales keyword and YYYYMM (e.g. 销售202602.xlsx)." + detail
        )

    # Inventory data
    inv_path = raw_data_dir / inventory_file
    if not inv_path.exists():
        raise FileNotFoundError(f"Inventory file not found: {inv_path}")

    try:
        inv_df = read_excel_first_sheet(inv_path)
        inv_df = ensure_inventory_brand_column(inv_df, brand_keywords)
        inventory_date_text = extract_inventory_date(inv_df)
        parsed_inventory_date = pd.to_datetime(inventory_date_text, errors="coerce")
        if pd.isna(parsed_inventory_date):
            raise ValueError(f"Invalid inventory date: {inventory_date_text}")
        inventory_date_ts = pd.Timestamp(parsed_inventory_date).normalize()
        inventory_date = inventory_date_ts.date().isoformat()
        output_file = resolve_output_file_path(config, display_name, inventory_date)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        inv_df, inv_store, inv_brand, inv_product, inv_barcode, inv_qty, inv_supplier_card = normalize_inventory_df(inv_df)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("normalize", exc) from exc
    if inv_brand is None:
        inv_df = inv_df.copy()
        inv_df["品牌"] = inv_df[inv_product].apply(lambda v: extract_brand_from_product(v, brand_keywords))
        inv_brand = "品牌"
    inventory_columns = [inv_store, inv_brand, inv_product, inv_barcode, inv_qty]
    if inv_supplier_card is not None:
        inventory_columns.append(inv_supplier_card)
    inv_df = inv_df[inventory_columns].copy()
    normalized_inventory_columns = ["store", "brand", "product", "barcode", "inventory_qty"]
    if inv_supplier_card is not None:
        normalized_inventory_columns.append("supplier_card")
    inv_df.columns = normalized_inventory_columns
    inv_df["barcode"] = inv_df["barcode"].apply(normalize_barcode_value)
    if "supplier_card" not in inv_df.columns:
        inv_df["supplier_card"] = None
    inv_df["supplier_card"] = inv_df["supplier_card"].apply(normalize_supplier_card_value)

    full_months = int(config.get("sales_window_full_months", 3))
    include_mtd = bool(config.get("sales_window_include_mtd", True))
    recent_days = int(config.get("sales_window_recent_days", 30))
    sales_date_dayfirst = bool(config.get("sales_date_dayfirst", False))
    sales_date_format = str(config.get("sales_date_format", ""))
    season_mode_raw = config.get("season_mode", False)
    fail_on_empty_window = bool(config.get("fail_on_empty_window", False))
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

    sales_data = []
    loaded_sales_file_count = 0
    invalid_sales_date_rows = 0
    for filepath in sales_candidates:
        if not filepath.exists():
            print(f"Warning: missing sales file {filepath}")
            continue
        try:
            df = read_excel_first_sheet(filepath)
            df, store_col, brand_col, product_col, barcode_col, qty_col, date_col, supplier_card_col = normalize_sales_df(df)
        except Exception as exc:  # noqa: BLE001
            raise stage_error("normalize", exc) from exc
        national_barcode_col = find_column(df.columns.tolist(), ["国条码"])
        sales_columns = [store_col, product_col, barcode_col, qty_col, date_col]
        normalized_sales_columns = ["store", "product", "barcode", "sales_qty", "sales_date"]
        if brand_col is not None:
            sales_columns.insert(1, brand_col)
            normalized_sales_columns.insert(1, "brand")
        if national_barcode_col is not None and national_barcode_col != barcode_col:
            sales_columns.append(national_barcode_col)
            normalized_sales_columns.append("national_barcode")
        if supplier_card_col is not None:
            sales_columns.append(supplier_card_col)
            normalized_sales_columns.append("supplier_card")
        df = df[sales_columns].copy()
        df.columns = normalized_sales_columns
        if "brand" not in df.columns:
            df["brand"] = None
        df["barcode"] = df["barcode"].apply(normalize_barcode_value)
        if "national_barcode" in df.columns:
            df["national_barcode"] = df["national_barcode"].apply(normalize_barcode_value)
        else:
            df["national_barcode"] = None
        df["display_barcode"] = df["national_barcode"].where(df["national_barcode"].notna(), df["barcode"])
        if "supplier_card" not in df.columns:
            df["supplier_card"] = None
        df["supplier_card"] = df["supplier_card"].apply(normalize_supplier_card_value)
        # Some sources (e.g. Wumei) have empty brand cells; keep rows by deriving brand from product.
        brand_series = df["brand"].apply(
            lambda v: None if v is None or str(v).strip() == "" or str(v).strip().lower() in {"nan", "none"} else str(v).strip()
        )
        df["brand"] = brand_series
        df["brand"] = df["brand"].fillna(df["product"].apply(lambda v: extract_brand_from_product(v, brand_keywords)))
        parsed_dates = parse_sales_dates(
            df["sales_date"],
            date_format=sales_date_format,
            dayfirst=sales_date_dayfirst,
        )
        invalid_sales_date_rows += int(parsed_dates.isna().sum())
        df["sales_date"] = parsed_dates
        df = df[df["sales_date"].notna()].copy()
        sales_data.append(df)
        loaded_sales_file_count += 1

    if not sales_data:
        raise RuntimeError("[input_read] No sales files were loaded.")

    carton_factor_path = Path(carton_factor_file)
    if not carton_factor_path.is_absolute():
        carton_factor_path = (BASE_DIR / carton_factor_path).resolve()
    try:
        carton_factor_df = load_carton_factor_df(carton_factor_path)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("input_read", exc) from exc
    try:
        sales_df = pd.concat(sales_data, ignore_index=True)
        sales_df["barcode_key"] = sales_df["barcode"].apply(normalize_barcode_value)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("metrics", exc) from exc

    mtd_end = inventory_date_ts if include_mtd else (inventory_date_ts.replace(day=1) - pd.Timedelta(days=1))
    mtd_start = (inventory_date_ts.replace(day=1) - pd.DateOffset(months=full_months)).normalize()
    recent_start = (inventory_date_ts - pd.Timedelta(days=recent_days - 1)).normalize()

    if sales_df.empty:
        raise RuntimeError("[metrics] Sales data has no valid dates after parsing 销售时间.")

    data_min_date = sales_df["sales_date"].min().normalize()
    data_max_date = sales_df["sales_date"].max().normalize()
    mtd_days = overlap_days(mtd_start, mtd_end, data_min_date, data_max_date)
    recent_days_effective = overlap_days(recent_start, inventory_date_ts, data_min_date, data_max_date)
    has_mtd_window_data = mtd_days > 0
    has_recent_window_data = recent_days_effective > 0
    if fail_on_empty_window and (not has_mtd_window_data or not has_recent_window_data):
        raise RuntimeError(
            "[metrics] Sales window has no overlapping data: "
            f"3M+MTD={has_mtd_window_data}, 30D={has_recent_window_data}"
        )

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
    sales_supplier = (
        sales_df.groupby(["store", "brand", "product", "barcode"], as_index=False)["supplier_card"]
        .agg(pick_first_non_empty)
    )
    sales_display_barcode = build_unambiguous_barcode_map(
        sales_df,
        ["store", "brand", "product", "barcode"],
        "display_barcode",
        "display_barcode",
    )
    sales_display_barcode_global = build_unambiguous_barcode_map(
        sales_df,
        ["barcode"],
        "display_barcode",
        "display_barcode_global",
    )
    sales_display_product_global = build_unambiguous_barcode_map(
        sales_df,
        ["brand", "product"],
        "display_barcode",
        "display_barcode_brand_product_global",
    )
    sales_totals = sales_totals.merge(
        sales_supplier,
        on=["store", "brand", "product", "barcode"],
        how="left",
    )
    sales_totals = sales_totals.merge(
        sales_display_barcode,
        on=["store", "brand", "product", "barcode"],
        how="left",
    )

    inv_totals = (
        inv_df.groupby(["store", "brand", "product", "barcode"], as_index=False)["inventory_qty"]
        .sum()
    )
    inv_totals["barcode_key"] = inv_totals["barcode"].apply(normalize_barcode_value)
    inv_supplier = (
        inv_df.groupby(["store", "brand", "product", "barcode"], as_index=False)["supplier_card"]
        .agg(pick_first_non_empty)
    )
    inv_supplier["barcode_key"] = inv_supplier["barcode"].apply(normalize_barcode_value)

    # Match sales to inventory: barcode match first, fallback to store+brand+product
    sales_barcode = sales_totals[sales_totals["barcode_key"].notna()].copy()

    detail = inv_totals.copy()
    detail = detail.merge(
        sales_barcode[["store", "barcode_key", "daily_sales_3m_mtd", "daily_sales_30d", "forecast_daily_sales", "display_barcode"]],
        on=["store", "barcode_key"],
        how="left",
    )

    fallback_sales = sales_totals.groupby(["store", "brand", "product"], as_index=False).agg({
        "daily_sales_3m_mtd": "sum",
        "daily_sales_30d": "sum",
        "forecast_daily_sales": "sum",
    })
    fallback_display_barcode = sales_totals.groupby(["store", "brand", "product"], as_index=False)["display_barcode"].agg(
        pick_first_non_empty
    )
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
    missing_display_mask = detail["display_barcode"].isna()
    if missing_display_mask.any():
        fallback_display = detail.loc[missing_display_mask].merge(
            fallback_display_barcode,
            on=["store", "brand", "product"],
            how="left",
            suffixes=("", "_fallback"),
        )
        detail.loc[missing_display_mask, "display_barcode"] = fallback_display["display_barcode_fallback"].values
    detail = detail.merge(
        sales_display_barcode_global.rename(columns={"barcode": "barcode_key"}),
        on="barcode_key",
        how="left",
    )
    missing_display_mask = detail["display_barcode"].isna()
    if missing_display_mask.any():
        detail.loc[missing_display_mask, "display_barcode"] = detail.loc[
            missing_display_mask, "display_barcode_global"
        ]
    detail = detail.drop(columns=["display_barcode_global"])

    detail = detail.merge(
        sales_display_product_global,
        on=["brand", "product"],
        how="left",
    )
    missing_display_mask = detail["display_barcode"].isna()
    if missing_display_mask.any():
        detail.loc[missing_display_mask, "display_barcode"] = detail.loc[
            missing_display_mask, "display_barcode_brand_product_global"
        ]
    detail = detail.drop(columns=["display_barcode_brand_product_global"])

    detail["daily_sales_3m_mtd"] = detail["daily_sales_3m_mtd"].fillna(0)
    detail["daily_sales_30d"] = detail["daily_sales_30d"].fillna(0)
    detail["forecast_daily_sales"] = detail["forecast_daily_sales"].fillna(0)
    detail = detail.merge(
        inv_supplier[["store", "barcode_key", "supplier_card"]],
        on=["store", "barcode_key"],
        how="left",
    )
    inv_supplier_fallback = inv_supplier.groupby(["store", "brand", "product"], as_index=False)["supplier_card"].agg(
        pick_first_non_empty
    )
    missing_supplier_mask = detail["supplier_card"].isna()
    if missing_supplier_mask.any():
        fallback = detail.loc[missing_supplier_mask].merge(
            inv_supplier_fallback,
            on=["store", "brand", "product"],
            how="left",
            suffixes=("", "_fallback"),
        )
        detail.loc[missing_supplier_mask, "supplier_card"] = fallback["supplier_card_fallback"].values

    sales_supplier_exact = sales_totals[["store", "barcode_key", "supplier_card"]].copy()
    missing_supplier_mask = detail["supplier_card"].isna()
    if missing_supplier_mask.any():
        fallback = detail.loc[missing_supplier_mask].merge(
            sales_supplier_exact,
            on=["store", "barcode_key"],
            how="left",
            suffixes=("", "_fallback"),
        )
        detail.loc[missing_supplier_mask, "supplier_card"] = fallback["supplier_card_fallback"].values

    sales_supplier_fallback = sales_totals.groupby(["store", "brand", "product"], as_index=False)["supplier_card"].agg(
        pick_first_non_empty
    )
    missing_supplier_mask = detail["supplier_card"].isna()
    if missing_supplier_mask.any():
        fallback = detail.loc[missing_supplier_mask].merge(
            sales_supplier_fallback,
            on=["store", "brand", "product"],
            how="left",
            suffixes=("", "_fallback"),
        )
        detail.loc[missing_supplier_mask, "supplier_card"] = fallback["supplier_card_fallback"].values
    detail["province"] = detail["supplier_card"].apply(map_province_by_supplier_card)
    if is_wumei_system:
        detail["barcode_output"] = detail["display_barcode"].where(detail["display_barcode"].notna(), detail["barcode"])
    else:
        detail["barcode_output"] = detail["barcode"]

    high_days = float(config.get("risk_days_high", 60))
    low_days = float(config.get("risk_days_low", 45))
    detail = apply_inventory_metrics(detail, low_days, high_days)
    
    detail = detail[[
        "store",
        "brand",
        "barcode",
        "barcode_output",
        "product",
        "daily_sales_3m_mtd",
        "daily_sales_30d",
        "forecast_daily_sales",
        "inventory_qty",
        "supplier_card",
        "province",
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
    store_summary = apply_inventory_metrics(store_summary, low_days, high_days)

    # Brand summary
    brand_summary = detail.groupby("brand", as_index=False).agg({
        "daily_sales_3m_mtd": "sum",
        "daily_sales_30d": "sum",
        "forecast_daily_sales": "sum",
        "inventory_qty": "sum",
    })
    brand_summary = apply_inventory_metrics(brand_summary, low_days, high_days)

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
    missing_sales["province"] = missing_sales["supplier_card"].apply(map_province_by_supplier_card)

    # Append missing items into detail table
    if not missing_sales.empty:
        missing_detail = missing_sales.copy()
        missing_detail["inventory_qty"] = 0
        missing_detail["inventory_sales_ratio"] = float("inf")
        missing_detail["turnover_rate"] = 0
        missing_detail["turnover_days"] = float("inf")
        missing_detail["risk_level"] = "高"
        missing_detail["province"] = missing_detail["supplier_card"].apply(map_province_by_supplier_card)
        if is_wumei_system:
            missing_detail["barcode_output"] = missing_detail["display_barcode"].where(
                missing_detail["display_barcode"].notna(), missing_detail["barcode"]
            )
        else:
            missing_detail["barcode_output"] = missing_detail["barcode"]
        missing_detail = missing_detail[[
            "store",
            "brand",
            "barcode",
            "barcode_output",
            "product",
            "daily_sales_3m_mtd",
            "daily_sales_30d",
            "forecast_daily_sales",
            "inventory_qty",
            "supplier_card",
            "province",
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
        "barcode_output": "商品条码",
        "product": "商品名称",
        "province": "省份",
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
    detail_out["商品条码"] = detail_out["商品条码"].apply(lambda x: normalize_barcode_value(x) or "")
    detail_output_columns = [
        "门店名称",
        "品牌",
        "商品条码",
        "商品名称",
    ]
    if enable_province_column:
        detail_output_columns.append("省份")
    detail_output_columns.extend([
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
    ])
    detail_out = detail_out[detail_output_columns]

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

    missing_sku_out = missing_sales[[
        "store",
        "brand",
        "display_barcode",
        "barcode",
        "product",
        "province",
        "daily_sales_3m_mtd",
        "daily_sales_30d",
    ]].rename(columns={
        "store": "门店名称",
        "brand": "品牌",
        "display_barcode": "商品条码",
        "product": "商品名称",
        "province": "省份",
        "daily_sales_3m_mtd": "近三月+本月迄今平均日销",
        "daily_sales_30d": "近30天平均日销售",
    })
    if is_wumei_system:
        missing_sku_out["商品条码"] = missing_sku_out["商品条码"].where(
            missing_sku_out["商品条码"].notna(), missing_sku_out["barcode"]
        )
    else:
        missing_sku_out["商品条码"] = missing_sku_out["barcode"]
    missing_sku_out = missing_sku_out.drop(columns=["barcode"])
    missing_sku_out["商品条码"] = missing_sku_out["商品条码"].apply(lambda x: normalize_barcode_value(x) or "")
    replenish_lookup_exact = (
        detail_out[["门店名称", "品牌", "商品条码", "商品名称", "建议补货数量"]]
        .groupby(["门店名称", "品牌", "商品条码", "商品名称"], as_index=False)["建议补货数量"]
        .max()
    )
    missing_sku_out = missing_sku_out.merge(
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
    missing_sku_out = missing_sku_out.merge(
        replenish_lookup_fallback,
        on=["门店名称", "品牌", "商品名称"],
        how="left",
    )
    missing_sku_out["建议补货数量"] = (
        missing_sku_out["建议补货数量"].fillna(missing_sku_out["建议补货数量_fallback"]).fillna(0).astype(int)
    )
    missing_sku_out = missing_sku_out.drop(columns=["建议补货数量_fallback"])
    missing_output_columns = ["门店名称", "品牌", "商品条码", "商品名称"]
    if enable_province_column:
        missing_output_columns.append("省份")
    missing_output_columns.extend(["近三月+本月迄今平均日销", "近30天平均日销售", "建议补货数量"])
    missing_sku_out = missing_sku_out[missing_output_columns]

    out_of_stock_out = detail_out[detail_out["缺货"] == "是"].copy()
    out_of_stock_columns = ["门店名称", "品牌", "商品条码", "商品名称"]
    if enable_province_column:
        out_of_stock_columns.append("省份")
    out_of_stock_columns.extend(
        ["近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "缺货", "风险等级", "建议补货数量"]
    )
    out_of_stock_out = out_of_stock_out[out_of_stock_columns]

    replenish_out = detail_out[detail_out["建议补货数量"] > 0].copy()
    replenish_output_columns = ["门店名称", "品牌", "商品条码", "商品名称"]
    if enable_province_column:
        replenish_output_columns.append("省份")
    replenish_output_columns.extend([
        "近三月+本月迄今平均日销",
        "近30天平均日销售",
        "库存数量",
        "缺货",
        "风险等级",
        "建议补货数量",
    ])
    replenish_out = replenish_out[replenish_output_columns]

    transfer_out = detail_out[detail_out["建议调出数量"] > 0].copy()
    transfer_output_columns = ["门店名称", "品牌", "商品条码", "商品名称"]
    if enable_province_column:
        transfer_output_columns.append("省份")
    transfer_output_columns.extend([
        "近三月+本月迄今平均日销",
        "近30天平均日销售",
        "库存数量",
        "风险等级",
        "建议调出数量",
    ])
    transfer_out = transfer_out[transfer_output_columns]

    factor_by_barcode = (
        carton_factor_df[carton_factor_df["商品条码"].notna()][["商品条码", "装箱数（因子）"]]
        .drop_duplicates(subset=["商品条码"], keep="first")
    )
    factor_by_product = (
        carton_factor_df[["商品名称", "装箱数（因子）"]]
        .drop_duplicates(subset=["商品名称"], keep="first")
    )

    def attach_factor_and_case_count(
        df: pd.DataFrame,
        qty_col: str,
        case_col: Optional[str] = None,
        case_with_unit: bool = False,
        include_factor_col: bool = True,
    ) -> pd.DataFrame:
        out = df.copy()
        out["商品条码"] = out["商品条码"].apply(normalize_barcode_value)
        out = out.merge(factor_by_barcode, on="商品条码", how="left")
        fallback = out["装箱数（因子）"].isna()
        if fallback.any():
            merged = out.loc[fallback, ["商品名称"]].merge(
                factor_by_product,
                on="商品名称",
                how="left",
                suffixes=("", "_fallback"),
            )
            fallback_factor_col = "装箱数（因子）_fallback" if "装箱数（因子）_fallback" in merged.columns else "装箱数（因子）"
            out.loc[fallback, "装箱数（因子）"] = merged[fallback_factor_col].values

        out["装箱数（因子）"] = pd.to_numeric(out["装箱数（因子）"], errors="coerce")
        valid_factor = out["装箱数（因子）"] > 0
        if case_col:
            out[case_col] = compute_case_counts(out[qty_col], out["装箱数（因子）"], use_peak_mode)
            if case_with_unit:
                out[case_col] = out[case_col].apply(lambda x: f"{int(x)}件" if pd.notna(x) else "")

        out.loc[valid_factor, "装箱数（因子）"] = (
            pd.to_numeric(out.loc[valid_factor, "装箱数（因子）"], errors="coerce").round().astype(int)
        )
        out.loc[~valid_factor, "装箱数（因子）"] = np.nan

        cols = out.columns.tolist()
        if "装箱数（因子）" in cols:
            cols.remove("装箱数（因子）")
            if include_factor_col:
                product_idx = cols.index("商品名称")
                cols.insert(product_idx + 1, "装箱数（因子）")
        if case_col and case_col in cols:
            cols.remove(case_col)
            cols.append(case_col)
        return out[cols]

    missing_sku_out = attach_factor_and_case_count(
        missing_sku_out,
        "建议补货数量",
        case_col="建议补货箱数",
        case_with_unit=True,
        include_factor_col=False,
    )
    out_of_stock_out = attach_factor_and_case_count(
        out_of_stock_out,
        "建议补货数量",
        case_col="建议补货箱数",
        case_with_unit=True,
        include_factor_col=False,
    )

    replenish_out = attach_factor_and_case_count(
        replenish_out,
        "建议补货数量",
        case_col="建议补货箱数",
        case_with_unit=True,
        include_factor_col=True,
    )
    transfer_out = attach_factor_and_case_count(
        transfer_out,
        "建议调出数量",
        case_col=None,
        include_factor_col=True,
    )
    if "建议调货箱数" in transfer_out.columns:
        transfer_out = transfer_out.drop(columns=["建议调货箱数"])

    # Summary sheet
    summary_rows = [
        ["风险等级-高", int((detail_out["风险等级"] == "高").sum())],
        ["风险等级-中", int((detail_out["风险等级"] == "中").sum())],
        ["风险等级-低", int((detail_out["风险等级"] == "低").sum())],
        ["缺货SKU数(库存=0)", int(len(out_of_stock_out))],
        ["库存缺失SKU数(销售有库存无)", int(len(missing_sku_out))],
        ["库存总量", float(detail_out["库存数量"].sum())],
        ["近三月+本月迄今平均日销总量", round(float(detail_out["近三月+本月迄今平均日销"].sum()), 1)],
        ["近30天平均日销售总量", round(float(detail_out["近30天平均日销售"].sum()), 1)],
        ["预测平均日销总量(季节模式后)", round(float(detail["forecast_daily_sales"].sum()), 1)],
    ]
    summary_out = pd.DataFrame(summary_rows, columns=["指标", "数值"])
    status_rows = [
        ["程序版本", PROGRAM_VERSION],
        ["配置快照", compute_config_snapshot(config)],
        ["系统名称", display_name],
        ["系统标识", system_id],
        ["库存日期", inventory_date],
        ["输入销售文件数", loaded_sales_file_count],
        ["输入文件总数", loaded_sales_file_count + 1],  # +1 inventory
        ["季节模式", "旺季(取高值)" if use_peak_mode else "淡季(取低值)"],
        ["严格自动扫描", "是" if strict_auto_scan else "否"],
        ["3M+MTD窗口有效", "是" if has_mtd_window_data else "否"],
        ["30天窗口有效", "是" if has_recent_window_data else "否"],
        ["3M+MTD窗口有效天数", int(mtd_days)],
        ["30天窗口有效天数", int(recent_days_effective)],
        ["销售无效日期行数", invalid_sales_date_rows],
        [
            "建议补货清单缺失装箱因子行数",
            int((replenish_out["装箱数（因子）"].isna()).sum()) if "装箱数（因子）" in replenish_out.columns else 0,
        ],
        [
            "建议调货清单缺失装箱因子行数",
            int((transfer_out["装箱数（因子）"].isna()).sum()) if "装箱数（因子）" in transfer_out.columns else 0,
        ],
        [
            "窗口数据状态",
            "正常" if (has_mtd_window_data and has_recent_window_data)
            else f"警告: 3M+MTD有效={has_mtd_window_data}, 30天有效={has_recent_window_data}",
        ],
        ["自动扫描忽略销售文件数", len(ignored_sales_files)],
    ]
    if ignored_sales_files:
        ignored_text = format_ignored_sales_files(ignored_sales_files)
        status_rows.append(["自动扫描忽略销售文件", ignored_text])
    status_out = pd.DataFrame(status_rows, columns=["状态项", "值"])

    sheets = {
        "明细": detail_out,
        "门店汇总": store_summary_out,
        "品牌汇总": brand_summary_out,
        "缺货清单": out_of_stock_out,
        "库存缺失SKU清单": missing_sku_out,
        "建议补货清单": replenish_out,
        "建议调货清单": transfer_out,
        "汇总": summary_out,
        "运行状态": status_out,
    }
    try:
        core_report_writer.write_report_with_style(
            output_file=output_file,
            display_name=display_name,
            inventory_date=inventory_date,
            sheets=sheets,
        )
    except Exception as exc:  # noqa: BLE001
        raise stage_error("write_report", exc) from exc

    print(f"[{display_name}] Report saved: {output_file}")
    return {
        "system_id": system_id,
        "display_name": display_name,
        "status": "SUCCESS",
        "message": "",
        "error_stage": "",
        "output_file": str(output_file),
        "input_files_count": int(loaded_sales_file_count + 1),  # sales + inventory
        "detail_rows": int(len(detail_out)),
        "missing_sku_rows": int(len(missing_sku_out)),
    }

def run_batch(global_config: Dict[str, Any]) -> int:
    validate_batch_config(global_config)
    return core_batch.run_batch(
        global_config=global_config,
        base_dir=BASE_DIR,
        build_system_config=build_system_config,
        resolve_expected_output_for_status=lambda cfg, name: resolve_expected_output_for_status(cfg, name),
        generate_report_for_system=generate_report_for_system,
    )


def main() -> int:
    config = load_config()
    run_mode = str(config.get("run_mode", "single")).lower()
    if run_mode == "batch":
        failures = run_batch(config)
        return 1 if failures > 0 else 0

    generate_report_for_system(config, config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
