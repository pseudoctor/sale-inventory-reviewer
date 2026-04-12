from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from . import config as core_config
from . import frame_schema as core_frame_schema
from . import io as core_io
from . import metrics as core_metrics
from .models import AppConfig, BarcodeMappingResult, InventoryPreparationResult, SalesLoadResult, WindowContext
from .system_rules import SystemRuleProfile

_CARTON_FACTOR_CACHE: Dict[Path, tuple[Optional[int], pd.DataFrame]] = {}


def load_carton_factor_cached(path: Path) -> pd.DataFrame:
    """按文件修改时间缓存装箱因子表，避免批量模式重复读取。"""
    try:
        mtime_ns: Optional[int] = path.stat().st_mtime_ns
    except OSError:
        mtime_ns = None

    cached = _CARTON_FACTOR_CACHE.get(path)
    if cached is not None:
        cached_mtime_ns, cached_df = cached
        if cached_mtime_ns == mtime_ns:
            return cached_df

    loaded_df = core_io.load_carton_factor_df(path)
    _CARTON_FACTOR_CACHE[path] = (mtime_ns, loaded_df)
    return loaded_df


def load_sales_data(
    sales_candidates: List[Path],
    brand_keywords: List[str],
    sales_date_format: str,
    sales_date_dayfirst: bool,
    require_sales_amount: bool,
) -> SalesLoadResult:
    """读取并标准化全部销售文件，返回合并后的销售明细与质量统计。"""
    sales_data: List[pd.DataFrame] = []
    loaded_sales_file_count = 0
    missing_sales_files: List[str] = []
    invalid_sales_date_rows = 0
    invalid_sales_qty_rows = 0

    for filepath in sales_candidates:
        if not filepath.exists():
            print(f"Warning: missing sales file {filepath}")
            missing_sales_files.append(filepath.name)
            continue
        df = core_io.read_excel_first_sheet(filepath)
        df, store_col, brand_col, product_col, barcode_col, qty_col, date_col, supplier_card_col = core_io.normalize_sales_df(df)
        store_code_col = core_io.find_column(df.columns.tolist(), ["门店编码", "store_code"])
        sales_amount_col = core_io.find_sales_amount_column(df.columns.tolist())
        # 门店销量排名调货汇总依赖销售额口径，启用该功能时缺失直接失败，避免静默生成错误结果。
        if require_sales_amount and sales_amount_col is None:
            raise ValueError(
                f"Missing required sales amount column in sales file: {filepath.name} "
                "(expected one of: 销售金额, 含税销售金额/元, 含税销售额/元, 销售额, sales_amount, amount)"
            )
        invalid_sales_qty_rows += int(df.attrs.get("invalid_qty_rows", 0))

        national_barcode_col = core_io.find_column(df.columns.tolist(), ["国条码"])
        product_code_col = core_io.find_column(df.columns.tolist(), ["商品编码", "product_code"])
        sales_columns = [store_col, product_col, barcode_col, qty_col, date_col]
        normalized_sales_columns = ["store", "product", "barcode", "sales_qty", "sales_date"]
        if store_code_col is not None:
            sales_columns.insert(1, store_code_col)
            normalized_sales_columns.insert(1, "store_code")
        if brand_col is not None:
            brand_insert_at = 2 if store_code_col is not None else 1
            sales_columns.insert(brand_insert_at, brand_col)
            normalized_sales_columns.insert(brand_insert_at, "brand")
        if national_barcode_col is not None and national_barcode_col != barcode_col:
            sales_columns.append(national_barcode_col)
            normalized_sales_columns.append("national_barcode_raw")
        if product_code_col is not None and product_code_col != barcode_col:
            sales_columns.append(product_code_col)
            normalized_sales_columns.append("product_code")
        if supplier_card_col is not None:
            sales_columns.append(supplier_card_col)
            normalized_sales_columns.append("supplier_card")
        if sales_amount_col is not None:
            sales_columns.append(sales_amount_col)
            normalized_sales_columns.append("sales_amount")

        df = df[sales_columns].copy()
        df.columns = normalized_sales_columns
        if "brand" not in df.columns:
            df["brand"] = None
        if "store_code" not in df.columns:
            df["store_code"] = None
        df["store_code"] = df["store_code"].apply(core_io.normalize_barcode_value)
        df["barcode"] = df["barcode"].apply(core_io.normalize_barcode_value)
        if "national_barcode_raw" in df.columns:
            df["national_barcode"] = df["national_barcode_raw"].apply(core_io.normalize_barcode_value)
            df = df.drop(columns=["national_barcode_raw"])
        elif barcode_col == "国条码":
            df["national_barcode"] = df["barcode"]
        else:
            df["national_barcode"] = None
        if "product_code" in df.columns:
            df["product_code"] = df["product_code"].apply(core_io.normalize_barcode_value)
        elif barcode_col == "商品编码":
            df["product_code"] = df["barcode"]
        else:
            df["product_code"] = None
        df["display_barcode"] = df["national_barcode"].where(df["national_barcode"].notna(), df["barcode"])
        if "supplier_card" not in df.columns:
            df["supplier_card"] = None
        df["supplier_card"] = df["supplier_card"].apply(core_io.normalize_supplier_card_value)
        if "sales_amount" in df.columns:
            sales_amount_series, _ = core_io.normalize_numeric_series(df["sales_amount"])
            df["sales_amount"] = sales_amount_series
        else:
            df["sales_amount"] = 0.0
        df = core_io.ensure_sales_brand_column(df, brand_keywords)

        parsed_dates = core_io.parse_sales_dates(df["sales_date"], date_format=sales_date_format, dayfirst=sales_date_dayfirst)
        invalid_sales_date_rows += int(parsed_dates.isna().sum())
        df["sales_date"] = parsed_dates
        df = df[df["sales_date"].notna()].copy()

        sales_data.append(core_frame_schema.validate_frame_columns(df, core_frame_schema.NORMALIZED_SALES_SCHEMA))
        loaded_sales_file_count += 1

    if not sales_data:
        raise RuntimeError("[input_read] No sales files were loaded.")

    sales_df = pd.concat(sales_data, ignore_index=True)
    if sales_df.empty:
        raise ValueError(
            "No valid sales rows after parsing dates. "
            "Please check sales date values and sales_date_format/sales_date_dayfirst settings."
        )
    return SalesLoadResult(
        sales_df=core_frame_schema.validate_frame_columns(sales_df, core_frame_schema.NORMALIZED_SALES_SCHEMA),
        loaded_sales_file_count=loaded_sales_file_count,
        missing_sales_files=missing_sales_files,
        invalid_sales_date_rows=invalid_sales_date_rows,
        invalid_sales_qty_rows=invalid_sales_qty_rows,
    )


def compute_window_context(
    sales_df: pd.DataFrame,
    inventory_date_ts: pd.Timestamp,
    full_months: int,
    include_mtd: bool,
    recent_days: int,
    fail_on_empty_window: bool,
) -> WindowContext:
    """基于库存日期与销售数据范围，计算两个销售窗口的有效区间。"""
    mtd_end = inventory_date_ts if include_mtd else (inventory_date_ts.replace(day=1) - pd.Timedelta(days=1))
    mtd_start = (inventory_date_ts.replace(day=1) - pd.DateOffset(months=full_months)).normalize()
    recent_start = (inventory_date_ts - pd.Timedelta(days=recent_days - 1)).normalize()

    data_min_date = sales_df["sales_date"].min().normalize()
    data_max_date = sales_df["sales_date"].max().normalize()
    mtd_days = core_metrics.overlap_days(mtd_start, mtd_end, data_min_date, data_max_date)
    recent_days_effective = core_metrics.overlap_days(recent_start, inventory_date_ts, data_min_date, data_max_date)
    has_mtd_window_data = mtd_days > 0
    has_recent_window_data = recent_days_effective > 0

    if fail_on_empty_window and (not has_mtd_window_data or not has_recent_window_data):
        raise RuntimeError(
            "[metrics] Sales window has no overlapping data: "
            f"3M+MTD={has_mtd_window_data}, 30D={has_recent_window_data}"
        )

    return WindowContext(
        mtd_start=mtd_start,
        mtd_end=mtd_end,
        recent_start=recent_start,
        mtd_days=mtd_days,
        recent_days_effective=recent_days_effective,
        has_mtd_window_data=has_mtd_window_data,
        has_recent_window_data=has_recent_window_data,
    )


def prepare_inventory_data(
    *,
    inv_path: Path,
    config: AppConfig,
    brand_keywords: List[str],
    display_name: str,
    base_dir: Path,
) -> InventoryPreparationResult:
    """读取库存表、标准化字段，并解析库存日期与输出文件名。"""
    inv_df = core_io.read_excel_first_sheet(inv_path)
    inv_df = core_io.ensure_inventory_brand_column(inv_df, brand_keywords)
    inventory_date_text = core_io.extract_inventory_date(inv_df)
    parsed_inventory_date = pd.to_datetime(inventory_date_text, errors="coerce")
    if pd.isna(parsed_inventory_date):
        raise ValueError(f"Invalid inventory date: {inventory_date_text}")
    inventory_date_ts = pd.Timestamp(parsed_inventory_date).normalize()
    inventory_date = inventory_date_ts.date().isoformat()
    output_file = core_config.resolve_output_file_path(config, display_name, inventory_date, base_dir)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    normalized_inv_df, inv_store, inv_brand, inv_product, inv_barcode, inv_qty, inv_supplier_card = core_io.normalize_inventory_df(inv_df)
    if inv_brand is None:
        raise ValueError("Inventory brand column normalization failed unexpectedly.")
    invalid_inventory_qty_rows = int(normalized_inv_df.attrs.get("invalid_qty_rows", 0))

    inv_store_code = core_io.find_column(normalized_inv_df.columns.tolist(), ["门店编码", "store_code"])
    inv_product_code = core_io.find_column(normalized_inv_df.columns.tolist(), ["商品编码", "product_code"])
    inventory_columns = [inv_store, inv_brand, inv_product, inv_barcode, inv_qty]
    if inv_store_code is not None:
        inventory_columns.insert(1, inv_store_code)
    if inv_supplier_card is not None:
        inventory_columns.append(inv_supplier_card)
    inv_product_code_values = (
        normalized_inv_df[inv_product_code].apply(core_io.normalize_barcode_value)
        if inv_product_code is not None
        else None
    )
    inv_df = normalized_inv_df[inventory_columns].copy()
    inv_df.columns = (
        ["store", "store_code", "brand", "product", "barcode", "inventory_qty"]
        if inv_store_code is not None
        else ["store", "brand", "product", "barcode", "inventory_qty"]
    ) + (
        ["supplier_card"] if inv_supplier_card is not None else []
    )
    if "store_code" not in inv_df.columns:
        inv_df["store_code"] = None
    inv_df["store_code"] = inv_df["store_code"].apply(core_io.normalize_barcode_value)
    inv_df["barcode"] = inv_df["barcode"].apply(core_io.normalize_barcode_value)
    if inv_product_code_values is not None:
        inv_df["product_code"] = inv_product_code_values.values
    elif inv_barcode == "商品编码":
        inv_df["product_code"] = inv_df["barcode"]
    else:
        inv_df["product_code"] = None
    if "supplier_card" not in inv_df.columns:
        inv_df["supplier_card"] = None
    inv_df["supplier_card"] = inv_df["supplier_card"].apply(core_io.normalize_supplier_card_value)

    return InventoryPreparationResult(
        inventory_df=core_frame_schema.validate_frame_columns(inv_df, core_frame_schema.NORMALIZED_INVENTORY_SCHEMA),
        output_file=output_file,
        inventory_date_ts=inventory_date_ts,
        inventory_date=inventory_date,
        invalid_inventory_qty_rows=invalid_inventory_qty_rows,
    )


def apply_wumei_barcode_mapping(
    *,
    inv_df: pd.DataFrame,
    sales_df: pd.DataFrame,
    profile: SystemRuleProfile,
) -> BarcodeMappingResult:
    """保留旧函数名作为兼容入口，内部委托系统规则模块处理。"""
    from . import system_rules as core_system_rules

    # 当前仍是透传实现，但通过 profile 进入后，未来可以按系统独立扩展。
    return core_system_rules.apply_inventory_barcode_mapping(
        inv_df=inv_df,
        sales_df=sales_df,
        profile=profile,
    )
