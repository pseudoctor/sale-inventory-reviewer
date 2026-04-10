from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from . import config as core_config
from . import io as core_io
from . import matching as core_matching
from . import metrics as core_metrics
from . import output_tables as core_output_tables
from . import report_writer as core_report_writer

SUPPLIER_CARD_PROVINCE_MAP = {
    "153085": "宁夏",
    "680249": "甘肃",
    "153412": "宁夏",
    "152901": "监狱系统",
}

_CARTON_FACTOR_CACHE: Dict[Path, tuple[Optional[int], pd.DataFrame]] = {}


def compute_config_snapshot(config: Dict[str, Any]) -> str:
    """生成配置快照摘要，用于报表中追踪本次运行口径。"""
    keys = [
        "risk_days_high",
        "risk_days_low",
        "sales_window_full_months",
        "sales_window_include_mtd",
        "sales_window_recent_days",
        "season_mode",
        "strict_auto_scan",
        "merge_detail_store_cells",
        "sales_date_dayfirst",
        "sales_date_format",
        "stagnant_outbound_mode",
        "stagnant_min_keep_qty",
    ]
    payload = "|".join(f"{k}={config.get(k)}" for k in keys)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def stage_error(stage: str, exc: Exception) -> RuntimeError:
    """统一包装阶段错误，便于批量模式汇总失败原因。"""
    return RuntimeError(f"[{stage}] {exc}")


def map_province_by_supplier_card(card: Optional[str]) -> str:
    """根据供商卡号映射省份，未知值统一回退。"""
    normalized = core_io.normalize_supplier_card_value(card)
    if normalized is None:
        return "其他/未知"
    return SUPPLIER_CARD_PROVINCE_MAP.get(str(normalized), "其他/未知")


def _parse_season_mode(season_mode_raw: Any) -> bool:
    """兼容布尔值和历史字符串配置。"""
    if isinstance(season_mode_raw, bool):
        return season_mode_raw
    mode_text = str(season_mode_raw).strip().lower()
    if mode_text in {"true", "peak"}:
        return True
    if mode_text in {"false", "off_peak"}:
        return False
    raise ValueError("season_mode must be true/false (or legacy peak/off_peak)")


def _effective_brand_keywords(config: Dict[str, Any]) -> List[str]:
    """返回有效品牌关键词列表，禁止空配置继续运行。"""
    brand_keywords = [str(b).strip() for b in (config.get("brand_keywords") or []) if str(b).strip()]
    if not brand_keywords:
        raise ValueError("brand_keywords cannot be empty. Please configure at least one brand keyword in config.yaml.")
    return brand_keywords


def _load_carton_factor_cached(path: Path) -> pd.DataFrame:
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


def _load_sales_data(
    sales_candidates: List[Path],
    brand_keywords: List[str],
    sales_date_format: str,
    sales_date_dayfirst: bool,
    require_sales_amount: bool,
) -> tuple[pd.DataFrame, int, List[str], int, int]:
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
                "(expected one of: 销售金额, 含税销售金额/元, 含税销售额/元, 销售额)"
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

        sales_data.append(df)
        loaded_sales_file_count += 1

    if not sales_data:
        raise RuntimeError("[input_read] No sales files were loaded.")

    sales_df = pd.concat(sales_data, ignore_index=True)
    if sales_df.empty:
        raise ValueError(
            "No valid sales rows after parsing dates. "
            "Please check sales date values and sales_date_format/sales_date_dayfirst settings."
        )
    return sales_df, loaded_sales_file_count, missing_sales_files, invalid_sales_date_rows, invalid_sales_qty_rows


def _build_store_sales_ranking_transfer_frame(
    detail: pd.DataFrame,
    sales_df: pd.DataFrame,
    sales_amount_start: pd.Timestamp,
    sales_amount_end: pd.Timestamp,
    sales_amount_range_label: str,
) -> pd.DataFrame:
    """基于当前明细和销售额，生成带时间区间的门店销量排名调货汇总页。"""
    store_amount_header = f"门店销售额总计({sales_amount_range_label})"
    item_amount_header = f"商品销售额({sales_amount_range_label})"
    sales_base = sales_df[
        (sales_df["sales_date"] >= sales_amount_start) & (sales_df["sales_date"] <= sales_amount_end)
    ].copy()
    sales_base["store_key"] = sales_base.get("store_code", pd.Series(index=sales_base.index)).apply(core_io.normalize_barcode_value)
    sales_base["store_key"] = sales_base["store_key"].where(sales_base["store_key"].notna(), sales_base["store"])
    sales_base["product_key"] = sales_base.get("product_code", pd.Series(index=sales_base.index)).apply(core_io.normalize_barcode_value)
    sales_base["barcode_key"] = sales_base["barcode"].apply(core_io.normalize_barcode_value)
    sales_base["product_key"] = sales_base["product_key"].where(sales_base["product_key"].notna(), sales_base["barcode_key"])

    store_amounts = (
        sales_base.groupby(["store_key"], as_index=False)["sales_amount"]
        .sum()
        .rename(columns={"sales_amount": "store_sales_amount"})
    )
    item_amounts = (
        sales_base.groupby(["store_key", "product_key"], as_index=False)["sales_amount"]
        .sum()
        .rename(columns={"sales_amount": "item_sales_amount"})
    )
    item_sales_qty = (
        sales_base.groupby(["store_key", "product_key"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "item_sales_qty"})
    )
    all_sales_base = sales_df.copy()
    all_sales_base["store_key"] = all_sales_base.get("store_code", pd.Series(index=all_sales_base.index)).apply(core_io.normalize_barcode_value)
    all_sales_base["store_key"] = all_sales_base["store_key"].where(
        all_sales_base["store_key"].notna(),
        all_sales_base["store"],
    )
    all_sales_base["product_key"] = all_sales_base.get("product_code", pd.Series(index=all_sales_base.index)).apply(core_io.normalize_barcode_value)
    all_sales_base["barcode_key"] = all_sales_base["barcode"].apply(core_io.normalize_barcode_value)
    all_sales_base["product_key"] = all_sales_base["product_key"].where(
        all_sales_base["product_key"].notna(),
        all_sales_base["barcode_key"],
    )
    global_item_amounts = (
        all_sales_base.groupby(["product_key"], as_index=False)["sales_amount"]
        .sum()
        .rename(columns={"sales_amount": "global_item_sales_amount"})
    )
    global_item_sales_qty = (
        all_sales_base.groupby(["product_key"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "global_item_sales_qty"})
    )
    current_store_all_amounts = (
        all_sales_base.groupby(["store_key", "product_key"], as_index=False)["sales_amount"]
        .sum()
        .rename(columns={"sales_amount": "current_store_all_sales_amount"})
    )
    current_store_all_qty = (
        all_sales_base.groupby(["store_key", "product_key"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "current_store_all_sales_qty"})
    )

    transfer = detail.copy()
    transfer = transfer.merge(store_amounts, on=["store_key"], how="left")
    transfer = transfer.merge(item_amounts, on=["store_key", "product_key"], how="left")
    transfer = transfer.merge(item_sales_qty, on=["store_key", "product_key"], how="left")
    transfer = transfer.merge(global_item_amounts, on=["product_key"], how="left")
    transfer = transfer.merge(global_item_sales_qty, on=["product_key"], how="left")
    transfer = transfer.merge(current_store_all_amounts, on=["store_key", "product_key"], how="left")
    transfer = transfer.merge(current_store_all_qty, on=["store_key", "product_key"], how="left")
    transfer["store_sales_amount"] = pd.to_numeric(transfer["store_sales_amount"], errors="coerce").fillna(0.0)
    transfer["item_sales_amount"] = pd.to_numeric(transfer["item_sales_amount"], errors="coerce").fillna(0.0)
    transfer["item_sales_qty"] = pd.to_numeric(transfer["item_sales_qty"], errors="coerce").fillna(0.0)
    transfer["global_item_sales_amount"] = pd.to_numeric(transfer["global_item_sales_amount"], errors="coerce").fillna(0.0)
    transfer["global_item_sales_qty"] = pd.to_numeric(transfer["global_item_sales_qty"], errors="coerce").fillna(0.0)
    transfer["current_store_all_sales_amount"] = pd.to_numeric(transfer["current_store_all_sales_amount"], errors="coerce").fillna(0.0)
    transfer["current_store_all_sales_qty"] = pd.to_numeric(transfer["current_store_all_sales_qty"], errors="coerce").fillna(0.0)
    transfer["unit_price"] = np.where(
        transfer["item_sales_qty"] > 0,
        transfer["item_sales_amount"] / transfer["item_sales_qty"],
        0.0,
    )
    transfer["other_store_sales_amount"] = np.maximum(
        0.0,
        transfer["global_item_sales_amount"] - transfer["current_store_all_sales_amount"],
    )
    transfer["other_store_sales_qty"] = np.maximum(
        0.0,
        transfer["global_item_sales_qty"] - transfer["current_store_all_sales_qty"],
    )
    transfer["fallback_unit_price"] = np.where(
        transfer["other_store_sales_qty"] > 0,
        transfer["other_store_sales_amount"] / transfer["other_store_sales_qty"],
        0.0,
    )
    # 当前门店窗口内没有销量时，回退到其它门店该商品的平均单价。
    transfer["unit_price"] = np.where(
        transfer["unit_price"] > 0,
        transfer["unit_price"],
        transfer["fallback_unit_price"],
    )
    transfer["inventory_amount"] = transfer["unit_price"] * pd.to_numeric(transfer["inventory_qty"], errors="coerce").fillna(0.0)

    zero_mtd_full_outbound_mask = (
        (pd.to_numeric(transfer["daily_sales_3m_mtd"], errors="coerce").fillna(0.0) == 0)
        & (pd.to_numeric(transfer["inventory_qty"], errors="coerce").fillna(0.0) > 0)
        & (transfer["out_of_stock"] == "否")
    )
    transfer["ranking_transfer_qty"] = np.where(
        zero_mtd_full_outbound_mask,
        transfer["inventory_qty"],
        transfer["suggest_outbound_qty"],
    )
    transfer = transfer[pd.to_numeric(transfer["ranking_transfer_qty"], errors="coerce").fillna(0) > 0].copy()
    if transfer.empty:
        return pd.DataFrame(
            columns=[
                "排名",
                "门店名称",
                "门店销售额总计",
                "品牌",
                "商品条码",
                "商品名称",
                "商品销售额",
                "商品单价",
                "调货数量",
                "库存金额",
                "近三月+本月迄今平均日销",
                "近30天平均日销售",
                "库存数量",
                "缺货",
                "风险等级",
                "库存周转天数",
            ]
        )

    transfer["排名"] = (
        transfer["store_sales_amount"].rank(method="dense", ascending=False).astype(int)
    )
    transfer["商品条码"] = transfer["barcode_output"].apply(lambda x: core_io.normalize_barcode_value(x) or "")
    transfer["调货数量"] = pd.to_numeric(transfer["ranking_transfer_qty"], errors="coerce").fillna(0).astype(int)

    transfer_out = transfer.rename(
            columns={
                "store": "门店名称",
            "store_sales_amount": store_amount_header,
                "brand": "品牌",
                "product": "商品名称",
            "item_sales_amount": item_amount_header,
            "unit_price": "商品单价",
            "daily_sales_3m_mtd": "近三月+本月迄今平均日销",
            "daily_sales_30d": "近30天平均日销售",
            "inventory_qty": "库存数量",
            "inventory_amount": "库存金额",
            "out_of_stock": "缺货",
            "risk_level": "风险等级",
            "turnover_days": "库存周转天数",
        }
    )[
        [
            "排名",
            "门店名称",
            store_amount_header,
            "品牌",
            "商品条码",
            "商品名称",
            item_amount_header,
            "商品单价",
            "调货数量",
            "库存金额",
            "近三月+本月迄今平均日销",
            "近30天平均日销售",
            "库存数量",
            "缺货",
            "风险等级",
            "库存周转天数",
        ]
    ]
    # 销售额在该汇总页只保留 1 位小数，满足业务展示要求。
    transfer_out[store_amount_header] = pd.to_numeric(transfer_out[store_amount_header], errors="coerce").round(1)
    transfer_out[item_amount_header] = pd.to_numeric(transfer_out[item_amount_header], errors="coerce").round(1)
    transfer_out["商品单价"] = pd.to_numeric(transfer_out["商品单价"], errors="coerce").round(1)
    transfer_out["库存金额"] = pd.to_numeric(transfer_out["库存金额"], errors="coerce").round(1)
    return transfer_out.sort_values(
        ["排名", "门店名称", "品牌", "商品名称", "商品条码"],
        ascending=[True, True, True, True, True],
    ).reset_index(drop=True)


def _compute_window_context(
    sales_df: pd.DataFrame,
    inventory_date_ts: pd.Timestamp,
    full_months: int,
    include_mtd: bool,
    recent_days: int,
    fail_on_empty_window: bool,
) -> Dict[str, Any]:
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

    return {
        "mtd_start": mtd_start,
        "mtd_end": mtd_end,
        "recent_start": recent_start,
        "mtd_days": mtd_days,
        "recent_days_effective": recent_days_effective,
        "has_mtd_window_data": has_mtd_window_data,
        "has_recent_window_data": has_recent_window_data,
    }


def _prepare_inventory_data(
    *,
    inv_path: Path,
    config: Dict[str, Any],
    brand_keywords: List[str],
    display_name: str,
    base_dir: Path,
) -> tuple[pd.DataFrame, Path, pd.Timestamp, str, int]:
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

    return inv_df, output_file, inventory_date_ts, inventory_date, invalid_inventory_qty_rows


def _apply_wumei_barcode_mapping(
    *,
    inv_df: pd.DataFrame,
    sales_df: pd.DataFrame,
    is_wumei_system: bool,
) -> tuple[pd.DataFrame, int, int, int, str]:
    """预留物美系统的条码特殊映射扩展点，当前保持透传。"""
    _ = sales_df
    if not is_wumei_system:
        return inv_df, 0, 0, 0, ""
    return inv_df, 0, 0, 0, ""


def _apply_recommendation_columns(
    detail: pd.DataFrame,
    low_days: float,
    high_days: float,
    stagnant_outbound_mode: str,
    stagnant_min_keep_qty: float,
) -> pd.DataFrame:
    """补充缺货、补货、调出建议列，并处理零销量积压库存。"""
    detail = detail.copy()
    detail["out_of_stock"] = np.where((detail["forecast_daily_sales"] > 0) & (detail["inventory_qty"] == 0), "是", "否")
    detail["daily_demand"] = detail["forecast_daily_sales"]
    detail["low_target_qty"] = detail["daily_demand"] * low_days
    detail["high_keep_qty"] = detail["daily_demand"] * high_days
    # 两个销售窗口都为 0 且仍有库存时，视为零销量积压库存。
    stagnant_mask = (
        (detail["daily_sales_3m_mtd"] <= 0)
        & (detail["daily_sales_30d"] <= 0)
        & (detail["inventory_qty"] > 0)
    )
    detail["need_qty"] = np.where(
        detail["forecast_daily_sales"] > 0,
        np.ceil(np.maximum(0, detail["low_target_qty"] - detail["inventory_qty"])),
        0,
    )
    detail["outbound_rule"] = np.where(stagnant_mask, "zero_sales_stagnant", "high_stock_overage")
    detail["suggest_outbound_qty"] = np.where(
        (detail["risk_level"] == "高") & (detail["forecast_daily_sales"] > 0),
        np.floor(np.maximum(0, detail["inventory_qty"] - detail["high_keep_qty"])),
        0,
    )
    # 零销量积压库存支持两种模式：全部调出，或保留最小安全库存后再调出。
    stagnant_keep_qty = 0 if stagnant_outbound_mode == "all_outbound" else stagnant_min_keep_qty
    detail["suggest_outbound_qty"] = np.where(
        stagnant_mask,
        np.floor(np.maximum(0, detail["inventory_qty"] - stagnant_keep_qty)),
        detail["suggest_outbound_qty"],
    )
    detail["suggest_replenish_qty"] = np.where(
        (detail["risk_level"] == "低") | (detail["out_of_stock"] == "是"),
        detail["need_qty"],
        0,
    )
    detail["suggest_outbound_qty"] = detail["suggest_outbound_qty"].astype(int)
    detail["suggest_replenish_qty"] = detail["suggest_replenish_qty"].astype(int)
    return detail


def _join_unique_text(series: pd.Series) -> str:
    """拼接去重后的文本，保持输出稳定。"""
    values: list[str] = []
    for value in series:
        text = str(value).strip()
        if text == "" or text.lower() in {"nan", "none"}:
            continue
        if text not in values:
            values.append(text)
    return " / ".join(values)


def _has_non_empty_text(series: pd.Series) -> pd.Series:
    """判断序列中的文本是否为非空有效值。"""
    return series.fillna("").astype(str).str.strip() != ""


def _build_product_code_catalog(sales_df: pd.DataFrame, inv_df: pd.DataFrame) -> pd.DataFrame:
    """汇总销售/库存两侧商品编码与标准名称，用于人工核对。"""
    sales_base = sales_df[["product_code", "brand", "product"]].copy()
    sales_base["product_code"] = sales_base["product_code"].apply(core_io.normalize_barcode_value)
    sales_base = sales_base.dropna(subset=["product_code"])
    sales_grouped = (
        sales_base.groupby("product_code", as_index=False)
        .agg(
            sales_brand=("brand", _join_unique_text),
            sales_product_name=("product", _join_unique_text),
        )
    )

    inv_base = inv_df[["product_code", "brand", "product"]].copy()
    inv_base["product_code"] = inv_base["product_code"].apply(core_io.normalize_barcode_value)
    inv_base = inv_base.dropna(subset=["product_code"])
    inv_grouped = (
        inv_base.groupby("product_code", as_index=False)
        .agg(
            inventory_brand=("brand", _join_unique_text),
            inventory_product_name=("product", _join_unique_text),
        )
    )

    catalog = sales_grouped.merge(inv_grouped, on="product_code", how="outer")
    catalog["brand"] = catalog["sales_brand"].where(
        catalog["sales_brand"].astype(str).str.strip() != "",
        catalog["inventory_brand"],
    )
    catalog["standard_product_name"] = catalog["sales_product_name"].where(
        catalog["sales_product_name"].astype(str).str.strip() != "",
        catalog["inventory_product_name"],
    )
    catalog["brand"] = catalog["brand"].fillna("")
    catalog["standard_product_name"] = catalog["standard_product_name"].fillna("")
    catalog["sales_product_name"] = catalog["sales_product_name"].fillna("")
    catalog["inventory_product_name"] = catalog["inventory_product_name"].fillna("")
    sales_exists = _has_non_empty_text(catalog["sales_product_name"])
    inventory_exists = _has_non_empty_text(catalog["inventory_product_name"])
    catalog["source_status"] = "仅库存表"
    catalog.loc[sales_exists & ~inventory_exists, "source_status"] = "仅销售表"
    catalog.loc[sales_exists & inventory_exists, "source_status"] = "两表均存在"
    return catalog[[
        "product_code",
        "brand",
        "standard_product_name",
        "sales_product_name",
        "inventory_product_name",
        "source_status",
    ]].sort_values(["product_code"]).reset_index(drop=True)


def _build_summary_frame(detail_out: pd.DataFrame, out_of_stock_out: pd.DataFrame, missing_sku_out: pd.DataFrame, detail: pd.DataFrame) -> pd.DataFrame:
    """构建报表核心指标汇总。"""
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
    return pd.DataFrame(summary_rows, columns=["指标", "数值"])


def _build_executive_overview_frame(summary_out: pd.DataFrame, status_out: pd.DataFrame) -> pd.DataFrame:
    """将核心指标和运行状态拼成统一总览页。"""
    summary_section = summary_out.copy()
    summary_section.insert(0, "分组", "核心指标")
    status_section = status_out.rename(columns={"状态项": "指标", "值": "数值"}).copy()
    status_section.insert(0, "分组", "运行状态")
    return pd.concat([summary_section, status_section], ignore_index=True)


def _build_status_frame(
    *,
    program_version: str,
    config: Dict[str, Any],
    display_name: str,
    system_id: str,
    inventory_date: str,
    loaded_sales_file_count: int,
    missing_sales_files: List[str],
    use_peak_mode: bool,
    strict_auto_scan: bool,
    has_mtd_window_data: bool,
    has_recent_window_data: bool,
    mtd_days: int,
    recent_days_effective: int,
    invalid_sales_date_rows: int,
    invalid_sales_qty_rows: int,
    invalid_inventory_qty_rows: int,
    replenish_out: pd.DataFrame,
    transfer_out: pd.DataFrame,
    mapping_stats: Dict[str, float],
    ignored_sales_files: List[str],
    is_wumei_system: bool,
    wumei_barcode_map_hits: int,
    wumei_barcode_map_fallback: int,
    wumei_barcode_map_conflicts: int,
    wumei_barcode_conflict_samples: str,
) -> pd.DataFrame:
    """构建运行状态页的数据质量与配置摘要。"""
    status_rows = [
        ["程序版本", program_version],
        ["配置快照", compute_config_snapshot(config)],
        ["系统名称", display_name],
        ["系统标识", system_id],
        ["库存日期", inventory_date],
        ["输入销售文件数", loaded_sales_file_count],
        ["缺失销售文件数", len(missing_sales_files)],
        ["输入文件总数", loaded_sales_file_count + 1],
        ["季节模式", "旺季(取高值)" if use_peak_mode else "淡季(取低值)"],
        ["严格自动扫描", "是" if strict_auto_scan else "否"],
        ["3M+MTD窗口有效", "是" if has_mtd_window_data else "否"],
        ["30天窗口有效", "是" if has_recent_window_data else "否"],
        ["3M+MTD窗口有效天数", int(mtd_days)],
        ["30天窗口有效天数", int(recent_days_effective)],
        ["销售无效日期行数", invalid_sales_date_rows],
        ["销售数量解析失败行数", int(invalid_sales_qty_rows)],
        ["库存数量解析失败行数", int(invalid_inventory_qty_rows)],
        ["建议补货清单缺失装箱因子行数", int((replenish_out["装箱数（因子）"].isna()).sum()) if "装箱数（因子）" in replenish_out.columns else 0],
        ["建议调货清单缺失装箱因子行数", int((transfer_out["装箱数（因子）"].isna()).sum()) if "装箱数（因子）" in transfer_out.columns else 0],
        ["同店同商品编码重复键数", int(mapping_stats.get("duplicate_store_product_keys", 0))],
        ["名称冲突键数", int(mapping_stats.get("name_conflict_keys", 0))],
        ["品牌冲突键数", int(mapping_stats.get("brand_conflict_keys", 0))],
        ["映射覆盖率", f"{float(mapping_stats.get('mapping_coverage_rate', 0.0)) * 100:.1f}%"],
        ["窗口数据状态", "正常" if (has_mtd_window_data and has_recent_window_data) else f"警告: 3M+MTD有效={has_mtd_window_data}, 30天有效={has_recent_window_data}"],
        ["自动扫描忽略销售文件数", len(ignored_sales_files)],
    ]
    if ignored_sales_files:
        status_rows.append(["自动扫描忽略销售文件", core_io.format_ignored_sales_files(ignored_sales_files)])
    if missing_sales_files:
        status_rows.append(["缺失销售文件", " | ".join(missing_sales_files)])
    return pd.DataFrame(status_rows, columns=["状态项", "值"])


def generate_report_for_system(
    system_cfg: Dict[str, Any],
    global_cfg: Optional[Dict[str, Any]] = None,
    *,
    base_dir: Path,
    program_version: str,
) -> Dict[str, Any]:
    """执行单个系统的完整报表流水线。"""
    config = dict(system_cfg)
    _ = global_cfg
    system_id = str(config.get("system_id", "single")).strip() or "single"
    display_name = str(config.get("display_name", system_id)).strip() or system_id
    is_wumei_system = "物美" in display_name
    province_column_enabled_cfg = config.get("province_column_enabled", None)
    enable_province_column = bool(province_column_enabled_cfg) if isinstance(province_column_enabled_cfg, bool) else is_wumei_system

    try:
        raw_data_dir = core_config.resolve_system_raw_data_dir(config, base_dir)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("config", exc) from exc

    configured_sales_files = [f for f in config.get("sales_files") or []]
    inventory_file = config.get("inventory_file") or ""
    carton_factor_file = config.get("carton_factor_file") or ""
    strict_auto_scan = bool(config.get("strict_auto_scan", False))
    brand_keywords = _effective_brand_keywords(config)

    try:
        sales_candidates = core_io.resolve_sales_candidates(raw_data_dir, configured_sales_files)
        ignored_sales_files = core_io.list_ignored_sales_files(raw_data_dir, configured_sales_files)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("input_read", exc) from exc

    if not configured_sales_files and not sales_candidates:
        ignored_text = core_io.format_ignored_sales_files(ignored_sales_files)
        detail = f" Ignored files: {ignored_text}" if ignored_text else ""
        if strict_auto_scan and ignored_sales_files:
            raise RuntimeError(
                "[input_read] strict_auto_scan=true and no valid sales files were detected."
                f" Ignored candidates: {ignored_text}"
            )
        raise RuntimeError(
            "[input_read] No auto-detected sales files in "
            f"{raw_data_dir}. Expected filename with sales keyword and YYYYMM "
            f"(e.g. 销售202602.xlsx).{detail}"
        )

    inv_path = raw_data_dir / inventory_file
    inventory_file_exists = inv_path.exists()
    if not inv_path.exists():
        raise RuntimeError(f"[input_read] Inventory file not found: {inv_path}")

    try:
        inv_df, output_file, inventory_date_ts, inventory_date, invalid_inventory_qty_rows = _prepare_inventory_data(
            inv_path=inv_path,
            config=config,
            brand_keywords=brand_keywords,
            display_name=display_name,
            base_dir=base_dir,
        )
    except Exception as exc:  # noqa: BLE001
        raise stage_error("normalize", exc) from exc

    full_months = int(config.get("sales_window_full_months", 3))
    include_mtd = bool(config.get("sales_window_include_mtd", True))
    recent_days = int(config.get("sales_window_recent_days", 30))
    sales_date_dayfirst = bool(config.get("sales_date_dayfirst", False))
    sales_date_format = str(config.get("sales_date_format", ""))
    use_peak_mode = _parse_season_mode(config.get("season_mode", False))
    fail_on_empty_window = bool(config.get("fail_on_empty_window", False))
    merge_detail_store_cells = bool(config.get("merge_detail_store_cells", True))
    enable_ranked_store_transfer_summary = bool(config.get("enable_ranked_store_transfer_summary", False))

    try:
        sales_df, loaded_sales_file_count, missing_sales_files, invalid_sales_date_rows, invalid_sales_qty_rows = _load_sales_data(
            sales_candidates,
            brand_keywords,
            sales_date_format,
            sales_date_dayfirst,
            enable_ranked_store_transfer_summary,
        )
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, RuntimeError) and str(exc).startswith("[input_read]"):
            raise
        raise stage_error("normalize", exc) from exc

    inv_df, wumei_barcode_map_hits, wumei_barcode_map_fallback, wumei_barcode_map_conflicts, wumei_barcode_conflict_samples = (
        _apply_wumei_barcode_mapping(
            inv_df=inv_df,
            sales_df=sales_df,
            is_wumei_system=is_wumei_system,
        )
    )

    carton_factor_path = Path(carton_factor_file)
    if not carton_factor_path.is_absolute():
        carton_factor_path = (base_dir / carton_factor_path).resolve()
    try:
        carton_factor_df = _load_carton_factor_cached(carton_factor_path)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("input_read", exc) from exc

    try:
        window_ctx = _compute_window_context(
            sales_df,
            inventory_date_ts,
            full_months,
            include_mtd,
            recent_days,
            fail_on_empty_window,
        )
    except Exception as exc:  # noqa: BLE001
        raise stage_error("metrics", exc) from exc

    mtd_start = window_ctx["mtd_start"]
    mtd_end = window_ctx["mtd_end"]
    recent_start = window_ctx["recent_start"]
    mtd_days = window_ctx["mtd_days"]
    recent_days_effective = window_ctx["recent_days_effective"]
    has_mtd_window_data = window_ctx["has_mtd_window_data"]
    has_recent_window_data = window_ctx["has_recent_window_data"]

    high_days = float(config.get("risk_days_high", 60))
    low_days = float(config.get("risk_days_low", 45))
    stagnant_outbound_mode = str(config.get("stagnant_outbound_mode", "keep_safety_stock"))
    stagnant_min_keep_qty = float(config.get("stagnant_min_keep_qty", 0))
    detail, missing_sales, store_summary, brand_summary, mapping_stats = core_matching.build_detail_with_matching(
        sales_df=sales_df,
        inv_df=inv_df,
        mtd_start=mtd_start,
        mtd_end=mtd_end,
        recent_start=recent_start,
        inventory_date_ts=inventory_date_ts,
        mtd_days=mtd_days,
        recent_days_effective=recent_days_effective,
        recent_days_natural=recent_days,
        has_mtd_window_data=has_mtd_window_data,
        has_recent_window_data=has_recent_window_data,
        use_peak_mode=use_peak_mode,
        low_days=low_days,
        high_days=high_days,
        is_wumei_system=is_wumei_system,
        province_mapper=map_province_by_supplier_card,
    )
    product_code_catalog = _build_product_code_catalog(sales_df, inv_df)
    detail = _apply_recommendation_columns(
        detail,
        low_days,
        high_days,
        stagnant_outbound_mode,
        stagnant_min_keep_qty,
    )
    sales_amount_range_label = f"{mtd_start.date().isoformat()}至{inventory_date_ts.date().isoformat()}"
    store_sales_ranking_transfer_out = (
        _build_store_sales_ranking_transfer_frame(
            detail,
            sales_df,
            mtd_start,
            inventory_date_ts,
            sales_amount_range_label,
        )
        if enable_ranked_store_transfer_summary
        else pd.DataFrame()
    )

    frames = core_output_tables.build_report_frames(
        detail=detail,
        missing_sales=missing_sales,
        store_summary=store_summary,
        brand_summary=brand_summary,
        product_code_catalog=product_code_catalog,
        carton_factor_df=carton_factor_df,
        is_wumei_system=is_wumei_system,
        enable_province_column=enable_province_column,
        use_peak_mode=use_peak_mode,
    )
    detail_out = frames["明细"]
    missing_sku_out = frames["库存缺失SKU清单"]
    out_of_stock_out = frames["缺货清单"]
    replenish_out = frames["建议补货清单"]
    transfer_out = frames["建议调货清单"]

    summary_out = _build_summary_frame(detail_out, out_of_stock_out, missing_sku_out, detail)
    status_out = _build_status_frame(
        program_version=program_version,
        config=config,
        display_name=display_name,
        system_id=system_id,
        inventory_date=inventory_date,
        loaded_sales_file_count=loaded_sales_file_count,
        missing_sales_files=missing_sales_files,
        use_peak_mode=use_peak_mode,
        strict_auto_scan=strict_auto_scan,
        has_mtd_window_data=has_mtd_window_data,
        has_recent_window_data=has_recent_window_data,
        mtd_days=mtd_days,
        recent_days_effective=recent_days_effective,
        invalid_sales_date_rows=invalid_sales_date_rows,
        invalid_sales_qty_rows=invalid_sales_qty_rows,
        invalid_inventory_qty_rows=invalid_inventory_qty_rows,
        replenish_out=replenish_out,
        transfer_out=transfer_out,
        mapping_stats=mapping_stats,
        ignored_sales_files=ignored_sales_files,
        is_wumei_system=is_wumei_system,
        wumei_barcode_map_hits=wumei_barcode_map_hits,
        wumei_barcode_map_fallback=wumei_barcode_map_fallback,
        wumei_barcode_map_conflicts=wumei_barcode_map_conflicts,
        wumei_barcode_conflict_samples=wumei_barcode_conflict_samples,
    )

    executive_overview_out = _build_executive_overview_frame(summary_out, status_out)

    sheets = {
        **{name: frame for name, frame in frames.items() if name not in {"使用说明", "商品编码对照清单"}},
    }
    if enable_ranked_store_transfer_summary:
        sheets["门店销量排名调货汇总"] = store_sales_ranking_transfer_out
    sheets["运行总览"] = executive_overview_out
    sheets["使用说明"] = frames["使用说明"]
    sheets["商品编码对照清单"] = frames["商品编码对照清单"]

    try:
        core_report_writer.write_report_with_style(
            output_file=output_file,
            display_name=display_name,
            inventory_date=inventory_date,
            sheets=sheets,
            merge_detail_store_cells=merge_detail_store_cells,
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
        "input_files_count": int(loaded_sales_file_count + 1),
        "loaded_sales_files": int(loaded_sales_file_count),
        "missing_sales_files": int(len(missing_sales_files)),
        "inventory_file_exists": bool(inventory_file_exists),
        "detail_rows": int(len(detail_out)),
        "missing_sku_rows": int(len(missing_sku_out)),
    }
