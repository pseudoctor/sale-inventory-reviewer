from __future__ import annotations

import hashlib
from typing import Any, Callable, Dict, List

import pandas as pd

from . import io as core_io
from .models import AppConfig, StatusFrameInput


def compute_config_snapshot(config: AppConfig) -> str:
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


def join_unique_text(series: pd.Series) -> str:
    """拼接去重后的文本，保持输出稳定。"""
    values: list[str] = []
    for value in series:
        text = str(value).strip()
        if text == "" or text.lower() in {"nan", "none"}:
            continue
        if text not in values:
            values.append(text)
    return " / ".join(values)


def has_non_empty_text(series: pd.Series) -> pd.Series:
    """判断序列中的文本是否为非空有效值。"""
    return series.fillna("").astype(str).str.strip() != ""


def build_product_code_catalog(sales_df: pd.DataFrame, inv_df: pd.DataFrame) -> pd.DataFrame:
    """汇总销售/库存两侧商品编码与标准名称，用于人工核对。"""
    sales_base = sales_df[["product_code", "brand", "product"]].copy()
    sales_base["product_code"] = sales_base["product_code"].apply(core_io.normalize_barcode_value)
    sales_base = sales_base.dropna(subset=["product_code"])
    sales_grouped = (
        sales_base.groupby("product_code", as_index=False)
        .agg(
            sales_brand=("brand", join_unique_text),
            sales_product_name=("product", join_unique_text),
        )
    )

    inv_base = inv_df[["product_code", "brand", "product"]].copy()
    inv_base["product_code"] = inv_base["product_code"].apply(core_io.normalize_barcode_value)
    inv_base = inv_base.dropna(subset=["product_code"])
    inv_grouped = (
        inv_base.groupby("product_code", as_index=False)
        .agg(
            inventory_brand=("brand", join_unique_text),
            inventory_product_name=("product", join_unique_text),
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
    sales_exists = has_non_empty_text(catalog["sales_product_name"])
    inventory_exists = has_non_empty_text(catalog["inventory_product_name"])
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


def build_summary_frame(
    detail_out: pd.DataFrame,
    out_of_stock_out: pd.DataFrame,
    missing_sku_out: pd.DataFrame,
    detail: pd.DataFrame,
) -> pd.DataFrame:
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


def build_executive_overview_frame(summary_out: pd.DataFrame, status_out: pd.DataFrame) -> pd.DataFrame:
    """将核心指标和运行状态拼成统一总览页。"""
    summary_section = summary_out.copy()
    summary_section.insert(0, "分组", "核心指标")
    status_section = status_out.rename(columns={"状态项": "指标", "值": "数值"}).copy()
    status_section.insert(0, "分组", "运行状态")
    return pd.concat([summary_section, status_section], ignore_index=True)


def build_status_frame(
    status_input: StatusFrameInput,
    config_snapshot_builder: Callable[[AppConfig], str] = compute_config_snapshot,
) -> pd.DataFrame:
    """构建运行状态页的数据质量与配置摘要。"""
    status_rows = [
        ["程序版本", status_input.program_version],
        ["配置快照", config_snapshot_builder(status_input.config)],
        ["系统名称", status_input.display_name],
        ["系统标识", status_input.system_id],
        ["库存日期", status_input.inventory_date],
        ["输入销售文件数", status_input.loaded_sales_file_count],
        ["缺失销售文件数", len(status_input.missing_sales_files)],
        ["输入文件总数", status_input.input_files_count],
        ["季节模式", "旺季(取高值)" if status_input.use_peak_mode else "淡季(取低值)"],
        ["严格自动扫描", "是" if status_input.strict_auto_scan else "否"],
        ["3M+MTD窗口有效", "是" if status_input.has_mtd_window_data else "否"],
        ["30天窗口有效", "是" if status_input.has_recent_window_data else "否"],
        ["3M+MTD窗口有效天数", int(status_input.mtd_days)],
        ["30天窗口有效天数", int(status_input.recent_days_effective)],
        ["销售无效日期行数", status_input.invalid_sales_date_rows],
        ["销售数量解析失败行数", int(status_input.invalid_sales_qty_rows)],
        ["库存数量解析失败行数", int(status_input.invalid_inventory_qty_rows)],
        ["建议补货清单缺失装箱因子行数", int((status_input.replenish_out["装箱数（因子）"].isna()).sum()) if "装箱数（因子）" in status_input.replenish_out.columns else 0],
        ["建议调货清单缺失装箱因子行数", int((status_input.transfer_out["装箱数（因子）"].isna()).sum()) if "装箱数（因子）" in status_input.transfer_out.columns else 0],
        ["同店同商品编码重复键数", int(status_input.mapping_stats.get("duplicate_store_product_keys", 0))],
        ["名称冲突键数", int(status_input.mapping_stats.get("name_conflict_keys", 0))],
        ["品牌冲突键数", int(status_input.mapping_stats.get("brand_conflict_keys", 0))],
        ["映射覆盖率", f"{float(status_input.mapping_stats.get('mapping_coverage_rate', 0.0)) * 100:.1f}%"],
        ["窗口数据状态", "正常" if (status_input.has_mtd_window_data and status_input.has_recent_window_data) else f"警告: 3M+MTD有效={status_input.has_mtd_window_data}, 30天有效={status_input.has_recent_window_data}"],
        ["自动扫描忽略销售文件数", len(status_input.ignored_sales_files)],
    ]
    if status_input.ignored_sales_files:
        status_rows.append(["自动扫描忽略销售文件", core_io.format_ignored_sales_files(status_input.ignored_sales_files)])
    if status_input.missing_sales_files:
        status_rows.append(["缺失销售文件", " | ".join(status_input.missing_sales_files)])

    return pd.DataFrame(status_rows, columns=["状态项", "值"])
