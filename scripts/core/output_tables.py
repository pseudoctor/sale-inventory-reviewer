from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from . import frame_columns as core_frame_columns
from . import frame_schema as core_frame_schema
from . import io as core_io
from . import report_content as core_report_content
from .models import ReportFrames


def compute_case_counts(qty: pd.Series, factor: pd.Series, use_peak_mode: bool) -> pd.Series:
    qty_num = pd.to_numeric(qty, errors="coerce").fillna(0)
    factor_num = pd.to_numeric(factor, errors="coerce")
    valid_factor = factor_num > 0
    raw_cases = np.where(valid_factor, qty_num / factor_num, np.nan)
    rounded = np.where(valid_factor, np.ceil(raw_cases) if use_peak_mode else np.floor(raw_cases), np.nan)
    min_one_case_mask = valid_factor & (qty_num > 0) & (raw_cases < 1)
    rounded = np.where(min_one_case_mask, 1, rounded)
    return pd.Series(rounded, index=qty.index).astype("Int64")


def _build_usage_guide_frame() -> pd.DataFrame:
    return pd.DataFrame(core_report_content.USAGE_GUIDE_ROWS, columns=core_frame_columns.USAGE_GUIDE_COLUMNS)


def _attach_factor_and_case_count(
    df: pd.DataFrame,
    factor_by_barcode: pd.DataFrame,
    factor_by_product: pd.DataFrame,
    qty_col: str,
    use_peak_mode: bool,
    case_col: str | None = None,
    case_with_unit: bool = False,
    include_factor_col: bool = True,
) -> pd.DataFrame:
    out = df.copy()
    out["商品条码"] = out["商品条码"].apply(core_io.normalize_barcode_value)
    out = out.merge(factor_by_barcode, on="商品条码", how="left")
    fallback = out["装箱数（因子）"].isna()
    if fallback.any():
        merged = out.loc[fallback, ["商品名称"]].merge(
            factor_by_product,
            on="商品名称",
            how="left",
            suffixes=("", "_fallback"),
        )
        fallback_col = "装箱数（因子）_fallback" if "装箱数（因子）_fallback" in merged.columns else "装箱数（因子）"
        out.loc[fallback, "装箱数（因子）"] = merged[fallback_col].values

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


def _build_item_columns(enable_province_column: bool, trailing_columns: List[str]) -> List[str]:
    columns = list(core_frame_columns.DETAIL_BASE_COLUMNS)
    if enable_province_column:
        columns.append("省份")
    columns.extend(trailing_columns)
    return columns


def _build_summary_frames(
    *,
    detail: pd.DataFrame,
    store_summary: pd.DataFrame,
    brand_summary: pd.DataFrame,
    enable_province_column: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """构建明细、门店汇总、品牌汇总三个基础工作表。"""
    detail_out = detail.rename(
        columns={
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
            "name_source_rule": "名称来源规则",
            "brand_source_rule": "品牌来源规则",
            "name_conflict_count": "同键名称数",
            "brand_conflict_count": "同键品牌数",
        }
    )
    detail_out["商品条码"] = detail_out["商品条码"].apply(lambda x: core_io.normalize_barcode_value(x) or "")
    detail_out = detail_out[_build_item_columns(enable_province_column, list(core_frame_columns.DETAIL_METRIC_COLUMNS))]

    store_summary_out = store_summary.rename(
        columns={
            "store": "门店名称",
            "daily_sales_3m_mtd": "近三月+本月迄今平均日销",
            "daily_sales_30d": "近30天平均日销售",
            "forecast_daily_sales": "预测平均日销(季节模式后)",
            "inventory_qty": "库存数量",
            "risk_level": "风险等级",
            "inventory_sales_ratio": "库存/销售比",
            "turnover_rate": "库存周转率",
            "turnover_days": "库存周转天数",
        }
    )[list(core_frame_columns.STORE_SUMMARY_COLUMNS)]
    store_summary_out["预测平均日销(季节模式后)"] = pd.to_numeric(
        store_summary_out["预测平均日销(季节模式后)"], errors="coerce"
    ).round(3)

    brand_summary_out = brand_summary.rename(
        columns={
            "brand": "品牌",
            "daily_sales_3m_mtd": "近三月+本月迄今平均日销",
            "daily_sales_30d": "近30天平均日销售",
            "forecast_daily_sales": "预测平均日销(季节模式后)",
            "inventory_qty": "库存数量",
            "risk_level": "风险等级",
            "inventory_sales_ratio": "库存/销售比",
            "turnover_rate": "库存周转率",
            "turnover_days": "库存周转天数",
        }
    )[list(core_frame_columns.BRAND_SUMMARY_COLUMNS)]
    brand_summary_out["预测平均日销(季节模式后)"] = pd.to_numeric(
        brand_summary_out["预测平均日销(季节模式后)"], errors="coerce"
    ).round(3)
    return detail_out, store_summary_out, brand_summary_out


def _build_missing_and_action_frames(
    *,
    detail_out: pd.DataFrame,
    missing_sales: pd.DataFrame,
    is_wumei_system: bool,
    enable_province_column: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """构建缺失SKU、缺货、补货、调货四类动作工作表。"""
    missing_sku_out = missing_sales[
        ["store", "brand", "display_barcode", "barcode", "product", "province", "daily_sales_3m_mtd", "daily_sales_30d"]
    ].rename(
        columns={
            "store": "门店名称",
            "brand": "品牌",
            "display_barcode": "商品条码",
            "product": "商品名称",
            "province": "省份",
            "daily_sales_3m_mtd": "近三月+本月迄今平均日销",
            "daily_sales_30d": "近30天平均日销售",
        }
    )
    missing_sku_out = missing_sku_out.drop(columns=["barcode"])
    missing_sku_out["商品条码"] = missing_sku_out["商品条码"].apply(lambda x: core_io.normalize_barcode_value(x) or "")

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
        .groupby(["门店名称", "品牌", "商品名称"], as_index=False)
        .agg(
            建议补货数量_fallback=("建议补货数量", "max"),
            同名商品候选数=("建议补货数量", "size"),
        )
    )
    missing_sku_out = missing_sku_out.merge(
        replenish_lookup_fallback,
        on=["门店名称", "品牌", "商品名称"],
        how="left",
    )
    missing_sku_out["建议补货数量_fallback"] = missing_sku_out["建议补货数量_fallback"].where(
        pd.to_numeric(missing_sku_out["同名商品候选数"], errors="coerce").fillna(0).eq(1),
        None,
    )
    missing_sku_out["建议补货数量"] = (
        missing_sku_out["建议补货数量"].fillna(missing_sku_out["建议补货数量_fallback"]).fillna(0).astype(int)
    )
    missing_sku_out = missing_sku_out.drop(columns=["建议补货数量_fallback", "同名商品候选数"])
    missing_sku_out = missing_sku_out[_build_item_columns(
        enable_province_column,
        ["近三月+本月迄今平均日销", "近30天平均日销售", "建议补货数量"],
    )]

    out_of_stock_out = detail_out[detail_out["缺货"] == "是"].copy()
    out_of_stock_out = out_of_stock_out[_build_item_columns(
        enable_province_column,
        ["近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "缺货", "风险等级", "建议补货数量"],
    )]

    replenish_out = detail_out[detail_out["建议补货数量"] > 0].copy()
    replenish_out = replenish_out[_build_item_columns(
        enable_province_column,
        ["近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "缺货", "风险等级", "建议补货数量"],
    )]

    transfer_out = detail_out[detail_out["建议调出数量"] > 0].copy()
    transfer_out = transfer_out[_build_item_columns(
        enable_province_column,
        ["近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "风险等级", "建议调出数量"],
    )]
    return missing_sku_out, out_of_stock_out, replenish_out, transfer_out


def _attach_case_columns_to_action_frames(
    *,
    missing_sku_out: pd.DataFrame,
    out_of_stock_out: pd.DataFrame,
    replenish_out: pd.DataFrame,
    transfer_out: pd.DataFrame,
    carton_factor_df: pd.DataFrame,
    use_peak_mode: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """为动作类工作表补充装箱因子与箱数列。"""
    factor_by_barcode = (
        carton_factor_df[carton_factor_df["商品条码"].notna()][["商品条码", "装箱数（因子）"]]
        .drop_duplicates(subset=["商品条码"], keep="first")
    )
    factor_by_product = carton_factor_df[["商品名称", "装箱数（因子）"]].drop_duplicates(subset=["商品名称"], keep="first")

    missing_sku_out = _attach_factor_and_case_count(
        missing_sku_out,
        factor_by_barcode,
        factor_by_product,
        "建议补货数量",
        use_peak_mode,
        case_col="建议补货箱数",
        case_with_unit=True,
        include_factor_col=False,
    )
    out_of_stock_out = _attach_factor_and_case_count(
        out_of_stock_out,
        factor_by_barcode,
        factor_by_product,
        "建议补货数量",
        use_peak_mode,
        case_col="建议补货箱数",
        case_with_unit=True,
        include_factor_col=False,
    )
    replenish_out = _attach_factor_and_case_count(
        replenish_out,
        factor_by_barcode,
        factor_by_product,
        "建议补货数量",
        use_peak_mode,
        case_col="建议补货箱数",
        case_with_unit=True,
        include_factor_col=True,
    )
    transfer_out = _attach_factor_and_case_count(
        transfer_out,
        factor_by_barcode,
        factor_by_product,
        "建议调出数量",
        use_peak_mode,
        case_col=None,
        include_factor_col=True,
    )
    return missing_sku_out, out_of_stock_out, replenish_out, transfer_out


def _build_product_code_catalog_frame(product_code_catalog: pd.DataFrame) -> pd.DataFrame:
    """构建商品编码对照清单工作表。"""
    return product_code_catalog.rename(
        columns={
            "product_code": "商品编码",
            "brand": "品牌",
            "standard_product_name": "标准商品名",
            "sales_product_name": "销售表商品名",
            "inventory_product_name": "库存商品名",
            "source_status": "来源状态",
        }
    )[list(core_frame_columns.PRODUCT_CODE_CATALOG_COLUMNS)]


def build_report_frames(
    *,
    detail: pd.DataFrame,
    missing_sales: pd.DataFrame,
    store_summary: pd.DataFrame,
    brand_summary: pd.DataFrame,
    product_code_catalog: pd.DataFrame,
    carton_factor_df: pd.DataFrame,
    is_wumei_system: bool,
    enable_province_column: bool,
    use_peak_mode: bool,
) -> ReportFrames:
    """构建全部报表工作表数据，并以显式结果对象返回。"""
    core_frame_schema.validate_frame_columns(detail, core_frame_schema.REPORT_FRAME_DETAIL_INPUT_SCHEMA)
    usage_guide_out = _build_usage_guide_frame()
    detail_out, store_summary_out, brand_summary_out = _build_summary_frames(
        detail=detail,
        store_summary=store_summary,
        brand_summary=brand_summary,
        enable_province_column=enable_province_column,
    )
    missing_sku_out, out_of_stock_out, replenish_out, transfer_out = _build_missing_and_action_frames(
        detail_out=detail_out,
        missing_sales=missing_sales,
        is_wumei_system=is_wumei_system,
        enable_province_column=enable_province_column,
    )
    missing_sku_out, out_of_stock_out, replenish_out, transfer_out = _attach_case_columns_to_action_frames(
        missing_sku_out=missing_sku_out,
        out_of_stock_out=out_of_stock_out,
        replenish_out=replenish_out,
        transfer_out=transfer_out,
        carton_factor_df=carton_factor_df,
        use_peak_mode=use_peak_mode,
    )
    product_code_catalog_out = _build_product_code_catalog_frame(product_code_catalog)

    report_frames = ReportFrames(
        usage_guide=usage_guide_out,
        detail=detail_out,
        store_summary=store_summary_out,
        brand_summary=brand_summary_out,
        out_of_stock=out_of_stock_out,
        missing_sku=missing_sku_out,
        replenish=replenish_out,
        transfer=transfer_out,
        product_code_catalog=product_code_catalog_out,
    )
    core_frame_schema.validate_named_frames(report_frames.items(), core_frame_schema.REPORT_FRAME_SCHEMAS)
    return report_frames
