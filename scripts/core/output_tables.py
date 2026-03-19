from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd

from . import io as core_io


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
    rows = [
        ["报表定位", "本报表用于识别库存过高、库存偏低、缺货和库存记录缺失的商品，并给出补货/调货参考。", "建议先看运行总览，再看异常清单，最后回到明细定位具体商品。"],
        ["推荐阅读顺序", "建议按 使用说明 -> 运行总览 -> 缺货清单 / 库存缺失SKU清单 / 建议补货清单 / 建议调货清单 -> 明细 -> 商品编码对照清单 的顺序查看。", "这样可以先判断整体风险，再看需要执行的动作，最后核对明细和商品名称映射。"],
        ["明细", "展示门店、品牌、商品维度的完整库存和销售情况，是最终核对底表。", "需要追溯单个商品时，以明细为准。"],
        ["门店汇总", "按门店汇总日销、库存、周转天数和风险等级。", "适合看门店整体库存结构是否偏高或偏低。"],
        ["品牌汇总", "按品牌汇总日销、库存、周转天数和风险等级。", "适合看品牌维度的库存压力。"],
        ["缺货清单", "库存表中存在该商品记录，但库存数量为 0，且预测日销大于 0。", "这是有库存记录但已经卖空的商品。"],
        ["库存缺失SKU清单", "销售表中存在该商品，但库存表里没有对应商品记录。", "这是库存数据缺记录，不等于库存为 0。"],
        ["建议补货清单", "所有建议补货数量大于 0 的商品都会进入该表。", "该表可能包含缺货商品、库存偏低商品，也可能包含库存缺失商品。"],
        ["建议调货清单", "库存偏高且达到调出条件的商品会进入该表。", "适合用于跨门店调货或减少积压。"],
        ["运行总览", "将原汇总和运行状态合并展示，包含核心经营指标、数据质量状态和映射覆盖率。", "适合管理层和业务负责人先看整体风险，再决定是否下钻。"],
        ["商品编码对照清单", "按商品编码汇总销售表商品名、库存商品名和来源状态，放在工作簿最后。", "适合在完成业务判断后，再核对同一商品编码在两张表中的名称是否一致。"],
        ["匹配逻辑", "当前销售和库存主要按 商品编码 + 门店编码 匹配；缺失时回退到现有兼容键。", "如果结果异常，优先检查商品编码、门店编码是否规范。"],
        ["近三月+本月迄今平均日销", "3M+MTD窗口销量总和 / 窗口有效覆盖天数。", "用于衡量中期销售水平。"],
        ["近30天平均日销售", "近 30 天销量总和 / 30 个自然日。", "用于衡量近期销售水平。"],
        ["预测平均日销(季节模式后)", "淡季取两种日销的较低值，旺季取较高值。", "这是库存周转和补货计算的基础口径。"],
        ["风险等级", "按库存周转天数划分：低 < 45 天，中 45-60 天，高 > 60 天。", "高风险通常是库存偏高，低风险通常是库存偏低或缺货。"],
        ["建议补货数量", "当商品缺货或库存不足以覆盖低库存阈值时，系统给出建议补货数量。", "建议结合门店实际促销、陈列和到货周期判断。"],
        ["建议调出数量", "当商品库存过高且销量不足时，系统给出建议调出数量。", "建议结合可调入门店需求一起使用。"],
        ["易混概念区分", "缺货清单 != 库存缺失SKU清单 != 建议补货清单。", "缺货是有记录但库存为 0；库存缺失是没有库存记录；建议补货是所有需要补货的商品集合。"],
    ]
    return pd.DataFrame(rows, columns=["模块", "说明", "使用建议"])


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
    columns = ["门店名称", "品牌", "商品条码", "商品名称"]
    if enable_province_column:
        columns.append("省份")
    columns.extend(trailing_columns)
    return columns


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
) -> Dict[str, pd.DataFrame]:
    usage_guide_out = _build_usage_guide_frame()
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
    detail_output_columns = _build_item_columns(
        enable_province_column,
        [
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
        ],
    )
    detail_out = detail_out[detail_output_columns]

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
    )[
        [
            "门店名称",
            "近三月+本月迄今平均日销",
            "近30天平均日销售",
            "预测平均日销(季节模式后)",
            "库存数量",
            "库存/销售比",
            "库存周转率",
            "库存周转天数",
            "风险等级",
        ]
    ]
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
    )[
        [
            "品牌",
            "近三月+本月迄今平均日销",
            "近30天平均日销售",
            "预测平均日销(季节模式后)",
            "库存数量",
            "库存/销售比",
            "库存周转率",
            "库存周转天数",
            "风险等级",
        ]
    ]
    brand_summary_out["预测平均日销(季节模式后)"] = pd.to_numeric(
        brand_summary_out["预测平均日销(季节模式后)"], errors="coerce"
    ).round(3)

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
    missing_sku_out["商品条码"] = (
        missing_sku_out["商品条码"].where(missing_sku_out["商品条码"].notna(), missing_sku_out["barcode"])
        if is_wumei_system
        else missing_sku_out["barcode"]
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

    missing_output_columns = _build_item_columns(
        enable_province_column,
        ["近三月+本月迄今平均日销", "近30天平均日销售", "建议补货数量"],
    )
    missing_sku_out = missing_sku_out[missing_output_columns]

    out_of_stock_out = detail_out[detail_out["缺货"] == "是"].copy()
    out_of_stock_columns = _build_item_columns(
        enable_province_column,
        ["近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "缺货", "风险等级", "建议补货数量"],
    )
    out_of_stock_out = out_of_stock_out[out_of_stock_columns]

    replenish_out = detail_out[detail_out["建议补货数量"] > 0].copy()
    replenish_output_columns = _build_item_columns(
        enable_province_column,
        ["近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "缺货", "风险等级", "建议补货数量"],
    )
    replenish_out = replenish_out[replenish_output_columns]

    transfer_out = detail_out[detail_out["建议调出数量"] > 0].copy()
    transfer_output_columns = _build_item_columns(
        enable_province_column,
        ["近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "风险等级", "建议调出数量"],
    )
    transfer_out = transfer_out[transfer_output_columns]

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

    product_code_catalog_out = product_code_catalog.rename(
        columns={
            "product_code": "商品编码",
            "brand": "品牌",
            "standard_product_name": "标准商品名",
            "sales_product_name": "销售表商品名",
            "inventory_product_name": "库存商品名",
            "source_status": "来源状态",
        }
    )[
        ["商品编码", "品牌", "标准商品名", "销售表商品名", "库存商品名", "来源状态"]
    ]

    return {
        "使用说明": usage_guide_out,
        "明细": detail_out,
        "门店汇总": store_summary_out,
        "品牌汇总": brand_summary_out,
        "缺货清单": out_of_stock_out,
        "库存缺失SKU清单": missing_sku_out,
        "建议补货清单": replenish_out,
        "建议调货清单": transfer_out,
        "商品编码对照清单": product_code_catalog_out,
    }
