from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd

from . import io as core_io
from . import recommendations as core_recommendations


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
        out[case_col] = core_recommendations.compute_case_counts(out[qty_col], out["装箱数（因子）"], use_peak_mode)
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


def build_report_frames(
    *,
    detail: pd.DataFrame,
    missing_sales: pd.DataFrame,
    store_summary: pd.DataFrame,
    brand_summary: pd.DataFrame,
    carton_factor_df: pd.DataFrame,
    is_wumei_system: bool,
    enable_province_column: bool,
    use_peak_mode: bool,
) -> Dict[str, pd.DataFrame]:
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
        }
    )
    detail_out["商品条码"] = detail_out["商品条码"].apply(lambda x: core_io.normalize_barcode_value(x) or "")
    detail_output_columns = ["门店名称", "品牌", "商品条码", "商品名称"]
    if enable_province_column:
        detail_output_columns.append("省份")
    detail_output_columns.extend(
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
        ]
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

    missing_output_columns = ["门店名称", "品牌", "商品条码", "商品名称"]
    if enable_province_column:
        missing_output_columns.append("省份")
    missing_output_columns.extend(["近三月+本月迄今平均日销", "近30天平均日销售", "建议补货数量"])
    missing_sku_out = missing_sku_out[missing_output_columns]

    out_of_stock_out = detail_out[detail_out["缺货"] == "是"].copy()
    out_of_stock_columns = ["门店名称", "品牌", "商品条码", "商品名称"]
    if enable_province_column:
        out_of_stock_columns.append("省份")
    out_of_stock_columns.extend(["近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "缺货", "风险等级", "建议补货数量"])
    out_of_stock_out = out_of_stock_out[out_of_stock_columns]

    replenish_out = detail_out[detail_out["建议补货数量"] > 0].copy()
    replenish_output_columns = ["门店名称", "品牌", "商品条码", "商品名称"]
    if enable_province_column:
        replenish_output_columns.append("省份")
    replenish_output_columns.extend(["近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "缺货", "风险等级", "建议补货数量"])
    replenish_out = replenish_out[replenish_output_columns]

    transfer_out = detail_out[detail_out["建议调出数量"] > 0].copy()
    transfer_output_columns = ["门店名称", "品牌", "商品条码", "商品名称"]
    if enable_province_column:
        transfer_output_columns.append("省份")
    transfer_output_columns.extend(["近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "风险等级", "建议调出数量"])
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

    return {
        "明细": detail_out,
        "门店汇总": store_summary_out,
        "品牌汇总": brand_summary_out,
        "缺货清单": out_of_stock_out,
        "库存缺失SKU清单": missing_sku_out,
        "建议补货清单": replenish_out,
        "建议调货清单": transfer_out,
    }
