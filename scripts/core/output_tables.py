from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd

from . import frame_schema as core_frame_schema
from . import io as core_io
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
    rows = [
        ["报表定位", "本报表用于识别库存过高、库存偏低、缺货、库存缺失以及零销量积压商品，并给出补货、调货和门店销售额排名参考。", "建议先看运行总览把握整体风险，再看行动类清单，最后回到明细和销售额调货汇总核对具体商品。"],
        ["推荐阅读顺序", "建议按 使用说明 -> 运行总览 -> 缺货清单 / 库存缺失SKU清单 / 建议补货清单 / 建议调货清单 / 门店销量排名调货汇总 -> 明细 -> 商品编码对照清单 的顺序查看。", "这样可以先判断整体风险，再看需要执行的动作，最后核对商品名称、销售额和匹配关系。"],
        ["明细", "展示门店、品牌、商品维度的完整库存和销售情况，是整个工作簿的基础底表。除使用说明、运行总览外，其它动作类清单都由明细加工而来。", "需要追溯单个商品的日销、库存、风险等级、补货量或调出量时，以明细为准。"],
        ["门店汇总", "按门店汇总近三月+本月迄今平均日销、近30天平均日销售、预测平均日销、库存数量、库存周转率、库存周转天数和风险等级。", "适合先看哪家门店整体库存压力更高，再决定是否下钻到明细或门店销量排名调货汇总。"],
        ["品牌汇总", "按品牌汇总日销、库存、周转天数和风险等级，帮助识别品牌维度的压货或缺货问题。", "适合品牌负责人或采销先看品牌层面的库存健康度，再回到门店或商品层。"],
        ["缺货清单", "库存表中存在该商品记录，且库存数量 = 0，同时预测平均日销 > 0。它表示商品在库存表里有记录，但已经卖空。", "这是优先级最高的补货检查清单之一，建议结合建议补货数量和门店实际到货周期处理。"],
        ["库存缺失SKU清单", "销售表中存在该商品，但库存表里没有对应商品记录。它表示库存数据缺记录，不等于库存数量为 0。", "遇到该表中的商品时，先核查库存主数据是否漏录，再决定是否补货。"],
        ["建议补货清单", "所有建议补货数量 > 0 的商品都会进入该表。它可能包含缺货商品、库存偏低商品，也可能包含库存缺失商品。", "不要把建议补货清单等同于缺货清单；建议补货清单是更大的动作集合。"],
        ["建议调货清单", "所有建议调出数量 > 0 的商品都会进入该表。该表使用当前系统的调出规则：库存偏高时按阈值计算调出量；零销量积压SKU根据配置可保留安全库存或全部调出。", "这是当前系统的标准调出动作清单，适合看按现有库存规则得到的可执行调出建议。"],
        ["门店销量排名调货汇总", "该表按近三个月完整自然月 + 本月迄今这一窗口汇总销售额，再按门店销售额降序排名，并列出有调货动作的商品。表中的“门店销售额总计(开始日期至结束日期)”表示该门店在该窗口内的总销售额；“商品销售额(开始日期至结束日期)”表示该门店该商品在同一窗口内的销售额；“商品单价”优先按当前门店该商品在同一窗口内的 商品销售额 / 商品销售数量 计算，若当前门店窗口内销量为 0，则回退到同一窗口内其他门店该商品的平均单价；“库存金额” = 商品单价 * 库存数量。调货数量采用混合口径：当近三月+本月迄今平均日销 = 0、库存数量 > 0 且非缺货时，直接按库存数量全调；其它商品沿用系统已有建议调出数量。", "这个表适合门店运营或区域负责人按近三个月至今的销售贡献度安排调货顺序，并结合库存金额评估压货价值。它不是简单复制建议调货清单，而是额外引入了带时间区间的销售额排名、单价和金额口径。"],
        ["运行总览", "将原汇总和运行状态合并展示，包含核心经营指标、数据质量状态、窗口有效性、映射覆盖率和输入文件统计。", "适合管理层和业务负责人先看整体风险，再决定是否下钻到具体清单。"],
        ["商品编码对照清单", "按商品编码汇总销售表商品名、库存表商品名和来源状态，放在工作簿最后，方便人工核对同一商品在两张表中的名称差异。", "完成业务判断后，如果发现同码不同名或匹配异常，优先查看该表。"],
        ["匹配逻辑", "当前销售和库存主要按 商品编码 + 门店编码 匹配；当编码缺失时，才回退到现有兼容键。名称、品牌展示优先采用销售侧最新记录，不足时回退库存侧。", "如果结果异常，优先检查商品编码、门店编码、条码和商品名称是否规范一致。"],
        ["近三月+本月迄今平均日销", "3M+MTD窗口销量总和 / 窗口有效覆盖天数。窗口默认是近三个月完整自然月 + 本月迄今；若销售数据覆盖不满整窗，则只按与实际销售日期重叠的有效天数计算。", "这是中期销量口径，也是门店销量排名调货汇总里“零中期销量是否全调”的判定依据。"],
        ["近30天平均日销售", "近30天销量总和 / 30个自然日。它强调近期销售表现，即使数据覆盖不满30天，也仍按30天自然日折算。", "适合识别近期销量是否突然上升或下降，和中期口径结合判断是否存在短期促销波动。"],
        ["预测平均日销(季节模式后)", "淡季模式取 近三月+本月迄今平均日销 与 近30天平均日销售 的较低值；旺季模式取两者较高值。", "这是库存周转、风险等级、建议补货数量和标准建议调出数量的基础口径。"],
        ["风险等级", "按库存周转天数划分：低 < 45 天，中 45-60 天，高 > 60 天。库存周转天数 = 库存数量 / 预测平均日销；当预测平均日销为 0 时，库存周转天数记为 inf。", "高风险通常表示库存偏高或零销量积压，低风险通常表示库存偏低或缺货。"],
        ["建议补货数量", "当商品缺货或库存不足以覆盖低库存阈值时，系统给出建议补货数量。计算核心是将库存补到低阈值对应的目标库存。", "建议结合门店实际促销、陈列、安全库存和到货周期判断，不建议机械照搬。"],
        ["建议调出数量", "建议调出数量是系统标准调出规则的结果。对于有销量的高库存商品，按高阈值库存保有量计算冗余库存；对于零销量积压SKU，则按配置决定保留安全库存后调出，或全部调出。", "建议调货清单直接使用这一列；门店销量排名调货汇总只在“近三月+本月迄今平均日销 = 0”场景下覆盖为全库存调出。"],
        ["易混概念区分", "缺货清单 != 库存缺失SKU清单 != 建议补货清单 != 建议调货清单 != 门店销量排名调货汇总。缺货是库存为0；库存缺失是库存表无记录；建议补货是需要补货的全集；建议调货是按系统标准调出规则产生的调货动作；门店销量排名调货汇总是在调货动作基础上叠加门店销售额排名和“零中期销量全调”口径后的管理视图。", "看动作时先分清表的口径，再决定是否执行补货或调货。"],
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
    detail_out = detail_out[_build_item_columns(
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
    )]

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
    )[
        ["商品编码", "品牌", "标准商品名", "销售表商品名", "库存商品名", "来源状态"]
    ]


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
    """构建全部报表工作表数据，并以显式结果对象返回。"""
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
