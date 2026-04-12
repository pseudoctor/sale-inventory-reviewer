from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


@dataclass(frozen=True)
class FrameSchema:
    """描述单个 DataFrame 在某个阶段应满足的列约束。"""

    name: str
    required_columns: tuple[str, ...]
    optional_columns: tuple[str, ...] = ()
    allow_unknown_columns: bool = True
    description: str = ""
    column_descriptions: dict[str, str] | None = None

    @property
    def allowed_columns(self) -> tuple[str, ...]:
        """返回该 schema 允许出现的全部列。"""
        return self.required_columns + self.optional_columns


def validate_frame_columns(df: pd.DataFrame, schema: FrameSchema) -> pd.DataFrame:
    """校验 DataFrame 是否包含指定阶段所需的关键列。"""
    missing_columns = [column for column in schema.required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(
            f"{schema.name} missing required columns: {', '.join(missing_columns)}. "
            f"Current columns: {', '.join(map(str, df.columns.tolist()))}"
        )
    if not schema.allow_unknown_columns:
        unexpected_columns = [column for column in df.columns if column not in schema.allowed_columns]
        if unexpected_columns:
            raise ValueError(
                f"{schema.name} contains unexpected columns: {', '.join(unexpected_columns)}. "
                f"Allowed columns: {', '.join(schema.allowed_columns)}"
            )
    return df


def validate_named_frames(frames: Iterable[tuple[str, pd.DataFrame]], schema_map: dict[str, FrameSchema]) -> None:
    """批量校验一组具名工作表数据。"""
    for frame_name, df in frames:
        schema = schema_map.get(frame_name)
        if schema is None:
            continue
        validate_frame_columns(df, schema)


NORMALIZED_SALES_SCHEMA = FrameSchema(
    name="input.sales.normalized",
    required_columns=(
        "store",
        "product",
        "barcode",
        "sales_qty",
        "sales_date",
        "brand",
        "store_code",
        "product_code",
        "display_barcode",
        "supplier_card",
        "sales_amount",
    ),
    optional_columns=("national_barcode",),
    allow_unknown_columns=False,
    description="销售标准化结果，供窗口计算、匹配和销售额调货汇总复用。",
    column_descriptions={
        "store": "标准化后的门店名称。",
        "product": "标准化后的商品名称。",
        "barcode": "标准化后的主条码或商品条码。",
        "sales_qty": "标准化后的销量数值。",
        "sales_date": "解析后的销售日期。",
        "brand": "补齐后的品牌名称。",
        "store_code": "标准化后的门店编码。",
        "product_code": "标准化后的商品编码。",
        "display_barcode": "用于展示的条码，优先国条码。",
        "supplier_card": "标准化后的供商卡号。",
        "sales_amount": "标准化后的销售金额。",
        "national_barcode": "标准化后的国条码，可选。",
    },
)

NORMALIZED_INVENTORY_SCHEMA = FrameSchema(
    name="input.inventory.normalized",
    required_columns=(
        "store",
        "brand",
        "product",
        "barcode",
        "inventory_qty",
        "store_code",
        "product_code",
        "supplier_card",
    ),
    allow_unknown_columns=False,
    description="库存标准化结果，供匹配、风险计算和输出表构建使用。",
    column_descriptions={
        "store": "标准化后的门店名称。",
        "brand": "补齐后的品牌名称。",
        "product": "标准化后的商品名称。",
        "barcode": "标准化后的库存条码。",
        "inventory_qty": "标准化后的库存数量。",
        "store_code": "标准化后的门店编码。",
        "product_code": "标准化后的商品编码。",
        "supplier_card": "标准化后的供商卡号。",
    },
)

MATCHING_DETAIL_SCHEMA = FrameSchema(
    name="analysis.matching.detail",
    required_columns=(
        "store_key",
        "product_key",
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
        "name_source_rule",
        "brand_source_rule",
        "name_conflict_count",
        "brand_conflict_count",
    ),
    allow_unknown_columns=False,
    description="销售库存匹配后的核心明细表，是后续动作建议和报表工作表的基础底表。",
)

MISSING_SALES_SCHEMA = FrameSchema(
    name="analysis.matching.missing_sales",
    required_columns=(
        "store_key",
        "product_key",
        "store",
        "brand",
        "product",
        "barcode",
        "display_barcode",
        "daily_sales_3m_mtd",
        "daily_sales_30d",
        "forecast_daily_sales",
        "supplier_card",
        "province",
    ),
    optional_columns=(
        "sales_qty_total",
        "sales_qty_3m_mtd",
        "sales_qty_30d",
        "display_product_name",
        "display_brand",
        "name_source_ts",
        "brand_source_ts",
        "name_conflict_count",
        "brand_conflict_count",
        "record_rows",
        "name_source_rule",
        "brand_source_rule",
    ),
    allow_unknown_columns=False,
    description="仅存在销售、库存表无记录且仍有预测销量的缺失 SKU 集合。",
)

REPORT_FRAME_SCHEMAS = {
    "使用说明": FrameSchema("report.usage_guide", ("模块", "说明", "使用建议"), allow_unknown_columns=False, description="工作簿使用说明页。"),
    "明细": FrameSchema(
        "report.detail",
        ("门店名称", "品牌", "商品条码", "商品名称", "建议调出数量", "建议补货数量"),
        optional_columns=("省份", "近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "缺货", "风险等级", "库存/销售比", "库存周转率", "库存周转天数"),
        allow_unknown_columns=False,
        description="工作簿主明细页。",
    ),
    "门店汇总": FrameSchema(
        "report.store_summary",
        ("门店名称", "预测平均日销(季节模式后)", "风险等级"),
        optional_columns=("近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "库存/销售比", "库存周转率", "库存周转天数"),
        allow_unknown_columns=False,
        description="按门店聚合的经营概览页。",
    ),
    "品牌汇总": FrameSchema(
        "report.brand_summary",
        ("品牌", "预测平均日销(季节模式后)", "风险等级"),
        optional_columns=("近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "库存/销售比", "库存周转率", "库存周转天数"),
        allow_unknown_columns=False,
        description="按品牌聚合的经营概览页。",
    ),
    "缺货清单": FrameSchema(
        "report.out_of_stock",
        ("门店名称", "品牌", "商品条码", "商品名称", "建议补货数量"),
        optional_columns=("省份", "近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "缺货", "风险等级", "建议补货箱数"),
        allow_unknown_columns=False,
        description="库存为 0 且仍有销量需求的缺货动作页。",
    ),
    "库存缺失SKU清单": FrameSchema(
        "report.missing_sku",
        ("门店名称", "品牌", "商品条码", "商品名称", "建议补货数量"),
        optional_columns=("省份", "近三月+本月迄今平均日销", "近30天平均日销售", "建议补货箱数"),
        allow_unknown_columns=False,
        description="销售存在但库存缺记录的缺失 SKU 页。",
    ),
    "建议补货清单": FrameSchema(
        "report.replenish",
        ("门店名称", "品牌", "商品条码", "商品名称", "建议补货数量"),
        optional_columns=("省份", "近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "缺货", "风险等级", "装箱数（因子）", "建议补货箱数"),
        allow_unknown_columns=False,
        description="建议补货动作页。",
    ),
    "建议调货清单": FrameSchema(
        "report.transfer",
        ("门店名称", "品牌", "商品条码", "商品名称", "建议调出数量"),
        optional_columns=("省份", "近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "风险等级", "装箱数（因子）"),
        allow_unknown_columns=False,
        description="建议调货动作页。",
    ),
    "商品编码对照清单": FrameSchema(
        "report.product_code_catalog",
        ("商品编码", "品牌", "标准商品名", "来源状态"),
        optional_columns=("销售表商品名", "库存商品名"),
        allow_unknown_columns=False,
        description="商品编码主数据对照页。",
    ),
}
