from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from . import frame_columns as core_frame_columns

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
    required_columns=core_frame_columns.NORMALIZED_SALES_REQUIRED_COLUMNS,
    optional_columns=core_frame_columns.NORMALIZED_SALES_OPTIONAL_COLUMNS,
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
    required_columns=core_frame_columns.NORMALIZED_INVENTORY_REQUIRED_COLUMNS,
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
    required_columns=core_frame_columns.MATCHING_DETAIL_COLUMNS,
    allow_unknown_columns=False,
    description="销售库存匹配后的核心明细表，是后续动作建议和报表工作表的基础底表。",
)

REPORT_FRAME_DETAIL_INPUT_SCHEMA = FrameSchema(
    name="report.detail_input",
    required_columns=core_frame_columns.REPORT_FRAME_DETAIL_INPUT_COLUMNS,
    optional_columns=core_frame_columns.REPORT_FRAME_DETAIL_OPTIONAL_COLUMNS,
    allow_unknown_columns=True,
    description="构建报表工作表前的推荐后明细表，必须包含动作建议与缺货标记列。",
)

MISSING_SALES_SCHEMA = FrameSchema(
    name="analysis.matching.missing_sales",
    required_columns=core_frame_columns.MISSING_SALES_REQUIRED_COLUMNS,
    optional_columns=core_frame_columns.MISSING_SALES_OPTIONAL_COLUMNS,
    allow_unknown_columns=False,
    description="仅存在销售、库存表无记录且仍有预测销量的缺失 SKU 集合。",
)

REPORT_FRAME_SCHEMAS = {
    "使用说明": FrameSchema("report.usage_guide", core_frame_columns.USAGE_GUIDE_COLUMNS, allow_unknown_columns=False, description="工作簿使用说明页。"),
    "明细": FrameSchema(
        "report.detail",
        core_frame_columns.DETAIL_BASE_COLUMNS + ("建议调出数量", "建议补货数量"),
        optional_columns=tuple(
            column for column in core_frame_columns.DETAIL_OUTPUT_COLUMNS if column not in core_frame_columns.DETAIL_BASE_COLUMNS + ("建议调出数量", "建议补货数量")
        ),
        allow_unknown_columns=False,
        description="工作簿主明细页。",
    ),
    "门店汇总": FrameSchema(
        "report.store_summary",
        ("门店名称", "预测平均日销(季节模式后)", "风险等级"),
        optional_columns=tuple(column for column in core_frame_columns.STORE_SUMMARY_COLUMNS if column not in ("门店名称", "预测平均日销(季节模式后)", "风险等级")),
        allow_unknown_columns=False,
        description="按门店聚合的经营概览页。",
    ),
    "品牌汇总": FrameSchema(
        "report.brand_summary",
        ("品牌", "预测平均日销(季节模式后)", "风险等级"),
        optional_columns=tuple(column for column in core_frame_columns.BRAND_SUMMARY_COLUMNS if column not in ("品牌", "预测平均日销(季节模式后)", "风险等级")),
        allow_unknown_columns=False,
        description="按品牌聚合的经营概览页。",
    ),
    "缺货清单": FrameSchema(
        "report.out_of_stock",
        core_frame_columns.OUT_OF_STOCK_REQUIRED_COLUMNS,
        optional_columns=core_frame_columns.OUT_OF_STOCK_OPTIONAL_COLUMNS,
        allow_unknown_columns=False,
        description="库存为 0 且仍有销量需求的缺货动作页。",
    ),
    "库存缺失SKU清单": FrameSchema(
        "report.missing_sku",
        core_frame_columns.MISSING_SKU_REQUIRED_COLUMNS,
        optional_columns=core_frame_columns.MISSING_SKU_OPTIONAL_COLUMNS,
        allow_unknown_columns=False,
        description="销售存在但库存缺记录的缺失 SKU 页。",
    ),
    "建议补货清单": FrameSchema(
        "report.replenish",
        core_frame_columns.REPLENISH_REQUIRED_COLUMNS,
        optional_columns=core_frame_columns.REPLENISH_OPTIONAL_COLUMNS,
        allow_unknown_columns=False,
        description="建议补货动作页。",
    ),
    "建议调货清单": FrameSchema(
        "report.transfer",
        core_frame_columns.TRANSFER_REQUIRED_COLUMNS,
        optional_columns=core_frame_columns.TRANSFER_OPTIONAL_COLUMNS,
        allow_unknown_columns=False,
        description="建议调货动作页。",
    ),
    "商品编码对照清单": FrameSchema(
        "report.product_code_catalog",
        ("商品编码", "品牌", "标准商品名", "来源状态"),
        optional_columns=tuple(column for column in core_frame_columns.PRODUCT_CODE_CATALOG_COLUMNS if column not in ("商品编码", "品牌", "标准商品名", "来源状态")),
        allow_unknown_columns=False,
        description="商品编码主数据对照页。",
    ),
}
