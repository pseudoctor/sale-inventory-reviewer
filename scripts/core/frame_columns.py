from __future__ import annotations

"""关键内部表与工作表的列契约常量。"""

NORMALIZED_SALES_REQUIRED_COLUMNS: tuple[str, ...] = (
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
)
NORMALIZED_SALES_OPTIONAL_COLUMNS: tuple[str, ...] = ("national_barcode",)

NORMALIZED_INVENTORY_REQUIRED_COLUMNS: tuple[str, ...] = (
    "store",
    "brand",
    "product",
    "barcode",
    "inventory_qty",
    "store_code",
    "product_code",
    "supplier_card",
)

MATCHING_DETAIL_COLUMNS: tuple[str, ...] = (
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
)

REPORT_FRAME_DETAIL_INPUT_COLUMNS: tuple[str, ...] = (
    "store",
    "brand",
    "barcode_output",
    "product",
    "province",
    "daily_sales_3m_mtd",
    "daily_sales_30d",
    "inventory_qty",
    "out_of_stock",
    "risk_level",
    "inventory_sales_ratio",
    "turnover_rate",
    "turnover_days",
    "suggest_outbound_qty",
    "suggest_replenish_qty",
    "name_source_rule",
    "brand_source_rule",
    "name_conflict_count",
    "brand_conflict_count",
)

REPORT_FRAME_DETAIL_OPTIONAL_COLUMNS: tuple[str, ...] = (
    "store_key",
    "product_key",
    "barcode",
    "forecast_daily_sales",
    "supplier_card",
    "daily_demand",
    "low_target_qty",
    "high_keep_qty",
    "need_qty",
    "outbound_rule",
)

MISSING_SALES_REQUIRED_COLUMNS: tuple[str, ...] = (
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
)

MISSING_SALES_OPTIONAL_COLUMNS: tuple[str, ...] = (
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
)

DETAIL_BASE_COLUMNS: tuple[str, ...] = ("门店名称", "品牌", "商品条码", "商品名称")
DETAIL_METRIC_COLUMNS: tuple[str, ...] = (
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
)
DETAIL_OPTIONAL_COLUMNS: tuple[str, ...] = ("省份",)
DETAIL_OUTPUT_COLUMNS: tuple[str, ...] = DETAIL_BASE_COLUMNS + DETAIL_OPTIONAL_COLUMNS + DETAIL_METRIC_COLUMNS

STORE_SUMMARY_COLUMNS: tuple[str, ...] = (
    "门店名称",
    "近三月+本月迄今平均日销",
    "近30天平均日销售",
    "预测平均日销(季节模式后)",
    "库存数量",
    "库存/销售比",
    "库存周转率",
    "库存周转天数",
    "风险等级",
)

BRAND_SUMMARY_COLUMNS: tuple[str, ...] = (
    "品牌",
    "近三月+本月迄今平均日销",
    "近30天平均日销售",
    "预测平均日销(季节模式后)",
    "库存数量",
    "库存/销售比",
    "库存周转率",
    "库存周转天数",
    "风险等级",
)

MISSING_SKU_REQUIRED_COLUMNS: tuple[str, ...] = ("门店名称", "品牌", "商品条码", "商品名称", "建议补货数量")
MISSING_SKU_OPTIONAL_COLUMNS: tuple[str, ...] = ("省份", "近三月+本月迄今平均日销", "近30天平均日销售", "建议补货箱数")

OUT_OF_STOCK_REQUIRED_COLUMNS: tuple[str, ...] = ("门店名称", "品牌", "商品条码", "商品名称", "建议补货数量")
OUT_OF_STOCK_OPTIONAL_COLUMNS: tuple[str, ...] = ("省份", "近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "缺货", "风险等级", "建议补货箱数")

REPLENISH_REQUIRED_COLUMNS: tuple[str, ...] = ("门店名称", "品牌", "商品条码", "商品名称", "建议补货数量")
REPLENISH_OPTIONAL_COLUMNS: tuple[str, ...] = ("省份", "近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "缺货", "风险等级", "装箱数（因子）", "建议补货箱数")

TRANSFER_REQUIRED_COLUMNS: tuple[str, ...] = ("门店名称", "品牌", "商品条码", "商品名称", "建议调出数量")
TRANSFER_OPTIONAL_COLUMNS: tuple[str, ...] = ("省份", "近三月+本月迄今平均日销", "近30天平均日销售", "库存数量", "风险等级", "装箱数（因子）")

PRODUCT_CODE_CATALOG_COLUMNS: tuple[str, ...] = (
    "商品编码",
    "品牌",
    "标准商品名",
    "销售表商品名",
    "库存商品名",
    "来源状态",
)

USAGE_GUIDE_COLUMNS: tuple[str, ...] = ("模块", "说明", "使用建议")
