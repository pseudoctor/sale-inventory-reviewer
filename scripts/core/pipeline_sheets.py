from __future__ import annotations

import pandas as pd

from .models import OutputStageResult


def build_workbook_sheets(outputs: OutputStageResult, *, include_ranked_store_transfer_summary: bool) -> dict[str, pd.DataFrame]:
    """Assemble final workbook sheets in display order."""
    sheets = {
        **{name: frame for name, frame in outputs.frames.items() if name not in {"使用说明", "商品编码对照清单"}},
    }
    if include_ranked_store_transfer_summary:
        sheets["门店销量排名调货汇总"] = outputs.store_sales_ranking_transfer_out
    sheets["运行总览"] = outputs.executive_overview_out
    sheets["使用说明"] = outputs.frames.usage_guide
    sheets["商品编码对照清单"] = outputs.frames.product_code_catalog
    return sheets
