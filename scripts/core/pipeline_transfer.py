from __future__ import annotations

import numpy as np
import pandas as pd

from . import io as core_io


def apply_recommendation_columns(
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


def build_store_sales_ranking_transfer_frame(
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
    # 单价兜底也限定在同一 3M+MTD 销售窗口内，且仅使用有效销量记录，保持与金额口径一致。
    all_sales_base = sales_base[pd.to_numeric(sales_base["sales_qty"], errors="coerce").fillna(0.0) > 0].copy()
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

    transfer["排名"] = transfer["store_sales_amount"].rank(method="dense", ascending=False).astype(int)
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
