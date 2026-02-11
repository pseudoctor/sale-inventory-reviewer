from __future__ import annotations

from typing import Callable

import pandas as pd

from . import io as core_io
from . import metrics as core_metrics


def build_detail_with_matching(
    *,
    sales_df: pd.DataFrame,
    inv_df: pd.DataFrame,
    mtd_start: pd.Timestamp,
    mtd_end: pd.Timestamp,
    recent_start: pd.Timestamp,
    inventory_date_ts: pd.Timestamp,
    mtd_days: int,
    recent_days_effective: int,
    has_mtd_window_data: bool,
    has_recent_window_data: bool,
    use_peak_mode: bool,
    low_days: float,
    high_days: float,
    is_wumei_system: bool,
    province_mapper: Callable[[str | None], str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    sales_df = sales_df.copy()
    sales_df["barcode_key"] = sales_df["barcode"].apply(core_io.normalize_barcode_value)
    sales_3m_mtd = sales_df[(sales_df["sales_date"] >= mtd_start) & (sales_df["sales_date"] <= mtd_end)]
    sales_30d = sales_df[(sales_df["sales_date"] >= recent_start) & (sales_df["sales_date"] <= inventory_date_ts)]

    sales_totals = (
        sales_df.groupby(["store", "brand", "product", "barcode"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "sales_qty_total"})
    )
    sales_totals = sales_totals.merge(
        sales_3m_mtd.groupby(["store", "brand", "product", "barcode"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "sales_qty_3m_mtd"}),
        on=["store", "brand", "product", "barcode"],
        how="left",
    )
    sales_totals = sales_totals.merge(
        sales_30d.groupby(["store", "brand", "product", "barcode"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "sales_qty_30d"}),
        on=["store", "brand", "product", "barcode"],
        how="left",
    )
    sales_totals["sales_qty_3m_mtd"] = sales_totals["sales_qty_3m_mtd"].fillna(0)
    sales_totals["sales_qty_30d"] = sales_totals["sales_qty_30d"].fillna(0)
    sales_totals["daily_sales_3m_mtd"] = sales_totals["sales_qty_3m_mtd"] / mtd_days if has_mtd_window_data else 0
    sales_totals["daily_sales_30d"] = sales_totals["sales_qty_30d"] / recent_days_effective if has_recent_window_data else 0
    sales_totals["forecast_daily_sales"] = core_metrics.combine_daily_sales(
        sales_totals["daily_sales_3m_mtd"],
        sales_totals["daily_sales_30d"],
        use_peak_mode,
    )

    sales_supplier = (
        sales_df.groupby(["store", "brand", "product", "barcode"])["supplier_card"]
        .agg(core_io.pick_first_non_empty)
        .reset_index(name="supplier_card")
    )
    sales_display_barcode = core_io.build_unambiguous_barcode_map(
        sales_df,
        ["store", "brand", "product", "barcode"],
        "display_barcode",
        "display_barcode",
    )
    sales_totals = sales_totals.merge(sales_supplier, on=["store", "brand", "product", "barcode"], how="left")
    sales_totals = sales_totals.merge(sales_display_barcode, on=["store", "brand", "product", "barcode"], how="left")
    sales_totals["barcode_key"] = sales_totals["barcode"].apply(core_io.normalize_barcode_value)

    inv_totals = inv_df.groupby(["store", "brand", "product", "barcode"], as_index=False)["inventory_qty"].sum()
    inv_totals["barcode_key"] = inv_totals["barcode"].apply(core_io.normalize_barcode_value)

    detail = inv_totals.copy()
    detail = detail.merge(
        sales_totals[["store", "barcode_key", "daily_sales_3m_mtd", "daily_sales_30d", "forecast_daily_sales", "display_barcode"]],
        on=["store", "barcode_key"],
        how="left",
    )

    fallback_sales = sales_totals.groupby(["store", "brand", "product"], as_index=False)[
        ["daily_sales_3m_mtd", "daily_sales_30d", "forecast_daily_sales"]
    ].sum()
    missing_mask = detail["forecast_daily_sales"].isna()
    if missing_mask.any():
        fallback = detail.loc[missing_mask].merge(
            fallback_sales,
            on=["store", "brand", "product"],
            how="left",
            suffixes=("", "_fallback"),
        )
        detail.loc[missing_mask, "daily_sales_3m_mtd"] = fallback["daily_sales_3m_mtd_fallback"].values
        detail.loc[missing_mask, "daily_sales_30d"] = fallback["daily_sales_30d_fallback"].values
        detail.loc[missing_mask, "forecast_daily_sales"] = fallback["forecast_daily_sales_fallback"].values

    sales_display_barcode_global = core_io.build_unambiguous_barcode_map(
        sales_df, ["barcode"], "display_barcode", "display_barcode_global"
    )
    detail = detail.merge(sales_display_barcode_global.rename(columns={"barcode": "barcode_key"}), on="barcode_key", how="left")
    detail["display_barcode"] = detail["display_barcode"].where(detail["display_barcode"].notna(), detail["display_barcode_global"])
    detail = detail.drop(columns=["display_barcode_global"])

    sales_display_product_global = core_io.build_unambiguous_barcode_map(
        sales_df,
        ["brand", "product"],
        "display_barcode",
        "display_barcode_brand_product_global",
    )
    detail = detail.merge(sales_display_product_global, on=["brand", "product"], how="left")
    detail["display_barcode"] = detail["display_barcode"].where(
        detail["display_barcode"].notna(), detail["display_barcode_brand_product_global"]
    )
    detail = detail.drop(columns=["display_barcode_brand_product_global"])

    detail["daily_sales_3m_mtd"] = detail["daily_sales_3m_mtd"].fillna(0)
    detail["daily_sales_30d"] = detail["daily_sales_30d"].fillna(0)
    detail["forecast_daily_sales"] = detail["forecast_daily_sales"].fillna(0)

    inv_supplier = (
        inv_df.groupby(["store", "brand", "product", "barcode"])["supplier_card"]
        .agg(core_io.pick_first_non_empty)
        .reset_index(name="supplier_card")
    )
    inv_supplier["barcode_key"] = inv_supplier["barcode"].apply(core_io.normalize_barcode_value)
    detail = detail.merge(inv_supplier[["store", "barcode_key", "supplier_card"]], on=["store", "barcode_key"], how="left")

    inv_supplier_fallback = (
        inv_supplier.groupby(["store", "brand", "product"])["supplier_card"]
        .agg(core_io.pick_first_non_empty)
        .reset_index(name="supplier_card")
    )
    mask = detail["supplier_card"].isna()
    if mask.any():
        fallback = detail.loc[mask].merge(inv_supplier_fallback, on=["store", "brand", "product"], how="left", suffixes=("", "_fallback"))
        detail.loc[mask, "supplier_card"] = fallback["supplier_card_fallback"].values

    sales_supplier_exact = sales_totals[["store", "barcode_key", "supplier_card"]].copy()
    mask = detail["supplier_card"].isna()
    if mask.any():
        fallback = detail.loc[mask].merge(sales_supplier_exact, on=["store", "barcode_key"], how="left", suffixes=("", "_fallback"))
        detail.loc[mask, "supplier_card"] = fallback["supplier_card_fallback"].values

    sales_supplier_fallback = (
        sales_totals.groupby(["store", "brand", "product"])["supplier_card"]
        .agg(core_io.pick_first_non_empty)
        .reset_index(name="supplier_card")
    )
    mask = detail["supplier_card"].isna()
    if mask.any():
        fallback = detail.loc[mask].merge(sales_supplier_fallback, on=["store", "brand", "product"], how="left", suffixes=("", "_fallback"))
        detail.loc[mask, "supplier_card"] = fallback["supplier_card_fallback"].values

    detail["province"] = detail["supplier_card"].apply(province_mapper)
    detail["barcode_output"] = detail["display_barcode"].where(detail["display_barcode"].notna(), detail["barcode"]) if is_wumei_system else detail["barcode"]

    detail = core_metrics.apply_inventory_metrics(detail, low_days, high_days)
    detail = detail[[
        "store", "brand", "barcode", "barcode_output", "product", "daily_sales_3m_mtd", "daily_sales_30d",
        "forecast_daily_sales", "inventory_qty", "supplier_card", "province", "risk_level", "inventory_sales_ratio",
        "turnover_rate", "turnover_days",
    ]].sort_values(["store", "brand", "product", "barcode"]).reset_index(drop=True)

    store_summary = detail.groupby("store", as_index=False).agg({
        "daily_sales_3m_mtd": "sum", "daily_sales_30d": "sum", "forecast_daily_sales": "sum", "inventory_qty": "sum"
    })
    store_summary = core_metrics.apply_inventory_metrics(store_summary, low_days, high_days)

    brand_summary = detail.groupby("brand", as_index=False).agg({
        "daily_sales_3m_mtd": "sum", "daily_sales_30d": "sum", "forecast_daily_sales": "sum", "inventory_qty": "sum"
    })
    brand_summary = core_metrics.apply_inventory_metrics(brand_summary, low_days, high_days)

    inv_barcode_keys_df = (
        inv_totals[["store", "barcode_key"]]
        .dropna(subset=["barcode_key"])
        .drop_duplicates()
        .assign(_has_inv_barcode=True)
    )
    inv_fallback_keys_df = (
        inv_totals[["store", "brand", "product"]]
        .drop_duplicates()
        .assign(_has_inv_fallback=True)
    )

    missing_sales = sales_totals.merge(
        inv_barcode_keys_df,
        on=["store", "barcode_key"],
        how="left",
    ).merge(
        inv_fallback_keys_df,
        on=["store", "brand", "product"],
        how="left",
    )
    missing_sales = missing_sales[
        missing_sales["_has_inv_barcode"].isna()
        & missing_sales["_has_inv_fallback"].isna()
        & (missing_sales["forecast_daily_sales"] > 0)
    ].copy()
    missing_sales = missing_sales.drop(columns=["_has_inv_barcode", "_has_inv_fallback"])
    missing_sales["province"] = missing_sales["supplier_card"].apply(province_mapper)

    if not missing_sales.empty:
        missing_detail = missing_sales.copy()
        missing_detail["inventory_qty"] = 0
        missing_detail["inventory_sales_ratio"] = float("inf")
        missing_detail["turnover_rate"] = 0
        missing_detail["turnover_days"] = float("inf")
        missing_detail["risk_level"] = "高"
        missing_detail["province"] = missing_detail["supplier_card"].apply(province_mapper)
        missing_detail["barcode_output"] = (
            missing_detail["display_barcode"].where(missing_detail["display_barcode"].notna(), missing_detail["barcode"])
            if is_wumei_system
            else missing_detail["barcode"]
        )
        missing_detail = missing_detail[[
            "store", "brand", "barcode", "barcode_output", "product", "daily_sales_3m_mtd", "daily_sales_30d",
            "forecast_daily_sales", "inventory_qty", "supplier_card", "province", "risk_level", "inventory_sales_ratio",
            "turnover_rate", "turnover_days",
        ]]
        detail = pd.concat([detail, missing_detail], ignore_index=True)

    detail = detail.sort_values(["store", "brand", "product", "barcode"]).reset_index(drop=True)
    return detail, missing_sales, store_summary, brand_summary
