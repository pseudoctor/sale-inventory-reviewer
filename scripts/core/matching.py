from __future__ import annotations

from typing import Callable

import pandas as pd

from . import io as core_io
from . import metrics as core_metrics


def _prepare_match_keys(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["store_key"] = out.get("store_code", pd.Series(index=out.index)).apply(core_io.normalize_barcode_value)
    out["product_key"] = out.get("product_code", pd.Series(index=out.index)).apply(core_io.normalize_barcode_value)
    out["barcode_key"] = out["barcode"].apply(core_io.normalize_barcode_value)
    out["store_key"] = out["store_key"].where(out["store_key"].notna(), out["store"])
    out["product_key"] = out["product_key"].where(out["product_key"].notna(), out["barcode_key"])
    return out


def _coalesce_store_column(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "store" in out.columns:
        return out
    for candidate in ["store_x", "store_y"]:
        if candidate in out.columns:
            out["store"] = out[candidate]
            break
    return out


def _build_sales_key_mapping(sales_df: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["store_key", "product_key"]
    sorted_sales = sales_df.copy()
    # Deterministic tie-break for same-day records:
    # 1) latest sales_date
    # 2) larger sales_qty
    # 3) lexical product/brand to keep output stable across runs
    sorted_sales["product_sort"] = sorted_sales["product"].fillna("").astype(str)
    sorted_sales["brand_sort"] = sorted_sales["brand"].fillna("").astype(str)
    sorted_sales = sorted_sales.sort_values(
        ["sales_date", "sales_qty", "product_sort", "brand_sort"],
        ascending=[True, True, True, True],
    )
    latest_rows = (
        sorted_sales
        .groupby(key_cols, as_index=False)
        .tail(1)[key_cols + ["product", "brand", "display_barcode", "sales_date"]]
        .rename(
            columns={
                "product": "display_product_name",
                "brand": "display_brand",
                "sales_date": "name_source_ts",
            }
        )
    )
    latest_rows["brand_source_ts"] = latest_rows["name_source_ts"]

    conflicts = (
        sorted_sales.groupby(key_cols, as_index=False)
        .agg(
            record_rows=("product", "size"),
            name_conflict_count=("product", "nunique"),
            brand_conflict_count=("brand", "nunique"),
        )
    )

    out = latest_rows.merge(conflicts, on=key_cols, how="left")
    for col in ["record_rows", "name_conflict_count", "brand_conflict_count"]:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0).astype(int)
    return out


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
    recent_days_natural: int = 30,
    has_mtd_window_data: bool,
    has_recent_window_data: bool,
    use_peak_mode: bool,
    low_days: float,
    high_days: float,
    is_wumei_system: bool,
    province_mapper: Callable[[str | None], str],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, float]]:
    sales_df = _prepare_match_keys(sales_df)
    sales_3m_mtd = sales_df[(sales_df["sales_date"] >= mtd_start) & (sales_df["sales_date"] <= mtd_end)]
    sales_30d = sales_df[(sales_df["sales_date"] >= recent_start) & (sales_df["sales_date"] <= inventory_date_ts)]

    sales_totals = (
        sales_df.groupby(["store_key", "product_key"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "sales_qty_total"})
    )
    sales_totals = sales_totals.merge(
        sales_3m_mtd.groupby(["store_key", "product_key"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "sales_qty_3m_mtd"}),
        on=["store_key", "product_key"],
        how="left",
    )
    sales_totals = sales_totals.merge(
        sales_30d.groupby(["store_key", "product_key"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "sales_qty_30d"}),
        on=["store_key", "product_key"],
        how="left",
    )
    sales_totals["sales_qty_3m_mtd"] = sales_totals["sales_qty_3m_mtd"].fillna(0)
    sales_totals["sales_qty_30d"] = sales_totals["sales_qty_30d"].fillna(0)
    sales_totals["daily_sales_3m_mtd"] = sales_totals["sales_qty_3m_mtd"] / mtd_days if has_mtd_window_data else 0
    sales_totals["daily_sales_30d"] = sales_totals["sales_qty_30d"] / recent_days_natural if has_recent_window_data else 0
    sales_totals["forecast_daily_sales"] = core_metrics.combine_daily_sales(
        sales_totals["daily_sales_3m_mtd"],
        sales_totals["daily_sales_30d"],
        use_peak_mode,
    )

    sales_mapping = _build_sales_key_mapping(sales_df)
    sales_supplier = (
        sales_df.groupby(["store_key", "product_key"])["supplier_card"]
        .agg(core_io.pick_first_non_empty)
        .reset_index(name="supplier_card")
    )
    sales_store = (
        sales_df.groupby(["store_key", "product_key"])["store"]
        .agg(core_io.pick_first_non_empty)
        .reset_index(name="store")
    )
    sales_totals = sales_totals.merge(sales_store, on=["store_key", "product_key"], how="left")
    sales_totals = sales_totals.merge(
        sales_mapping[
            [
                "store_key",
                "product_key",
                "display_product_name",
                "display_brand",
                "display_barcode",
                "name_source_ts",
                "brand_source_ts",
                "name_conflict_count",
                "brand_conflict_count",
                "record_rows",
            ]
        ],
        on=["store_key", "product_key"],
        how="left",
    )
    sales_totals = sales_totals.merge(sales_supplier, on=["store_key", "product_key"], how="left")

    inv_df = _prepare_match_keys(inv_df)
    inv_totals = (
        inv_df.groupby(["store_key", "product_key"], as_index=False)
        .agg(
            store=("store", core_io.pick_first_non_empty),
            inventory_qty=("inventory_qty", "sum"),
            inventory_brand=("brand", core_io.pick_first_non_empty),
            inventory_product=("product", core_io.pick_first_non_empty),
            inventory_barcode=("barcode", core_io.pick_first_non_empty),
            inventory_supplier_card=("supplier_card", core_io.pick_first_non_empty),
        )
    )

    detail = inv_totals.copy()
    detail = detail.merge(
        sales_totals[
            [
                "store_key",
                "product_key",
                "daily_sales_3m_mtd",
                "daily_sales_30d",
                "forecast_daily_sales",
                "display_product_name",
                "display_brand",
                "display_barcode",
                "supplier_card",
                "name_source_ts",
                "brand_source_ts",
                "name_conflict_count",
                "brand_conflict_count",
            ]
        ],
        on=["store_key", "product_key"],
        how="left",
    )

    detail["daily_sales_3m_mtd"] = detail["daily_sales_3m_mtd"].fillna(0)
    detail["daily_sales_30d"] = detail["daily_sales_30d"].fillna(0)
    detail["forecast_daily_sales"] = detail["forecast_daily_sales"].fillna(0)
    detail["product"] = detail["display_product_name"].where(detail["display_product_name"].notna(), detail["inventory_product"])
    detail["brand"] = detail["display_brand"].where(detail["display_brand"].notna(), detail["inventory_brand"])
    detail["product"] = detail["product"].fillna("")
    detail["brand"] = detail["brand"].fillna("")
    detail.loc[detail["brand"].astype(str).str.strip() == "", "brand"] = "其他/未知"

    detail["name_source_rule"] = detail["display_product_name"].notna().map(
        {True: "latest_sales_name", False: "inventory_fallback"}
    )
    detail["brand_source_rule"] = detail["display_brand"].notna().map(
        {True: "latest_sales_brand", False: "inventory_fallback"}
    )
    detail["name_conflict_count"] = pd.to_numeric(detail["name_conflict_count"], errors="coerce").fillna(0).astype(int)
    detail["brand_conflict_count"] = pd.to_numeric(detail["brand_conflict_count"], errors="coerce").fillna(0).astype(int)

    mapping_coverage_rate = float(detail["display_product_name"].notna().mean()) if len(detail) > 0 else 1.0

    detail["supplier_card"] = detail["inventory_supplier_card"].where(
        detail["inventory_supplier_card"].notna(),
        detail["supplier_card"],
    )
    detail["province"] = detail["supplier_card"].apply(province_mapper)
    detail["barcode"] = detail["inventory_barcode"].where(detail["inventory_barcode"].notna(), detail["product_key"])
    detail["barcode_output"] = (
        detail["display_barcode"].where(detail["display_barcode"].notna(), detail["barcode"])
        if is_wumei_system
        else detail["barcode"]
    )
    detail = _coalesce_store_column(detail)

    detail = core_metrics.apply_inventory_metrics(detail, low_days, high_days)
    detail = detail[[
        "store", "brand", "barcode", "barcode_output", "product", "daily_sales_3m_mtd", "daily_sales_30d",
        "forecast_daily_sales", "inventory_qty", "supplier_card", "province", "risk_level", "inventory_sales_ratio",
        "turnover_rate", "turnover_days", "name_source_rule", "brand_source_rule",
        "name_conflict_count", "brand_conflict_count",
    ]].sort_values(["store", "brand", "product", "barcode"]).reset_index(drop=True)

    store_summary = detail.groupby("store", as_index=False).agg({
        "daily_sales_3m_mtd": "sum", "daily_sales_30d": "sum", "forecast_daily_sales": "sum", "inventory_qty": "sum"
    })
    store_summary = core_metrics.apply_inventory_metrics(store_summary, low_days, high_days)

    brand_summary = detail.groupby("brand", as_index=False).agg({
        "daily_sales_3m_mtd": "sum", "daily_sales_30d": "sum", "forecast_daily_sales": "sum", "inventory_qty": "sum"
    })
    brand_summary = core_metrics.apply_inventory_metrics(brand_summary, low_days, high_days)

    missing_sales = sales_totals.merge(
        inv_totals[["store_key", "product_key"]]
        .dropna(subset=["product_key"])
        .drop_duplicates()
        .assign(_has_inv_product=True),
        on=["store_key", "product_key"],
        how="left",
    )
    missing_sales = missing_sales[
        missing_sales["_has_inv_product"].isna() & (missing_sales["forecast_daily_sales"] > 0)
    ].copy()
    missing_sales = missing_sales.drop(columns=["_has_inv_product"])
    missing_sales["product"] = missing_sales["display_product_name"].fillna("")
    missing_sales["brand"] = missing_sales["display_brand"].fillna("")
    missing_sales.loc[missing_sales["brand"].astype(str).str.strip() == "", "brand"] = "其他/未知"
    missing_sales["barcode"] = missing_sales["product_key"]
    missing_sales["name_source_rule"] = "latest_sales_name"
    missing_sales["brand_source_rule"] = "latest_sales_brand"
    missing_sales["name_conflict_count"] = pd.to_numeric(missing_sales["name_conflict_count"], errors="coerce").fillna(0).astype(int)
    missing_sales["brand_conflict_count"] = pd.to_numeric(missing_sales["brand_conflict_count"], errors="coerce").fillna(0).astype(int)
    missing_sales["province"] = missing_sales["supplier_card"].apply(province_mapper)
    missing_sales = _coalesce_store_column(missing_sales)

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
            "turnover_rate", "turnover_days", "name_source_rule", "brand_source_rule",
            "name_conflict_count", "brand_conflict_count",
        ]]
        detail = pd.concat([detail, missing_detail], ignore_index=True)

    detail = detail.sort_values(["store", "brand", "product", "barcode"]).reset_index(drop=True)
    mapping_stats = {
        "duplicate_store_product_keys": int((sales_mapping["record_rows"] > 1).sum()),
        "name_conflict_keys": int((sales_mapping["name_conflict_count"] > 1).sum()),
        "brand_conflict_keys": int((sales_mapping["brand_conflict_count"] > 1).sum()),
        "mapping_coverage_rate": mapping_coverage_rate,
    }
    return detail, missing_sales, store_summary, brand_summary, mapping_stats
