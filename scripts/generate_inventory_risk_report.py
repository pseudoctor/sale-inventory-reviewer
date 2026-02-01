#!/usr/bin/env python3
"""Generate inventory risk Excel report from last two months of sales and inventory data."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re

import pandas as pd
import yaml
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill

BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"

DEFAULT_CONFIG = {
    "raw_data_dir": "./raw_data",
    "output_file": "./reports/inventory_risk_report.xlsx",
    "sales_files": [],
    "inventory_file": "",
    "risk_days_high": 60,
    "risk_days_low": 45,
}


def load_config() -> Dict:
    config = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
        config.update({k: loaded.get(k, v) for k, v in config.items()})
    return config


def parse_month_label(filename: str) -> str:
    match = re.search(r"(\d{4})(\d{2})", filename)
    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return Path(filename).stem


def extract_month_key(filename: str) -> Optional[int]:
    match = re.search(r"(\d{4})(\d{2})", filename)
    if not match:
        return None
    return int(match.group(1) + match.group(2))


def clean_barcode(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "" or text.lower() in {"nan", "none"}:
        return None
    return text


def find_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in columns:
            return c
    return None


def normalize_sales_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, str, str, str]:
    df = df.copy()
    df.columns = df.columns.str.strip()

    store_col = find_column(df.columns, ["门店名称", "门店", "store"])
    brand_col = find_column(df.columns, ["品牌", "brand"])
    product_col = find_column(df.columns, ["商品名称", "商品", "product"])
    barcode_col = find_column(df.columns, ["商品条码", "条码", "商品编码.1", "商品编码", "barcode"])
    qty_col = find_column(df.columns, ["销售数量", "数量", "sales_qty", "qty"])

    if not all([store_col, brand_col, product_col, barcode_col, qty_col]):
        missing = [
            name
            for name, col in [
                ("门店名称", store_col),
                ("品牌", brand_col),
                ("商品名称", product_col),
                ("商品条码", barcode_col),
                ("销售数量", qty_col),
            ]
            if col is None
        ]
        raise ValueError(f"Missing required sales columns: {', '.join(missing)}")

    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)

    return df, store_col, brand_col, product_col, barcode_col, qty_col


def normalize_inventory_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, str, str, str, str]:
    df = df.copy()
    df.columns = df.columns.str.strip()

    store_col = find_column(df.columns, ["门店名称", "门店", "store"])
    brand_col = find_column(df.columns, ["品牌", "brand"])
    product_col = find_column(df.columns, ["商品名称", "商品", "product"])
    barcode_col = find_column(df.columns, ["商品条码", "条码", "商品编码.1", "商品编码", "barcode"])
    qty_col = find_column(df.columns, ["数量", "库存数量", "inventory_qty", "qty"])

    if not all([store_col, brand_col, product_col, barcode_col, qty_col]):
        missing = [
            name
            for name, col in [
                ("门店名称", store_col),
                ("品牌", brand_col),
                ("商品名称", product_col),
                ("商品条码", barcode_col),
                ("数量", qty_col),
            ]
            if col is None
        ]
        raise ValueError(f"Missing required inventory columns: {', '.join(missing)}")

    df[qty_col] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0)

    return df, store_col, brand_col, product_col, barcode_col, qty_col


def main() -> None:
    config = load_config()

    raw_data_dir = Path(config["raw_data_dir"])
    if not raw_data_dir.is_absolute():
        raw_data_dir = (BASE_DIR / raw_data_dir).resolve()

    output_file = Path(config["output_file"])
    if not output_file.is_absolute():
        output_file = (BASE_DIR / output_file).resolve()
    output_file.parent.mkdir(parents=True, exist_ok=True)

    configured_sales_files = [f for f in config.get("sales_files") or []]
    inventory_file = config.get("inventory_file") or ""

    sales_data = []
    months_loaded: List[str] = []

    # Auto-detect sales files and pick latest two months
    if configured_sales_files:
        candidate_files = [raw_data_dir / f for f in configured_sales_files]
    else:
        candidate_files = list(raw_data_dir.glob("*.xlsx"))

    sales_candidates = []
    for path in candidate_files:
        month_key = extract_month_key(path.name)
        if month_key is None:
            continue
        sales_candidates.append((month_key, path))

    sales_candidates.sort(key=lambda x: x[0])
    recent_candidates = sales_candidates[-2:] if len(sales_candidates) >= 2 else sales_candidates

    for _, filepath in recent_candidates:
        filename = filepath.name
        filepath = raw_data_dir / filename
        if not filepath.exists():
            print(f"Warning: missing sales file {filepath}")
            continue
        df = pd.read_excel(filepath, sheet_name=0)
        df, store_col, brand_col, product_col, barcode_col, qty_col = normalize_sales_df(df)
        month_label = parse_month_label(filename)
        df = df[[store_col, brand_col, product_col, barcode_col, qty_col]].copy()
        df.columns = ["store", "brand", "product", "barcode", "sales_qty"]
        df["month"] = month_label
        sales_data.append(df)
        months_loaded.append(month_label)

    if not sales_data:
        raise FileNotFoundError("No sales files were loaded.")

    sales_df = pd.concat(sales_data, ignore_index=True)

    # Use last two months from sorted file detection
    available_months = [m for m in months_loaded if m]
    recent_months = available_months[-2:] if len(available_months) >= 2 else available_months
    if not recent_months:
        raise ValueError("No recent months available for calculation.")

    recent_sales = sales_df[sales_df["month"].isin(recent_months)]
    monthly_sales = (
        recent_sales.groupby(["month", "store", "brand", "product", "barcode"], as_index=False)["sales_qty"]
        .sum()
    )

    months_count = len(recent_months)
    sales_totals = (
        monthly_sales.groupby(["store", "brand", "product", "barcode"], as_index=False)["sales_qty"]
        .sum()
        .rename(columns={"sales_qty": "sales_qty_total"})
    )
    sales_totals["avg_sales_qty"] = sales_totals["sales_qty_total"] / months_count
    sales_totals["barcode_key"] = sales_totals["barcode"].apply(clean_barcode)

    # Inventory data
    inv_path = raw_data_dir / inventory_file
    if not inv_path.exists():
        raise FileNotFoundError(f"Inventory file not found: {inv_path}")

    inv_df = pd.read_excel(inv_path, sheet_name=0)
    inv_df, inv_store, inv_brand, inv_product, inv_barcode, inv_qty = normalize_inventory_df(inv_df)
    inv_df = inv_df[[inv_store, inv_brand, inv_product, inv_barcode, inv_qty]].copy()
    inv_df.columns = ["store", "brand", "product", "barcode", "inventory_qty"]
    inv_df["barcode"] = inv_df["barcode"].apply(clean_barcode)

    inv_totals = (
        inv_df.groupby(["store", "brand", "product", "barcode"], as_index=False)["inventory_qty"]
        .sum()
    )
    inv_totals["barcode_key"] = inv_totals["barcode"].apply(clean_barcode)

    # Match sales to inventory: barcode match first, fallback to store+brand+product
    sales_barcode = sales_totals[sales_totals["barcode_key"].notna()].copy()
    inv_barcode = inv_totals[inv_totals["barcode_key"].notna()].copy()

    detail = inv_totals.copy()
    detail = detail.merge(
        sales_barcode[["store", "barcode_key", "avg_sales_qty"]],
        on=["store", "barcode_key"],
        how="left",
    )

    fallback_sales = sales_totals.groupby(["store", "brand", "product"], as_index=False)["avg_sales_qty"].sum()
    missing_mask = detail["avg_sales_qty"].isna()
    if missing_mask.any():
        fallback = detail.loc[missing_mask].merge(
            fallback_sales,
            on=["store", "brand", "product"],
            how="left",
            suffixes=("", "_fallback"),
        )
        detail.loc[missing_mask, "avg_sales_qty"] = fallback["avg_sales_qty_fallback"].values

    detail["avg_sales_qty"] = detail["avg_sales_qty"].fillna(0)

    detail["inventory_sales_ratio"] = detail.apply(
        lambda row: row["inventory_qty"] / row["avg_sales_qty"] if row["avg_sales_qty"] > 0 else float("inf"),
        axis=1,
    )
    detail["turnover_rate"] = detail.apply(
        lambda row: row["avg_sales_qty"] / row["inventory_qty"] if row["inventory_qty"] > 0 else 0,
        axis=1,
    )
    detail["turnover_days"] = detail.apply(
        lambda row: round(30 / row["turnover_rate"]) if row["turnover_rate"] > 0 else float("inf"),
        axis=1,
    )

    high_days = float(config.get("risk_days_high", 60))
    low_days = float(config.get("risk_days_low", 45))

    def classify(days: float) -> str:
        if days > high_days:
            return "高"
        if days < low_days:
            return "低"
        return "中"

    detail["risk_level"] = detail["turnover_days"].apply(classify)
    
    detail = detail[[
        "store",
        "brand",
        "barcode",
        "product",
        "avg_sales_qty",
        "inventory_qty",
        "risk_level",
        "inventory_sales_ratio",
        "turnover_rate",
        "turnover_days",
    ]]

    detail = detail.sort_values(["store", "brand", "product", "barcode"]).reset_index(drop=True)

    # Store summary
    store_summary = detail.groupby("store", as_index=False).agg({
        "avg_sales_qty": "sum",
        "inventory_qty": "sum",
    })
    store_summary["inventory_sales_ratio"] = store_summary.apply(
        lambda row: row["inventory_qty"] / row["avg_sales_qty"] if row["avg_sales_qty"] > 0 else float("inf"),
        axis=1,
    )
    store_summary["turnover_rate"] = store_summary.apply(
        lambda row: row["avg_sales_qty"] / row["inventory_qty"] if row["inventory_qty"] > 0 else 0,
        axis=1,
    )
    store_summary["turnover_days"] = store_summary.apply(
        lambda row: round(30 / row["turnover_rate"]) if row["turnover_rate"] > 0 else float("inf"),
        axis=1,
    )
    store_summary["risk_level"] = store_summary["turnover_days"].apply(classify)

    # Brand summary
    brand_summary = detail.groupby("brand", as_index=False).agg({
        "avg_sales_qty": "sum",
        "inventory_qty": "sum",
    })
    brand_summary["inventory_sales_ratio"] = brand_summary.apply(
        lambda row: row["inventory_qty"] / row["avg_sales_qty"] if row["avg_sales_qty"] > 0 else float("inf"),
        axis=1,
    )
    brand_summary["turnover_rate"] = brand_summary.apply(
        lambda row: row["avg_sales_qty"] / row["inventory_qty"] if row["inventory_qty"] > 0 else 0,
        axis=1,
    )
    brand_summary["turnover_days"] = brand_summary.apply(
        lambda row: round(30 / row["turnover_rate"]) if row["turnover_rate"] > 0 else float("inf"),
        axis=1,
    )
    brand_summary["risk_level"] = brand_summary["turnover_days"].apply(classify)

    # Rename columns to Chinese for output
    detail_out = detail.rename(columns={
        "store": "门店名称",
        "brand": "品牌",
        "barcode": "商品条码",
        "product": "商品名称",
        "avg_sales_qty": "近两月月均销售数量",
        "inventory_qty": "库存数量",
        "risk_level": "风险等级",
        "inventory_sales_ratio": "库存/销售比",
        "turnover_rate": "库存周转率",
        "turnover_days": "库存周转天数",
    })
    detail_out["商品条码"] = detail_out["商品条码"].astype(str)

    store_summary_out = store_summary.rename(columns={
        "store": "门店名称",
        "avg_sales_qty": "近两月月均销售数量",
        "inventory_qty": "库存数量",
        "risk_level": "风险等级",
        "inventory_sales_ratio": "库存/销售比",
        "turnover_rate": "库存周转率",
        "turnover_days": "库存周转天数",
    })

    brand_summary_out = brand_summary.rename(columns={
        "brand": "品牌",
        "avg_sales_qty": "近两月月均销售数量",
        "inventory_qty": "库存数量",
        "risk_level": "风险等级",
        "inventory_sales_ratio": "库存/销售比",
        "turnover_rate": "库存周转率",
        "turnover_days": "库存周转天数",
    })

    # Missing-in-inventory list: sales with qty but no inventory match
    inv_barcode_keys = set(zip(inv_totals["store"], inv_totals["barcode_key"]))
    inv_fallback_keys = set(zip(inv_totals["store"], inv_totals["brand"], inv_totals["product"]))

    def is_missing(row) -> bool:
        if row["barcode_key"] is not None:
            if (row["store"], row["barcode_key"]) in inv_barcode_keys:
                return False
        if (row["store"], row["brand"], row["product"]) in inv_fallback_keys:
            return False
        return True

    missing_sales = sales_totals.copy()
    missing_sales["is_missing"] = missing_sales.apply(is_missing, axis=1)
    missing_sales = missing_sales[missing_sales["is_missing"] & (missing_sales["avg_sales_qty"] > 0)]

    # Append missing items into detail table
    if not missing_sales.empty:
        missing_detail = missing_sales.copy()
        missing_detail["inventory_qty"] = 0
        missing_detail["inventory_sales_ratio"] = float("inf")
        missing_detail["turnover_rate"] = 0
        missing_detail["turnover_days"] = float("inf")
        missing_detail["risk_level"] = "高"
        missing_detail = missing_detail[[
            "store",
            "brand",
            "barcode",
            "product",
            "avg_sales_qty",
            "inventory_qty",
                "risk_level",
            "inventory_sales_ratio",
            "turnover_rate",
            "turnover_days",
        ]]
        detail = pd.concat([detail, missing_detail], ignore_index=True)

    detail = detail.sort_values(["store", "brand", "product", "barcode"]).reset_index(drop=True)

    missing_out = missing_sales[[
        "store",
        "brand",
        "barcode",
        "product",
        "avg_sales_qty",
    ]].rename(columns={
        "store": "门店名称",
        "brand": "品牌",
        "barcode": "商品条码",
        "product": "商品名称",
        "avg_sales_qty": "近两月月均销售数量",
    })
    missing_out["商品条码"] = missing_out["商品条码"].astype(str)

    # Summary sheet
    summary_rows = [
        ["风险等级-高", int((detail_out["风险等级"] == "高").sum())],
        ["风险等级-中", int((detail_out["风险等级"] == "中").sum())],
        ["风险等级-低", int((detail_out["风险等级"] == "低").sum())],
        ["缺货/库存缺失SKU数", int(len(missing_out))],
        ["库存总量", float(detail_out["库存数量"].sum())],
        ["近两月月均销售总量", float(detail_out["近两月月均销售数量"].sum())],
    ]
    summary_out = pd.DataFrame(summary_rows, columns=["指标", "数值"])

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        detail_out.to_excel(writer, sheet_name="明细", index=False)
        store_summary_out.to_excel(writer, sheet_name="门店汇总", index=False)
        brand_summary_out.to_excel(writer, sheet_name="品牌汇总", index=False)
        missing_out.to_excel(writer, sheet_name="缺货清单", index=False)
        summary_out.to_excel(writer, sheet_name="汇总", index=False)

    # Apply styling and merging
    wb = load_workbook(output_file)

    header_fill = PatternFill(start_color="E8EAF6", end_color="E8EAF6", fill_type="solid")
    header_font = Font(bold=True)
    center = Alignment(horizontal="center", vertical="center")
    left = Alignment(horizontal="left", vertical="center")

    risk_fills = {
        "高": PatternFill(start_color="F44336", end_color="F44336", fill_type="solid"),
        "中": PatternFill(start_color="F59E0B", end_color="F59E0B", fill_type="solid"),
        "低": PatternFill(start_color="4CAF50", end_color="4CAF50", fill_type="solid"),
    }
    out_of_stock_fill = PatternFill(start_color="7C3AED", end_color="7C3AED", fill_type="solid")

    def style_sheet(ws, sheet_name: str):
        ws.freeze_panes = "C2" if sheet_name == "明细" else "A2"
        ws.auto_filter.ref = ws.dimensions
        for cell in ws[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center

        # Auto width based on cell content (with caps for readability)
        headers = [c.value for c in ws[1]]
        preferred_widths = {
            "门店名称": 18,
            "品牌": 10,
            "商品条码": 16,
            "商品名称": 36,
            "近两月月均销售数量": 18,
            "库存数量": 10,
            "风险等级": 8,
            "库存/销售比": 12,
            "库存周转率": 12,
            "库存周转天数": 12,
        }

        for col in ws.columns:
            max_len = 0
            col_letter = col[0].column_letter
            header = col[0].value
            for cell in col:
                if cell.value is None:
                    continue
                max_len = max(max_len, len(str(cell.value)))
            auto_width = max_len + 2
            if header in preferred_widths:
                ws.column_dimensions[col_letter].width = max(preferred_widths[header], min(auto_width, 40))
            else:
                ws.column_dimensions[col_letter].width = min(auto_width, 40)

        # Align text columns to left, numbers to center
        for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
            for cell in row:
                if isinstance(cell.value, (int, float)):
                    cell.alignment = center
                else:
                    cell.alignment = left

        # Wrap long text columns for readability
        wrap_columns = {"门店名称", "商品名称"}
        for col_idx, header in enumerate(headers, start=1):
            if header in wrap_columns:
                for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
                    row[col_idx - 1].alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)

        # Risk color
        headers = [c.value for c in ws[1]]
        if "风险等级" in headers:
            risk_idx = headers.index("风险等级") + 1
            for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
                cell = row[risk_idx - 1]
                fill = risk_fills.get(cell.value)
                if fill:
                    cell.fill = fill
                    cell.font = Font(color="FFFFFF", bold=True)
                    cell.alignment = center

        # Barcode column as text
        if "商品条码" in headers:
            barcode_idx = headers.index("商品条码") + 1
            for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
                row[barcode_idx - 1].number_format = "@"

        # Out-of-stock highlight (avg sales > 0, inventory == 0)
        if "近两月月均销售数量" in headers and "库存数量" in headers:
            avg_idx = headers.index("近两月月均销售数量") + 1
            inv_idx = headers.index("库存数量") + 1
            for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
                avg_val = row[avg_idx - 1].value or 0
                inv_val = row[inv_idx - 1].value or 0
                if avg_val > 0 and inv_val == 0:
                    for cell in row:
                        cell.fill = out_of_stock_fill
                        cell.font = Font(color="FFFFFF", bold=True)

        # Number format
        number_formats = {
            "近两月月均销售数量": "0.0",
            "库存数量": "0",
            "库存/销售比": "0.0",
            "库存周转率": "0.0%",
            "库存周转天数": "0",
        }
        for name, fmt in number_formats.items():
            if name in headers:
                idx = headers.index(name) + 1
                for row in ws.iter_rows(min_row=2, max_row=ws.max_row):
                    row[idx - 1].number_format = fmt

    for sheet in ["明细", "门店汇总", "品牌汇总", "缺货清单", "汇总"]:
        style_sheet(wb[sheet], sheet)

    # Merge same store in Detail sheet
    ws_detail = wb["明细"]
    headers = [c.value for c in ws_detail[1]]
    if "门店名称" in headers:
        store_idx = headers.index("门店名称") + 1
        start_row = 2
        current = ws_detail.cell(row=2, column=store_idx).value
        for row in range(3, ws_detail.max_row + 2):
            value = ws_detail.cell(row=row, column=store_idx).value if row <= ws_detail.max_row else None
            if value != current:
                end_row = row - 1
                if end_row > start_row:
                    ws_detail.merge_cells(start_row=start_row, start_column=store_idx, end_row=end_row, end_column=store_idx)
                    ws_detail.cell(row=start_row, column=store_idx).alignment = center
                start_row = row
                current = value

    # Apply borders after merges for all active cells
    from openpyxl.styles import Border, Side
    thin = Side(style="thin", color="000000")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for ws in wb.worksheets:
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column):
            for cell in row:
                cell.border = border

    wb.save(output_file)

    print(f"Report saved: {output_file}")


if __name__ == "__main__":
    main()
