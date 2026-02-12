from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

PREFERRED_WIDTHS = {
    "门店名称": 18,
    "品牌": 10,
    "商品条码": 16,
    "商品名称": 36,
    "省份": 10,
    "装箱数（因子）": 12,
    "近三月+本月迄今平均日销": 22,
    "近30天平均日销售": 16,
    "库存数量": 10,
    "风险等级": 8,
    "库存/销售比": 12,
    "库存周转率": 12,
    "库存周转天数": 12,
    "建议调出数量": 12,
    "建议补货数量": 12,
    "建议补货箱数": 12,
    "名称来源规则": 14,
    "品牌来源规则": 14,
    "同键名称数": 10,
    "同键品牌数": 10,
}

NUMBER_FORMATS = {
    "近三月+本月迄今平均日销": "0.000",
    "近30天平均日销售": "0.000",
    "预测平均日销(季节模式后)": "0.000",
    "库存数量": "0",
    "装箱数（因子）": "0",
    "库存/销售比": "0.0",
    "库存周转率": "0.0%",
    "库存周转天数": "0",
    "建议调出数量": "0",
    "建议补货数量": "0",
    "同键名称数": "0",
    "同键品牌数": "0",
}

SUMMARY_ONE_DECIMAL_METRICS = {
    "近三月+本月迄今平均日销总量",
    "近30天平均日销售总量",
    "预测平均日销总量(季节模式后)",
}


def write_report_with_style(
    output_file: Path,
    display_name: str,
    inventory_date: str,
    sheets: Dict[str, pd.DataFrame],
) -> None:
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

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
        ws.insert_rows(1)
        title = f"{display_name} | 库存日期：{inventory_date}"
        ws.cell(row=1, column=1, value=title)
        ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=ws.max_column)
        ws.cell(row=1, column=1).font = Font(bold=True)
        ws.cell(row=1, column=1).alignment = center

        ws.freeze_panes = "E3" if sheet_name == "明细" else "A3"
        last_col = ws.max_column
        ws.auto_filter.ref = f"A2:{ws.cell(row=2, column=last_col).column_letter}{ws.max_row}"

        headers = [c.value for c in ws[2]]
        for cell in ws[2]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = center

        for col in ws.columns:
            max_len = 0
            col_letter = ws.cell(row=2, column=col[0].column).column_letter
            header = ws.cell(row=2, column=col[0].column).value
            for cell in col:
                if cell.value is None:
                    continue
                max_len = max(max_len, len(str(cell.value)))
            auto_width = max_len + 2
            if header in PREFERRED_WIDTHS:
                ws.column_dimensions[col_letter].width = max(PREFERRED_WIDTHS[header], min(auto_width, 40))
            else:
                ws.column_dimensions[col_letter].width = min(auto_width, 40)

        header_to_idx = {name: idx + 1 for idx, name in enumerate(headers)}
        wrap_columns = {"门店名称", "商品名称"}
        for row in ws.iter_rows(min_row=3, max_row=ws.max_row):
            for col_idx, cell in enumerate(row, start=1):
                header = headers[col_idx - 1]
                if header in wrap_columns:
                    cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
                elif isinstance(cell.value, (int, float)):
                    cell.alignment = center
                else:
                    cell.alignment = left
                if header in NUMBER_FORMATS:
                    cell.number_format = NUMBER_FORMATS[header]

            risk_idx = header_to_idx.get("风险等级")
            if risk_idx is not None:
                risk_cell = row[risk_idx - 1]
                risk_fill = risk_fills.get(risk_cell.value)
                if risk_fill:
                    risk_cell.fill = risk_fill
                    risk_cell.font = Font(color="FFFFFF", bold=True)
                    risk_cell.alignment = center

            barcode_idx = header_to_idx.get("商品条码")
            if barcode_idx is not None:
                row[barcode_idx - 1].number_format = "@"

            avg_idx = header_to_idx.get("近三月+本月迄今平均日销")
            inv_idx = header_to_idx.get("库存数量")
            if avg_idx is not None and inv_idx is not None:
                avg_val = row[avg_idx - 1].value or 0
                inv_val = row[inv_idx - 1].value or 0
                if avg_val > 0 and inv_val == 0:
                    for cell in row:
                        cell.fill = out_of_stock_fill
                        cell.font = Font(color="FFFFFF", bold=True)

        if sheet_name == "汇总" and "指标" in header_to_idx and "数值" in header_to_idx:
            metric_idx = header_to_idx["指标"] - 1
            value_idx = header_to_idx["数值"] - 1
            for row in ws.iter_rows(min_row=3, max_row=ws.max_row):
                metric_name = row[metric_idx].value
                if metric_name in SUMMARY_ONE_DECIMAL_METRICS and isinstance(row[value_idx].value, (int, float)):
                    row[value_idx].number_format = "0.0"

    for sheet_name in sheets.keys():
        style_sheet(wb[sheet_name], sheet_name)

    ws_detail = wb["明细"] if "明细" in wb.sheetnames else None
    if ws_detail is not None:
        headers = [c.value for c in ws_detail[2]]
        if "门店名称" in headers:
            store_idx = headers.index("门店名称") + 1
            start_row = 3
            current = ws_detail.cell(row=3, column=store_idx).value
            for row in range(4, ws_detail.max_row + 2):
                value = ws_detail.cell(row=row, column=store_idx).value if row <= ws_detail.max_row else None
                if value != current:
                    end_row = row - 1
                    if end_row > start_row:
                        ws_detail.merge_cells(start_row=start_row, start_column=store_idx, end_row=end_row, end_column=store_idx)
                        ws_detail.cell(row=start_row, column=store_idx).alignment = center
                    start_row = row
                    current = value

    thin = Side(style="thin", color="000000")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for ws in wb.worksheets:
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column):
            for cell in row:
                cell.border = border

    wb.save(output_file)
