from __future__ import annotations

import math
from pathlib import Path
from typing import Dict

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

PREFERRED_WIDTHS = {
    "排名": 8,
    "分组": 12,
    "指标": 34,
    "数值": 26,
    "模块": 16,
    "说明": 72,
    "使用建议": 56,
    "门店名称": 18,
    "门店销售额总计": 18,
    "品牌": 10,
    "商品条码": 16,
    "商品名称": 36,
    "商品销售额": 16,
    "商品单价": 12,
    "调货数量": 12,
    "库存金额": 16,
    "省份": 10,
    "装箱数（因子）": 12,
    "近三月+本月迄今平均日销": 22,
    "近30天平均日销售": 16,
    "库存数量": 10,
    "风险等级": 8,
    "库存/销售比": 9,
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
    "门店销售额总计": "0.0",
    "商品销售额": "0.0",
    "商品单价": "0.0",
    "调货数量": "0",
    "库存金额": "0.0",
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


def _set_sheet_title(ws, display_name: str, inventory_date: str, center: Alignment) -> None:
    """为每个工作表写入统一标题行。"""
    ws.insert_rows(1)
    title = f"{display_name} | 库存日期：{inventory_date}"
    ws.cell(row=1, column=1, value=title)
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=ws.max_column)
    ws.row_dimensions[1].height = 26
    ws.cell(row=1, column=1).font = Font(bold=True, size=14, color="FFFFFF")
    ws.cell(row=1, column=1).fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    ws.cell(row=1, column=1).alignment = center


def _resolve_preferred_width(header: object) -> float | None:
    """按固定列名或前缀规则解析列宽。"""
    header_text = str(header)
    if header_text in PREFERRED_WIDTHS:
        return PREFERRED_WIDTHS[header_text]
    if header_text.startswith("门店销售额总计("):
        return PREFERRED_WIDTHS["门店销售额总计"]
    if header_text.startswith("商品销售额("):
        return PREFERRED_WIDTHS["商品销售额"]
    return None


def _resolve_number_format(header: object) -> str | None:
    """按固定列名或前缀规则解析数字格式。"""
    header_text = str(header)
    if header_text in NUMBER_FORMATS:
        return NUMBER_FORMATS[header_text]
    if header_text.startswith("门店销售额总计("):
        return NUMBER_FORMATS["门店销售额总计"]
    if header_text.startswith("商品销售额("):
        return NUMBER_FORMATS["商品销售额"]
    return None


def _set_freeze_and_filter(ws, sheet_name: str) -> None:
    """设置冻结窗格与筛选范围。"""
    if sheet_name == "使用说明":
        ws.freeze_panes = "A3"
    else:
        ws.freeze_panes = "E3" if sheet_name == "明细" else "A3"
    last_col = ws.max_column
    ws.auto_filter.ref = f"A2:{ws.cell(row=2, column=last_col).column_letter}{ws.max_row}"


def _style_headers(ws, header_fill: PatternFill, header_font: Font, center: Alignment) -> list:
    """应用表头样式并返回表头列表。"""
    headers = [c.value for c in ws[2]]
    ws.row_dimensions[2].height = 22
    for cell in ws[2]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = center
    return headers


def _apply_column_widths(ws) -> None:
    """根据预设宽度和内容长度调整列宽。"""
    for col in ws.columns:
        max_len = 0
        col_letter = ws.cell(row=2, column=col[0].column).column_letter
        header = ws.cell(row=2, column=col[0].column).value
        for cell in col:
            if cell.value is None:
                continue
            max_len = max(max_len, len(str(cell.value)))
        auto_width = max_len + 2
        preferred_width = _resolve_preferred_width(header)
        if preferred_width is not None:
            ws.column_dimensions[col_letter].width = max(preferred_width, min(auto_width, 40))
        else:
            ws.column_dimensions[col_letter].width = min(auto_width, 40)


def _estimate_row_height(ws, row_idx: int, wrap_columns: set[str], min_height: int = 20) -> float:
    """估算自动换行后的行高。"""
    max_lines = 1
    is_usage_guide = ws.title == "使用说明"
    for col_idx in range(1, ws.max_column + 1):
        header = ws.cell(row=2, column=col_idx).value
        if header not in wrap_columns:
            continue
        value = ws.cell(row=row_idx, column=col_idx).value
        if value in (None, ""):
            continue
        col_letter = ws.cell(row=2, column=col_idx).column_letter
        width = ws.column_dimensions[col_letter].width or PREFERRED_WIDTHS.get(str(header), 16)
        chars_per_line = max(int(width * (0.75 if is_usage_guide else 1.1)), 8 if is_usage_guide else 12)
        line_count = 0
        for paragraph in str(value).splitlines() or [""]:
            line_count += max(1, math.ceil(len(paragraph) / chars_per_line))
        max_lines = max(max_lines, line_count)
    line_height = 24 if is_usage_guide else 18
    padding = 8 if is_usage_guide else 0
    base_height = 34 if is_usage_guide else min_height
    return max(base_height, line_height * max_lines + padding)


def _style_data_rows(
    ws,
    headers: list,
    left: Alignment,
    center: Alignment,
    risk_fills: Dict[str, PatternFill],
    out_of_stock_fill: PatternFill,
    band_fill: PatternFill,
    transfer_qty_fill: PatternFill,
) -> Dict[str, int]:
    """统一处理数据行样式、数字格式和条件高亮。"""
    header_to_idx = {name: idx + 1 for idx, name in enumerate(headers)}
    wrap_columns = {"门店名称", "商品名称", "说明", "使用建议", "指标", "数值"}
    for row in ws.iter_rows(min_row=3, max_row=ws.max_row):
        for col_idx, cell in enumerate(row, start=1):
            header = headers[col_idx - 1]
            if header in wrap_columns:
                cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
            elif isinstance(cell.value, (int, float)):
                cell.alignment = center
            else:
                cell.alignment = left
            resolved_number_format = _resolve_number_format(header)
            if resolved_number_format is not None:
                cell.number_format = resolved_number_format
            if ws.title != "使用说明" and row[0].row % 2 == 1:
                cell.fill = band_fill

        risk_idx = header_to_idx.get("风险等级")
        if risk_idx is not None and ws.title != "使用说明":
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
        if avg_idx is not None and inv_idx is not None and ws.title != "使用说明":
            avg_val = row[avg_idx - 1].value or 0
            inv_val = row[inv_idx - 1].value or 0
            if avg_val > 0 and inv_val == 0:
                inventory_cell = row[inv_idx - 1]
                inventory_cell.fill = out_of_stock_fill
                inventory_cell.font = Font(color="FFFFFF", bold=True)
                out_flag_idx = header_to_idx.get("缺货")
                if out_flag_idx is not None:
                    row[out_flag_idx - 1].fill = out_of_stock_fill
                    row[out_flag_idx - 1].font = Font(color="FFFFFF", bold=True)
                replenish_idx = header_to_idx.get("建议补货数量")
                if replenish_idx is not None:
                    row[replenish_idx - 1].fill = PatternFill(
                        start_color="FDE68A",
                        end_color="FDE68A",
                        fill_type="solid",
                    )
                    row[replenish_idx - 1].font = Font(color="9A3412", bold=True)

        transfer_qty_idx = header_to_idx.get("调货数量")
        if transfer_qty_idx is not None and ws.title == "门店销量排名调货汇总":
            transfer_qty_cell = row[transfer_qty_idx - 1]
            transfer_qty_value = pd.to_numeric(transfer_qty_cell.value, errors="coerce")
            if pd.notna(transfer_qty_value) and float(transfer_qty_value) > 0:
                transfer_qty_cell.fill = transfer_qty_fill
                transfer_qty_cell.font = Font(color="9A3412", bold=True)
        ws.row_dimensions[row[0].row].height = _estimate_row_height(ws, row[0].row, wrap_columns)
    return header_to_idx


def _apply_overview_metric_format(ws, header_to_idx: Dict[str, int]) -> None:
    if "指标" not in header_to_idx or "数值" not in header_to_idx:
        return
    group_idx = header_to_idx.get("分组")
    metric_idx = header_to_idx["指标"] - 1
    value_idx = header_to_idx["数值"] - 1
    for row in ws.iter_rows(min_row=3, max_row=ws.max_row):
        if group_idx is not None and row[group_idx - 1].value != "核心指标":
            continue
        metric_name = row[metric_idx].value
        if metric_name in SUMMARY_ONE_DECIMAL_METRICS and isinstance(row[value_idx].value, (int, float)):
            row[value_idx].number_format = "0.0"


def _merge_detail_store_cells(ws_detail, center: Alignment) -> None:
    headers = [c.value for c in ws_detail[2]]
    if "门店名称" not in headers:
        return
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


def _apply_borders(wb) -> None:
    thin = Side(style="thin", color="D6E4F0")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for ws in wb.worksheets:
        for row in ws.iter_rows(min_row=1, max_row=ws.max_row, max_col=ws.max_column):
            for cell in row:
                cell.border = border


def write_report_with_style(
    output_file: Path,
    display_name: str,
    inventory_date: str,
    sheets: Dict[str, pd.DataFrame],
    merge_detail_store_cells: bool = True,
) -> None:
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    wb = load_workbook(output_file)
    header_fill = PatternFill(start_color="EAF1F8", end_color="EAF1F8", fill_type="solid")
    header_font = Font(bold=True, color="1F3556")
    center = Alignment(horizontal="center", vertical="center")
    left = Alignment(horizontal="left", vertical="center")
    risk_fills = {
        "高": PatternFill(start_color="D92D20", end_color="D92D20", fill_type="solid"),
        "中": PatternFill(start_color="F79009", end_color="F79009", fill_type="solid"),
        "低": PatternFill(start_color="10B981", end_color="10B981", fill_type="solid"),
    }
    out_of_stock_fill = PatternFill(start_color="EF4444", end_color="EF4444", fill_type="solid")
    band_fill = PatternFill(start_color="FAFCFE", end_color="FAFCFE", fill_type="solid")
    transfer_qty_fill = PatternFill(start_color="FED7AA", end_color="FED7AA", fill_type="solid")

    for sheet_name in sheets.keys():
        ws = wb[sheet_name]
        ws.sheet_view.showGridLines = False
        ws.sheet_properties.tabColor = "1F4E79" if sheet_name == "运行总览" else "7B94B0"
        _set_sheet_title(ws, display_name, inventory_date, center)
        _set_freeze_and_filter(ws, sheet_name)
        headers = _style_headers(ws, header_fill, header_font, center)
        _apply_column_widths(ws)
        header_to_idx = _style_data_rows(
            ws,
            headers,
            left,
            center,
            risk_fills,
            out_of_stock_fill,
            band_fill,
            transfer_qty_fill,
        )
        if sheet_name == "运行总览":
            _apply_overview_metric_format(ws, header_to_idx)

    ws_detail = wb["明细"] if "明细" in wb.sheetnames else None
    if ws_detail is not None and merge_detail_store_cells:
        _merge_detail_store_cells(ws_detail, center)

    _apply_borders(wb)
    wb.save(output_file)
