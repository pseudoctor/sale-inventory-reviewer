from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from . import config as core_config
from . import io as core_io
from . import matching as core_matching
from . import metrics as core_metrics
from . import output_tables as core_output_tables
from . import recommendations as core_recommendations
from . import report_writer as core_report_writer

SUPPLIER_CARD_PROVINCE_MAP = {
    "153085": "宁夏",
    "680249": "甘肃",
    "153412": "宁夏",
    "152901": "监狱系统",
}

DEFAULT_BRAND_KEYWORDS = [
    "伊利",
    "蒙牛",
    "南国",
    "阿宝乐",
    "畅鲜选",
    "蜀珑珠",
    "永璞",
    "雅士利",
    "大漠银根",
    "谷栗村",
    "君乐宝",
    "秦俑",
    "红色拖拉机",
    "飞鹤",
    "雀巢",
    "Seesaw",
    "佳贝艾特",
]

_CARTON_FACTOR_CACHE: Dict[Path, pd.DataFrame] = {}


def compute_config_snapshot(config: Dict[str, Any]) -> str:
    keys = [
        "risk_days_high",
        "risk_days_low",
        "sales_window_full_months",
        "sales_window_include_mtd",
        "sales_window_recent_days",
        "season_mode",
        "strict_auto_scan",
        "sales_date_dayfirst",
        "sales_date_format",
    ]
    payload = "|".join(f"{k}={config.get(k)}" for k in keys)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def stage_error(stage: str, exc: Exception) -> RuntimeError:
    return RuntimeError(f"[{stage}] {exc}")


def map_province_by_supplier_card(card: Optional[str]) -> str:
    normalized = core_io.normalize_supplier_card_value(card)
    return core_recommendations.map_province_by_supplier_card(normalized, SUPPLIER_CARD_PROVINCE_MAP)


def _parse_season_mode(season_mode_raw: Any) -> bool:
    if isinstance(season_mode_raw, bool):
        return season_mode_raw
    mode_text = str(season_mode_raw).strip().lower()
    if mode_text in {"true", "peak"}:
        return True
    if mode_text in {"false", "off_peak"}:
        return False
    raise ValueError("season_mode must be true/false (or legacy peak/off_peak)")


def _effective_brand_keywords(config: Dict[str, Any]) -> List[str]:
    brand_keywords = [str(b).strip() for b in (config.get("brand_keywords") or []) if str(b).strip()]
    return brand_keywords or DEFAULT_BRAND_KEYWORDS


def _load_carton_factor_cached(path: Path) -> pd.DataFrame:
    cached = _CARTON_FACTOR_CACHE.get(path)
    if cached is None:
        cached = core_io.load_carton_factor_df(path)
        _CARTON_FACTOR_CACHE[path] = cached
    return cached


def _load_sales_data(
    sales_candidates: List[Path],
    brand_keywords: List[str],
    sales_date_format: str,
    sales_date_dayfirst: bool,
) -> tuple[pd.DataFrame, int, List[str], int]:
    sales_data: List[pd.DataFrame] = []
    loaded_sales_file_count = 0
    missing_sales_files: List[str] = []
    invalid_sales_date_rows = 0

    for filepath in sales_candidates:
        if not filepath.exists():
            print(f"Warning: missing sales file {filepath}")
            missing_sales_files.append(filepath.name)
            continue
        df = core_io.read_excel_first_sheet(filepath)
        df, store_col, brand_col, product_col, barcode_col, qty_col, date_col, supplier_card_col = core_io.normalize_sales_df(df)

        national_barcode_col = core_io.find_column(df.columns.tolist(), ["国条码"])
        sales_columns = [store_col, product_col, barcode_col, qty_col, date_col]
        normalized_sales_columns = ["store", "product", "barcode", "sales_qty", "sales_date"]
        if brand_col is not None:
            sales_columns.insert(1, brand_col)
            normalized_sales_columns.insert(1, "brand")
        if national_barcode_col is not None and national_barcode_col != barcode_col:
            sales_columns.append(national_barcode_col)
            normalized_sales_columns.append("national_barcode")
        if supplier_card_col is not None:
            sales_columns.append(supplier_card_col)
            normalized_sales_columns.append("supplier_card")

        df = df[sales_columns].copy()
        df.columns = normalized_sales_columns
        if "brand" not in df.columns:
            df["brand"] = None
        df["barcode"] = df["barcode"].apply(core_io.normalize_barcode_value)
        if "national_barcode" in df.columns:
            df["national_barcode"] = df["national_barcode"].apply(core_io.normalize_barcode_value)
        else:
            df["national_barcode"] = None
        df["display_barcode"] = df["national_barcode"].where(df["national_barcode"].notna(), df["barcode"])
        if "supplier_card" not in df.columns:
            df["supplier_card"] = None
        df["supplier_card"] = df["supplier_card"].apply(core_io.normalize_supplier_card_value)

        brand_series = df["brand"].apply(
            lambda v: None if v is None or str(v).strip() == "" or str(v).strip().lower() in {"nan", "none"} else str(v).strip()
        )
        df["brand"] = brand_series.fillna(df["product"].apply(lambda v: core_io.extract_brand_from_product(v, brand_keywords)))

        parsed_dates = core_io.parse_sales_dates(df["sales_date"], date_format=sales_date_format, dayfirst=sales_date_dayfirst)
        invalid_sales_date_rows += int(parsed_dates.isna().sum())
        df["sales_date"] = parsed_dates
        df = df[df["sales_date"].notna()].copy()

        sales_data.append(df)
        loaded_sales_file_count += 1

    if not sales_data:
        raise RuntimeError("[input_read] No sales files were loaded.")

    sales_df = pd.concat(sales_data, ignore_index=True)
    return sales_df, loaded_sales_file_count, missing_sales_files, invalid_sales_date_rows


def _compute_window_context(
    sales_df: pd.DataFrame,
    inventory_date_ts: pd.Timestamp,
    full_months: int,
    include_mtd: bool,
    recent_days: int,
    fail_on_empty_window: bool,
) -> Dict[str, Any]:
    mtd_end = inventory_date_ts if include_mtd else (inventory_date_ts.replace(day=1) - pd.Timedelta(days=1))
    mtd_start = (inventory_date_ts.replace(day=1) - pd.DateOffset(months=full_months)).normalize()
    recent_start = (inventory_date_ts - pd.Timedelta(days=recent_days - 1)).normalize()

    data_min_date = sales_df["sales_date"].min().normalize()
    data_max_date = sales_df["sales_date"].max().normalize()
    mtd_days = core_metrics.overlap_days(mtd_start, mtd_end, data_min_date, data_max_date)
    recent_days_effective = core_metrics.overlap_days(recent_start, inventory_date_ts, data_min_date, data_max_date)
    has_mtd_window_data = mtd_days > 0
    has_recent_window_data = recent_days_effective > 0

    if fail_on_empty_window and (not has_mtd_window_data or not has_recent_window_data):
        raise RuntimeError(
            "[metrics] Sales window has no overlapping data: "
            f"3M+MTD={has_mtd_window_data}, 30D={has_recent_window_data}"
        )

    return {
        "mtd_start": mtd_start,
        "mtd_end": mtd_end,
        "recent_start": recent_start,
        "mtd_days": mtd_days,
        "recent_days_effective": recent_days_effective,
        "has_mtd_window_data": has_mtd_window_data,
        "has_recent_window_data": has_recent_window_data,
    }


def generate_report_for_system(
    system_cfg: Dict[str, Any],
    global_cfg: Optional[Dict[str, Any]] = None,
    *,
    base_dir: Path,
    program_version: str,
) -> Dict[str, Any]:
    config = dict(system_cfg)
    _ = global_cfg
    system_id = str(config.get("system_id", "single")).strip() or "single"
    display_name = str(config.get("display_name", system_id)).strip() or system_id
    is_wumei_system = "物美" in display_name
    province_column_enabled_cfg = config.get("province_column_enabled", None)
    enable_province_column = bool(province_column_enabled_cfg) if isinstance(province_column_enabled_cfg, bool) else is_wumei_system

    try:
        raw_data_dir = core_config.resolve_system_raw_data_dir(config, base_dir)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("config", exc) from exc

    configured_sales_files = [f for f in config.get("sales_files") or []]
    inventory_file = config.get("inventory_file") or ""
    carton_factor_file = config.get("carton_factor_file") or ""
    strict_auto_scan = bool(config.get("strict_auto_scan", False))
    brand_keywords = _effective_brand_keywords(config)

    try:
        sales_candidates = core_io.resolve_sales_candidates(raw_data_dir, configured_sales_files)
        ignored_sales_files = core_io.list_ignored_sales_files(raw_data_dir, configured_sales_files)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("input_read", exc) from exc

    if not configured_sales_files and not sales_candidates:
        ignored_text = core_io.format_ignored_sales_files(ignored_sales_files)
        detail = f" Ignored files: {ignored_text}" if ignored_text else ""
        if strict_auto_scan and ignored_sales_files:
            raise RuntimeError(
                "[input_read] strict_auto_scan=true and no valid sales files were detected."
                f" Ignored candidates: {ignored_text}"
            )
        raise FileNotFoundError(
            f"No auto-detected sales files in {raw_data_dir}. "
            "Expected filename with sales keyword and YYYYMM (e.g. 销售202602.xlsx)." + detail
        )

    inv_path = raw_data_dir / inventory_file
    inventory_file_exists = inv_path.exists()
    if not inv_path.exists():
        raise FileNotFoundError(f"Inventory file not found: {inv_path}")

    try:
        inv_df = core_io.read_excel_first_sheet(inv_path)
        inv_df = core_io.ensure_inventory_brand_column(inv_df, brand_keywords)
        inventory_date_text = core_io.extract_inventory_date(inv_df)
        parsed_inventory_date = pd.to_datetime(inventory_date_text, errors="coerce")
        if pd.isna(parsed_inventory_date):
            raise ValueError(f"Invalid inventory date: {inventory_date_text}")
        inventory_date_ts = pd.Timestamp(parsed_inventory_date).normalize()
        inventory_date = inventory_date_ts.date().isoformat()
        output_file = core_config.resolve_output_file_path(config, display_name, inventory_date, base_dir)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        inv_df, inv_store, inv_brand, inv_product, inv_barcode, inv_qty, inv_supplier_card = core_io.normalize_inventory_df(inv_df)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("normalize", exc) from exc

    if inv_brand is None:
        inv_df = inv_df.copy()
        inv_df["品牌"] = inv_df[inv_product].apply(lambda v: core_io.extract_brand_from_product(v, brand_keywords))
        inv_brand = "品牌"

    inventory_columns = [inv_store, inv_brand, inv_product, inv_barcode, inv_qty]
    if inv_supplier_card is not None:
        inventory_columns.append(inv_supplier_card)
    inv_df = inv_df[inventory_columns].copy()
    inv_df.columns = ["store", "brand", "product", "barcode", "inventory_qty"] + (["supplier_card"] if inv_supplier_card is not None else [])
    inv_df["barcode"] = inv_df["barcode"].apply(core_io.normalize_barcode_value)
    if "supplier_card" not in inv_df.columns:
        inv_df["supplier_card"] = None
    inv_df["supplier_card"] = inv_df["supplier_card"].apply(core_io.normalize_supplier_card_value)

    full_months = int(config.get("sales_window_full_months", 3))
    include_mtd = bool(config.get("sales_window_include_mtd", True))
    recent_days = int(config.get("sales_window_recent_days", 30))
    sales_date_dayfirst = bool(config.get("sales_date_dayfirst", False))
    sales_date_format = str(config.get("sales_date_format", ""))
    use_peak_mode = _parse_season_mode(config.get("season_mode", False))
    fail_on_empty_window = bool(config.get("fail_on_empty_window", False))

    try:
        sales_df, loaded_sales_file_count, missing_sales_files, invalid_sales_date_rows = _load_sales_data(
            sales_candidates,
            brand_keywords,
            sales_date_format,
            sales_date_dayfirst,
        )
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, RuntimeError) and str(exc).startswith("[input_read]"):
            raise
        raise stage_error("normalize", exc) from exc

    carton_factor_path = Path(carton_factor_file)
    if not carton_factor_path.is_absolute():
        carton_factor_path = (base_dir / carton_factor_path).resolve()
    try:
        carton_factor_df = _load_carton_factor_cached(carton_factor_path)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("input_read", exc) from exc

    try:
        window_ctx = _compute_window_context(
            sales_df,
            inventory_date_ts,
            full_months,
            include_mtd,
            recent_days,
            fail_on_empty_window,
        )
    except Exception as exc:  # noqa: BLE001
        raise stage_error("metrics", exc) from exc

    mtd_start = window_ctx["mtd_start"]
    mtd_end = window_ctx["mtd_end"]
    recent_start = window_ctx["recent_start"]
    mtd_days = window_ctx["mtd_days"]
    recent_days_effective = window_ctx["recent_days_effective"]
    has_mtd_window_data = window_ctx["has_mtd_window_data"]
    has_recent_window_data = window_ctx["has_recent_window_data"]

    high_days = float(config.get("risk_days_high", 60))
    low_days = float(config.get("risk_days_low", 45))
    detail, missing_sales, store_summary, brand_summary = core_matching.build_detail_with_matching(
        sales_df=sales_df,
        inv_df=inv_df,
        mtd_start=mtd_start,
        mtd_end=mtd_end,
        recent_start=recent_start,
        inventory_date_ts=inventory_date_ts,
        mtd_days=mtd_days,
        recent_days_effective=recent_days_effective,
        has_mtd_window_data=has_mtd_window_data,
        has_recent_window_data=has_recent_window_data,
        use_peak_mode=use_peak_mode,
        low_days=low_days,
        high_days=high_days,
        is_wumei_system=is_wumei_system,
        province_mapper=map_province_by_supplier_card,
    )
    detail["out_of_stock"] = np.where((detail["forecast_daily_sales"] > 0) & (detail["inventory_qty"] == 0), "是", "否")

    detail["daily_demand"] = detail["forecast_daily_sales"]
    detail["low_target_qty"] = detail["daily_demand"] * low_days
    detail["high_keep_qty"] = detail["daily_demand"] * high_days
    detail["need_qty"] = np.where(
        detail["forecast_daily_sales"] > 0,
        np.ceil(np.maximum(0, detail["low_target_qty"] - detail["inventory_qty"])),
        0,
    )
    detail["suggest_outbound_qty"] = np.where(
        (detail["risk_level"] == "高") & (detail["forecast_daily_sales"] > 0),
        np.floor(np.maximum(0, detail["inventory_qty"] - detail["high_keep_qty"])),
        0,
    )
    detail["suggest_replenish_qty"] = np.where(
        (detail["risk_level"] == "低") | (detail["out_of_stock"] == "是"),
        detail["need_qty"],
        0,
    )
    detail["suggest_outbound_qty"] = detail["suggest_outbound_qty"].astype(int)
    detail["suggest_replenish_qty"] = detail["suggest_replenish_qty"].astype(int)

    frames = core_output_tables.build_report_frames(
        detail=detail,
        missing_sales=missing_sales,
        store_summary=store_summary,
        brand_summary=brand_summary,
        carton_factor_df=carton_factor_df,
        is_wumei_system=is_wumei_system,
        enable_province_column=enable_province_column,
        use_peak_mode=use_peak_mode,
    )
    detail_out = frames["明细"]
    missing_sku_out = frames["库存缺失SKU清单"]
    out_of_stock_out = frames["缺货清单"]
    replenish_out = frames["建议补货清单"]
    transfer_out = frames["建议调货清单"]

    summary_rows = [
        ["风险等级-高", int((detail_out["风险等级"] == "高").sum())],
        ["风险等级-中", int((detail_out["风险等级"] == "中").sum())],
        ["风险等级-低", int((detail_out["风险等级"] == "低").sum())],
        ["缺货SKU数(库存=0)", int(len(out_of_stock_out))],
        ["库存缺失SKU数(销售有库存无)", int(len(missing_sku_out))],
        ["库存总量", float(detail_out["库存数量"].sum())],
        ["近三月+本月迄今平均日销总量", round(float(detail_out["近三月+本月迄今平均日销"].sum()), 1)],
        ["近30天平均日销售总量", round(float(detail_out["近30天平均日销售"].sum()), 1)],
        ["预测平均日销总量(季节模式后)", round(float(detail["forecast_daily_sales"].sum()), 1)],
    ]
    summary_out = pd.DataFrame(summary_rows, columns=["指标", "数值"])

    status_rows = [
        ["程序版本", program_version],
        ["配置快照", compute_config_snapshot(config)],
        ["系统名称", display_name],
        ["系统标识", system_id],
        ["库存日期", inventory_date],
        ["输入销售文件数", loaded_sales_file_count],
        ["缺失销售文件数", len(missing_sales_files)],
        ["输入文件总数", loaded_sales_file_count + 1],
        ["季节模式", "旺季(取高值)" if use_peak_mode else "淡季(取低值)"],
        ["严格自动扫描", "是" if strict_auto_scan else "否"],
        ["3M+MTD窗口有效", "是" if has_mtd_window_data else "否"],
        ["30天窗口有效", "是" if has_recent_window_data else "否"],
        ["3M+MTD窗口有效天数", int(mtd_days)],
        ["30天窗口有效天数", int(recent_days_effective)],
        ["销售无效日期行数", invalid_sales_date_rows],
        ["建议补货清单缺失装箱因子行数", int((replenish_out["装箱数（因子）"].isna()).sum()) if "装箱数（因子）" in replenish_out.columns else 0],
        ["建议调货清单缺失装箱因子行数", int((transfer_out["装箱数（因子）"].isna()).sum()) if "装箱数（因子）" in transfer_out.columns else 0],
        ["窗口数据状态", "正常" if (has_mtd_window_data and has_recent_window_data) else f"警告: 3M+MTD有效={has_mtd_window_data}, 30天有效={has_recent_window_data}"],
        ["自动扫描忽略销售文件数", len(ignored_sales_files)],
    ]
    if ignored_sales_files:
        status_rows.append(["自动扫描忽略销售文件", core_io.format_ignored_sales_files(ignored_sales_files)])
    if missing_sales_files:
        status_rows.append(["缺失销售文件", " | ".join(missing_sales_files)])
    status_out = pd.DataFrame(status_rows, columns=["状态项", "值"])

    sheets = dict(frames)
    sheets["汇总"] = summary_out
    sheets["运行状态"] = status_out

    try:
        core_report_writer.write_report_with_style(
            output_file=output_file,
            display_name=display_name,
            inventory_date=inventory_date,
            sheets=sheets,
        )
    except Exception as exc:  # noqa: BLE001
        raise stage_error("write_report", exc) from exc

    print(f"[{display_name}] Report saved: {output_file}")
    return {
        "system_id": system_id,
        "display_name": display_name,
        "status": "SUCCESS",
        "message": "",
        "error_stage": "",
        "output_file": str(output_file),
        "input_files_count": int(loaded_sales_file_count + 1),
        "loaded_sales_files": int(loaded_sales_file_count),
        "missing_sales_files": int(len(missing_sales_files)),
        "inventory_file_exists": bool(inventory_file_exists),
        "detail_rows": int(len(detail_out)),
        "missing_sku_rows": int(len(missing_sku_out)),
    }
