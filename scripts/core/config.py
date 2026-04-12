from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import yaml

from .models import AppConfig, BatchSystemConfig

DEFAULT_LEGACY_OUTPUT_FILES = {
    "./reports/inventory_risk_report.xlsx",
    "reports/inventory_risk_report.xlsx",
    "inventory_risk_report.xlsx",
}
AUTO_OUTPUT_DATE_PLACEHOLDER = "{库存日期}"

DEFAULT_CONFIG: AppConfig = {
    "run_mode": "single",
    "display_name": "",
    "system_id": "",
    "raw_data_dir": "./raw_data",
    "output_file": "./reports/inventory_risk_report.xlsx",
    "sales_files": [],
    "inventory_file": "",
    "risk_days_high": 60,
    "risk_days_low": 45,
    "sales_window_full_months": 3,
    "sales_window_include_mtd": True,
    "sales_window_recent_days": 30,
    "sales_date_dayfirst": False,
    "sales_date_format": "",
    "season_mode": False,
    "fail_on_empty_window": False,
    "strict_auto_scan": False,
    "merge_detail_store_cells": True,
    "enable_ranked_store_transfer_summary": False,
    "stagnant_outbound_mode": "keep_safety_stock",
    "stagnant_min_keep_qty": 0,
    "carton_factor_file": "./data/sku装箱数.xlsx",
    "brand_keywords": [],
    "batch": {
        "continue_on_error": True,
        "summary_output_file": "./reports/batch_run_summary.xlsx",
        "systems": [],
    },
    "province_column_enabled": None,
}


def _validate_bool_field(config: AppConfig, key: str, default: bool, error_message: str) -> bool:
    """校验布尔配置项，非法时抛出明确错误。"""
    value = config.get(key, default)
    if not isinstance(value, bool):
        raise ValueError(error_message)
    return value


def _normalize_sales_file_entries(entries: list[str], field_name: str) -> list[str]:
    """限制销售文件列表只能使用 raw_data_dir 下的相对路径。"""
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in entries:
        value = raw.strip()
        path = Path(value)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError(
                f"{field_name} entries must stay within raw_data_dir using relative paths only (no absolute path or '..'): {raw}"
            )
        if value in seen:
            raise ValueError(f"{field_name} contains duplicated entry: {value}")
        seen.add(value)
        normalized.append(value)
    return normalized


def validate_config(config: AppConfig) -> AppConfig:
    """校验并规范化主配置，供单系统和批量模式共用。"""
    run_mode = str(config.get("run_mode", "single")).strip().lower()
    if run_mode not in {"single", "batch"}:
        raise ValueError("config.run_mode must be one of: single, batch.")
    config["run_mode"] = run_mode

    if not isinstance(config.get("raw_data_dir"), str) or not str(config["raw_data_dir"]).strip():
        raise ValueError("config.raw_data_dir must be a non-empty string.")
    display_name = config.get("display_name", "")
    if display_name is not None and not isinstance(display_name, str):
        raise ValueError("config.display_name must be a string.")
    system_id = config.get("system_id", "")
    if system_id is not None and not isinstance(system_id, str):
        raise ValueError("config.system_id must be a string.")
    output_file = config.get("output_file", "")
    if output_file is None:
        output_file = ""
    if not isinstance(output_file, str):
        raise ValueError("config.output_file must be a string.")
    config["output_file"] = output_file.strip()

    if run_mode == "single":
        if not isinstance(config.get("inventory_file"), str) or not str(config["inventory_file"]).strip():
            raise ValueError("config.inventory_file must be a non-empty string in single mode.")

    carton_factor_file = config.get("carton_factor_file", "")
    if not isinstance(carton_factor_file, str) or not carton_factor_file.strip():
        raise ValueError("config.carton_factor_file must be a non-empty string.")

    sales_files = config.get("sales_files")
    if not isinstance(sales_files, list):
        raise ValueError("config.sales_files must be a list.")
    if not all(isinstance(x, str) and x.strip() for x in sales_files):
        raise ValueError("config.sales_files entries must be non-empty strings.")
    config["sales_files"] = _normalize_sales_file_entries(sales_files, "config.sales_files")

    risk_days_high = float(config.get("risk_days_high", 60))
    risk_days_low = float(config.get("risk_days_low", 45))
    if risk_days_high <= 0 or risk_days_low <= 0:
        raise ValueError("risk_days_high and risk_days_low must be positive.")
    if risk_days_low >= risk_days_high:
        raise ValueError("risk_days_low must be smaller than risk_days_high.")

    full_months = int(config.get("sales_window_full_months", 3))
    if full_months < 0:
        raise ValueError("sales_window_full_months must be >= 0.")
    recent_days = int(config.get("sales_window_recent_days", 30))
    if recent_days <= 0:
        raise ValueError("sales_window_recent_days must be > 0.")

    _validate_bool_field(config, "sales_window_include_mtd", True, "sales_window_include_mtd must be true/false.")
    _validate_bool_field(config, "sales_date_dayfirst", False, "sales_date_dayfirst must be true/false.")
    sales_date_format = config.get("sales_date_format", "")
    if not isinstance(sales_date_format, str):
        raise ValueError("sales_date_format must be a string.")

    season_mode = config.get("season_mode", False)
    if not isinstance(season_mode, (bool, str)):
        raise ValueError("season_mode must be true/false or legacy string peak/off_peak.")
    if isinstance(season_mode, str):
        mode_text = season_mode.strip().lower()
        if mode_text not in {"true", "false", "peak", "off_peak"}:
            raise ValueError("season_mode string must be one of: true, false, peak, off_peak.")

    _validate_bool_field(config, "fail_on_empty_window", False, "fail_on_empty_window must be true/false.")
    _validate_bool_field(config, "strict_auto_scan", False, "strict_auto_scan must be true/false.")
    _validate_bool_field(
        config,
        "merge_detail_store_cells",
        True,
        "merge_detail_store_cells must be true/false.",
    )
    _validate_bool_field(
        config,
        "enable_ranked_store_transfer_summary",
        False,
        "enable_ranked_store_transfer_summary must be true/false.",
    )
    stagnant_outbound_mode = str(config.get("stagnant_outbound_mode", "keep_safety_stock")).strip().lower()
    if stagnant_outbound_mode not in {"keep_safety_stock", "all_outbound"}:
        raise ValueError("stagnant_outbound_mode must be one of: keep_safety_stock, all_outbound.")
    config["stagnant_outbound_mode"] = stagnant_outbound_mode
    stagnant_min_keep_qty = float(config.get("stagnant_min_keep_qty", 0))
    if stagnant_min_keep_qty < 0:
        raise ValueError("stagnant_min_keep_qty must be >= 0.")
    config["stagnant_min_keep_qty"] = stagnant_min_keep_qty

    brand_keywords = config.get("brand_keywords")
    if not isinstance(brand_keywords, list):
        raise ValueError("brand_keywords must be a list.")
    if not all(isinstance(x, str) for x in brand_keywords):
        raise ValueError("brand_keywords entries must be strings.")

    province_column_enabled = config.get("province_column_enabled", None)
    if province_column_enabled is not None and not isinstance(province_column_enabled, bool):
        raise ValueError("province_column_enabled must be true/false when provided.")

    batch = config.get("batch", {})
    if not isinstance(batch, dict):
        raise ValueError("config.batch must be a dict.")
    continue_on_error = batch.get("continue_on_error", True)
    if not isinstance(continue_on_error, bool):
        raise ValueError("config.batch.continue_on_error must be true/false.")
    summary_output_file = batch.get("summary_output_file", "./reports/batch_run_summary.xlsx")
    if not isinstance(summary_output_file, str) or not summary_output_file.strip():
        raise ValueError("config.batch.summary_output_file must be a non-empty string.")
    systems = batch.get("systems", [])
    if not isinstance(systems, list):
        raise ValueError("config.batch.systems must be a list.")

    batch["continue_on_error"] = continue_on_error
    batch["summary_output_file"] = summary_output_file
    batch["systems"] = systems
    config["batch"] = batch
    return cast(AppConfig, config)


def validate_batch_config(config: AppConfig, base_dir: Path) -> None:
    """校验批量模式下每个系统的关键配置约束。"""
    batch_cfg = config.get("batch", {})
    systems = batch_cfg.get("systems", [])
    if not systems:
        raise ValueError("batch mode requires at least one system in config.batch.systems.")

    seen_ids = set()
    seen_display_names = set()
    seen_output_paths = set()

    for idx, system in enumerate(systems, start=1):
        if not isinstance(system, dict):
            raise ValueError(f"batch.systems[{idx}] must be a dict.")
        typed_system = cast(BatchSystemConfig, system)
        enabled = typed_system.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError(f"batch.systems[{idx}].enabled must be true/false.")
        display_name = typed_system.get("display_name")
        if not isinstance(display_name, str) or not display_name.strip():
            raise ValueError(f"batch.systems[{idx}].display_name must be a non-empty string.")
        display_name = display_name.strip()
        if display_name in seen_display_names:
            raise ValueError(f"Duplicated display_name in batch.systems: {display_name}")
        seen_display_names.add(display_name)

        system_id = str(typed_system.get("system_id", "")).strip() or display_name
        if system_id in seen_ids:
            raise ValueError(f"Duplicated identity in batch.systems: {system_id}")
        seen_ids.add(system_id)

        # Disabled systems are intentionally skippable placeholders in batch mode.
        if not enabled:
            continue

        sales_files = typed_system.get("sales_files")
        if not isinstance(sales_files, list) or not sales_files:
            raise ValueError(f"batch.systems[{idx}].sales_files must be a non-empty list.")
        if not all(isinstance(x, str) and x.strip() for x in sales_files):
            raise ValueError(f"batch.systems[{idx}].sales_files entries must be non-empty strings.")
        typed_system["sales_files"] = _normalize_sales_file_entries(
            sales_files,
            f"batch.systems[{idx}].sales_files",
        )

        inv_file = typed_system.get("inventory_file")
        if not isinstance(inv_file, str) or not inv_file.strip():
            raise ValueError(f"batch.systems[{idx}].inventory_file must be a non-empty string.")

        out_file = typed_system.get("output_file")
        if out_file is not None:
            if not isinstance(out_file, str) or not out_file.strip():
                raise ValueError(f"batch.systems[{idx}].output_file must be a non-empty string if provided.")
            out_path = Path(out_file.strip())
            normalized = str((base_dir / out_path).resolve()) if not out_path.is_absolute() else str(out_path.resolve())
            if normalized in seen_output_paths:
                raise ValueError(f"Duplicated output_file in batch.systems[{idx}]: {out_file}")
            seen_output_paths.add(normalized)


def build_system_config(system_cfg: BatchSystemConfig, global_cfg: AppConfig) -> AppConfig:
    """将全局配置与单个系统配置合并为最终运行配置。"""
    merged = dict(global_cfg)
    merged["enabled"] = bool(system_cfg.get("enabled", True))
    merged["sales_files"] = list(system_cfg["sales_files"])
    merged["inventory_file"] = str(system_cfg["inventory_file"]).strip()
    merged["display_name"] = str(system_cfg["display_name"]).strip()
    raw_system_id = str(system_cfg.get("system_id", "")).strip()
    merged["system_id"] = raw_system_id or merged["display_name"]
    merged["data_subdir"] = str(system_cfg.get("data_subdir", "")).strip()

    output_file = system_cfg.get("output_file")
    merged["output_file"] = output_file.strip() if isinstance(output_file, str) and output_file.strip() else ""

    carton_factor_file = system_cfg.get("carton_factor_file")
    if isinstance(carton_factor_file, str) and carton_factor_file.strip():
        merged["carton_factor_file"] = carton_factor_file.strip()

    if "province_column_enabled" in system_cfg:
        merged["province_column_enabled"] = system_cfg.get("province_column_enabled")

    return validate_config(cast(AppConfig, merged))


def load_config(config_path: Path) -> AppConfig:
    """从 YAML 读取配置并套用默认值。"""
    config = cast(AppConfig, deepcopy(DEFAULT_CONFIG))
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f) or {}
        for key, value in loaded.items():
            if key == "batch" and isinstance(value, dict):
                config["batch"].update(value)
            else:
                config[key] = value
    return validate_config(config)


def resolve_system_raw_data_dir(config: AppConfig, base_dir: Path) -> Path:
    raw_data_dir = Path(config["raw_data_dir"])
    if not raw_data_dir.is_absolute():
        raw_data_dir = (base_dir / raw_data_dir).resolve()
    subdir = str(config.get("data_subdir", "")).strip()
    if subdir:
        raw_data_dir = (raw_data_dir / subdir).resolve()
    if not raw_data_dir.exists() or not raw_data_dir.is_dir():
        raise FileNotFoundError(f"Raw data directory not found for system '{config.get('display_name', '')}': {raw_data_dir}")
    return raw_data_dir


def resolve_output_file_path(config: AppConfig, display_name: str, inventory_date: str, base_dir: Path) -> Path:
    configured_output = str(config.get("output_file", "")).strip()
    if not configured_output or configured_output in DEFAULT_LEGACY_OUTPUT_FILES:
        configured_output = f"./reports/{display_name}{inventory_date.replace('-', '')}库存预警.xlsx"
    path = Path(configured_output)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def resolve_expected_output_for_status(config: AppConfig | BatchSystemConfig, display_name: str, base_dir: Path) -> str:
    configured_output = str(config.get("output_file", "")).strip()
    if not configured_output or configured_output in DEFAULT_LEGACY_OUTPUT_FILES:
        configured_output = f"./reports/{display_name}{AUTO_OUTPUT_DATE_PLACEHOLDER}库存预警.xlsx"
    path = Path(configured_output)
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return str(path)
