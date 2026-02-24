from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

BASE_SINGLE_CONFIG: Dict[str, Any] = {
    "run_mode": "single",
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
}

BASE_BATCH_CONFIG: Dict[str, Any] = {
    "run_mode": "batch",
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
    "carton_factor_file": "./data/sku装箱数.xlsx",
    "brand_keywords": [],
}


def write_excel(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, index=False)


def build_single_config(
    *,
    system_id: str,
    display_name: str,
    raw_data_dir: str,
    data_subdir: str,
    sales_files: List[str],
    inventory_file: str,
    output_file: str,
    carton_factor_file: str,
    brand_keywords: List[str],
    **overrides: Any,
) -> Dict[str, Any]:
    config = deepcopy(BASE_SINGLE_CONFIG)
    config.update(
        {
            "system_id": system_id,
            "display_name": display_name,
            "raw_data_dir": raw_data_dir,
            "data_subdir": data_subdir,
            "sales_files": list(sales_files),
            "inventory_file": inventory_file,
            "output_file": output_file,
            "carton_factor_file": carton_factor_file,
            "brand_keywords": list(brand_keywords),
        }
    )
    config.update(overrides)
    return config


def build_batch_config(
    *,
    systems: List[Dict[str, Any]],
    summary_output_file: str = "./reports/batch_run_summary.xlsx",
    continue_on_error: bool = True,
    **overrides: Any,
) -> Dict[str, Any]:
    config = deepcopy(BASE_BATCH_CONFIG)
    config.update(overrides)
    config["batch"] = {
        "continue_on_error": continue_on_error,
        "summary_output_file": summary_output_file,
        "systems": systems,
    }
    return config
