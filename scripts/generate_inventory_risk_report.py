#!/usr/bin/env python3
"""Generate inventory risk Excel report from windowed daily sales and inventory data."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import sys

import pandas as pd

if __package__ in {None, ""}:
    # Allow running as `python3 scripts/generate_inventory_risk_report.py`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.core import batch as core_batch
from scripts.core import config as core_config
from scripts.core import io as core_io
from scripts.core import metrics as core_metrics
from scripts.core import pipeline as core_pipeline
from scripts.core import recommendations as core_recommendations

BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"
PROGRAM_VERSION = "1.1.0"


def validate_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return core_config.validate_config(config)


def validate_batch_config(config: Dict[str, Any]) -> None:
    core_config.validate_batch_config(config, BASE_DIR)


def build_system_config(system_cfg: Dict[str, Any], global_cfg: Dict[str, Any]) -> Dict[str, Any]:
    return core_config.build_system_config(system_cfg, global_cfg)


def resolve_system_raw_data_dir(config: Dict[str, Any]) -> Path:
    return core_config.resolve_system_raw_data_dir(config, BASE_DIR)


def resolve_output_file_path(config: Dict[str, Any], display_name: str, inventory_date: str) -> Path:
    return core_config.resolve_output_file_path(config, display_name, inventory_date, BASE_DIR)


def resolve_expected_output_for_status(config: Dict[str, Any], display_name: str) -> str:
    return core_config.resolve_expected_output_for_status(config, display_name, BASE_DIR)


def load_config() -> Dict[str, Any]:
    return core_config.load_config(CONFIG_PATH)


def extract_month_key(filename: str) -> Optional[int]:
    return core_io.extract_month_key(filename)


def find_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    return core_io.find_column(columns, candidates)


def read_excel_first_sheet(path: Path) -> pd.DataFrame:
    return core_io.read_excel_first_sheet(path)


def resolve_sales_candidates(raw_data_dir: Path, configured_sales_files: List[str]) -> List[Path]:
    return core_io.resolve_sales_candidates(raw_data_dir, configured_sales_files)


def list_ignored_sales_files(raw_data_dir: Path, configured_sales_files: List[str]) -> List[str]:
    return core_io.list_ignored_sales_files(raw_data_dir, configured_sales_files)


def normalize_sales_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, Optional[str], str, str, str, str, Optional[str]]:
    return core_io.normalize_sales_df(df)


def overlap_days(
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    data_min_date: pd.Timestamp,
    data_max_date: pd.Timestamp,
) -> int:
    return core_metrics.overlap_days(start_date, end_date, data_min_date, data_max_date)


def combine_daily_sales(
    daily_sales_3m_mtd: pd.Series,
    daily_sales_30d: pd.Series,
    use_peak_mode: bool,
) -> pd.Series:
    return core_metrics.combine_daily_sales(daily_sales_3m_mtd, daily_sales_30d, use_peak_mode)


def classify_risk_levels(turnover_days: pd.Series, low_days: float, high_days: float) -> pd.Series:
    return core_metrics.classify_risk_levels(turnover_days, low_days, high_days)


def normalize_inventory_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, str, Optional[str], str, str, str, Optional[str]]:
    return core_io.normalize_inventory_df(df)


def normalize_barcode_value(value) -> Optional[str]:
    return core_io.normalize_barcode_value(value)


def parse_sales_dates(raw_dates: pd.Series, date_format: str, dayfirst: bool) -> pd.Series:
    return core_io.parse_sales_dates(raw_dates, date_format, dayfirst)


def compute_case_counts(qty: pd.Series, factor: pd.Series, use_peak_mode: bool) -> pd.Series:
    return core_recommendations.compute_case_counts(qty, factor, use_peak_mode)


def map_province_by_supplier_card(card: Optional[str]) -> str:
    return core_pipeline.map_province_by_supplier_card(card)


def apply_inventory_metrics(df: pd.DataFrame, low_days: float, high_days: float) -> pd.DataFrame:
    return core_metrics.apply_inventory_metrics(df, low_days, high_days)


def generate_report_for_system(system_cfg: Dict[str, Any], global_cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return core_pipeline.generate_report_for_system(
        system_cfg,
        global_cfg,
        base_dir=BASE_DIR,
        program_version=PROGRAM_VERSION,
    )


def run_batch(global_config: Dict[str, Any]) -> int:
    validate_batch_config(global_config)
    return core_batch.run_batch(
        global_config=global_config,
        base_dir=BASE_DIR,
        build_system_config=build_system_config,
        resolve_expected_output_for_status=lambda cfg, name: resolve_expected_output_for_status(cfg, name),
        generate_report_for_system=generate_report_for_system,
    )


def main() -> int:
    config = load_config()
    run_mode = str(config.get("run_mode", "single")).lower()
    if run_mode == "batch":
        failures = run_batch(config)
        return 1 if failures > 0 else 0

    generate_report_for_system(config, config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
