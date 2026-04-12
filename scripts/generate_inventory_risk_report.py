#!/usr/bin/env python3
"""Generate inventory risk Excel report from windowed daily sales and inventory data."""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import sys

if __package__ in {None, ""}:
    # Allow running as `python3 scripts/generate_inventory_risk_report.py`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.core import batch as core_batch
from scripts.core import config as core_config
from scripts.core.models import AppConfig, ReportRunResult
from scripts.core import pipeline as core_pipeline

BASE_DIR = Path(__file__).parent.parent
CONFIG_PATH = BASE_DIR / "config.yaml"
PROGRAM_VERSION = "1.1.0"

def load_config() -> AppConfig:
    return core_config.load_config(CONFIG_PATH)

# Keep only aliases that are still used by tests and external imports.
validate_config = core_config.validate_config
validate_batch_config = core_config.validate_batch_config
build_system_config = core_config.build_system_config
resolve_system_raw_data_dir = core_config.resolve_system_raw_data_dir
resolve_output_file_path = core_config.resolve_output_file_path
resolve_expected_output_for_status = core_config.resolve_expected_output_for_status
map_province_by_supplier_card = core_pipeline.map_province_by_supplier_card

__all__ = [
    "PROGRAM_VERSION",
    "load_config",
    "validate_config",
    "validate_batch_config",
    "build_system_config",
    "resolve_system_raw_data_dir",
    "resolve_output_file_path",
    "resolve_expected_output_for_status",
    "map_province_by_supplier_card",
    "generate_report_for_system",
    "run_batch",
    "main",
]


def generate_report_for_system(system_cfg: AppConfig, global_cfg: Optional[AppConfig] = None) -> ReportRunResult:
    return core_pipeline.generate_report_for_system(
        system_cfg,
        global_cfg,
        base_dir=BASE_DIR,
        program_version=PROGRAM_VERSION,
    )


def run_batch(global_config: AppConfig) -> int:
    core_config.validate_batch_config(global_config, BASE_DIR)
    return core_batch.run_batch(
        global_config=global_config,
        base_dir=BASE_DIR,
        build_system_config=core_config.build_system_config,
        resolve_expected_output_for_status=lambda cfg, name: core_config.resolve_expected_output_for_status(cfg, name, BASE_DIR),
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
