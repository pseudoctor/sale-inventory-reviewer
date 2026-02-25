import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from scripts.generate_inventory_risk_report import (
    build_system_config,
    resolve_output_file_path,
    resolve_expected_output_for_status,
    resolve_system_raw_data_dir,
    run_batch,
    validate_batch_config,
    validate_config,
)
from scripts.core import config as core_config
from tests.helpers import build_batch_config


class BatchModeTest(unittest.TestCase):
    def test_validate_config_single_allows_empty_output_file_for_auto_naming(self):
        cfg = {
            "run_mode": "single",
            "raw_data_dir": "./raw_data",
            "output_file": "",
            "sales_files": [],
            "inventory_file": "库存.xlsx",
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
            "batch": {"continue_on_error": True, "summary_output_file": "./reports/batch_run_summary.xlsx", "systems": []},
        }
        out = validate_config(cfg)
        self.assertEqual(out["output_file"], "")

    def test_validate_config_single_backward_compatible(self):
        cfg = {
            "raw_data_dir": "./raw_data",
            "output_file": "./reports/inventory_risk_report.xlsx",
            "sales_files": [],
            "inventory_file": "库存.xlsx",
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
            "batch": {"continue_on_error": True, "summary_output_file": "./reports/batch_run_summary.xlsx", "systems": []},
        }
        out = validate_config(cfg)
        self.assertEqual(out["run_mode"], "single")

    def test_validate_batch_config_rejects_duplicate_system_id(self):
        cfg = validate_config(
            {
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
                "batch": {
                    "continue_on_error": True,
                    "summary_output_file": "./reports/batch_run_summary.xlsx",
                    "systems": [
                        {
                            "system_id": "dup",
                            "display_name": "A",
                            "sales_files": ["a.xlsx"],
                            "inventory_file": "inv_a.xlsx",
                        },
                        {
                            "system_id": "dup",
                            "display_name": "B",
                            "sales_files": ["b.xlsx"],
                            "inventory_file": "inv_b.xlsx",
                        },
                    ],
                },
            }
        )
        with self.assertRaises(ValueError):
            validate_batch_config(cfg)

    def test_validate_batch_config_rejects_duplicate_display_name(self):
        cfg = validate_config(
            {
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
                "batch": {
                    "continue_on_error": True,
                    "summary_output_file": "./reports/batch_run_summary.xlsx",
                    "systems": [
                        {
                            "system_id": "a1",
                            "display_name": "同名系统",
                            "sales_files": ["a.xlsx"],
                            "inventory_file": "inv_a.xlsx",
                        },
                        {
                            "system_id": "a2",
                            "display_name": "同名系统",
                            "sales_files": ["b.xlsx"],
                            "inventory_file": "inv_b.xlsx",
                        },
                    ],
                },
            }
        )
        with self.assertRaises(ValueError):
            validate_batch_config(cfg)

    def test_validate_batch_config_rejects_duplicate_explicit_output_file(self):
        cfg = validate_config(
            {
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
                "batch": {
                    "continue_on_error": True,
                    "summary_output_file": "./reports/batch_run_summary.xlsx",
                    "systems": [
                        {
                            "system_id": "a1",
                            "display_name": "系统A",
                            "sales_files": ["a.xlsx"],
                            "inventory_file": "inv_a.xlsx",
                            "output_file": "./reports/conflict.xlsx",
                        },
                        {
                            "system_id": "a2",
                            "display_name": "系统B",
                            "sales_files": ["b.xlsx"],
                            "inventory_file": "inv_b.xlsx",
                            "output_file": "./reports/conflict.xlsx",
                        },
                    ],
                },
            }
        )
        with self.assertRaises(ValueError):
            validate_batch_config(cfg)

    def test_validate_batch_config_rejects_duplicate_sales_files_in_system(self):
        cfg = validate_config(
            {
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
                "batch": {
                    "continue_on_error": True,
                    "summary_output_file": "./reports/batch_run_summary.xlsx",
                    "systems": [
                        {
                            "system_id": "a1",
                            "display_name": "系统A",
                            "sales_files": ["a.xlsx", "a.xlsx"],
                            "inventory_file": "inv_a.xlsx",
                        }
                    ],
                },
            }
        )
        with self.assertRaises(ValueError):
            validate_batch_config(cfg)

    def test_validate_batch_config_rejects_sales_files_parent_path(self):
        cfg = validate_config(
            {
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
                "batch": {
                    "continue_on_error": True,
                    "summary_output_file": "./reports/batch_run_summary.xlsx",
                    "systems": [
                        {
                            "system_id": "a1",
                            "display_name": "系统A",
                            "sales_files": ["../a.xlsx"],
                            "inventory_file": "inv_a.xlsx",
                        }
                    ],
                },
            }
        )
        with self.assertRaises(ValueError):
            validate_batch_config(cfg)

    def test_validate_batch_config_allows_disabled_system_without_files(self):
        cfg = core_config.validate_config(
            {
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
                "batch": {
                    "continue_on_error": True,
                    "summary_output_file": "./reports/batch_run_summary.xlsx",
                    "systems": [
                        {"display_name": "停用占位系统", "enabled": False},
                    ],
                },
            }
        )
        core_config.validate_batch_config(cfg, Path.cwd())

    def test_build_system_config_uses_default_output_and_carton(self):
        global_cfg = validate_config(
            {
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
                "carton_factor_file": "./data/global_carton.xlsx",
                "brand_keywords": [],
                "batch": {"continue_on_error": True, "summary_output_file": "./reports/batch_run_summary.xlsx", "systems": []},
            }
        )
        system_cfg = {
            "display_name": "系统一",
            "sales_files": ["sales.xlsx"],
            "inventory_file": "inv.xlsx",
            "data_subdir": "系统一",
        }
        merged = build_system_config(system_cfg, global_cfg)
        self.assertEqual(merged["output_file"], "")
        self.assertEqual(merged["carton_factor_file"], "./data/global_carton.xlsx")
        self.assertEqual(merged["system_id"], "系统一")
        self.assertEqual(merged["data_subdir"], "系统一")
        self.assertIsNone(merged.get("province_column_enabled"))

    def test_build_system_config_allows_province_column_override(self):
        global_cfg = validate_config(
            {
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
                "carton_factor_file": "./data/global_carton.xlsx",
                "brand_keywords": [],
                "province_column_enabled": False,
                "batch": {"continue_on_error": True, "summary_output_file": "./reports/batch_run_summary.xlsx", "systems": []},
            }
        )
        system_cfg = {
            "display_name": "系统一",
            "sales_files": ["sales.xlsx"],
            "inventory_file": "inv.xlsx",
            "province_column_enabled": True,
        }
        merged = build_system_config(system_cfg, global_cfg)
        self.assertTrue(merged["province_column_enabled"])

    def test_resolve_system_raw_data_dir_with_subdir(self):
        cfg = {"raw_data_dir": "./raw_data", "data_subdir": "宁夏物美"}
        path = resolve_system_raw_data_dir(cfg)
        self.assertTrue(str(path).endswith("raw_data/宁夏物美"))

    def test_resolve_system_raw_data_dir_raises_for_missing_subdir(self):
        cfg = {"raw_data_dir": "./raw_data", "data_subdir": "不存在的系统目录"}
        with self.assertRaises(FileNotFoundError):
            resolve_system_raw_data_dir(cfg)

    def test_resolve_output_file_path_respects_explicit_output(self):
        cfg = {"output_file": "./reports/陕西华润_inventory_risk_report.xlsx"}
        out = resolve_output_file_path(cfg, "陕西华润", "2026-02-08")
        self.assertTrue(str(out).endswith("reports/陕西华润_inventory_risk_report.xlsx"))

    def test_resolve_output_file_path_uses_auto_name_when_legacy_default(self):
        cfg = {"output_file": "./reports/inventory_risk_report.xlsx"}
        out = resolve_output_file_path(cfg, "陕西华润", "2026-02-08")
        self.assertTrue(str(out).endswith("reports/陕西华润20260208库存预警.xlsx"))

    def test_resolve_expected_output_for_status_auto_name_has_placeholder(self):
        cfg = {"output_file": "./reports/inventory_risk_report.xlsx"}
        out = resolve_expected_output_for_status(cfg, "陕西华润")
        self.assertTrue(out.endswith("reports/陕西华润{库存日期}库存预警.xlsx"))

    def test_run_batch_continue_on_error_true(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary_path = Path(tmp) / "batch_run_summary.xlsx"
            cfg = validate_config(
                build_batch_config(
                    continue_on_error=True,
                    summary_output_file=str(summary_path),
                    carton_factor_file="./data/carton.xlsx",
                    systems=[
                        {
                            "system_id": "bad",
                            "display_name": "坏系统",
                            "enabled": True,
                            "data_subdir": "坏系统",
                            "sales_files": ["a.xlsx"],
                            "inventory_file": "a_inv.xlsx",
                            "output_file": "./reports/a.xlsx",
                        },
                        {
                            "system_id": "ok",
                            "display_name": "好系统",
                            "enabled": True,
                            "data_subdir": "好系统",
                            "sales_files": ["b.xlsx"],
                            "inventory_file": "b_inv.xlsx",
                            "output_file": "./reports/b.xlsx",
                        },
                    ],
                )
            )

            def fake_generate(system_cfg, _):
                if system_cfg["system_id"] == "bad":
                    raise FileNotFoundError("missing input")
                return {
                    "system_id": "ok",
                    "display_name": "好系统",
                    "status": "SUCCESS",
                    "message": "",
                    "output_file": "./reports/b.xlsx",
                    "detail_rows": 12,
                    "missing_sku_rows": 2,
                }

            with patch("scripts.generate_inventory_risk_report.generate_report_for_system", side_effect=fake_generate):
                failures = run_batch(cfg)

            self.assertEqual(failures, 1)
            self.assertTrue(summary_path.exists())
            summary = pd.read_excel(summary_path)
            self.assertEqual(len(summary), 2)
            self.assertIn("FAILED", summary["status"].tolist())
            self.assertIn("SUCCESS", summary["status"].tolist())
            self.assertIn("data_subdir", summary.columns)
            self.assertIn("enabled", summary.columns)
            self.assertIn("error_stage", summary.columns)
            self.assertIn("input_files_count", summary.columns)
            self.assertIn("loaded_sales_files", summary.columns)
            self.assertIn("missing_sales_files", summary.columns)
            self.assertIn("inventory_file_exists", summary.columns)

    def test_run_batch_enabled_false_becomes_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary_path = Path(tmp) / "batch_run_summary.xlsx"
            cfg = validate_config(
                build_batch_config(
                    continue_on_error=True,
                    summary_output_file=str(summary_path),
                    carton_factor_file="./data/carton.xlsx",
                    systems=[
                        {
                            "display_name": "停用系统",
                            "enabled": False,
                            "data_subdir": "停用系统",
                            "sales_files": ["a.xlsx"],
                            "inventory_file": "a_inv.xlsx",
                            "output_file": "./reports/a.xlsx",
                        }
                    ],
                )
            )

            with patch("scripts.generate_inventory_risk_report.generate_report_for_system") as mocked_generate:
                failures = run_batch(cfg)
            self.assertEqual(failures, 0)
            mocked_generate.assert_not_called()
            summary = pd.read_excel(summary_path)
            self.assertEqual(summary.iloc[0]["status"], "SKIPPED")
            self.assertEqual(summary.iloc[0]["message"], "disabled")
            self.assertIn("error_stage", summary.columns)
            self.assertIn("input_files_count", summary.columns)
            self.assertIn("loaded_sales_files", summary.columns)
            self.assertIn("missing_sales_files", summary.columns)
            self.assertIn("inventory_file_exists", summary.columns)

    def test_run_batch_continue_on_error_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary_path = Path(tmp) / "batch_run_summary.xlsx"
            cfg = validate_config(
                build_batch_config(
                    continue_on_error=False,
                    summary_output_file=str(summary_path),
                    carton_factor_file="./data/carton.xlsx",
                    systems=[
                        {
                            "system_id": "bad",
                            "display_name": "坏系统",
                            "enabled": True,
                            "data_subdir": "坏系统",
                            "sales_files": ["a.xlsx"],
                            "inventory_file": "a_inv.xlsx",
                            "output_file": "./reports/a.xlsx",
                        },
                        {
                            "system_id": "ok",
                            "display_name": "好系统",
                            "enabled": True,
                            "data_subdir": "好系统",
                            "sales_files": ["b.xlsx"],
                            "inventory_file": "b_inv.xlsx",
                            "output_file": "./reports/b.xlsx",
                        },
                    ],
                )
            )

            with patch(
                "scripts.generate_inventory_risk_report.generate_report_for_system",
                side_effect=FileNotFoundError("missing input"),
            ):
                failures = run_batch(cfg)

            self.assertEqual(failures, 1)
            summary = pd.read_excel(summary_path)
            self.assertEqual(len(summary), 1)
            self.assertEqual(summary.iloc[0]["status"], "FAILED")
            self.assertIn("error_stage", summary.columns)
            self.assertIn("input_files_count", summary.columns)
            self.assertIn("loaded_sales_files", summary.columns)
            self.assertIn("missing_sales_files", summary.columns)
            self.assertIn("inventory_file_exists", summary.columns)


if __name__ == "__main__":
    unittest.main()
