import unittest

from scripts.core import pipeline as core_pipeline
from scripts.core.config import validate_config


class ConfigValidationTest(unittest.TestCase):
    def test_validate_config_rejects_invalid_risk_thresholds(self):
        with self.assertRaises(ValueError):
            validate_config(
                {
                    "raw_data_dir": "./raw_data",
                    "output_file": "./reports/x.xlsx",
                    "sales_files": [],
                    "inventory_file": "库存.xlsx",
                    "risk_days_high": 45,
                    "risk_days_low": 45,
                    "sales_window_full_months": 3,
                    "sales_window_include_mtd": True,
                    "sales_window_recent_days": 30,
                    "season_mode": False,
                    "brand_keywords": ["品牌A"],
                }
            )

    def test_validate_config_rejects_invalid_strict_auto_scan_type(self):
        with self.assertRaises(ValueError):
            validate_config(
                {
                    "raw_data_dir": "./raw_data",
                    "output_file": "./reports/x.xlsx",
                    "sales_files": [],
                    "inventory_file": "库存.xlsx",
                    "risk_days_high": 60,
                    "risk_days_low": 45,
                    "sales_window_full_months": 3,
                    "sales_window_include_mtd": True,
                    "sales_window_recent_days": 30,
                    "season_mode": False,
                    "strict_auto_scan": "yes",
                    "brand_keywords": ["品牌A"],
                }
            )

    def test_validate_config_rejects_duplicate_sales_files(self):
        with self.assertRaises(ValueError):
            validate_config(
                {
                    "raw_data_dir": "./raw_data",
                    "output_file": "./reports/x.xlsx",
                    "sales_files": ["sales_202601.xlsx", "sales_202601.xlsx"],
                    "inventory_file": "库存.xlsx",
                    "risk_days_high": 60,
                    "risk_days_low": 45,
                    "sales_window_full_months": 3,
                    "sales_window_include_mtd": True,
                    "sales_window_recent_days": 30,
                    "season_mode": False,
                    "brand_keywords": ["品牌A"],
                }
            )

    def test_validate_config_rejects_sales_files_parent_path(self):
        with self.assertRaises(ValueError):
            validate_config(
                {
                    "raw_data_dir": "./raw_data",
                    "output_file": "./reports/x.xlsx",
                    "sales_files": ["../outside.xlsx"],
                    "inventory_file": "库存.xlsx",
                    "risk_days_high": 60,
                    "risk_days_low": 45,
                    "sales_window_full_months": 3,
                    "sales_window_include_mtd": True,
                    "sales_window_recent_days": 30,
                    "season_mode": False,
                    "brand_keywords": ["品牌A"],
                }
            )

    def test_validate_config_rejects_data_subdir_parent_path(self):
        with self.assertRaises(ValueError):
            validate_config(
                {
                    "raw_data_dir": "./raw_data",
                    "data_subdir": "../outside",
                    "output_file": "./reports/x.xlsx",
                    "sales_files": [],
                    "inventory_file": "库存.xlsx",
                    "risk_days_high": 60,
                    "risk_days_low": 45,
                    "sales_window_full_months": 3,
                    "sales_window_include_mtd": True,
                    "sales_window_recent_days": 30,
                    "season_mode": False,
                    "brand_keywords": ["品牌A"],
                }
            )

    def test_validate_config_rejects_output_file_outside_reports(self):
        with self.assertRaises(ValueError):
            validate_config(
                {
                    "raw_data_dir": "./raw_data",
                    "output_file": "../outside.xlsx",
                    "sales_files": [],
                    "inventory_file": "库存.xlsx",
                    "risk_days_high": 60,
                    "risk_days_low": 45,
                    "sales_window_full_months": 3,
                    "sales_window_include_mtd": True,
                    "sales_window_recent_days": 30,
                    "season_mode": False,
                    "brand_keywords": ["品牌A"],
                }
            )

    def test_validate_config_rejects_batch_summary_output_file_outside_reports(self):
        with self.assertRaises(ValueError):
            validate_config(
                {
                    "run_mode": "batch",
                    "raw_data_dir": "./raw_data",
                    "output_file": "",
                    "sales_files": [],
                    "inventory_file": "",
                    "risk_days_high": 60,
                    "risk_days_low": 45,
                    "sales_window_full_months": 3,
                    "sales_window_include_mtd": True,
                    "sales_window_recent_days": 30,
                    "season_mode": False,
                    "carton_factor_file": "./data/sku装箱数.xlsx",
                    "brand_keywords": ["品牌A"],
                    "batch": {"continue_on_error": True, "summary_output_file": "../escape.xlsx", "systems": []},
                }
            )

    def test_effective_brand_keywords_rejects_empty(self):
        with self.assertRaises(ValueError):
            core_pipeline._effective_brand_keywords({"brand_keywords": []})

    def test_validate_config_rejects_empty_brand_keywords(self):
        with self.assertRaises(ValueError):
            validate_config(
                {
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
            )


if __name__ == "__main__":
    unittest.main()
