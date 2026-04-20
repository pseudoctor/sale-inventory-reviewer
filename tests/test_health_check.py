import unittest
from io import StringIO
from pathlib import Path
import tempfile
from unittest.mock import patch
import pandas as pd

from scripts import health_check


class HealthCheckTest(unittest.TestCase):
    def test_check_python_returns_no_error_on_supported_runtime(self):
        errors = health_check._check_python()
        self.assertIsInstance(errors, list)

    def test_check_dependencies_reports_missing_package(self):
        def fake_import(name):
            if name == "openpyxl":
                raise ModuleNotFoundError("missing openpyxl")
            return object()

        with patch("scripts.health_check.importlib.import_module", side_effect=fake_import):
            errors = health_check._check_dependencies()
        self.assertTrue(any("openpyxl" in err for err in errors))

    def test_check_dependencies_reports_broken_yaml_module(self):
        class BrokenYamlModule:
            __file__ = None

        def fake_import(name):
            if name == "yaml":
                return BrokenYamlModule()
            return object()

        with patch("scripts.health_check.importlib.import_module", side_effect=fake_import):
            errors = health_check._check_dependencies()
        self.assertTrue(any("broken dependency 'yaml'" in err for err in errors))
        self.assertTrue(any("safe_load" in err for err in errors))

    def test_main_skips_config_check_when_dependencies_fail(self):
        with patch("scripts.health_check._check_python", return_value=[]), patch(
            "scripts.health_check._check_dependencies", return_value=["missing dependency 'yaml': broken"]
        ), patch("scripts.health_check._check_config_and_paths", return_value=[]) as config_check, patch(
            "sys.stdout", new_callable=StringIO
        ):
            code = health_check.main()
        self.assertEqual(code, 1)
        config_check.assert_not_called()

    def test_single_mode_auto_scan_reports_no_sales_candidates(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw_data"
            raw.mkdir()
            reports = root / "reports"
            reports.mkdir()
            (raw / "库存.xlsx").touch()
            (raw / "abc.xlsx").touch()

            cfg = {
                "run_mode": "single",
                "raw_data_dir": str(raw),
                "inventory_file": "库存.xlsx",
                "sales_files": [],
                "output_file": "",
                "carton_factor_file": "./data/sku装箱数.xlsx",
                "risk_days_high": 60,
                "risk_days_low": 45,
                "sales_window_full_months": 3,
                "sales_window_include_mtd": True,
                "sales_window_recent_days": 30,
                "season_mode": False,
                "brand_keywords": [],
                "batch": {"continue_on_error": True, "summary_output_file": "./reports/batch_run_summary.xlsx", "systems": []},
            }
            with patch("scripts.health_check.BASE_DIR", root), patch("scripts.health_check.CONFIG_PATH", root / "config.yaml"), patch(
                "scripts.health_check.core_config.load_config", return_value=cfg
            ):
                errors = health_check._check_config_and_paths()
            self.assertTrue(any("no auto-detected sales files" in err for err in errors))
            self.assertTrue(any("brand_keywords is empty" in err for err in errors))

    def test_single_mode_auto_scan_checks_sales_amount_column_when_ranked_summary_enabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw_data"
            raw.mkdir()
            reports = root / "reports"
            reports.mkdir()
            data_dir = root / "data"
            data_dir.mkdir()
            pd.DataFrame({"门店名称": ["A店"], "商品名称": ["SKU1"], "商品条码": ["1"], "库存数量": [1]}).to_excel(raw / "库存.xlsx", index=False)
            pd.DataFrame(
                {"门店名称": ["A店"], "商品名称": ["SKU1"], "商品条码": ["1"], "销售数量": [1], "销售时间": ["2026-02-01"]}
            ).to_excel(raw / "销售202602.xlsx", index=False)
            pd.DataFrame({"商品条码": ["1"], "商品名称": ["SKU1"], "装箱数（因子）": [6]}).to_excel(data_dir / "sku装箱数.xlsx", index=False)

            cfg = {
                "run_mode": "single",
                "raw_data_dir": str(raw),
                "inventory_file": "库存.xlsx",
                "sales_files": [],
                "output_file": "",
                "carton_factor_file": str(data_dir / "sku装箱数.xlsx"),
                "risk_days_high": 60,
                "risk_days_low": 45,
                "sales_window_full_months": 3,
                "sales_window_include_mtd": True,
                "sales_window_recent_days": 30,
                "season_mode": False,
                "enable_ranked_store_transfer_summary": True,
                "brand_keywords": ["测试"],
                "batch": {"continue_on_error": True, "summary_output_file": "./reports/batch_run_summary.xlsx", "systems": []},
            }
            with (
                patch("scripts.health_check.BASE_DIR", root),
                patch("scripts.health_check.CONFIG_PATH", root / "config.yaml"),
                patch("scripts.health_check.core_config.load_config", return_value=cfg),
            ):
                errors = health_check._check_config_and_paths()
            self.assertTrue(any("sales amount column" in err for err in errors))

    def test_check_config_and_paths_requires_xlrd_for_xls_inputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw_data"
            raw.mkdir()
            reports = root / "reports"
            reports.mkdir()
            (raw / "库存.xls").touch()
            (raw / "销售202602.xls").touch()

            cfg = {
                "run_mode": "single",
                "raw_data_dir": str(raw),
                "inventory_file": "库存.xls",
                "sales_files": ["销售202602.xls"],
                "output_file": "",
                "carton_factor_file": "./data/sku装箱数.xlsx",
                "risk_days_high": 60,
                "risk_days_low": 45,
                "sales_window_full_months": 3,
                "sales_window_include_mtd": True,
                "sales_window_recent_days": 30,
                "season_mode": False,
                "enable_ranked_store_transfer_summary": True,
                "brand_keywords": ["测试"],
                "batch": {"continue_on_error": True, "summary_output_file": "./reports/batch_run_summary.xlsx", "systems": []},
            }

            def fake_import(name):
                if name == "xlrd":
                    raise ModuleNotFoundError("missing xlrd")
                return object()

            with (
                patch("scripts.health_check.BASE_DIR", root),
                patch("scripts.health_check.CONFIG_PATH", root / "config.yaml"),
                patch("scripts.health_check.core_config.load_config", return_value=cfg),
                patch("scripts.health_check.importlib.import_module", side_effect=fake_import),
            ):
                errors = health_check._check_config_and_paths()
            self.assertTrue(any("missing dependency 'xlrd'" in err for err in errors))

    def test_check_config_and_paths_does_not_require_xlrd_for_xlsx_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw_data"
            raw.mkdir()
            reports = root / "reports"
            reports.mkdir()
            (raw / "库存.xlsx").touch()
            (raw / "销售202602.xlsx").touch()

            cfg = {
                "run_mode": "single",
                "raw_data_dir": str(raw),
                "inventory_file": "库存.xlsx",
                "sales_files": ["销售202602.xlsx"],
                "output_file": "",
                "carton_factor_file": "./data/sku装箱数.xlsx",
                "risk_days_high": 60,
                "risk_days_low": 45,
                "sales_window_full_months": 3,
                "sales_window_include_mtd": True,
                "sales_window_recent_days": 30,
                "season_mode": False,
                "enable_ranked_store_transfer_summary": True,
                "brand_keywords": ["测试"],
                "batch": {"continue_on_error": True, "summary_output_file": "./reports/batch_run_summary.xlsx", "systems": []},
            }

            def fake_import(name):
                if name == "xlrd":
                    raise ModuleNotFoundError("missing xlrd")
                return object()

            with (
                patch("scripts.health_check.BASE_DIR", root),
                patch("scripts.health_check.CONFIG_PATH", root / "config.yaml"),
                patch("scripts.health_check.core_config.load_config", return_value=cfg),
                patch("scripts.health_check.importlib.import_module", side_effect=fake_import),
            ):
                errors = health_check._check_config_and_paths()
            self.assertFalse(any("xlrd" in err for err in errors))

    def test_check_config_and_paths_reports_missing_carton_factor_in_single_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw_data"
            raw.mkdir()
            reports = root / "reports"
            reports.mkdir()
            (raw / "库存.xlsx").touch()
            (raw / "销售202602.xlsx").touch()

            cfg = {
                "run_mode": "single",
                "raw_data_dir": str(raw),
                "inventory_file": "库存.xlsx",
                "sales_files": ["销售202602.xlsx"],
                "output_file": "",
                "carton_factor_file": "./data/sku装箱数.xlsx",
                "risk_days_high": 60,
                "risk_days_low": 45,
                "sales_window_full_months": 3,
                "sales_window_include_mtd": True,
                "sales_window_recent_days": 30,
                "season_mode": False,
                "enable_ranked_store_transfer_summary": True,
                "brand_keywords": ["测试"],
                "batch": {"continue_on_error": True, "summary_output_file": "./reports/batch_run_summary.xlsx", "systems": []},
            }

            with (
                patch("scripts.health_check.BASE_DIR", root),
                patch("scripts.health_check.CONFIG_PATH", root / "config.yaml"),
                patch("scripts.health_check.core_config.load_config", return_value=cfg),
            ):
                errors = health_check._check_config_and_paths()
            self.assertTrue(any("carton factor file not found" in err for err in errors))

    def test_check_config_and_paths_reports_missing_sales_amount_column_in_single_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw_data"
            raw.mkdir()
            reports = root / "reports"
            reports.mkdir()
            data_dir = root / "data"
            data_dir.mkdir()
            pd.DataFrame({"门店名称": ["A店"], "商品名称": ["SKU1"], "商品条码": ["1"], "库存数量": [1]}).to_excel(raw / "库存.xlsx", index=False)
            pd.DataFrame(
                {"门店名称": ["A店"], "商品名称": ["SKU1"], "商品条码": ["1"], "销售数量": [1], "销售时间": ["2026-02-01"]}
            ).to_excel(raw / "销售202602.xlsx", index=False)
            pd.DataFrame({"商品条码": ["1"], "商品名称": ["SKU1"], "装箱数（因子）": [6]}).to_excel(data_dir / "sku装箱数.xlsx", index=False)

            cfg = {
                "run_mode": "single",
                "raw_data_dir": str(raw),
                "inventory_file": "库存.xlsx",
                "sales_files": ["销售202602.xlsx"],
                "output_file": "",
                "carton_factor_file": str(data_dir / "sku装箱数.xlsx"),
                "risk_days_high": 60,
                "risk_days_low": 45,
                "sales_window_full_months": 3,
                "sales_window_include_mtd": True,
                "sales_window_recent_days": 30,
                "season_mode": False,
                "enable_ranked_store_transfer_summary": True,
                "brand_keywords": ["测试"],
                "batch": {"continue_on_error": True, "summary_output_file": "./reports/batch_run_summary.xlsx", "systems": []},
            }

            with (
                patch("scripts.health_check.BASE_DIR", root),
                patch("scripts.health_check.CONFIG_PATH", root / "config.yaml"),
                patch("scripts.health_check.core_config.load_config", return_value=cfg),
            ):
                errors = health_check._check_config_and_paths()
            self.assertTrue(any("sales amount column" in err for err in errors))

    def test_check_config_and_paths_single_mode_uses_data_subdir(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw_data"
            reports = root / "reports"
            data_dir = root / "data"
            system_raw = raw / "陕西华润"
            system_raw.mkdir(parents=True)
            reports.mkdir()
            data_dir.mkdir()
            pd.DataFrame({"门店名称": ["A店"], "商品名称": ["SKU1"], "商品条码": ["1"], "库存数量": [1]}).to_excel(system_raw / "库存.xlsx", index=False)
            pd.DataFrame(
                {"门店名称": ["A店"], "商品名称": ["SKU1"], "商品条码": ["1"], "销售数量": [1], "销售时间": ["2026-02-01"]}
            ).to_excel(system_raw / "销售202602.xlsx", index=False)
            pd.DataFrame({"商品条码": ["1"], "商品名称": ["SKU1"], "装箱数（因子）": [6]}).to_excel(data_dir / "sku装箱数.xlsx", index=False)

            cfg = {
                "run_mode": "single",
                "raw_data_dir": str(raw),
                "data_subdir": "陕西华润",
                "inventory_file": "库存.xlsx",
                "sales_files": ["销售202602.xlsx"],
                "output_file": "",
                "carton_factor_file": str(data_dir / "sku装箱数.xlsx"),
                "risk_days_high": 60,
                "risk_days_low": 45,
                "sales_window_full_months": 3,
                "sales_window_include_mtd": True,
                "sales_window_recent_days": 30,
                "season_mode": False,
                "brand_keywords": ["测试"],
                "batch": {"continue_on_error": True, "summary_output_file": "./reports/batch_run_summary.xlsx", "systems": []},
            }

            with (
                patch("scripts.health_check.BASE_DIR", root),
                patch("scripts.health_check.CONFIG_PATH", root / "config.yaml"),
                patch("scripts.health_check.core_config.load_config", return_value=cfg),
            ):
                errors = health_check._check_config_and_paths()
            self.assertEqual(errors, [])

    def test_check_config_and_paths_reports_missing_carton_factor_in_batch_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw_data"
            raw.mkdir()
            reports = root / "reports"
            reports.mkdir()
            system_raw = raw / "陕西华润"
            system_raw.mkdir()
            (system_raw / "陕西华润库存.xlsx").touch()
            (system_raw / "陕西华润销售202602.xlsx").touch()

            cfg = {
                "run_mode": "batch",
                "raw_data_dir": str(raw),
                "inventory_file": "",
                "sales_files": [],
                "output_file": "",
                "carton_factor_file": "./data/sku装箱数.xlsx",
                "risk_days_high": 60,
                "risk_days_low": 45,
                "sales_window_full_months": 3,
                "sales_window_include_mtd": True,
                "sales_window_recent_days": 30,
                "season_mode": False,
                "enable_ranked_store_transfer_summary": True,
                "brand_keywords": ["测试"],
                "batch": {
                    "continue_on_error": True,
                    "summary_output_file": "./reports/batch_run_summary.xlsx",
                    "systems": [
                        {
                            "enabled": True,
                            "system_id": "shaanxi_huarun",
                            "display_name": "陕西华润",
                            "data_subdir": "陕西华润",
                            "sales_files": ["陕西华润销售202602.xlsx"],
                            "inventory_file": "陕西华润库存.xlsx",
                        }
                    ],
                },
            }

            with (
                patch("scripts.health_check.BASE_DIR", root),
                patch("scripts.health_check.CONFIG_PATH", root / "config.yaml"),
                patch("scripts.health_check.core_config.load_config", return_value=cfg),
            ):
                errors = health_check._check_config_and_paths()
            self.assertTrue(any("missing carton factor file" in err for err in errors))

    def test_check_config_and_paths_reports_missing_sales_amount_column_in_batch_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw = root / "raw_data"
            raw.mkdir()
            reports = root / "reports"
            reports.mkdir()
            data_dir = root / "data"
            data_dir.mkdir()
            pd.DataFrame({"商品条码": ["1"], "商品名称": ["SKU1"], "装箱数（因子）": [6]}).to_excel(data_dir / "sku装箱数.xlsx", index=False)
            system_raw = raw / "陕西华润"
            system_raw.mkdir()
            pd.DataFrame({"门店名称": ["A店"], "商品名称": ["SKU1"], "商品条码": ["1"], "库存数量": [1]}).to_excel(system_raw / "陕西华润库存.xlsx", index=False)
            pd.DataFrame(
                {"门店名称": ["A店"], "商品名称": ["SKU1"], "商品条码": ["1"], "销售数量": [1], "销售时间": ["2026-02-01"]}
            ).to_excel(system_raw / "陕西华润销售202602.xlsx", index=False)

            cfg = {
                "run_mode": "batch",
                "raw_data_dir": str(raw),
                "inventory_file": "",
                "sales_files": [],
                "output_file": "",
                "carton_factor_file": str(data_dir / "sku装箱数.xlsx"),
                "risk_days_high": 60,
                "risk_days_low": 45,
                "sales_window_full_months": 3,
                "sales_window_include_mtd": True,
                "sales_window_recent_days": 30,
                "season_mode": False,
                "enable_ranked_store_transfer_summary": True,
                "brand_keywords": ["测试"],
                "batch": {
                    "continue_on_error": True,
                    "summary_output_file": "./reports/batch_run_summary.xlsx",
                    "systems": [
                        {
                            "enabled": True,
                            "system_id": "shaanxi_huarun",
                            "display_name": "陕西华润",
                            "data_subdir": "陕西华润",
                            "sales_files": ["陕西华润销售202602.xlsx"],
                            "inventory_file": "陕西华润库存.xlsx",
                            "carton_factor_file": str(data_dir / "sku装箱数.xlsx"),
                        }
                    ],
                },
            }

            with (
                patch("scripts.health_check.BASE_DIR", root),
                patch("scripts.health_check.CONFIG_PATH", root / "config.yaml"),
                patch("scripts.health_check.core_config.load_config", return_value=cfg),
            ):
                errors = health_check._check_config_and_paths()
            self.assertTrue(any("sales amount column" in err for err in errors))


if __name__ == "__main__":
    unittest.main()
