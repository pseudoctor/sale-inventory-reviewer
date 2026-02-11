import unittest
from pathlib import Path
import tempfile
from unittest.mock import patch

from scripts import health_check


class HealthCheckTest(unittest.TestCase):
    def test_check_python_returns_no_error_on_supported_runtime(self):
        errors = health_check._check_python()
        self.assertIsInstance(errors, list)

    def test_check_dependencies_reports_missing_package(self):
        def fake_import(name):
            if name == "xlrd":
                raise ModuleNotFoundError("missing xlrd")
            return object()

        with patch("scripts.health_check.importlib.import_module", side_effect=fake_import):
            errors = health_check._check_dependencies()
        self.assertTrue(any("xlrd" in err for err in errors))

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


if __name__ == "__main__":
    unittest.main()
