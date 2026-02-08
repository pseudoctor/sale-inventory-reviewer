import unittest

import pandas as pd

from scripts.generate_inventory_risk_report import (
    classify_risk_levels,
    combine_daily_sales,
    normalize_barcode_value,
    overlap_days,
    parse_sales_dates,
    validate_config,
)


class CoreCalculationsTest(unittest.TestCase):
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
                    "brand_keywords": [],
                }
            )

    def test_overlap_days_with_overlap(self):
        start = pd.Timestamp("2025-11-01")
        end = pd.Timestamp("2026-02-06")
        data_min = pd.Timestamp("2025-12-01")
        data_max = pd.Timestamp("2026-01-31")
        self.assertEqual(overlap_days(start, end, data_min, data_max), 62)

    def test_overlap_days_without_overlap_returns_zero(self):
        start = pd.Timestamp("2025-11-01")
        end = pd.Timestamp("2025-11-30")
        data_min = pd.Timestamp("2025-12-01")
        data_max = pd.Timestamp("2026-01-31")
        self.assertEqual(overlap_days(start, end, data_min, data_max), 0)

    def test_combine_daily_sales_peak_mode(self):
        a = pd.Series([1.0, 2.0, 3.0])
        b = pd.Series([3.0, 1.0, 3.5])
        out = combine_daily_sales(a, b, True)
        self.assertEqual(out.tolist(), [3.0, 2.0, 3.5])

    def test_combine_daily_sales_off_peak_mode(self):
        a = pd.Series([1.0, 2.0, 3.0])
        b = pd.Series([3.0, 1.0, 3.5])
        out = combine_daily_sales(a, b, False)
        self.assertEqual(out.tolist(), [1.0, 1.0, 3.0])

    def test_classify_risk_levels(self):
        days = pd.Series([30, 45, 50, 61, float("inf")])
        out = classify_risk_levels(days, 45, 60)
        self.assertEqual(out.tolist(), ["低", "中", "中", "高", "高"])

    def test_normalize_barcode_value(self):
        self.assertEqual(normalize_barcode_value(6907992633671.0), "6907992633671")
        self.assertEqual(normalize_barcode_value("6907992633671.0"), "6907992633671")
        self.assertEqual(normalize_barcode_value("6.907992633671E12"), "6907992633671")
        self.assertEqual(normalize_barcode_value(" 6907992633671 "), "6907992633671")
        self.assertIsNone(normalize_barcode_value("nan"))

    def test_parse_sales_dates_with_explicit_format(self):
        raw = pd.Series(["2026/02/01", "2026/02/02", "bad"])
        parsed = parse_sales_dates(raw, "%Y/%m/%d", dayfirst=False)
        self.assertEqual(parsed.notna().sum(), 2)

    def test_parse_sales_dates_with_dayfirst(self):
        raw = pd.Series(["01/02/2026"])
        parsed = parse_sales_dates(raw, "", dayfirst=True)
        self.assertEqual(str(parsed.iloc[0].date()), "2026-02-01")


if __name__ == "__main__":
    unittest.main()
