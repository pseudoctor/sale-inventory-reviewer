import unittest
from pathlib import Path
import tempfile

import pandas as pd

from scripts.generate_inventory_risk_report import (
    apply_inventory_metrics,
    classify_risk_levels,
    combine_daily_sales,
    compute_case_counts,
    normalize_barcode_value,
    overlap_days,
    parse_sales_dates,
    resolve_sales_candidates,
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

    def test_apply_inventory_metrics_uses_precise_turnover_for_risk(self):
        df = pd.DataFrame(
            {
                "forecast_daily_sales": [0.2921348315, 1.0],
                "inventory_qty": [13, 30],
            }
        )
        out = apply_inventory_metrics(df, low_days=45, high_days=60)
        self.assertEqual(out["risk_level"].tolist(), ["低", "低"])
        self.assertEqual(out["turnover_days"].tolist(), [44.0, 30.0])

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

    def test_compute_case_counts_peak_and_offpeak(self):
        qty = pd.Series([13, 7, 0])
        factor = pd.Series([6, 4, 5])
        peak = compute_case_counts(qty, factor, use_peak_mode=True)
        off_peak = compute_case_counts(qty, factor, use_peak_mode=False)
        self.assertEqual(peak.tolist(), [3, 2, 0])
        self.assertEqual(off_peak.tolist(), [2, 1, 0])

    def test_resolve_sales_candidates_respects_configured_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            custom = root / "sales_custom_name.xlsx"
            custom.touch()
            out = resolve_sales_candidates(root, ["sales_custom_name.xlsx"])
            self.assertEqual(out, [custom])

    def test_resolve_sales_candidates_autodetect_by_yyyymm(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "foo.txt").touch()
            jan = root / "202601.xlsx"
            dec = root / "202512.xlsx"
            bad = root / "sales.xlsx"
            jan.touch()
            dec.touch()
            bad.touch()
            out = resolve_sales_candidates(root, [])
            self.assertEqual(out, [dec, jan])


if __name__ == "__main__":
    unittest.main()
