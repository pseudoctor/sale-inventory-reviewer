import unittest

import pandas as pd

from scripts.generate_inventory_risk_report import (
    classify_risk_levels,
    combine_daily_sales,
    overlap_days,
)


class CoreCalculationsTest(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
