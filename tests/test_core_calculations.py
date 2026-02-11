import unittest
from pathlib import Path
import tempfile
from unittest.mock import patch

import pandas as pd

from scripts.generate_inventory_risk_report import (
    apply_inventory_metrics,
    classify_risk_levels,
    combine_daily_sales,
    compute_case_counts,
    list_ignored_sales_files,
    map_province_by_supplier_card,
    normalize_barcode_value,
    normalize_inventory_df,
    normalize_sales_df,
    read_excel_first_sheet,
    overlap_days,
    parse_sales_dates,
    resolve_sales_candidates,
    validate_config,
)
from scripts.core import io as core_io
from scripts.core import pipeline as core_pipeline


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
                    "brand_keywords": [],
                }
            )

    def test_effective_brand_keywords_rejects_empty(self):
        with self.assertRaises(ValueError):
            core_pipeline._effective_brand_keywords({"brand_keywords": []})

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

    def test_map_province_by_supplier_card(self):
        self.assertEqual(map_province_by_supplier_card("153085"), "宁夏")
        self.assertEqual(map_province_by_supplier_card("680249.0"), "甘肃")
        self.assertEqual(map_province_by_supplier_card("153412"), "宁夏")
        self.assertEqual(map_province_by_supplier_card("152901"), "监狱系统")
        self.assertEqual(map_province_by_supplier_card("999999"), "其他/未知")
        self.assertEqual(map_province_by_supplier_card(None), "其他/未知")

    def test_normalize_inventory_df_supports_current_inventory_column(self):
        df = pd.DataFrame(
            {
                "门店名称": ["A店"],
                "商品名称": ["SKU1"],
                "商品条码": ["123"],
                "当前库存": ["10"],
                "供商卡号": ["153085"],
            }
        )
        out_df, _, _, _, _, qty_col, supplier_col = normalize_inventory_df(df)
        self.assertEqual(qty_col, "当前库存")
        self.assertEqual(supplier_col, "供商卡号")
        self.assertEqual(float(out_df[qty_col].iloc[0]), 10.0)

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
            jan = root / "销售202601.xlsx"
            dec = root / "销售202512.xlsx"
            feb_xls = root / "销售202602.xls"
            bad = root / "sales.xlsx"
            inv = root / "库存202602.xlsx"
            jan.touch()
            dec.touch()
            feb_xls.touch()
            bad.touch()
            inv.touch()
            out = resolve_sales_candidates(root, [])
            self.assertEqual(out, [dec, jan, feb_xls])

    def test_list_ignored_sales_files_reports_non_sales_and_inventory_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "库存202601.xlsx").touch()
            (root / "202601.xlsx").touch()
            (root / "销售文件.xlsx").touch()
            ignored = list_ignored_sales_files(root, [])
            self.assertTrue(any("inventory-like" in x for x in ignored))
            self.assertTrue(any("missing sales keyword" in x for x in ignored))

    def test_normalize_sales_df_allows_missing_brand_column(self):
        df = pd.DataFrame(
            {
                "门店名称": ["A店"],
                "商品名称": ["SKU1"],
                "商品条码": ["123"],
                "销售数量": ["2"],
                "销售时间": ["2026-02-01"],
            }
        )
        _, store_col, brand_col, product_col, barcode_col, qty_col, date_col, supplier_col = normalize_sales_df(df)
        self.assertEqual(store_col, "门店名称")
        self.assertIsNone(brand_col)
        self.assertEqual(product_col, "商品名称")
        self.assertEqual(barcode_col, "商品条码")
        self.assertEqual(qty_col, "销售数量")
        self.assertEqual(date_col, "销售时间")
        self.assertIsNone(supplier_col)

    def test_normalize_sales_df_supports_national_barcode_only(self):
        df = pd.DataFrame(
            {
                "门店名称": ["A店"],
                "商品名称": ["SKU1"],
                "国条码": ["6901234567890"],
                "销售数量": ["2"],
                "销售时间": ["2026-02-01"],
            }
        )
        _, _, _, _, barcode_col, _, _, _ = normalize_sales_df(df)
        self.assertEqual(barcode_col, "国条码")

    def test_load_carton_factor_cached_hits_loader_once(self):
        core_pipeline._CARTON_FACTOR_CACHE.clear()
        fake_path = Path("/tmp/fake_factor.xlsx")
        fake_df = pd.DataFrame({"商品条码": ["1"], "商品名称": ["A"], "装箱数（因子）": [6]})
        with patch("scripts.core.pipeline.core_io.load_carton_factor_df", return_value=fake_df) as mocked:
            out1 = core_pipeline._load_carton_factor_cached(fake_path)
            out2 = core_pipeline._load_carton_factor_cached(fake_path)
        self.assertIs(out1, fake_df)
        self.assertIs(out2, fake_df)
        self.assertEqual(mocked.call_count, 1)

    def test_normalize_sales_df_prefers_national_barcode_when_multiple_exist(self):
        df = pd.DataFrame(
            {
                "门店名称": ["A店"],
                "商品名称": ["SKU1"],
                "国条码": ["6901234567890"],
                "商品编码": ["871602"],
                "销售数量": ["2"],
                "销售时间": ["2026-02-01"],
            }
        )
        _, _, _, _, barcode_col, _, _, _ = normalize_sales_df(df)
        self.assertEqual(barcode_col, "国条码")

    def test_normalize_inventory_df_supports_national_barcode_only(self):
        df = pd.DataFrame(
            {
                "门店名称": ["A店"],
                "商品名称": ["SKU1"],
                "国条码": ["6901234567890"],
                "当前库存": ["10"],
            }
        )
        out_df, _, _, _, barcode_col, qty_col, _ = normalize_inventory_df(df)
        self.assertEqual(barcode_col, "国条码")
        self.assertEqual(qty_col, "当前库存")
        self.assertEqual(float(out_df[qty_col].iloc[0]), 10.0)

    def test_normalize_inventory_df_prefers_national_barcode_when_multiple_exist(self):
        df = pd.DataFrame(
            {
                "门店名称": ["A店"],
                "商品名称": ["SKU1"],
                "国条码": ["6901234567890"],
                "商品编码": ["871602"],
                "当前库存": ["10"],
            }
        )
        _, _, _, _, barcode_col, _, _ = normalize_inventory_df(df)
        self.assertEqual(barcode_col, "国条码")

    def test_read_excel_first_sheet_requires_xlrd_for_xls(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test.xls"
            path.touch()
            with patch("scripts.core.io.importlib.util.find_spec", return_value=None):
                with self.assertRaises(ModuleNotFoundError):
                    read_excel_first_sheet(path)

    def test_extract_brand_from_product_prefers_earliest_position(self):
        product = "特选奶源伊利蒙牛联名款"
        brands = ["蒙牛", "伊利"]
        out = core_io.extract_brand_from_product(product, brands)
        self.assertEqual(out, "伊利")

    def test_ensure_inventory_brand_column_fills_missing_and_blank(self):
        df = pd.DataFrame(
            {
                "门店名称": ["A店", "A店", "A店"],
                "商品名称": ["伊利高钙奶", "蒙牛纯牛奶", "无品牌商品"],
                "品牌": [None, "", "  "],
                "库存数量": [1, 2, 3],
            }
        )
        out = core_io.ensure_inventory_brand_column(df, ["伊利", "蒙牛"])
        self.assertEqual(out["品牌"].tolist(), ["伊利", "蒙牛", "其他"])

    def test_ensure_sales_brand_column_adds_brand_when_column_missing(self):
        df = pd.DataFrame(
            {
                "store": ["A店", "A店"],
                "product": ["伊利高钙奶", "未知商品"],
                "barcode": ["1", "2"],
                "sales_qty": [1, 2],
            }
        )
        out = core_io.ensure_sales_brand_column(df, ["伊利", "蒙牛"])
        self.assertEqual(out["brand"].tolist(), ["伊利", "其他"])


if __name__ == "__main__":
    unittest.main()
