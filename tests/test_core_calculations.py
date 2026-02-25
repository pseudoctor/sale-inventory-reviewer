import unittest
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from scripts.generate_inventory_risk_report import (
    apply_inventory_metrics,
    classify_risk_levels,
    combine_daily_sales,
    compute_case_counts,
    extract_month_key,
    generate_report_for_system,
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
from scripts.core import matching as core_matching
from scripts.core import output_tables as core_output_tables
from scripts.core import pipeline as core_pipeline
from tests.helpers import build_single_config, write_excel


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
                    "brand_keywords": [],
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

    def test_normalize_sales_df_parses_thousand_separator_qty(self):
        df = pd.DataFrame(
            {
                "门店名称": ["A店"],
                "商品名称": ["SKU1"],
                "商品条码": ["123"],
                "销售数量": ["1,234"],
                "销售时间": ["2026-02-01"],
            }
        )
        out_df, _, _, _, _, qty_col, _, _ = normalize_sales_df(df)
        self.assertEqual(float(out_df[qty_col].iloc[0]), 1234.0)
        self.assertEqual(int(out_df.attrs.get("invalid_qty_rows", -1)), 0)

    def test_normalize_inventory_df_tracks_invalid_qty_rows(self):
        df = pd.DataFrame(
            {
                "门店名称": ["A店", "A店", "A店"],
                "商品名称": ["SKU1", "SKU2", "SKU3"],
                "商品条码": ["1", "2", "3"],
                "当前库存": ["12", "bad", ""],
            }
        )
        out_df, _, _, _, _, qty_col, _ = normalize_inventory_df(df)
        self.assertEqual(out_df[qty_col].tolist(), [12.0, 0.0, 0.0])
        self.assertEqual(int(out_df.attrs.get("invalid_qty_rows", -1)), 1)

    def test_resolve_sales_candidates_respects_configured_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            custom = root / "sales_custom_name.xlsx"
            custom.touch()
            out = resolve_sales_candidates(root, ["sales_custom_name.xlsx"])
            self.assertEqual([p.resolve() for p in out], [custom.resolve()])

    def test_resolve_sales_candidates_rejects_configured_path_outside_raw_data_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError):
                resolve_sales_candidates(root, ["../outside.xlsx"])

    def test_resolve_sales_candidates_autodetect_by_yyyymm(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "foo.txt").touch()
            jan = root / "销售202601.xlsx"
            dec = root / "销售202512.xlsx"
            feb_xls = root / "销售202602.xls"
            invalid_month = root / "销售202613.xlsx"
            bad = root / "sales.xlsx"
            inv = root / "库存202602.xlsx"
            jan.touch()
            dec.touch()
            feb_xls.touch()
            invalid_month.touch()
            bad.touch()
            inv.touch()
            out = resolve_sales_candidates(root, [])
            self.assertEqual(out, [dec, jan, feb_xls])

    def test_extract_month_key_rejects_invalid_month(self):
        self.assertEqual(extract_month_key("销售202612.xlsx"), 202612)
        self.assertIsNone(extract_month_key("销售202613.xlsx"))

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

    def test_load_carton_factor_cached_reload_when_file_mtime_changed(self):
        core_pipeline._CARTON_FACTOR_CACHE.clear()
        fake_path = Path("/tmp/fake_factor.xlsx")
        first_df = pd.DataFrame({"商品条码": ["1"], "商品名称": ["A"], "装箱数（因子）": [6]})
        second_df = pd.DataFrame({"商品条码": ["1"], "商品名称": ["A"], "装箱数（因子）": [8]})

        with (
            patch("scripts.core.pipeline.core_io.load_carton_factor_df", side_effect=[first_df, second_df]) as mocked_loader,
            patch.object(
                Path,
                "stat",
                side_effect=[
                    SimpleNamespace(st_mtime_ns=100),
                    SimpleNamespace(st_mtime_ns=100),
                    SimpleNamespace(st_mtime_ns=200),
                ],
            ),
        ):
            out1 = core_pipeline._load_carton_factor_cached(fake_path)
            out2 = core_pipeline._load_carton_factor_cached(fake_path)
            out3 = core_pipeline._load_carton_factor_cached(fake_path)

        self.assertIs(out1, first_df)
        self.assertIs(out2, first_df)
        self.assertIs(out3, second_df)
        self.assertEqual(mocked_loader.call_count, 2)

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

    def test_ensure_inventory_brand_column_supports_product_alias(self):
        df = pd.DataFrame(
            {
                "门店名称": ["A店"],
                "商品": ["伊利高钙奶"],
                "品牌": [""],
                "库存数量": [1],
            }
        )
        out = core_io.ensure_inventory_brand_column(df, ["伊利", "蒙牛"])
        self.assertEqual(out["品牌"].tolist(), ["伊利"])

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

    def test_store_and_brand_summary_forecast_daily_sales_rounded_to_3_decimals(self):
        detail = pd.DataFrame(
            columns=[
                "store",
                "brand",
                "barcode_output",
                "product",
                "province",
                "daily_sales_3m_mtd",
                "daily_sales_30d",
                "inventory_qty",
                "out_of_stock",
                "risk_level",
                "inventory_sales_ratio",
                "turnover_rate",
                "turnover_days",
                "suggest_outbound_qty",
                "suggest_replenish_qty",
                "name_source_rule",
                "brand_source_rule",
                "name_conflict_count",
                "brand_conflict_count",
            ]
        )
        missing_sales = pd.DataFrame(
            columns=[
                "store",
                "brand",
                "display_barcode",
                "barcode",
                "product",
                "province",
                "daily_sales_3m_mtd",
                "daily_sales_30d",
            ]
        )
        store_summary = pd.DataFrame(
            {
                "store": ["A店"],
                "daily_sales_3m_mtd": [1.0],
                "daily_sales_30d": [1.0],
                "forecast_daily_sales": [1.23456],
                "inventory_qty": [10],
                "risk_level": ["中"],
                "inventory_sales_ratio": [10.0],
                "turnover_rate": [0.1],
                "turnover_days": [10.0],
            }
        )
        brand_summary = pd.DataFrame(
            {
                "brand": ["品牌A"],
                "daily_sales_3m_mtd": [1.0],
                "daily_sales_30d": [1.0],
                "forecast_daily_sales": [2.34567],
                "inventory_qty": [10],
                "risk_level": ["中"],
                "inventory_sales_ratio": [10.0],
                "turnover_rate": [0.1],
                "turnover_days": [10.0],
            }
        )
        carton_factor_df = pd.DataFrame(columns=["商品条码", "商品名称", "装箱数（因子）"])

        frames = core_output_tables.build_report_frames(
            detail=detail,
            missing_sales=missing_sales,
            store_summary=store_summary,
            brand_summary=brand_summary,
            carton_factor_df=carton_factor_df,
            is_wumei_system=False,
            enable_province_column=False,
            use_peak_mode=False,
        )

        self.assertEqual(float(frames["门店汇总"]["预测平均日销(季节模式后)"].iloc[0]), 1.235)
        self.assertEqual(float(frames["品牌汇总"]["预测平均日销(季节模式后)"].iloc[0]), 2.346)

    def test_matching_uses_store_barcode_key_and_latest_name_brand(self):
        sales_df = pd.DataFrame(
            {
                "store": ["门店A", "门店A", "门店A"],
                "brand": ["品牌旧", "品牌新", "品牌X"],
                "product": ["旧名称", "新名称", "独立SKU"],
                "barcode": ["6901", "6901", "6902"],
                "display_barcode": ["6901", "6901", "6902"],
                "sales_qty": [3, 7, 5],
                "sales_date": pd.to_datetime(["2026-02-01", "2026-02-08", "2026-02-08"]),
                "supplier_card": [None, "153085", "153085"],
            }
        )
        inv_df = pd.DataFrame(
            {
                "store": ["门店A", "门店A"],
                "brand": ["库存品牌", "库存品牌X"],
                "product": ["库存名称", "独立SKU"],
                "barcode": ["6901", "6902"],
                "inventory_qty": [20, 10],
                "supplier_card": [None, None],
            }
        )

        detail, missing_sales, store_summary, brand_summary, mapping_stats = core_matching.build_detail_with_matching(
            sales_df=sales_df,
            inv_df=inv_df,
            mtd_start=pd.Timestamp("2026-02-01"),
            mtd_end=pd.Timestamp("2026-02-09"),
            recent_start=pd.Timestamp("2026-01-11"),
            inventory_date_ts=pd.Timestamp("2026-02-09"),
            mtd_days=9,
            recent_days_effective=30,
            has_mtd_window_data=True,
            has_recent_window_data=True,
            use_peak_mode=False,
            low_days=45,
            high_days=60,
            is_wumei_system=False,
            province_mapper=lambda _: "其他/未知",
        )

        self.assertEqual(len(detail), 2)
        key_row = detail.loc[detail["barcode"] == "6901"].iloc[0]
        self.assertEqual(key_row["product"], "新名称")
        self.assertEqual(key_row["brand"], "品牌新")
        self.assertEqual(key_row["name_source_rule"], "latest_sales_name")
        self.assertEqual(key_row["brand_source_rule"], "latest_sales_brand")
        self.assertEqual(int(key_row["name_conflict_count"]), 2)
        self.assertEqual(int(key_row["brand_conflict_count"]), 2)
        self.assertEqual(mapping_stats["duplicate_store_barcode_keys"], 1)
        self.assertEqual(mapping_stats["name_conflict_keys"], 1)
        self.assertEqual(mapping_stats["brand_conflict_keys"], 1)
        self.assertEqual(len(missing_sales), 0)
        self.assertEqual(len(store_summary), 1)
        self.assertGreaterEqual(len(brand_summary), 1)

    def test_matching_tie_break_is_stable_for_same_day_records(self):
        sales_df = pd.DataFrame(
            {
                "store": ["门店A", "门店A"],
                "brand": ["品牌B", "品牌A"],
                "product": ["名称B", "名称A"],
                "barcode": ["6901", "6901"],
                "display_barcode": ["6901", "6901"],
                "sales_qty": [5, 5],
                "sales_date": pd.to_datetime(["2026-02-08", "2026-02-08"]),
                "supplier_card": ["153085", "153085"],
            }
        )
        inv_df = pd.DataFrame(
            {
                "store": ["门店A"],
                "brand": ["库存品牌"],
                "product": ["库存名称"],
                "barcode": ["6901"],
                "inventory_qty": [10],
                "supplier_card": [None],
            }
        )

        detail, *_ = core_matching.build_detail_with_matching(
            sales_df=sales_df,
            inv_df=inv_df,
            mtd_start=pd.Timestamp("2026-02-01"),
            mtd_end=pd.Timestamp("2026-02-09"),
            recent_start=pd.Timestamp("2026-01-11"),
            inventory_date_ts=pd.Timestamp("2026-02-09"),
            mtd_days=9,
            recent_days_effective=30,
            has_mtd_window_data=True,
            has_recent_window_data=True,
            use_peak_mode=False,
            low_days=45,
            high_days=60,
            is_wumei_system=False,
            province_mapper=lambda _: "其他/未知",
        )

        row = detail.iloc[0]
        self.assertEqual(row["product"], "名称B")
        self.assertEqual(row["brand"], "品牌B")

    def test_wumei_inventory_code_maps_to_national_barcode_and_avoids_split_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            system_dir = root / "raw_data" / "宁夏物美"
            data_dir = root / "data"
            reports_dir = root / "reports"
            system_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)
            reports_dir.mkdir(parents=True, exist_ok=True)

            write_excel(
                pd.DataFrame(
                {
                    "门店名称": ["物美A店"],
                    "商品编码": ["817620"],
                    "国条码": ["6970000000001"],
                    "商品名称": ["测试SKU"],
                    "品牌": ["品牌Y"],
                    "销售数量": [10],
                    "销售时间": ["2026-02-08"],
                }
                ),
                system_dir / "销售202602.xlsx",
            )
            write_excel(
                pd.DataFrame(
                {
                    "门店名称": ["物美A店"],
                    "商品编码": ["817620"],
                    "商品名称": ["测试SKU"],
                    "当前库存": [5],
                    "库存日期": ["2026-02-09"],
                }
                ),
                system_dir / "库存.xlsx",
            )
            write_excel(
                pd.DataFrame(
                {
                    "商品条码": ["6970000000001"],
                    "商品名称": ["测试SKU"],
                    "装箱数（因子）": [6],
                }
                ),
                data_dir / "sku装箱数.xlsx",
            )

            output_file = reports_dir / "宁夏物美20260209库存预警.xlsx"
            config = build_single_config(
                system_id="ningxia_wumei",
                display_name="宁夏物美",
                raw_data_dir=str(root / "raw_data"),
                data_subdir="宁夏物美",
                sales_files=["销售202602.xlsx"],
                inventory_file="库存.xlsx",
                output_file=str(output_file),
                carton_factor_file=str(data_dir / "sku装箱数.xlsx"),
                brand_keywords=["品牌Y"],
            )
            generate_report_for_system(config, config)

            detail = pd.read_excel(output_file, sheet_name="明细", header=1)
            detail["门店名称"] = detail["门店名称"].ffill()
            self.assertEqual(len(detail), 1)
            self.assertEqual(str(detail.iloc[0]["商品条码"]), "6970000000001")
            self.assertGreater(float(detail.iloc[0]["近三月+本月迄今平均日销"]), 0)

            status = pd.read_excel(output_file, sheet_name="运行状态", header=1)
            status_map = dict(zip(status["状态项"], status["值"]))
            self.assertEqual(int(status_map["物美条码映射命中行数"]), 1)
            self.assertEqual(int(status_map["物美条码映射回退行数"]), 0)

    def test_wumei_inventory_barcode_falls_back_to_product_code_when_mapping_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            system_dir = root / "raw_data" / "宁夏物美"
            data_dir = root / "data"
            reports_dir = root / "reports"
            system_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)
            reports_dir.mkdir(parents=True, exist_ok=True)

            write_excel(
                pd.DataFrame(
                {
                    "门店名称": ["物美B店"],
                    "商品编码": ["111111"],
                    "国条码": ["6991111111111"],
                    "商品名称": ["无映射销售SKU"],
                    "品牌": ["品牌M"],
                    "销售数量": [0],
                    "销售时间": ["2026-02-08"],
                }
                ),
                system_dir / "销售202602.xlsx",
            )
            write_excel(
                pd.DataFrame(
                {
                    "门店名称": ["物美B店"],
                    "商品编码": ["222222"],
                    "商品名称": ["库存SKU"],
                    "当前库存": [9],
                    "库存日期": ["2026-02-09"],
                }
                ),
                system_dir / "库存.xlsx",
            )
            write_excel(
                pd.DataFrame(
                {
                    "商品条码": ["6991111111111"],
                    "商品名称": ["无映射销售SKU"],
                    "装箱数（因子）": [6],
                }
                ),
                data_dir / "sku装箱数.xlsx",
            )

            output_file = reports_dir / "宁夏物美20260209库存预警.xlsx"
            config = build_single_config(
                system_id="ningxia_wumei",
                display_name="宁夏物美",
                raw_data_dir=str(root / "raw_data"),
                data_subdir="宁夏物美",
                sales_files=["销售202602.xlsx"],
                inventory_file="库存.xlsx",
                output_file=str(output_file),
                carton_factor_file=str(data_dir / "sku装箱数.xlsx"),
                brand_keywords=["品牌M"],
            )
            generate_report_for_system(config, config)

            detail = pd.read_excel(output_file, sheet_name="明细", header=1)
            detail["门店名称"] = detail["门店名称"].ffill()
            self.assertEqual(len(detail), 1)
            self.assertEqual(str(detail.iloc[0]["商品条码"]), "222222")

            status = pd.read_excel(output_file, sheet_name="运行状态", header=1)
            status_map = dict(zip(status["状态项"], status["值"]))
            self.assertEqual(int(status_map["物美条码映射命中行数"]), 0)
            self.assertEqual(int(status_map["物美条码映射回退行数"]), 1)

    def test_generate_report_fails_with_clear_message_when_all_sales_dates_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            system_dir = root / "raw_data" / "测试系统"
            data_dir = root / "data"
            reports_dir = root / "reports"
            system_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)
            reports_dir.mkdir(parents=True, exist_ok=True)

            write_excel(
                pd.DataFrame(
                    {
                        "门店名称": ["测试店"],
                        "商品名称": ["测试SKU"],
                        "商品条码": ["6900000000001"],
                        "销售数量": [5],
                        "销售时间": ["not-a-date"],
                    }
                ),
                system_dir / "销售202602.xlsx",
            )
            write_excel(
                pd.DataFrame(
                    {
                        "门店名称": ["测试店"],
                        "商品名称": ["测试SKU"],
                        "商品条码": ["6900000000001"],
                        "当前库存": [10],
                        "库存日期": ["2026-02-10"],
                    }
                ),
                system_dir / "库存.xlsx",
            )
            write_excel(
                pd.DataFrame(
                    {
                        "商品条码": ["6900000000001"],
                        "商品名称": ["测试SKU"],
                        "装箱数（因子）": [6],
                    }
                ),
                data_dir / "sku装箱数.xlsx",
            )

            config = build_single_config(
                system_id="demo",
                display_name="测试系统",
                raw_data_dir=str(root / "raw_data"),
                data_subdir="测试系统",
                sales_files=["销售202602.xlsx"],
                inventory_file="库存.xlsx",
                output_file=str(reports_dir / "测试系统20260210库存预警.xlsx"),
                carton_factor_file=str(data_dir / "sku装箱数.xlsx"),
                brand_keywords=["测试"],
            )

            with self.assertRaises(RuntimeError) as ctx:
                generate_report_for_system(config, config)
            self.assertIn("[normalize]", str(ctx.exception))
            self.assertIn("No valid sales rows after parsing dates", str(ctx.exception))

    def test_generate_report_marks_missing_inventory_as_input_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            system_dir = root / "raw_data" / "测试系统"
            data_dir = root / "data"
            reports_dir = root / "reports"
            system_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)
            reports_dir.mkdir(parents=True, exist_ok=True)

            write_excel(
                pd.DataFrame(
                    {
                        "门店名称": ["测试店"],
                        "商品名称": ["测试SKU"],
                        "商品条码": ["6900000000001"],
                        "销售数量": [5],
                        "销售时间": ["2026-02-10"],
                    }
                ),
                system_dir / "销售202602.xlsx",
            )
            write_excel(
                pd.DataFrame(
                    {
                        "商品条码": ["6900000000001"],
                        "商品名称": ["测试SKU"],
                        "装箱数（因子）": [6],
                    }
                ),
                data_dir / "sku装箱数.xlsx",
            )

            config = build_single_config(
                system_id="demo",
                display_name="测试系统",
                raw_data_dir=str(root / "raw_data"),
                data_subdir="测试系统",
                sales_files=["销售202602.xlsx"],
                inventory_file="缺失库存.xlsx",
                output_file=str(reports_dir / "测试系统20260210库存预警.xlsx"),
                carton_factor_file=str(data_dir / "sku装箱数.xlsx"),
                brand_keywords=["测试"],
            )

            with self.assertRaises(RuntimeError) as ctx:
                generate_report_for_system(config, config)
            self.assertIn("[input_read]", str(ctx.exception))
            self.assertIn("Inventory file not found", str(ctx.exception))

    def test_status_reports_invalid_numeric_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            system_dir = root / "raw_data" / "陕西华润"
            data_dir = root / "data"
            reports_dir = root / "reports"
            system_dir.mkdir(parents=True, exist_ok=True)
            data_dir.mkdir(parents=True, exist_ok=True)
            reports_dir.mkdir(parents=True, exist_ok=True)

            write_excel(
                pd.DataFrame(
                {
                    "门店名称": ["门店A", "门店A"],
                    "商品名称": ["SKU1", "SKU2"],
                    "商品条码": ["6901", "6902"],
                    "销售数量": ["1,200", "bad"],
                    "销售时间": ["2026-02-08", "2026-02-08"],
                }
                ),
                system_dir / "销售202602.xlsx",
            )
            write_excel(
                pd.DataFrame(
                {
                    "门店名称": ["门店A"],
                    "商品名称": ["SKU1"],
                    "商品条码": ["6901"],
                    "库存数量": ["oops"],
                    "库存日期": ["2026-02-09"],
                }
                ),
                system_dir / "库存.xlsx",
            )
            write_excel(
                pd.DataFrame(
                {
                    "商品条码": ["6901"],
                    "商品名称": ["SKU1"],
                    "装箱数（因子）": [6],
                }
                ),
                data_dir / "sku装箱数.xlsx",
            )

            output_file = reports_dir / "陕西华润20260209库存预警.xlsx"
            config = build_single_config(
                system_id="shaanxi_huarun",
                display_name="陕西华润",
                raw_data_dir=str(root / "raw_data"),
                data_subdir="陕西华润",
                sales_files=["销售202602.xlsx"],
                inventory_file="库存.xlsx",
                output_file=str(output_file),
                carton_factor_file=str(data_dir / "sku装箱数.xlsx"),
                brand_keywords=["品牌X"],
            )
            generate_report_for_system(config, config)

            status = pd.read_excel(output_file, sheet_name="运行状态", header=1)
            status_map = dict(zip(status["状态项"], status["值"]))
            self.assertEqual(int(status_map["销售数量解析失败行数"]), 1)
            self.assertEqual(int(status_map["库存数量解析失败行数"]), 1)


if __name__ == "__main__":
    unittest.main()
