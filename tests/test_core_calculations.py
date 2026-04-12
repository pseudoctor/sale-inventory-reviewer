import unittest
from pathlib import Path
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from scripts.generate_inventory_risk_report import generate_report_for_system
from scripts.core.config import validate_config
from scripts.core import frame_columns as core_frame_columns
from scripts.core import io as core_io
from scripts.core import matching as core_matching
from scripts.core.metrics import apply_inventory_metrics, classify_risk_levels, combine_daily_sales, overlap_days
from scripts.core import output_tables as core_output_tables
from scripts.core import pipeline as core_pipeline
from scripts.core import frame_schema as core_frame_schema
from scripts.core import pipeline_inputs as core_pipeline_inputs
from scripts.core import pipeline_outputs as core_pipeline_outputs
from scripts.core import system_rules as core_system_rules
from scripts.core.models import ReportFrames, StatusFrameInput
from scripts.core.output_tables import compute_case_counts
from tests.helpers import build_single_config, write_excel


def _read_overview_group(output_file: Path, group_name: str) -> pd.DataFrame:
    overview = pd.read_excel(output_file, sheet_name="运行总览", header=1)
    return overview.loc[overview["分组"] == group_name].copy()


class CoreCalculationsTest(unittest.TestCase):
    def test_usage_guide_frame_contains_key_logic_rows(self):
        frames = core_output_tables.build_report_frames(
            detail=pd.DataFrame(columns=list(core_frame_columns.REPORT_FRAME_DETAIL_INPUT_COLUMNS)),
            missing_sales=pd.DataFrame(columns=list(core_frame_columns.MISSING_SALES_REQUIRED_COLUMNS + core_frame_columns.MISSING_SALES_OPTIONAL_COLUMNS)),
            store_summary=pd.DataFrame(columns=[
                "store", "daily_sales_3m_mtd", "daily_sales_30d", "forecast_daily_sales", "inventory_qty",
                "risk_level", "inventory_sales_ratio", "turnover_rate", "turnover_days",
            ]),
            brand_summary=pd.DataFrame(columns=[
                "brand", "daily_sales_3m_mtd", "daily_sales_30d", "forecast_daily_sales", "inventory_qty",
                "risk_level", "inventory_sales_ratio", "turnover_rate", "turnover_days",
            ]),
            product_code_catalog=pd.DataFrame(columns=[
                "product_code", "brand", "standard_product_name", "sales_product_name", "inventory_product_name", "source_status"
            ]),
            carton_factor_df=pd.DataFrame(columns=["商品条码", "商品名称", "装箱数（因子）"]),
            is_wumei_system=False,
            enable_province_column=False,
            use_peak_mode=False,
        )
        guide = frames["使用说明"]
        guide_map = dict(zip(guide["模块"], guide["说明"]))
        self.assertIn("缺货清单", guide_map)
        self.assertIn("库存缺失SKU清单", guide_map)
        self.assertIn("建议补货清单", guide_map)
        self.assertIn("商品编码 + 门店编码", guide_map["匹配逻辑"])
        self.assertIn("缺货清单", guide_map["易混概念区分"])

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
        self.assertEqual(core_io.normalize_barcode_value(6907992633671.0), "6907992633671")
        self.assertEqual(core_io.normalize_barcode_value("6907992633671.0"), "6907992633671")
        self.assertEqual(core_io.normalize_barcode_value("6.907992633671E12"), "6907992633671")
        self.assertEqual(core_io.normalize_barcode_value(" 6907992633671 "), "6907992633671")
        self.assertIsNone(core_io.normalize_barcode_value("nan"))

    def test_parse_sales_dates_with_explicit_format(self):
        raw = pd.Series(["2026/02/01", "2026/02/02", "bad"])
        parsed = core_io.parse_sales_dates(raw, "%Y/%m/%d", dayfirst=False)
        self.assertEqual(parsed.notna().sum(), 2)

    def test_parse_sales_dates_with_dayfirst(self):
        raw = pd.Series(["01/02/2026"])
        parsed = core_io.parse_sales_dates(raw, "", dayfirst=True)
        self.assertEqual(str(parsed.iloc[0].date()), "2026-02-01")

    def test_compute_case_counts_peak_and_offpeak(self):
        qty = pd.Series([13, 7, 1, 0, 2])
        factor = pd.Series([6, 4, 6, 5, 0])
        peak = compute_case_counts(qty, factor, use_peak_mode=True)
        off_peak = compute_case_counts(qty, factor, use_peak_mode=False)
        self.assertEqual(peak.tolist()[:4], [3, 2, 1, 0])
        self.assertEqual(off_peak.tolist()[:4], [2, 1, 1, 0])
        self.assertTrue(pd.isna(peak.iloc[4]))
        self.assertTrue(pd.isna(off_peak.iloc[4]))

    def test_map_province_by_supplier_card(self):
        self.assertEqual(core_pipeline.map_province_by_supplier_card("153085"), "宁夏")
        self.assertEqual(core_pipeline.map_province_by_supplier_card("680249.0"), "甘肃")
        self.assertEqual(core_pipeline.map_province_by_supplier_card("153412"), "宁夏")
        self.assertEqual(core_pipeline.map_province_by_supplier_card("152901"), "监狱系统")
        self.assertEqual(core_pipeline.map_province_by_supplier_card("999999"), "其他/未知")
        self.assertEqual(core_pipeline.map_province_by_supplier_card(None), "其他/未知")

    def test_resolve_system_rule_profile_defaults_to_wumei_rules(self):
        profile = core_system_rules.resolve_system_rule_profile("宁夏物美", {})
        self.assertTrue(profile.is_wumei_system)
        self.assertTrue(profile.enable_province_column)

    def test_resolve_system_rule_profile_allows_explicit_override(self):
        profile = core_system_rules.resolve_system_rule_profile("宁夏物美", {"province_column_enabled": False})
        self.assertTrue(profile.is_wumei_system)
        self.assertFalse(profile.enable_province_column)

    def test_resolve_system_rule_profile_keeps_non_wumei_default_closed(self):
        profile = core_system_rules.resolve_system_rule_profile("陕西华润", {})
        self.assertFalse(profile.is_wumei_system)
        self.assertFalse(profile.enable_province_column)

    def test_compute_window_context_returns_dataclass(self):
        sales_df = pd.DataFrame({"sales_date": pd.to_datetime(["2026-02-01", "2026-02-09"])})
        result = core_pipeline_inputs.compute_window_context(
            sales_df=sales_df,
            inventory_date_ts=pd.Timestamp("2026-02-09"),
            full_months=3,
            include_mtd=True,
            recent_days=30,
            fail_on_empty_window=False,
        )
        self.assertEqual(str(result.mtd_end.date()), "2026-02-09")
        self.assertTrue(result.has_mtd_window_data)

    def test_apply_wumei_barcode_mapping_returns_result_object(self):
        profile = core_system_rules.resolve_system_rule_profile("宁夏物美", {})
        inv_df = pd.DataFrame({"商品编码": ["1"]})
        sales_df = pd.DataFrame({"商品编码": ["1"]})
        result = core_pipeline_inputs.apply_wumei_barcode_mapping(
            inv_df=inv_df,
            sales_df=sales_df,
            profile=profile,
        )
        self.assertIs(result.inventory_df, inv_df)
        self.assertEqual(result.hits, 0)

    def test_matching_returns_result_object(self):
        sales_df = pd.DataFrame(
            {
                "store": ["A店"],
                "store_code": ["1001"],
                "product": ["SKU1"],
                "brand": ["品牌A"],
                "barcode": ["6901"],
                "product_code": ["P1"],
                "display_barcode": ["6901"],
                "sales_qty": [9],
                "sales_date": [pd.Timestamp("2026-02-08")],
                "supplier_card": ["153085"],
            }
        )
        inv_df = pd.DataFrame(
            {
                "store": ["A店"],
                "store_code": ["1001"],
                "product": ["SKU1"],
                "brand": ["品牌A"],
                "barcode": ["6901"],
                "product_code": ["P1"],
                "inventory_qty": [10],
                "supplier_card": ["153085"],
            }
        )
        result = core_matching.build_detail_with_matching(
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
            province_mapper=core_pipeline.map_province_by_supplier_card,
        )
        self.assertEqual(len(result.detail), 1)
        self.assertIn("mapping_coverage_rate", result.mapping_stats)

    def test_build_status_frame_accepts_status_input_object(self):
        status_input = StatusFrameInput(
            program_version="1.0.0",
            config={"risk_days_high": 60, "risk_days_low": 45},
            display_name="测试系统",
            system_id="test",
            inventory_date="2026-02-09",
            loaded_sales_file_count=1,
            missing_sales_files=[],
            use_peak_mode=False,
            strict_auto_scan=False,
            has_mtd_window_data=True,
            has_recent_window_data=True,
            mtd_days=9,
            recent_days_effective=30,
            invalid_sales_date_rows=0,
            invalid_sales_qty_rows=0,
            invalid_inventory_qty_rows=0,
            replenish_out=pd.DataFrame(columns=["装箱数（因子）"]),
            transfer_out=pd.DataFrame(columns=["装箱数（因子）"]),
            mapping_stats={"mapping_coverage_rate": 1.0},
            ignored_sales_files=[],
            is_wumei_system=False,
            wumei_barcode_map_hits=0,
            wumei_barcode_map_fallback=0,
            wumei_barcode_map_conflicts=0,
            wumei_barcode_conflict_samples="",
        )
        out = core_pipeline_outputs.build_status_frame(status_input)
        self.assertIn("程序版本", out["状态项"].tolist())

    def test_build_report_frames_returns_report_frames_object(self):
        frames = core_output_tables.build_report_frames(
            detail=pd.DataFrame(columns=list(core_frame_columns.REPORT_FRAME_DETAIL_INPUT_COLUMNS)),
            missing_sales=pd.DataFrame(columns=list(core_frame_columns.MISSING_SALES_REQUIRED_COLUMNS + core_frame_columns.MISSING_SALES_OPTIONAL_COLUMNS)),
            store_summary=pd.DataFrame(columns=[
                "store", "daily_sales_3m_mtd", "daily_sales_30d", "forecast_daily_sales", "inventory_qty",
                "risk_level", "inventory_sales_ratio", "turnover_rate", "turnover_days",
            ]),
            brand_summary=pd.DataFrame(columns=[
                "brand", "daily_sales_3m_mtd", "daily_sales_30d", "forecast_daily_sales", "inventory_qty",
                "risk_level", "inventory_sales_ratio", "turnover_rate", "turnover_days",
            ]),
            product_code_catalog=pd.DataFrame(columns=[
                "product_code", "brand", "standard_product_name", "sales_product_name", "inventory_product_name", "source_status"
            ]),
            carton_factor_df=pd.DataFrame(columns=["商品条码", "商品名称", "装箱数（因子）"]),
            is_wumei_system=False,
            enable_province_column=False,
            use_peak_mode=False,
        )
        self.assertIsInstance(frames, ReportFrames)
        self.assertTrue(frames.usage_guide.equals(frames["使用说明"]))

    def test_validate_frame_columns_reports_missing_required_columns(self):
        with self.assertRaises(ValueError):
            core_frame_schema.validate_frame_columns(
                pd.DataFrame({"store": ["A店"]}),
                core_frame_schema.NORMALIZED_SALES_SCHEMA,
            )

    def test_validate_frame_columns_reports_unexpected_columns_when_schema_is_strict(self):
        with self.assertRaises(ValueError):
            core_frame_schema.validate_frame_columns(
                pd.DataFrame(
                    {
                        "store": ["A店"],
                        "product": ["SKU1"],
                        "barcode": ["6901"],
                        "sales_qty": [1],
                        "sales_date": [pd.Timestamp("2026-02-01")],
                        "brand": ["品牌A"],
                        "store_code": ["1001"],
                        "product_code": ["P1"],
                        "display_barcode": ["6901"],
                        "supplier_card": ["153085"],
                        "sales_amount": [10.0],
                        "unexpected_col": ["x"],
                    }
                ),
                core_frame_schema.NORMALIZED_SALES_SCHEMA,
            )

    def test_frame_schema_contains_semantic_descriptions(self):
        self.assertEqual(core_frame_schema.NORMALIZED_SALES_SCHEMA.name, "input.sales.normalized")
        self.assertTrue(core_frame_schema.NORMALIZED_SALES_SCHEMA.description)
        self.assertIn("sales_amount", core_frame_schema.NORMALIZED_SALES_SCHEMA.column_descriptions)
        self.assertEqual(core_frame_schema.NORMALIZED_SALES_SCHEMA.required_columns, core_frame_columns.NORMALIZED_SALES_REQUIRED_COLUMNS)
        self.assertEqual(core_frame_schema.REPORT_FRAME_SCHEMAS["使用说明"].required_columns, core_frame_columns.USAGE_GUIDE_COLUMNS)

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
        out_df, _, _, _, _, qty_col, supplier_col = core_io.normalize_inventory_df(df)
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
        out_df, _, _, _, _, qty_col, _, _ = core_io.normalize_sales_df(df)
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
        out_df, _, _, _, _, qty_col, _ = core_io.normalize_inventory_df(df)
        self.assertEqual(out_df[qty_col].tolist(), [12.0, 0.0, 0.0])
        self.assertEqual(int(out_df.attrs.get("invalid_qty_rows", -1)), 1)

    def test_resolve_sales_candidates_respects_configured_names(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            custom = root / "sales_custom_name.xlsx"
            custom.touch()
            out = core_io.resolve_sales_candidates(root, ["sales_custom_name.xlsx"])
            self.assertEqual([p.resolve() for p in out], [custom.resolve()])

    def test_resolve_sales_candidates_rejects_configured_path_outside_raw_data_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(ValueError):
                core_io.resolve_sales_candidates(root, ["../outside.xlsx"])

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
            out = core_io.resolve_sales_candidates(root, [])
            self.assertEqual(out, [dec, jan, feb_xls])

    def test_extract_month_key_rejects_invalid_month(self):
        self.assertEqual(core_io.extract_month_key("销售202612.xlsx"), 202612)
        self.assertIsNone(core_io.extract_month_key("销售202613.xlsx"))

    def test_list_ignored_sales_files_reports_non_sales_and_inventory_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "库存202601.xlsx").touch()
            (root / "202601.xlsx").touch()
            (root / "销售文件.xlsx").touch()
            ignored = core_io.list_ignored_sales_files(root, [])
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
        _, store_col, brand_col, product_col, barcode_col, qty_col, date_col, supplier_col = core_io.normalize_sales_df(df)
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
        _, _, _, _, barcode_col, _, _, _ = core_io.normalize_sales_df(df)
        self.assertEqual(barcode_col, "国条码")

    def test_find_sales_amount_column_supports_common_variants(self):
        self.assertEqual(core_io.find_sales_amount_column(["销售金额", "其他列"]), "销售金额")
        self.assertEqual(core_io.find_sales_amount_column(["含税销售金额/元", "其他列"]), "含税销售金额/元")
        self.assertEqual(core_io.find_sales_amount_column(["含税销售额/元", "其他列"]), "含税销售额/元")
        self.assertIsNone(core_io.find_sales_amount_column(["销售数量", "其他列"]))

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
        _, _, _, _, barcode_col, _, _, _ = core_io.normalize_sales_df(df)
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
        out_df, _, _, _, barcode_col, qty_col, _ = core_io.normalize_inventory_df(df)
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
        _, _, _, _, barcode_col, _, _ = core_io.normalize_inventory_df(df)
        self.assertEqual(barcode_col, "国条码")

    def test_read_excel_first_sheet_requires_xlrd_for_xls(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "test.xls"
            path.touch()
            with patch("scripts.core.io.importlib.util.find_spec", return_value=None):
                with self.assertRaises(ModuleNotFoundError):
                    core_io.read_excel_first_sheet(path)

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
            product_code_catalog=pd.DataFrame(
                columns=["product_code", "brand", "standard_product_name", "sales_product_name", "inventory_product_name", "source_status"]
            ),
            carton_factor_df=carton_factor_df,
            is_wumei_system=False,
            enable_province_column=False,
            use_peak_mode=False,
        )

        self.assertEqual(float(frames["门店汇总"]["预测平均日销(季节模式后)"].iloc[0]), 1.235)
        self.assertEqual(float(frames["品牌汇总"]["预测平均日销(季节模式后)"].iloc[0]), 2.346)

    def test_replenish_case_count_uses_min_one_case_rule_in_off_peak(self):
        detail = pd.DataFrame(
            {
                "store": ["A店"],
                "brand": ["品牌A"],
                "barcode_output": ["6900000000001"],
                "product": ["SKU1"],
                "province": ["其他/未知"],
                "daily_sales_3m_mtd": [0.1],
                "daily_sales_30d": [0.1],
                "inventory_qty": [0],
                "out_of_stock": ["是"],
                "risk_level": ["低"],
                "inventory_sales_ratio": [0.0],
                "turnover_rate": [0.0],
                "turnover_days": [0.0],
                "suggest_outbound_qty": [0],
                "suggest_replenish_qty": [1],
                "name_source_rule": ["latest_sales_name"],
                "brand_source_rule": ["latest_sales_brand"],
                "name_conflict_count": [1],
                "brand_conflict_count": [1],
            }
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
                "daily_sales_3m_mtd": [0.1],
                "daily_sales_30d": [0.1],
                "forecast_daily_sales": [0.1],
                "inventory_qty": [0],
                "risk_level": ["低"],
                "inventory_sales_ratio": [0.0],
                "turnover_rate": [0.0],
                "turnover_days": [0.0],
            }
        )
        brand_summary = pd.DataFrame(
            {
                "brand": ["品牌A"],
                "daily_sales_3m_mtd": [0.1],
                "daily_sales_30d": [0.1],
                "forecast_daily_sales": [0.1],
                "inventory_qty": [0],
                "risk_level": ["低"],
                "inventory_sales_ratio": [0.0],
                "turnover_rate": [0.0],
                "turnover_days": [0.0],
            }
        )
        carton_factor_df = pd.DataFrame(
            {
                "商品条码": ["6900000000001"],
                "商品名称": ["SKU1"],
                "装箱数（因子）": [6],
            }
        )

        frames = core_output_tables.build_report_frames(
            detail=detail,
            missing_sales=missing_sales,
            store_summary=store_summary,
            brand_summary=brand_summary,
            product_code_catalog=pd.DataFrame(
                columns=["product_code", "brand", "standard_product_name", "sales_product_name", "inventory_product_name", "source_status"]
            ),
            carton_factor_df=carton_factor_df,
            is_wumei_system=False,
            enable_province_column=False,
            use_peak_mode=False,
        )

        replenish = frames["建议补货清单"]
        self.assertEqual(replenish.iloc[0]["建议补货数量"], 1)
        self.assertEqual(replenish.iloc[0]["建议补货箱数"], "1件")

    def test_build_store_sales_ranking_transfer_frame_uses_mtd_zero_for_full_outbound(self):
        detail = pd.DataFrame(
            {
                "store_key": ["S1", "S1"],
                "product_key": ["P1", "P2"],
                "store": ["门店A", "门店A"],
                "brand": ["品牌A", "品牌B"],
                "barcode": ["6901", "6902"],
                "barcode_output": ["6901", "6902"],
                "product": ["SKU1", "SKU2"],
                "daily_sales_3m_mtd": [0.0, 0.5],
                "daily_sales_30d": [0.0, 0.1],
                "forecast_daily_sales": [0.0, 0.5],
                "inventory_qty": [10, 20],
                "supplier_card": [None, None],
                "province": ["其他/未知", "其他/未知"],
                "risk_level": ["高", "高"],
                "inventory_sales_ratio": [float("inf"), 40.0],
                "turnover_rate": [0.0, 0.75],
                "turnover_days": [float("inf"), 40.0],
                "name_source_rule": ["latest_sales_name", "latest_sales_name"],
                "brand_source_rule": ["latest_sales_brand", "latest_sales_brand"],
                "name_conflict_count": [1, 1],
                "brand_conflict_count": [1, 1],
                "out_of_stock": ["否", "否"],
                "suggest_outbound_qty": [0, 6],
                "suggest_replenish_qty": [0, 0],
            }
        )
        sales_df = pd.DataFrame(
            {
                "store": ["门店A", "门店A", "门店B"],
                "store_code": ["S1", "S1", "S2"],
                "product": ["SKU1", "SKU2", "SKU1"],
                "barcode": ["6901", "6902", "6901"],
                "product_code": ["P1", "P2", "P1"],
                "sales_qty": [0, 5, 20],
                "sales_amount": [100.0, 50.0, 300.0],
                "sales_date": pd.to_datetime(["2026-02-01", "2026-02-02", "2026-01-15"]),
            }
        )

        out = core_pipeline._build_store_sales_ranking_transfer_frame(
            detail,
            sales_df,
            pd.Timestamp("2026-02-01"),
            pd.Timestamp("2026-02-02"),
            "2026-02-01至2026-02-02",
        )

        self.assertEqual(out["门店销售额总计(2026-02-01至2026-02-02)"].tolist(), [150.0, 150.0])
        self.assertEqual(out["商品销售额(2026-02-01至2026-02-02)"].tolist(), [100.0, 50.0])
        self.assertEqual(out["商品单价"].tolist(), [0.0, 10.0])
        self.assertEqual(out["库存金额"].tolist(), [0.0, 200.0])
        self.assertEqual(out["调货数量"].tolist(), [10, 6])
        self.assertEqual(out["排名"].tolist(), [1, 1])

    def test_matching_uses_store_code_product_code_and_latest_name_brand(self):
        sales_df = pd.DataFrame(
            {
                "store": ["门店A", "门店A", "门店A"],
                "store_code": ["S1", "S1", "S1"],
                "brand": ["品牌旧", "品牌新", "品牌X"],
                "product": ["旧名称", "新名称", "独立SKU"],
                "barcode": ["6901", "6909", "6902"],
                "product_code": ["P1", "P1", "P2"],
                "display_barcode": ["6901", "6901", "6902"],
                "sales_qty": [3, 7, 5],
                "sales_date": pd.to_datetime(["2026-02-01", "2026-02-08", "2026-02-08"]),
                "supplier_card": [None, "153085", "153085"],
            }
        )
        inv_df = pd.DataFrame(
            {
                "store": ["门店A", "门店A"],
                "store_code": ["S1", "S1"],
                "brand": ["库存品牌", "库存品牌X"],
                "product": ["库存名称", "独立SKU"],
                "barcode": ["INV-P1", "6902"],
                "product_code": ["P1", "P2"],
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
        key_row = detail.loc[detail["barcode"] == "INV-P1"].iloc[0]
        self.assertEqual(key_row["product"], "新名称")
        self.assertEqual(key_row["brand"], "品牌新")
        self.assertEqual(key_row["name_source_rule"], "latest_sales_name")
        self.assertEqual(key_row["brand_source_rule"], "latest_sales_brand")
        self.assertEqual(int(key_row["name_conflict_count"]), 2)
        self.assertEqual(int(key_row["brand_conflict_count"]), 2)
        self.assertEqual(mapping_stats["duplicate_store_product_keys"], 1)
        self.assertEqual(mapping_stats["name_conflict_keys"], 1)
        self.assertEqual(mapping_stats["brand_conflict_keys"], 1)
        self.assertEqual(len(missing_sales), 0)
        self.assertEqual(len(store_summary), 1)
        self.assertGreaterEqual(len(brand_summary), 1)

    def test_matching_tie_break_is_stable_for_same_day_records(self):
        sales_df = pd.DataFrame(
            {
                "store": ["门店A", "门店A"],
                "store_code": ["S1", "S1"],
                "brand": ["品牌B", "品牌A"],
                "product": ["名称B", "名称A"],
                "barcode": ["6901", "6909"],
                "product_code": ["P1", "P1"],
                "display_barcode": ["6901", "6901"],
                "sales_qty": [5, 5],
                "sales_date": pd.to_datetime(["2026-02-08", "2026-02-08"]),
                "supplier_card": ["153085", "153085"],
            }
        )
        inv_df = pd.DataFrame(
            {
                "store": ["门店A"],
                "store_code": ["S1"],
                "brand": ["库存品牌"],
                "product": ["库存名称"],
                "barcode": ["INV-P1"],
                "product_code": ["P1"],
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

    def test_wumei_matches_by_store_code_and_product_code_and_keeps_national_barcode_output(self):
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
                    "门店编码": ["1001"],
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
                    "门店编码": ["1001"],
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

            status = _read_overview_group(output_file, "运行状态")
            status_map = dict(zip(status["指标"], status["数值"]))
            self.assertEqual(int(status_map["同店同商品编码重复键数"]), 0)

    def test_wumei_different_product_code_does_not_match_even_when_national_barcode_exists(self):
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
                    "门店编码": ["1002"],
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
                    "门店编码": ["1002"],
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
            self.assertEqual(float(detail.iloc[0]["近三月+本月迄今平均日销"]), 0.0)

            status = _read_overview_group(output_file, "运行状态")
            status_map = dict(zip(status["指标"], status["数值"]))
            self.assertEqual(int(status_map["同店同商品编码重复键数"]), 0)

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

            status = _read_overview_group(output_file, "运行状态")
            status_map = dict(zip(status["指标"], status["数值"]))
            self.assertEqual(int(status_map["销售数量解析失败行数"]), 1)
            self.assertEqual(int(status_map["库存数量解析失败行数"]), 1)

    def test_product_code_catalog_sheet_contains_source_status(self):
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
                        "门店编码": ["101", "101"],
                        "门店名称": ["门店A", "门店A"],
                        "商品编码": ["P1", "P2"],
                        "商品条码": ["6901", "6902"],
                        "商品名称": ["销售SKU1", "仅销售SKU"],
                        "品牌": ["品牌A", "品牌A"],
                        "销售数量": [10, 5],
                        "销售时间": ["2026-02-08", "2026-02-08"],
                    }
                ),
                system_dir / "销售202602.xlsx",
            )
            write_excel(
                pd.DataFrame(
                    {
                        "门店编码": ["101", "101"],
                        "门店名称": ["门店A", "门店A"],
                        "商品编码": ["P1", "P3"],
                        "商品编码.1": ["6901", "6903"],
                        "商品名称": ["库存SKU1", "仅库存SKU"],
                        "品牌": ["品牌A", "品牌B"],
                        "库存数量": [20, 7],
                        "库存日期": ["2026-02-09", "2026-02-09"],
                    }
                ),
                system_dir / "库存.xlsx",
            )
            write_excel(
                pd.DataFrame(
                    {
                        "商品条码": ["6901", "6902", "6903"],
                        "商品名称": ["销售SKU1", "仅销售SKU", "仅库存SKU"],
                        "装箱数（因子）": [6, 6, 6],
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
                brand_keywords=["品牌A", "品牌B"],
            )
            generate_report_for_system(config, config)

            catalog = pd.read_excel(output_file, sheet_name="商品编码对照清单", header=1)
            status_by_code = dict(zip(catalog["商品编码"], catalog["来源状态"]))
            self.assertEqual(status_by_code["P1"], "两表均存在")
            self.assertEqual(status_by_code["P2"], "仅销售表")
            self.assertEqual(status_by_code["P3"], "仅库存表")


if __name__ == "__main__":
    unittest.main()
