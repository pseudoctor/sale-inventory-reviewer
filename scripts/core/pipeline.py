from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

import pandas as pd

from . import config as core_config
from . import io as core_io
from . import matching as core_matching
from . import output_tables as core_output_tables
from . import pipeline_inputs as core_pipeline_inputs
from . import pipeline_outputs as core_pipeline_outputs
from . import pipeline_transfer as core_pipeline_transfer
from . import report_writer as core_report_writer
from . import system_rules as core_system_rules
from .models import AnalysisStageResult, AppConfig, InputStageResult, OutputStageResult, ReportRunContext, ReportRunResult, StatusFrameInput
_CARTON_FACTOR_CACHE = core_pipeline_inputs._CARTON_FACTOR_CACHE


def stage_error(stage: str, exc: Exception) -> RuntimeError:
    """统一包装阶段错误，便于批量模式汇总失败原因。"""
    return RuntimeError(f"[{stage}] {exc}")


def map_province_by_supplier_card(card: Optional[str]) -> str:
    """保留旧导出入口，内部转发到系统规则模块。"""
    return core_system_rules.map_province_by_supplier_card(card)


def _parse_season_mode(season_mode_raw: Any) -> bool:
    """兼容布尔值和历史字符串配置。"""
    if isinstance(season_mode_raw, bool):
        return season_mode_raw
    mode_text = str(season_mode_raw).strip().lower()
    if mode_text in {"true", "peak"}:
        return True
    if mode_text in {"false", "off_peak"}:
        return False
    raise ValueError("season_mode must be true/false (or legacy peak/off_peak)")


def _effective_brand_keywords(config: AppConfig) -> List[str]:
    """返回有效品牌关键词列表，禁止空配置继续运行。"""
    brand_keywords = [str(b).strip() for b in (config.get("brand_keywords") or []) if str(b).strip()]
    if not brand_keywords:
        raise ValueError("brand_keywords cannot be empty. Please configure at least one brand keyword in config.yaml.")
    return brand_keywords


def _build_run_context(config: AppConfig, *, base_dir: Path, program_version: str) -> ReportRunContext:
    """构建单系统执行所需的基础运行上下文。"""
    system_id = str(config.get("system_id", "single")).strip() or "single"
    display_name = str(config.get("display_name", system_id)).strip() or system_id
    rule_profile = core_system_rules.resolve_system_rule_profile(display_name, config)
    return ReportRunContext(
        config=config,
        base_dir=base_dir,
        program_version=program_version,
        system_id=system_id,
        display_name=display_name,
        rule_profile=rule_profile,
        configured_sales_files=[f for f in config.get("sales_files") or []],
        inventory_file=str(config.get("inventory_file") or ""),
        carton_factor_file=str(config.get("carton_factor_file") or ""),
        strict_auto_scan=bool(config.get("strict_auto_scan", False)),
        brand_keywords=_effective_brand_keywords(config),
        full_months=int(config.get("sales_window_full_months", 3)),
        include_mtd=bool(config.get("sales_window_include_mtd", True)),
        recent_days=int(config.get("sales_window_recent_days", 30)),
        sales_date_dayfirst=bool(config.get("sales_date_dayfirst", False)),
        sales_date_format=str(config.get("sales_date_format", "")),
        use_peak_mode=_parse_season_mode(config.get("season_mode", False)),
        fail_on_empty_window=bool(config.get("fail_on_empty_window", False)),
        merge_detail_store_cells=bool(config.get("merge_detail_store_cells", True)),
        enable_ranked_store_transfer_summary=bool(config.get("enable_ranked_store_transfer_summary", False)),
        high_days=float(config.get("risk_days_high", 60)),
        low_days=float(config.get("risk_days_low", 45)),
        stagnant_outbound_mode=str(config.get("stagnant_outbound_mode", "keep_safety_stock")),
        stagnant_min_keep_qty=float(config.get("stagnant_min_keep_qty", 0)),
    )


def _prepare_input_stage(ctx: ReportRunContext) -> InputStageResult:
    """准备输入阶段：解析路径、读取库存与销售、加载装箱因子。"""
    try:
        ctx.raw_data_dir = core_config.resolve_system_raw_data_dir(ctx.config, ctx.base_dir)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("config", exc) from exc

    try:
        sales_candidates = core_io.resolve_sales_candidates(ctx.raw_data_dir, ctx.configured_sales_files or [])
        ctx.ignored_sales_files = core_io.list_ignored_sales_files(ctx.raw_data_dir, ctx.configured_sales_files or [])
    except Exception as exc:  # noqa: BLE001
        raise stage_error("input_read", exc) from exc

    if not (ctx.configured_sales_files or []) and not sales_candidates:
        ignored_text = core_io.format_ignored_sales_files(ctx.ignored_sales_files or [])
        detail = f" Ignored files: {ignored_text}" if ignored_text else ""
        if ctx.strict_auto_scan and ctx.ignored_sales_files:
            raise RuntimeError(
                "[input_read] strict_auto_scan=true and no valid sales files were detected."
                f" Ignored candidates: {ignored_text}"
            )
        raise RuntimeError(
            "[input_read] No auto-detected sales files in "
            f"{ctx.raw_data_dir}. Expected filename with sales keyword and YYYYMM "
            f"(e.g. 销售202602.xlsx).{detail}"
        )

    inv_path = ctx.raw_data_dir / ctx.inventory_file
    ctx.inventory_file_exists = inv_path.exists()
    if not inv_path.exists():
        raise RuntimeError(f"[input_read] Inventory file not found: {inv_path}")

    try:
        inventory_prep = core_pipeline_inputs.prepare_inventory_data(
            inv_path=inv_path,
            config=ctx.config,
            brand_keywords=ctx.brand_keywords or [],
            display_name=ctx.display_name,
            base_dir=ctx.base_dir,
        )
    except Exception as exc:  # noqa: BLE001
        raise stage_error("normalize", exc) from exc

    ctx.output_file = inventory_prep.output_file
    ctx.inventory_date_ts = inventory_prep.inventory_date_ts
    ctx.inventory_date = inventory_prep.inventory_date

    try:
        sales_load = core_pipeline_inputs.load_sales_data(
            sales_candidates,
            ctx.brand_keywords or [],
            ctx.sales_date_format,
            ctx.sales_date_dayfirst,
            ctx.enable_ranked_store_transfer_summary,
        )
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, RuntimeError) and str(exc).startswith("[input_read]"):
            raise
        raise stage_error("normalize", exc) from exc

    barcode_mapping = core_pipeline_inputs.apply_wumei_barcode_mapping(
        inv_df=inventory_prep.inventory_df,
        sales_df=sales_load.sales_df,
        profile=ctx.rule_profile,
    )

    carton_factor_path = Path(ctx.carton_factor_file)
    if not carton_factor_path.is_absolute():
        carton_factor_path = (ctx.base_dir / carton_factor_path).resolve()
    try:
        carton_factor_df = _load_carton_factor_cached(carton_factor_path)
    except Exception as exc:  # noqa: BLE001
        raise stage_error("input_read", exc) from exc

    return InputStageResult(
        sales_df=sales_load.sales_df,
        inventory_df=barcode_mapping.inventory_df,
        carton_factor_df=carton_factor_df,
        loaded_sales_file_count=sales_load.loaded_sales_file_count,
        missing_sales_files=sales_load.missing_sales_files,
        invalid_sales_date_rows=sales_load.invalid_sales_date_rows,
        invalid_sales_qty_rows=sales_load.invalid_sales_qty_rows,
        invalid_inventory_qty_rows=inventory_prep.invalid_inventory_qty_rows,
        wumei_barcode_map_hits=barcode_mapping.hits,
        wumei_barcode_map_fallback=barcode_mapping.fallback,
        wumei_barcode_map_conflicts=barcode_mapping.conflicts,
        wumei_barcode_conflict_samples=barcode_mapping.conflict_samples,
    )


def _build_analysis_stage(ctx: ReportRunContext, inputs: InputStageResult) -> AnalysisStageResult:
    """分析阶段：计算窗口、匹配明细、补货调货建议和状态汇总。"""
    try:
        window_ctx = core_pipeline_inputs.compute_window_context(
            inputs.sales_df,
            ctx.inventory_date_ts,
            ctx.full_months,
            ctx.include_mtd,
            ctx.recent_days,
            ctx.fail_on_empty_window,
        )
    except Exception as exc:  # noqa: BLE001
        raise stage_error("metrics", exc) from exc

    matching_result = core_matching.build_detail_with_matching(
        sales_df=inputs.sales_df,
        inv_df=inputs.inventory_df,
        mtd_start=window_ctx.mtd_start,
        mtd_end=window_ctx.mtd_end,
        recent_start=window_ctx.recent_start,
        inventory_date_ts=ctx.inventory_date_ts,
        mtd_days=window_ctx.mtd_days,
        recent_days_effective=window_ctx.recent_days_effective,
        recent_days_natural=ctx.recent_days,
        has_mtd_window_data=window_ctx.has_mtd_window_data,
        has_recent_window_data=window_ctx.has_recent_window_data,
        use_peak_mode=ctx.use_peak_mode,
        low_days=ctx.low_days,
        high_days=ctx.high_days,
        is_wumei_system=ctx.rule_profile.is_wumei_system,
        province_mapper=map_province_by_supplier_card,
    )

    detail = core_pipeline_transfer.apply_recommendation_columns(
        matching_result.detail,
        ctx.low_days,
        ctx.high_days,
        ctx.stagnant_outbound_mode,
        ctx.stagnant_min_keep_qty,
    )

    product_code_catalog = core_pipeline_outputs.build_product_code_catalog(inputs.sales_df, inputs.inventory_df)
    frames = core_output_tables.build_report_frames(
        detail=detail,
        missing_sales=matching_result.missing_sales,
        store_summary=matching_result.store_summary,
        brand_summary=matching_result.brand_summary,
        product_code_catalog=product_code_catalog,
        carton_factor_df=inputs.carton_factor_df,
        is_wumei_system=ctx.rule_profile.is_wumei_system,
        enable_province_column=ctx.rule_profile.enable_province_column,
        use_peak_mode=ctx.use_peak_mode,
    )

    detail_out = frames.detail
    missing_sku_out = frames.missing_sku
    out_of_stock_out = frames.out_of_stock
    replenish_out = frames.replenish
    transfer_out = frames.transfer
    summary_out = core_pipeline_outputs.build_summary_frame(detail_out, out_of_stock_out, missing_sku_out, detail)
    status_out = core_pipeline_outputs.build_status_frame(
        StatusFrameInput(
            program_version=ctx.program_version,
            config=ctx.config,
            display_name=ctx.display_name,
            system_id=ctx.system_id,
            inventory_date=ctx.inventory_date,
            input_files_count=inputs.loaded_sales_file_count + len(inputs.missing_sales_files) + 1,
            loaded_sales_file_count=inputs.loaded_sales_file_count,
            missing_sales_files=inputs.missing_sales_files,
            use_peak_mode=ctx.use_peak_mode,
            strict_auto_scan=ctx.strict_auto_scan,
            has_mtd_window_data=window_ctx.has_mtd_window_data,
            has_recent_window_data=window_ctx.has_recent_window_data,
            mtd_days=window_ctx.mtd_days,
            recent_days_effective=window_ctx.recent_days_effective,
            invalid_sales_date_rows=inputs.invalid_sales_date_rows,
            invalid_sales_qty_rows=inputs.invalid_sales_qty_rows,
            invalid_inventory_qty_rows=inputs.invalid_inventory_qty_rows,
            replenish_out=replenish_out,
            transfer_out=transfer_out,
            mapping_stats=matching_result.mapping_stats,
            ignored_sales_files=ctx.ignored_sales_files or [],
            is_wumei_system=ctx.rule_profile.is_wumei_system,
            wumei_barcode_map_hits=inputs.wumei_barcode_map_hits,
            wumei_barcode_map_fallback=inputs.wumei_barcode_map_fallback,
            wumei_barcode_map_conflicts=inputs.wumei_barcode_map_conflicts,
            wumei_barcode_conflict_samples=inputs.wumei_barcode_conflict_samples,
        )
    )

    sales_amount_range_label = f"{window_ctx.mtd_start.date().isoformat()}至{ctx.inventory_date_ts.date().isoformat()}"
    store_sales_ranking_transfer_out = (
        _build_store_sales_ranking_transfer_frame(
            detail,
            inputs.sales_df,
            window_ctx.mtd_start,
            ctx.inventory_date_ts,
            sales_amount_range_label,
        )
        if ctx.enable_ranked_store_transfer_summary
        else pd.DataFrame()
    )

    return AnalysisStageResult(
        detail=detail,
        missing_sales=matching_result.missing_sales,
        store_summary=matching_result.store_summary,
        brand_summary=matching_result.brand_summary,
        mapping_stats=matching_result.mapping_stats,
        frames=frames,
        product_code_catalog=product_code_catalog,
        store_sales_ranking_transfer_out=store_sales_ranking_transfer_out,
        summary_out=summary_out,
        status_out=status_out,
    )


def _build_output_stage(ctx: ReportRunContext, analysis: AnalysisStageResult) -> OutputStageResult:
    """输出组装阶段：准备工作表集合和执行总览。"""
    executive_overview_out = core_pipeline_outputs.build_executive_overview_frame(analysis.summary_out, analysis.status_out)
    return OutputStageResult(
        frames=analysis.frames,
        detail_out=analysis.frames.detail,
        missing_sku_out=analysis.frames.missing_sku,
        out_of_stock_out=analysis.frames.out_of_stock,
        replenish_out=analysis.frames.replenish,
        transfer_out=analysis.frames.transfer,
        executive_overview_out=executive_overview_out,
        store_sales_ranking_transfer_out=analysis.store_sales_ranking_transfer_out,
    )


def _write_report_stage(ctx: ReportRunContext, outputs: OutputStageResult) -> None:
    """写报告阶段：组装最终工作表并落盘。"""
    sheets = {
        **{name: frame for name, frame in outputs.frames.items() if name not in {"使用说明", "商品编码对照清单"}},
    }
    if ctx.enable_ranked_store_transfer_summary:
        sheets["门店销量排名调货汇总"] = outputs.store_sales_ranking_transfer_out
    sheets["运行总览"] = outputs.executive_overview_out
    sheets["使用说明"] = outputs.frames.usage_guide
    sheets["商品编码对照清单"] = outputs.frames.product_code_catalog

    try:
        core_report_writer.write_report_with_style(
            output_file=ctx.output_file,
            display_name=ctx.display_name,
            inventory_date=ctx.inventory_date,
            sheets=sheets,
            merge_detail_store_cells=ctx.merge_detail_store_cells,
        )
    except Exception as exc:  # noqa: BLE001
        raise stage_error("write_report", exc) from exc


_load_carton_factor_cached = core_pipeline_inputs.load_carton_factor_cached
_build_store_sales_ranking_transfer_frame = core_pipeline_transfer.build_store_sales_ranking_transfer_frame


def generate_report_for_system(
    system_cfg: AppConfig,
    global_cfg: Optional[AppConfig] = None,
    *,
    base_dir: Path,
    program_version: str,
) -> ReportRunResult:
    """执行单个系统的完整报表流水线。"""
    config = AppConfig(dict(system_cfg))
    _ = global_cfg
    ctx = _build_run_context(config, base_dir=base_dir, program_version=program_version)
    inputs = _prepare_input_stage(ctx)
    analysis = _build_analysis_stage(ctx, inputs)
    outputs = _build_output_stage(ctx, analysis)
    _write_report_stage(ctx, outputs)

    print(f"[{ctx.display_name}] Report saved: {ctx.output_file}")
    return {
        "system_id": ctx.system_id,
        "display_name": ctx.display_name,
        "status": "SUCCESS",
        "message": "",
        "error_stage": "",
        "output_file": str(ctx.output_file),
        "input_files_count": int(inputs.loaded_sales_file_count + 1),
        "loaded_sales_files": int(inputs.loaded_sales_file_count),
        "missing_sales_files": int(len(inputs.missing_sales_files)),
        "inventory_file_exists": bool(ctx.inventory_file_exists),
        "detail_rows": int(len(outputs.detail_out)),
        "missing_sku_rows": int(len(outputs.missing_sku_out)),
    }
