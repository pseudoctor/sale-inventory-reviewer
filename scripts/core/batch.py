from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Callable, List, Optional, cast

import pandas as pd

from .models import AppConfig, BatchSummaryRecord, BatchSystemConfig, ReportRunResult

SUMMARY_COLUMNS = [
    "system_id",
    "display_name",
    "enabled",
    "data_subdir",
    "status",
    "message",
    "error_stage",
    "output_file",
    "duration_sec",
    "generated_at",
    "input_files_count",
    "loaded_sales_files",
    "missing_sales_files",
    "inventory_file_exists",
    "detail_rows",
    "missing_sku_rows",
]

def _make_status_record(
    *,
    system_id: str,
    display_name: str,
    status: str,
    message: str,
    output_file: str,
    error_stage: str = "",
) -> BatchSummaryRecord:
    """创建批量汇总所需的基础状态记录。"""
    return BatchSummaryRecord.make_status(
        system_id=system_id,
        display_name=display_name,
        status=status,
        message=message,
        output_file=output_file,
        error_stage=error_stage,
    )

def _finalize_record(
    record: BatchSummaryRecord,
    *,
    start_time: datetime,
    enabled: bool,
    data_subdir: str,
    duration_sec: Optional[float] = None,
) -> BatchSummaryRecord:
    """补全运行时字段，生成最终批量汇总记录。"""
    return record.finalize(
        start_time=start_time,
        enabled=enabled,
        data_subdir=data_subdir,
        duration_sec=duration_sec,
    )


def run_batch(
    global_config: AppConfig,
    base_dir: Path,
    build_system_config: Callable[[BatchSystemConfig, AppConfig], AppConfig],
    resolve_expected_output_for_status: Callable[[AppConfig | BatchSystemConfig, str], str],
    generate_report_for_system: Callable[[AppConfig, Optional[AppConfig]], ReportRunResult],
) -> int:
    """执行批量模式并输出汇总表。"""
    batch_cfg = global_config["batch"]
    continue_on_error = bool(batch_cfg.get("continue_on_error", True))
    summary_output_file = Path(batch_cfg.get("summary_output_file", "./reports/batch_run_summary.xlsx"))
    if not summary_output_file.is_absolute():
        summary_output_file = (base_dir / summary_output_file).resolve()
    summary_output_file.parent.mkdir(parents=True, exist_ok=True)

    records: List[BatchSummaryRecord] = []
    failure_count = 0

    for system in batch_cfg.get("systems", []):
        start_time = datetime.now()
        typed_system = cast(BatchSystemConfig, system)
        enabled = bool(typed_system.get("enabled", True))
        display_name = str(typed_system.get("display_name", typed_system.get("system_id", "unknown")))
        system_id = str(typed_system.get("system_id", "")).strip() or display_name
        data_subdir = str(typed_system.get("data_subdir", "")).strip()
        expected_output_file = resolve_expected_output_for_status(typed_system, display_name)
        merged_config: Optional[AppConfig] = None
        preflight_error: Optional[Exception] = None

        try:
            merged_config = build_system_config(typed_system, global_config)
            display_name = merged_config["display_name"]
            system_id = merged_config["system_id"]
            expected_output_file = resolve_expected_output_for_status(merged_config, display_name)
        except Exception as exc:  # noqa: BLE001
            merged_config = None
            preflight_error = exc

        if not enabled:
            record = _make_status_record(
                system_id=system_id,
                display_name=display_name,
                status="SKIPPED",
                message="disabled",
                output_file=expected_output_file,
            )
            records.append(
                _finalize_record(
                    record,
                    start_time=start_time,
                    enabled=False,
                    data_subdir=data_subdir,
                    duration_sec=0.0,
                )
            )
            print(f"[{display_name}] Skipped: disabled")
            continue

        if preflight_error is not None:
            failure_count += 1
            record = _make_status_record(
                system_id=system_id,
                display_name=display_name,
                status="FAILED",
                message=str(preflight_error),
                output_file=expected_output_file,
                error_stage="config",
            )
            records.append(
                _finalize_record(
                    record,
                    start_time=start_time,
                    enabled=True,
                    data_subdir=data_subdir,
                )
            )
            print(f"[{display_name}] Failed during config preflight: {preflight_error}")
            if not continue_on_error:
                break
            continue

        try:
            print(f"[{display_name}] Start generating report...")
            record = BatchSummaryRecord.from_report_result(generate_report_for_system(merged_config, global_config))
        except Exception as exc:  # noqa: BLE001
            failure_count += 1
            message = str(exc)
            error_stage = "unknown"
            if message.startswith("[") and "] " in message:
                error_stage = message[1:].split("]", 1)[0]
                message = message.split("] ", 1)[1]
            record = _make_status_record(
                system_id=system_id,
                display_name=display_name,
                status="FAILED",
                message=message,
                output_file=expected_output_file,
                error_stage=error_stage,
            )
            print(f"[{display_name}] Failed: {message}")
            if not continue_on_error:
                records.append(
                    _finalize_record(
                        record,
                        start_time=start_time,
                        enabled=True,
                        data_subdir=data_subdir,
                    )
                )
                break

        records.append(
            _finalize_record(
                record,
                start_time=start_time,
                enabled=True,
                data_subdir=data_subdir,
            )
        )

    summary_df = pd.DataFrame([record.to_row() for record in records], columns=SUMMARY_COLUMNS)
    summary_df.to_excel(summary_output_file, index=False)
    print(f"Batch summary saved: {summary_output_file}")
    return failure_count
