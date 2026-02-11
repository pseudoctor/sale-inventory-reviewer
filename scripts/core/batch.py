from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd


def run_batch(
    global_config: Dict[str, Any],
    base_dir: Path,
    build_system_config: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, Any]],
    resolve_expected_output_for_status: Callable[[Dict[str, Any], str], str],
    generate_report_for_system: Callable[[Dict[str, Any], Optional[Dict[str, Any]]], Dict[str, Any]],
) -> int:
    batch_cfg = global_config["batch"]
    continue_on_error = bool(batch_cfg.get("continue_on_error", True))
    summary_output_file = Path(batch_cfg.get("summary_output_file", "./reports/batch_run_summary.xlsx"))
    if not summary_output_file.is_absolute():
        summary_output_file = (base_dir / summary_output_file).resolve()
    summary_output_file.parent.mkdir(parents=True, exist_ok=True)

    records: List[Dict[str, Any]] = []
    failure_count = 0

    for system in batch_cfg.get("systems", []):
        start_time = datetime.now()
        enabled = bool(system.get("enabled", True))
        display_name = str(system.get("display_name", system.get("system_id", "unknown")))
        system_id = str(system.get("system_id", "")).strip() or display_name
        data_subdir = str(system.get("data_subdir", "")).strip()
        expected_output_file = resolve_expected_output_for_status(system, display_name)
        merged_config: Optional[Dict[str, Any]] = None
        preflight_error: Optional[Exception] = None

        try:
            merged_config = build_system_config(system, global_config)
            display_name = merged_config["display_name"]
            system_id = merged_config["system_id"]
            expected_output_file = resolve_expected_output_for_status(merged_config, display_name)
        except Exception as exc:  # noqa: BLE001
            merged_config = None
            preflight_error = exc

        if not enabled:
            records.append(
                {
                    "system_id": system_id,
                    "display_name": display_name,
                    "enabled": False,
                    "data_subdir": data_subdir,
                    "status": "SKIPPED",
                    "message": "disabled",
                    "error_stage": "",
                    "output_file": expected_output_file,
                    "duration_sec": 0.0,
                    "generated_at": datetime.now().isoformat(timespec="seconds"),
                    "input_files_count": 0,
                    "loaded_sales_files": 0,
                    "missing_sales_files": 0,
                    "inventory_file_exists": False,
                    "detail_rows": 0,
                    "missing_sku_rows": 0,
                }
            )
            print(f"[{display_name}] Skipped: disabled")
            continue

        if preflight_error is not None:
            failure_count += 1
            records.append(
                {
                    "system_id": system_id,
                    "display_name": display_name,
                    "enabled": True,
                    "data_subdir": data_subdir,
                    "status": "FAILED",
                    "message": str(preflight_error),
                    "error_stage": "config",
                    "output_file": expected_output_file,
                    "duration_sec": round((datetime.now() - start_time).total_seconds(), 3),
                    "generated_at": datetime.now().isoformat(timespec="seconds"),
                    "input_files_count": 0,
                    "loaded_sales_files": 0,
                    "missing_sales_files": 0,
                    "inventory_file_exists": False,
                    "detail_rows": 0,
                    "missing_sku_rows": 0,
                }
            )
            print(f"[{display_name}] Failed during config preflight: {preflight_error}")
            if not continue_on_error:
                break
            continue

        try:
            print(f"[{display_name}] Start generating report...")
            record = generate_report_for_system(merged_config, global_config)
        except Exception as exc:  # noqa: BLE001
            failure_count += 1
            message = str(exc)
            error_stage = "unknown"
            if message.startswith("[") and "] " in message:
                error_stage = message[1:].split("]", 1)[0]
                message = message.split("] ", 1)[1]
            record = {
                "system_id": system_id,
                "display_name": display_name,
                "enabled": True,
                "data_subdir": data_subdir,
                "status": "FAILED",
                "message": message,
                "error_stage": error_stage,
                "output_file": expected_output_file,
                "input_files_count": 0,
                "loaded_sales_files": 0,
                "missing_sales_files": 0,
                "inventory_file_exists": False,
                "detail_rows": 0,
                "missing_sku_rows": 0,
            }
            print(f"[{display_name}] Failed: {message}")
            if not continue_on_error:
                record["duration_sec"] = round((datetime.now() - start_time).total_seconds(), 3)
                record["generated_at"] = datetime.now().isoformat(timespec="seconds")
                records.append(record)
                break

        record["enabled"] = True
        record["data_subdir"] = data_subdir
        record["duration_sec"] = round((datetime.now() - start_time).total_seconds(), 3)
        record["generated_at"] = datetime.now().isoformat(timespec="seconds")
        records.append(record)

    summary_df = pd.DataFrame(
        records,
        columns=[
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
        ],
    )
    summary_df.to_excel(summary_output_file, index=False)
    print(f"Batch summary saved: {summary_output_file}")
    return failure_count
