import json
from pathlib import Path

import pandas as pd

from scripts.core import batch as core_batch


SNAPSHOT_PATH = Path(__file__).parent / "snapshots" / "golden_batch_summary_snapshot.json"


def _normalize_summary(df: pd.DataFrame) -> list[dict]:
    cols = [
        "system_id",
        "display_name",
        "enabled",
        "status",
        "message",
        "error_stage",
        "output_file",
        "input_files_count",
        "loaded_sales_files",
        "missing_sales_files",
        "inventory_file_exists",
        "detail_rows",
        "missing_sku_rows",
    ]
    out = []
    for row in df[cols].to_dict(orient="records"):
        item = {}
        for key, value in row.items():
            if pd.isna(value):
                item[key] = None
            elif isinstance(value, float):
                item[key] = int(value) if float(value).is_integer() else round(float(value), 3)
            elif key == "output_file":
                item[key] = Path(str(value)).name
            else:
                item[key] = value
        out.append(item)
    return out


def test_batch_summary_snapshot_contract(tmp_path: Path):
    summary_path = tmp_path / "reports" / "batch_run_summary.xlsx"
    config = {
        "batch": {
            "continue_on_error": True,
            "summary_output_file": str(summary_path),
            "systems": [
                {"system_id": "sys_a", "display_name": "系统A", "enabled": True, "data_subdir": "A"},
                {"system_id": "sys_b", "display_name": "系统B", "enabled": False, "data_subdir": "B"},
                {"system_id": "sys_c", "display_name": "系统C", "enabled": True, "data_subdir": "C"},
            ],
        }
    }

    def build_system_config(system_cfg, _global_cfg):
        return dict(system_cfg)

    def resolve_expected_output_for_status(cfg, display_name):
        _ = cfg
        return str(tmp_path / "reports" / f"{display_name}{{库存日期}}库存预警.xlsx")

    def generate_report_for_system(system_cfg, _global_cfg):
        sid = system_cfg["system_id"]
        if sid == "sys_a":
            return {
                "system_id": "sys_a",
                "display_name": "系统A",
                "status": "SUCCESS",
                "message": "",
                "error_stage": "",
                "output_file": str(tmp_path / "reports" / "系统A20260209库存预警.xlsx"),
                "input_files_count": 4,
                "loaded_sales_files": 3,
                "missing_sales_files": 0,
                "inventory_file_exists": True,
                "detail_rows": 123,
                "missing_sku_rows": 7,
            }
        if sid == "sys_c":
            raise RuntimeError("[input_read] Inventory file not found")
        raise AssertionError(f"Unexpected system: {sid}")

    failures = core_batch.run_batch(
        global_config=config,
        base_dir=tmp_path,
        build_system_config=build_system_config,
        resolve_expected_output_for_status=resolve_expected_output_for_status,
        generate_report_for_system=generate_report_for_system,
    )

    assert failures == 1
    summary_df = pd.read_excel(summary_path)
    actual = _normalize_summary(summary_df)

    with SNAPSHOT_PATH.open("r", encoding="utf-8") as f:
        expected = json.load(f)

    assert actual == expected
