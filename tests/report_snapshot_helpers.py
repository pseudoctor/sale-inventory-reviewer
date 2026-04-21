from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

SNAPSHOT_DIR = Path(__file__).parent / "snapshots"


def normalize_records(df: pd.DataFrame, cols: list[str]) -> list[dict]:
    out = []
    for row in df[cols].to_dict(orient="records"):
        normalized = {}
        for key, value in row.items():
            if pd.isna(value):
                normalized[key] = None
            elif isinstance(value, float):
                normalized[key] = round(value, 3)
            else:
                normalized[key] = value
        out.append(normalized)
    return out


def write_excel(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(path, index=False)


def read_overview_group(output_file: Path, group_name: str) -> pd.DataFrame:
    overview = pd.read_excel(output_file, sheet_name="运行总览", header=1)
    return overview.loc[overview["分组"] == group_name].copy()


def capture_report(output_file: Path, expected_snapshot_name: str) -> None:
    usage_guide = pd.read_excel(output_file, sheet_name="使用说明", header=1)
    detail = pd.read_excel(output_file, sheet_name="明细", header=1)
    detail["门店名称"] = detail["门店名称"].ffill()
    catalog = pd.read_excel(output_file, sheet_name="商品编码对照清单", header=1)
    replenish = pd.read_excel(output_file, sheet_name="建议补货清单", header=1)
    transfer = pd.read_excel(output_file, sheet_name="建议调货清单", header=1)
    summary = read_overview_group(output_file, "核心指标")

    actual = {
        "sheet_names": pd.ExcelFile(output_file).sheet_names,
        "usage_guide": normalize_records(usage_guide, ["模块", "说明", "使用建议"]),
        "detail": normalize_records(
            detail,
            ["门店名称", "商品名称", "商品条码", "近三月+本月迄今平均日销", "库存数量", "风险等级", "建议调出数量", "建议补货数量"],
        ),
        "catalog": normalize_records(catalog, ["商品编码", "品牌", "标准商品名", "销售表商品名", "库存商品名", "来源状态"]),
        "replenish": normalize_records(
            replenish,
            [c for c in ["门店名称", "商品名称", "省份", "装箱数（因子）", "建议补货数量", "建议补货箱数"] if c in replenish.columns],
        ),
        "transfer": normalize_records(
            transfer,
            [c for c in ["门店名称", "商品名称", "省份", "装箱数（因子）", "建议调出数量"] if c in transfer.columns],
        ),
        "summary": {k: float(v) for k, v in zip(summary["指标"], summary["数值"])},
    }

    expected_path = SNAPSHOT_DIR / expected_snapshot_name
    with expected_path.open("r", encoding="utf-8") as f:
        expected = json.load(f)

    assert actual == expected
